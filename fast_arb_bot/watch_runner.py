from __future__ import annotations

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook

from cross_arb_bot.matcher import build_opportunities, kalshi_taker_fee, polymarket_crypto_taker_fee
from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo, MatchedMarketPair

from real_arb_bot.clients import OrderResult
from real_arb_bot.engine import RealArbEngine

from fast_arb_bot.executor import FastArbExecutor, FastExecutionResult, _empty_order


@dataclass
class WatchedPair:
    opportunity: CrossVenueOpportunity
    matched: MatchedMarketPair


class FastArbWatchRunner:
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    STATUS_INTERVAL_SECONDS = 15
    HIGH_EDGE_THRESHOLD = 0.15

    def __init__(self, engine: RealArbEngine, dry_run: bool = False) -> None:
        self.engine = engine
        self.dry_run = dry_run

        fast_cfg = engine.config.get("fast_arb", {})
        self.budget_usd: float = float(fast_cfg.get("budget_usd", 40.0))
        self.max_realized_loss_usd: float = float(fast_cfg.get("max_realized_loss_usd", 20.0))
        self.liquidity_ratio: float = float(fast_cfg.get("order_book_min_liquidity_ratio", 1.5))
        self.scan_interval: float = float(fast_cfg.get("scan_interval_seconds", 60.0))

        safety_cfg = engine.config.get("safety", {})
        self.kalshi_slippage_cents: int = int(safety_cfg.get("kalshi_slippage_cents", 1))
        self.pm_price_buffer: float = safety_cfg.get("polymarket_price_buffer_cents", 2) / 100.0

        self.executor = FastArbExecutor(
            pm_trader=engine.pm_trader,
            kalshi_trader=engine.kalshi_trader,
            db=engine.db,
            kalshi_slippage_cents=self.kalshi_slippage_cents,
            pm_price_buffer=self.pm_price_buffer,
        )

        self.live_books: dict[str, TopOfBook] = {}
        self.live_books_kalshi: dict[str, KalshiTopOfBook] = {}
        self.watch_by_pair_key: dict[str, WatchedPair] = {}
        self.pairs_by_asset_id: dict[str, set[str]] = {}
        self.pairs_by_kalshi_ticker: dict[str, set[str]] = {}
        self._kalshi_ws: KalshiWebSocketClient | None = None
        self._signal_lock = threading.Lock()
        self._last_skip_log: dict[str, float] = {}
        self._SKIP_LOG_INTERVAL = 30.0
        self._last_completion_attempt: dict[str, float] = {}
        self._COMPLETION_COOLDOWN = 5.0  # секунд между попытками докупки
        self._cached_balances: dict = {"polymarket": None, "kalshi": None}
        self._last_scan_ts: float = 0.0

        # Подменяем функцию статуса в Telegram-нотификаторе
        if engine.notifier:
            engine.notifier._get_status = self._get_status_text

    # ── Основной цикл ──────────────────────────────────────────────────

    def run(self) -> None:
        ws: MarketWebSocketClient | None = None
        last_status = 0.0

        try:
            while True:
                now = time.time()
                if now - self._last_scan_ts >= self.scan_interval:
                    ws = self._refresh_watchlist(prev_ws=ws)
                    self._last_scan_ts = time.time()

                if (now - last_status) >= self.STATUS_INTERVAL_SECONDS:
                    self._print_status()
                    last_status = now

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[fast-arb] Stopped.")
        finally:
            if ws is not None:
                ws.stop()
            if self._kalshi_ws is not None:
                self._kalshi_ws.stop()

    # ── Refresh watchlist ──────────────────────────────────────────────

    def _refresh_watchlist(self, prev_ws: MarketWebSocketClient | None = None) -> MarketWebSocketClient | None:
        print("[fast-arb] Scanning markets...")
        self.engine.pm_clob._dead_tokens.clear()
        self.engine.scan(execute=False)
        pm_markets, kalshi_markets, matches, opportunities = self.engine.last_snapshot

        # Обновляем балансы при каждом скане (кешируем для hot path)
        self._cached_balances = self.engine.get_real_balances()

        max_edge = self.engine.trading["max_lock_edge"]
        tracking_candidates = self._build_tracking_candidates(matches)
        match_index = {
            f"{m.polymarket.market_id}:{m.kalshi.market_id}": m
            for m in matches
            if m.kalshi.venue == "kalshi"
        }

        watched: dict[str, WatchedPair] = {}
        pairs_by_asset: dict[str, set[str]] = {}
        pairs_by_kalshi: dict[str, set[str]] = {}
        asset_ids: list[str] = []

        for opp in tracking_candidates:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if opp.buy_yes_venue not in {"polymarket", "kalshi"} or opp.buy_no_venue not in {"polymarket", "kalshi"}:
                continue

            watch_key = self._watch_key(opp)
            watched[watch_key] = WatchedPair(opportunity=opp, matched=matched)
            for token_id in [matched.polymarket.yes_token_id, matched.polymarket.no_token_id]:
                if not token_id:
                    continue
                asset_ids.append(token_id)
                pairs_by_asset.setdefault(token_id, set()).add(watch_key)

            kalshi_ticker = matched.kalshi.market_id
            if kalshi_ticker:
                pairs_by_kalshi.setdefault(kalshi_ticker, set()).add(watch_key)

        self.watch_by_pair_key = watched
        self.pairs_by_asset_id = pairs_by_asset
        self.pairs_by_kalshi_ticker = pairs_by_kalshi

        # Пробуем сразу исполнить найденные возможности
        self._try_immediate_open(opportunities, match_index)

        new_asset_ids = sorted(set(asset_ids))
        new_kalshi_tickers = sorted(set(pairs_by_kalshi.keys()))
        self._refresh_kalshi_ws(new_kalshi_tickers)

        if not new_asset_ids:
            self.live_books = {}
            print("[fast-arb] No candidates for live monitoring.")
            return None

        kept = set(new_asset_ids)
        self.live_books = {k: v for k, v in self.live_books.items() if k in kept}

        prev_asset_ids = getattr(self, "_current_asset_ids", [])
        if new_asset_ids == prev_asset_ids and prev_ws is not None:
            print(f"[fast-arb] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens (WS reused).")
            self._current_asset_ids = new_asset_ids
            return prev_ws

        if prev_ws is not None:
            prev_ws.stop()
        print(f"[fast-arb] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens.")
        self._current_asset_ids = new_asset_ids
        ws = MarketWebSocketClient(
            url=self.MARKET_WS_URL,
            asset_ids=new_asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

    def _try_immediate_open(self, opportunities: list, match_index: dict) -> None:
        if not opportunities:
            return
        balances = self._cached_balances
        if balances["polymarket"] is None or balances["kalshi"] is None:
            print("[fast-arb] Пропуск немедленного открытия: нет балансов")
            return
        for opp in opportunities:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                continue
            halt, reason = self._check_loss_limits(new_position_cost=opp.total_cost)
            if halt:
                print(f"[fast-arb] HALT {reason} (немедленное открытие)")
                if "realized_losses" in reason:
                    self.engine.safety.dry_run = True
                return
            executed, yes_leg, no_leg = self._apply_execution_pricing_parallel(opp, matched)
            if executed is None:
                continue
            self._execute_and_record(executed, matched, yes_leg, no_leg)
            self._cached_balances = self.engine.get_real_balances()

    # ── Kalshi WS ──────────────────────────────────────────────────────

    def _refresh_kalshi_ws(self, new_tickers: list[str]) -> None:
        prev_tickers = getattr(self, "_current_kalshi_tickers", [])
        if new_tickers == prev_tickers and self._kalshi_ws is not None:
            return
        if self._kalshi_ws is not None:
            self._kalshi_ws.stop()
            self._kalshi_ws = None
        self._current_kalshi_tickers = new_tickers
        kept = set(new_tickers)
        self.live_books_kalshi = {k: v for k, v in self.live_books_kalshi.items() if k in kept}
        if not new_tickers:
            return
        try:
            self._kalshi_ws = KalshiWebSocketClient(
                tickers=new_tickers,
                on_update=self.on_kalshi_update,
            )
            self._kalshi_ws.start()
            print(f"[fast-arb] Kalshi WS started for {len(new_tickers)} tickers.")
        except Exception as exc:
            print(f"[fast-arb] Kalshi WS unavailable: {exc}")
            self._kalshi_ws = None

    def on_kalshi_update(self, ticker: str, top: KalshiTopOfBook) -> None:
        self.live_books_kalshi[ticker] = top
        for pair_key in self.pairs_by_kalshi_ticker.get(ticker, set()):
            self._maybe_open_pair(pair_key)

    def on_ws_message(self, payload: dict) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self.on_ws_message(item)
            return

        event_type = payload.get("event_type")
        if event_type not in {"book", "best_bid_ask"}:
            return

        asset_id = str(payload.get("asset_id", ""))
        if not asset_id:
            return

        if event_type == "book":
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.0
            best_bid = float(bids[0]["price"]) if bids else 0.0
            timestamp = int(payload.get("timestamp", 0) or 0)
        else:
            best_ask = float(payload.get("best_ask", 0) or 0)
            best_bid = float(payload.get("best_bid", 0) or 0)
            timestamp = int(payload.get("timestamp", 0) or 0)

        self.live_books[asset_id] = TopOfBook(best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp)
        for pair_key in self.pairs_by_asset_id.get(asset_id, set()):
            self._maybe_open_pair(pair_key)

    # ── Hot path ───────────────────────────────────────────────────────

    def _maybe_open_pair(self, pair_key: str) -> None:
        watched = self.watch_by_pair_key.get(pair_key)
        if watched is None:
            return
        opp = watched.opportunity
        matched = watched.matched

        # 1. Проверяем существующую позицию (любое направление)
        any_open = self.engine.db.conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND status='open'",
            (opp.pair_key,),
        ).fetchone() is not None
        if any_open:
            self._maybe_complete_one_legged(opp, matched)
            return

        # 2. Лимит одновременных позиций
        open_count = len(self.engine.db.get_open_positions())
        if open_count >= self.engine.trading["max_open_pairs"]:
            self._skip_log(
                pair_key,
                f"[fast-arb][SKIP] {opp.symbol} | reason=max_open_pairs ({open_count})",
            )
            return

        # 3. Быстрая оценка edge из WS-данных
        yes_book = self.live_books.get(matched.polymarket.yes_token_id or "")
        no_book = self.live_books.get(matched.polymarket.no_token_id or "")
        if yes_book is None or no_book is None:
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            return

        kalshi_live = self.live_books_kalshi.get(matched.kalshi.market_id or "")
        rough_yes = (
            yes_book.best_ask if opp.buy_yes_venue == "polymarket"
            else (kalshi_live.best_yes_ask if kalshi_live else matched.kalshi.yes_ask)
        )
        rough_no = (
            no_book.best_ask if opp.buy_no_venue == "polymarket"
            else (kalshi_live.best_no_ask if kalshi_live else matched.kalshi.no_ask)
        )
        rough_edge = 1.0 - (rough_yes + rough_no)
        if rough_edge < self.engine.trading["min_lock_edge"]:
            return
        if rough_edge > self.engine.trading["max_lock_edge"]:
            rough_polarized = (rough_yes > 0.5) != (rough_no > 0.5)
            if not rough_polarized:
                return

        # 4. Проверка min/max цены ноги (в памяти)
        min_p = self.engine.safety.min_leg_price
        max_p = self.engine.safety.max_leg_price
        if rough_yes < min_p or rough_no < min_p or rough_yes > max_p or rough_no > max_p:
            return

        # 5. Loss limit check (SQL)
        halt, reason = self._check_loss_limits()
        if halt:
            self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | {reason}")
            if "realized_losses" in reason:
                self.engine.safety.dry_run = True
            return

        with self._signal_lock:
            any_open = self.engine.db.conn.execute(
                "SELECT 1 FROM positions WHERE pair_key=? AND status='open'",
                (opp.pair_key,),
            ).fetchone() is not None
            if any_open:
                return

            # 5. Параллельная проверка стаканов + 1.5x margin
            executed, yes_leg, no_leg = self._apply_execution_pricing_parallel(opp, matched)
            if executed is None:
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=insufficient_liquidity (1.5x margin)\n"
                    f"              YES: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO:  {self.engine._format_leg_summary(no_leg)}",
                )
                return
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | edge_disappeared ({executed.edge_per_share:.4f})",
                )
                return

            # Финальная проверка лимита с точной стоимостью позиции
            halt2, reason2 = self._check_loss_limits(new_position_cost=executed.total_cost)
            if halt2:
                self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | {reason2}")
                if "realized_losses" in reason2:
                    self.engine.safety.dry_run = True
                return

            # 6. Выставляем ордера параллельно
            self._execute_and_record(executed, matched, yes_leg, no_leg)

    # ── Исполнение ─────────────────────────────────────────────────────

    def _execute_and_record(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> None:
        print(
            f"\n[fast-arb][OPEN] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
            f"                 ask_sum={opp.ask_sum:.4f} edge={opp.edge_per_share:.4f} "
            f"cost=${opp.total_cost:.2f} exp_profit=${opp.expected_profit:.2f}"
        )

        if self.dry_run:
            print("[fast-arb][DRY] Пропуск реального исполнения (dry_run)")
            return

        # Прописываем правильные token_id для PM ног
        mapped_yes_leg = yes_leg
        mapped_no_leg = no_leg
        if opp.buy_yes_venue == "polymarket" and yes_leg and matched.polymarket.yes_token_id:
            mapped_yes_leg = replace(yes_leg, market_id=matched.polymarket.yes_token_id)
        if opp.buy_no_venue == "polymarket" and no_leg and matched.polymarket.no_token_id:
            mapped_no_leg = replace(no_leg, market_id=matched.polymarket.no_token_id)

        result: FastExecutionResult = self.executor.execute_pair_parallel(
            opp, mapped_yes_leg, mapped_no_leg
        )

        if result.execution_status == "both_filled":
            print(
                f"[fast-arb][OPEN] SUCCESS | "
                f"kalshi={result.kalshi_order.shares_matched:.2f}@{result.kalshi_order.fill_price:.4f} | "
                f"pm={result.polymarket_order.shares_matched:.2f}@{result.polymarket_order.fill_price:.4f}"
            )
        else:
            print(f"[fast-arb][OPEN] {result.execution_status.upper()} | {result.reason}")
            if result.execution_status == "failed":
                return

        kalshi_res = result.kalshi_order or _empty_order("no_order")
        pm_res = result.polymarket_order or _empty_order("no_order")

        self.engine.db.open_position(
            opportunity=opp,
            kalshi_result=kalshi_res,
            polymarket_result=pm_res,
            execution_status=result.execution_status,
            route="parallel",
            polymarket_snapshot_open=None,
            kalshi_snapshot_open=None,
            yes_leg=yes_leg,
            no_leg=no_leg,
        )

        if self.engine.notifier:
            kalshi_fill = result.kalshi_order.fill_price if result.kalshi_order else 0.0
            pm_fill = result.polymarket_order.fill_price if result.polymarket_order else 0.0
            self.engine.notifier.notify_open(
                symbol=opp.symbol,
                yes_venue=opp.buy_yes_venue,
                no_venue=opp.buy_no_venue,
                yes_ask=opp.yes_ask,
                no_ask=opp.no_ask,
                ask_sum=opp.ask_sum,
                edge=opp.edge_per_share,
                cost=opp.total_cost,
                expected_profit=opp.expected_profit,
                execution_status=result.execution_status,
                kalshi_fill=kalshi_fill,
                pm_fill=pm_fill,
            )

    # ── Execution pricing (параллельные REST + 1.5x margin) ───────────

    def _apply_execution_pricing_parallel(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
    ) -> tuple[CrossVenueOpportunity | None, ExecutionLegInfo | None, ExecutionLegInfo | None]:
        from cross_arb_bot.models import NormalizedMarket

        yes_market = matched.polymarket if opp.buy_yes_venue == "polymarket" else matched.kalshi
        no_market = matched.polymarket if opp.buy_no_venue == "polymarket" else matched.kalshi

        tiered_stake = self.engine._stake_for_edge(opp.edge_per_share)
        shares = min(tiered_stake / opp.ask_sum, opp.shares)

        min_edge = float(self.engine.config.get("trading", {}).get("min_lock_edge", 0.04))
        max_yes_price = 1.0 - opp.no_ask - min_edge
        max_no_price = 1.0 - opp.yes_ask - min_edge

        # Параллельные REST-запросы к обоим стаканам
        with ThreadPoolExecutor(max_workers=2) as pool:
            yes_future = pool.submit(
                self.engine._execution_leg_info,
                opp.buy_yes_venue, yes_market, "yes", shares, max_yes_price,
            )
            no_future = pool.submit(
                self.engine._execution_leg_info,
                opp.buy_no_venue, no_market, "no", shares, max_no_price,
            )
            yes_leg = yes_future.result()
            no_leg = no_future.result()

        if yes_leg is None or no_leg is None:
            return None, yes_leg, no_leg
        if yes_leg.filled_shares + 1e-6 < shares or no_leg.filled_shares + 1e-6 < shares:
            return None, yes_leg, no_leg

        # Проверка 1.5x margin
        min_required = self.liquidity_ratio * shares
        if yes_leg.usable_shares < min_required or no_leg.usable_shares < min_required:
            return None, yes_leg, no_leg

        yes_ask = yes_leg.avg_price
        no_ask = no_leg.avg_price
        ask_sum = yes_ask + no_ask
        edge_per_share = 1.0 - ask_sum
        capital_used = yes_leg.total_cost + no_leg.total_cost

        pm_fee = 0.0
        kalshi_fee = 0.0
        if opp.buy_yes_venue == "polymarket":
            pm_fee += self.engine._pm_fee(shares, yes_ask)
        else:
            kalshi_fee += self.engine._kalshi_fee(shares, yes_ask)
        if opp.buy_no_venue == "polymarket":
            pm_fee += self.engine._pm_fee(shares, no_ask)
        else:
            kalshi_fee += self.engine._kalshi_fee(shares, no_ask)

        total_fee = pm_fee + kalshi_fee
        total_cost = capital_used + total_fee

        executed = replace(
            opp,
            shares=shares,
            yes_ask=yes_ask, no_ask=no_ask, ask_sum=ask_sum,
            edge_per_share=edge_per_share, capital_used=capital_used,
            polymarket_fee=pm_fee, kalshi_fee=kalshi_fee,
            total_fee=total_fee, total_cost=total_cost,
            expected_profit=shares - total_cost,
        )
        return executed, yes_leg, no_leg

    # ── Loss limit ─────────────────────────────────────────────────────

    def _check_loss_limits(self, new_position_cost: float = 0.0) -> tuple[bool, str]:
        """Проверяет бюджет и лимит потерь.

        Бюджет = budget_usd + max(0, total_realized_pnl) — растёт с выигрышами.
        Стоп если реализованные потери > max_realized_loss_usd.

        Возвращает (halt, reason).
        """
        try:
            # Суммарный реализованный PnL
            cursor = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(actual_pnl), 0) FROM positions WHERE status='resolved'"
            )
            realized_pnl = float(cursor.fetchone()[0])

            # Стоп по потерям
            if realized_pnl < -self.max_realized_loss_usd:
                return True, f"realized_losses_exceeded (pnl=${realized_pnl:+.2f}, limit=-${self.max_realized_loss_usd:.0f})"

            # Эффективный бюджет растёт с выигрышами
            effective_budget = self.budget_usd + max(0.0, realized_pnl)

            # Открытые позиции + новая не должны превышать бюджет
            cursor = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(CASE "
                "WHEN execution_status='one_legged_kalshi' "
                "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
                "WHEN execution_status='one_legged_polymarket' "
                "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
                "ELSE total_cost END), 0) FROM positions WHERE status='open'"
            )
            open_cost = float(cursor.fetchone()[0]) + new_position_cost
            if open_cost > effective_budget:
                return True, (
                    f"budget_exceeded (open=${open_cost:.2f} > budget=${effective_budget:.2f}"
                    f" [base=${self.budget_usd:.0f} + wins=${max(0.0, realized_pnl):.2f}])"
                )

        except Exception:
            pass
        return False, ""

    # ── Вспомогательные ───────────────────────────────────────────────

    def _build_tracking_candidates(self, matches: list[MatchedMarketPair]) -> list[CrossVenueOpportunity]:
        candidates: list[CrossVenueOpportunity] = []
        stake_per_pair_usd = float(self.engine.trading["stake_per_pair_usd"])
        for item in matches:
            pm = item.polymarket
            ka = item.kalshi
            legs = [
                ("polymarket", "kalshi", pm.yes_ask, ka.no_ask),
                ("kalshi", "polymarket", ka.yes_ask, pm.no_ask),
            ]
            for yes_venue, no_venue, yes_ask, no_ask in legs:
                ask_sum = yes_ask + no_ask
                if ask_sum <= 0:
                    continue
                edge_per_share = 1.0 - ask_sum
                shares = stake_per_pair_usd / ask_sum
                polymarket_fee = 0.0
                kalshi_fee = 0.0
                if yes_venue == "polymarket":
                    polymarket_fee += polymarket_crypto_taker_fee(shares, yes_ask)
                else:
                    kalshi_fee += kalshi_taker_fee(shares, yes_ask)
                if no_venue == "polymarket":
                    polymarket_fee += polymarket_crypto_taker_fee(shares, no_ask)
                else:
                    kalshi_fee += kalshi_taker_fee(shares, no_ask)
                total_fee = polymarket_fee + kalshi_fee
                capital_used = ask_sum * shares
                candidates.append(
                    CrossVenueOpportunity(
                        pair_key=f"{pm.market_id}:{ka.market_id}",
                        polymarket_market_id=pm.market_id,
                        kalshi_market_id=ka.market_id,
                        symbol=pm.symbol,
                        title=f"{pm.title} <> {ka.title}",
                        expiry=min(pm.expiry, ka.expiry),
                        polymarket_title=pm.title,
                        kalshi_title=ka.title,
                        match_score=item.score,
                        expiry_delta_seconds=abs((pm.expiry - ka.expiry).total_seconds()),
                        polymarket_reference_price=pm.reference_price,
                        kalshi_reference_price=ka.reference_price,
                        polymarket_rules=pm.rules_text,
                        kalshi_rules=ka.rules_text,
                        buy_yes_venue=yes_venue,
                        buy_no_venue=no_venue,
                        yes_ask=yes_ask,
                        no_ask=no_ask,
                        ask_sum=ask_sum,
                        edge_per_share=edge_per_share,
                        shares=shares,
                        capital_used=capital_used,
                        polymarket_fee=polymarket_fee,
                        kalshi_fee=kalshi_fee,
                        total_fee=total_fee,
                        total_cost=capital_used + total_fee,
                        expected_payout=shares,
                        expected_profit=shares - (capital_used + total_fee),
                    )
                )
        return candidates

    def _maybe_complete_one_legged(self, opp: CrossVenueOpportunity, matched: MatchedMarketPair) -> None:
        """Если есть одноногая позиция по паре — пробуем докупить вторую ногу."""
        # Cooldown: не пробуем чаще раза в минуту
        import time as _time
        now_ts = _time.time()
        last = self._last_completion_attempt.get(opp.pair_key, 0.0)
        if now_ts - last < self._COMPLETION_COOLDOWN:
            return
        self._last_completion_attempt[opp.pair_key] = now_ts

        row = self.engine.db.conn.execute(
            "SELECT id, execution_status, venue_yes, venue_no, "
            "kalshi_fill_price, kalshi_fill_shares, "
            "polymarket_fill_price, polymarket_fill_shares "
            "FROM positions WHERE pair_key=? AND status='open' "
            "AND execution_status IN ('one_legged_kalshi','one_legged_polymarket')",
            (opp.pair_key,),
        ).fetchone()
        if row is None:
            return

        exec_status = row["execution_status"]
        pos_id = row["id"]
        min_edge = float(self.engine.trading["min_lock_edge"])

        # Определяем какая нога уже есть и по какой цене
        if exec_status == "one_legged_kalshi":
            # Kalshi есть, нужна PM нога
            # venue_yes из БД — какая площадка покупала YES при открытии
            db_venue_yes = row["venue_yes"]
            existing_fill_price = float(row["kalshi_fill_price"] or 0)
            missing_venue = "polymarket"
            missing_side = "yes" if db_venue_yes == "polymarket" else "no"
            missing_token = (
                matched.polymarket.yes_token_id if missing_side == "yes"
                else matched.polymarket.no_token_id
            )
            # Текущая цена пропущенной ноги из WS
            book = self.live_books.get(missing_token or "")
            if book is None or book.best_ask <= 0:
                return
            current_ask = book.best_ask
        else:
            # one_legged_polymarket: Kalshi ордер уже рестингует — не трогаем
            return

        # Проверяем: с учётом уже уплаченной цены есть ли смысл докупать
        total_ask = existing_fill_price + current_ask
        completion_edge = 1.0 - total_ask
        if completion_edge < min_edge:
            return

        # Цена ноги в разумных пределах
        if current_ask < self.engine.safety.min_leg_price or current_ask > self.engine.safety.max_leg_price:
            return

        with self._signal_lock:
            # Повторная проверка что позиция ещё одноногая
            row2 = self.engine.db.conn.execute(
                "SELECT id FROM positions WHERE id=? AND status='open' "
                "AND execution_status IN ('one_legged_kalshi','one_legged_polymarket')",
                (pos_id,),
            ).fetchone()
            if row2 is None:
                return

            print(
                f"\n[fast-arb][COMPLETE] {opp.symbol} | докупаем {missing_venue}:{missing_side.upper()}\n"
                f"  existing={existing_fill_price:.4f} + current={current_ask:.4f} "
                f"= {total_ask:.4f} | edge={completion_edge:.4f}"
            )

            if self.dry_run:
                print("[fast-arb][DRY] Пропуск")
                return

            # Покупаем только недостающую ногу
            shares = float(self.engine.db.conn.execute(
                "SELECT shares FROM positions WHERE id=?", (pos_id,)
            ).fetchone()[0])

            try:
                if missing_venue == "polymarket":
                    limit_price = math.floor((current_ask + self.pm_price_buffer) * 100) / 100.0
                    order = self.executor._place_pm_limit_order(missing_token, limit_price, shares)
                else:
                    price_cents = round(current_ask * 100) + self.kalshi_slippage_cents
                    price_cents = min(price_cents, 99)
                    count = max(1, math.floor(shares))
                    order = self.executor._place_kalshi_order(
                        matched.kalshi.market_id, missing_side, count, price_cents
                    )
            except Exception as e:
                print(f"[fast-arb][COMPLETE] ошибка ордера: {e}")
                return

            if order is None or order.shares_matched <= 0:
                status_str = order.status if order else "None returned"
                print(f"[fast-arb][COMPLETE] не заполнился: {status_str}")
                return

            print(
                f"[fast-arb][COMPLETE] SUCCESS | {missing_venue} fill={order.shares_matched:.2f}"
                f"@{order.fill_price:.4f} | fee=${order.fee:.4f}"
            )

            # Обновляем позицию в DB
            now = __import__("datetime").datetime.utcnow().isoformat()
            if missing_venue == "polymarket":
                self.engine.db.conn.execute(
                    "UPDATE positions SET execution_status='both_filled', "
                    "polymarket_fill_price=?, polymarket_fill_shares=?, polymarket_order_fee=?, "
                    "polymarket_order_id=?, polymarket_order_status=?, execution_completed_at=? "
                    "WHERE id=?",
                    (order.fill_price, order.shares_matched, order.fee,
                     order.order_id, order.status, now, pos_id),
                )
            else:
                self.engine.db.conn.execute(
                    "UPDATE positions SET execution_status='both_filled', "
                    "kalshi_fill_price=?, kalshi_fill_shares=?, kalshi_order_fee=?, "
                    "kalshi_order_id=?, kalshi_order_status=?, execution_completed_at=? "
                    "WHERE id=?",
                    (order.fill_price, order.shares_matched, order.fee,
                     order.order_id, order.status, now, pos_id),
                )
            self.engine.db.conn.commit()

            if self.engine.notifier:
                self.engine.notifier._send(
                    f"🔄 <b>ДОКУПЛЕНА нога: {opp.symbol}</b>\n"
                    f"{missing_venue}:{missing_side.upper()} @ {order.fill_price:.4f} "
                    f"(fill={order.shares_matched:.2f})\n"
                    f"edge итого={completion_edge:.4f} | позиция закрыта в both_filled"
                )

    def _get_status_text(self) -> str:
        base = self.engine.get_status_text()

        open_cost = 0.0
        realized_pnl = 0.0
        one_legged_count = 0
        try:
            c1 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(CASE "
                "WHEN execution_status='one_legged_kalshi' "
                "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
                "WHEN execution_status='one_legged_polymarket' "
                "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
                "ELSE total_cost END), 0) FROM positions WHERE status='open'"
            )
            open_cost = float(c1.fetchone()[0])
            c2 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(actual_pnl), 0) FROM positions WHERE status='resolved'"
            )
            realized_pnl = float(c2.fetchone()[0])
            c3 = self.engine.db.conn.execute(
                "SELECT COUNT(*) FROM positions "
                "WHERE execution_status IN ('one_legged_kalshi','one_legged_polymarket') AND status='open'"
            )
            one_legged_count = int(c3.fetchone()[0])
        except Exception:
            pass

        effective_budget = self.budget_usd + max(0.0, realized_pnl)
        budget_line = (
            f"\n\n💰 <b>Бюджет (fast_arb)</b>\n"
            f"Базовый: <b>${self.budget_usd:.0f}</b>"
            + (f" + выигрыши ${max(0.0, realized_pnl):.2f} = <b>${effective_budget:.2f}</b>" if realized_pnl > 0 else "")
            + f"\nОткрыто: <b>${open_cost:.2f}</b> / ${effective_budget:.2f}\n"
            f"Свободно: <b>${max(0.0, effective_budget - open_cost):.2f}</b>\n"
            f"P&L реализованный: <b>${realized_pnl:+.2f}</b> (стоп при -${self.max_realized_loss_usd:.0f})"
        )
        if one_legged_count:
            budget_line += f"\nОдноногих открытых: <b>{one_legged_count}</b>"
        if realized_pnl <= -self.max_realized_loss_usd:
            budget_line += "\n⛔ <b>СТОП: лимит потерь достигнут</b>"

        return base + budget_line

    def _watch_key(self, opp: CrossVenueOpportunity) -> str:
        return f"{opp.pair_key}|{opp.buy_yes_venue}|{opp.buy_no_venue}"

    def _skip_log(self, pair_key: str, msg: str) -> None:
        now = time.time()
        if now - self._last_skip_log.get(pair_key, 0.0) < self._SKIP_LOG_INTERVAL:
            return
        self._last_skip_log[pair_key] = now
        print(msg)

    # ── Резолв одноногих позиций ───────────────────────────────────────

    def _resolve_one_legged_positions(self) -> None:
        """Резолвим одноногие позиции до стандартного resolver.

        Стандартный resolver пытается сделать PM redeem даже если pm_shares=0
        (при one_legged_kalshi), что ломает процесс. Резолвим здесь сами,
        стандартный resolver увидит позицию уже закрытой и пропустит.
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for position in self.engine.db.get_open_positions():
            row = self.engine.db.conn.execute(
                "SELECT execution_status FROM positions WHERE id=?", (position.id,)
            ).fetchone()
            if row is None:
                continue
            exec_status = row["execution_status"]
            if exec_status not in ("one_legged_kalshi", "one_legged_polymarket"):
                continue
            if position.expiry > now:
                continue

            self._resolve_one_legged(position, exec_status)

    def _resolve_one_legged(self, position, exec_status: str) -> None:
        row = self.engine.db.conn.execute(
            "SELECT kalshi_fill_price, kalshi_fill_shares, kalshi_order_fee, "
            "polymarket_fill_price, polymarket_fill_shares, polymarket_order_fee "
            "FROM positions WHERE id=?", (position.id,)
        ).fetchone()

        k_shares = float(row["kalshi_fill_shares"] or 0)
        k_price  = float(row["kalshi_fill_price"]  or 0)
        k_fee    = float(row["kalshi_order_fee"]   or 0)
        pm_shares = float(row["polymarket_fill_shares"] or 0)
        pm_price  = float(row["polymarket_fill_price"]  or 0)
        pm_fee    = float(row["polymarket_order_fee"]   or 0)

        resolver = self.engine.resolver

        if exec_status == "one_legged_kalshi":
            # Только Kalshi нога — не делаем PM redeem
            kalshi_result, kalshi_snapshot = resolver._check_kalshi(position)
            if kalshi_result is None:
                return  # Рынок ещё не разрешён

            kalshi_side = "yes" if position.venue_yes == "kalshi" else "no"
            kalshi_won = (kalshi_result == kalshi_side)
            kalshi_payout = k_shares * 1.0 if kalshi_won else 0.0
            kalshi_cost = k_shares * k_price + k_fee
            pnl = kalshi_payout - kalshi_cost

            self.engine.db.resolve_position(
                position_id=position.id,
                winning_side=kalshi_result,
                pnl=round(pnl, 6),
                actual_pnl=round(pnl, 6),
                polymarket_result="not_traded",
                kalshi_result="won" if kalshi_won else "lost",
                lock_valid=False,
                kalshi_snapshot_resolved=kalshi_snapshot,
            )
            tag = f"WIN +${pnl:.2f}" if pnl > 0 else f"LOSE ${pnl:.2f}"
            print(
                f"[fast-arb][resolve] {position.symbol} | one_legged_kalshi | "
                f"kalshi={'WIN' if kalshi_won else 'LOSE'} {k_shares}x@{k_price} | pnl=${pnl:+.2f} ({tag})"
            )

        else:  # one_legged_polymarket
            # Только PM нога
            pm_result, pm_snapshot = resolver._check_polymarket(position)
            if pm_result is None:
                return  # Рынок ещё не разрешён

            pm_side = "yes" if position.venue_yes == "polymarket" else "no"
            pm_won = (pm_result == pm_side)
            pm_cost = pm_shares * pm_price + pm_fee

            pm_payout_real = 0.0
            if pm_won and pm_shares > 0:
                pm_market_id = (
                    position.market_yes if position.venue_yes == "polymarket"
                    else position.market_no
                )
                print(f"[fast-arb][resolve] Polymarket redeem (one_legged): {pm_market_id}")
                redeem = self.engine.pm_trader.redeem(pm_market_id)
                if not redeem.success and not redeem.pending:
                    print(f"[fast-arb][resolve] redeem failed: {redeem.error}, retry later")
                    self.engine.db.audit("redeem_retry_pending", position.id, {
                        "symbol": position.symbol,
                        "market_id": pm_market_id,
                        "error": redeem.error or "unknown_error",
                    })
                    return
                pm_payout_real = redeem.payout_usdc if redeem.success else 0.0

            pnl = (pm_payout_real if pm_won and pm_shares > 0 else 0.0) - pm_cost

            self.engine.db.resolve_position(
                position_id=position.id,
                winning_side=pm_result,
                pnl=round(pnl, 6),
                actual_pnl=round(pnl, 6),
                polymarket_result="won" if pm_won else "lost",
                kalshi_result="not_traded",
                lock_valid=False,
                polymarket_snapshot_resolved=pm_snapshot,
            )
            tag = f"WIN +${pnl:.2f}" if pnl > 0 else f"LOSE ${pnl:.2f}"
            print(
                f"[fast-arb][resolve] {position.symbol} | one_legged_polymarket | "
                f"pm={'WIN' if pm_won else 'LOSE'} {pm_shares:.2f}x@{pm_price} | pnl=${pnl:+.2f} ({tag})"
            )

        if self.engine.notifier:
            if exec_status == "one_legged_kalshi":
                notify_pm = "not_traded"
                notify_kalshi = "won" if kalshi_won else "lost"
            else:
                notify_pm = "won" if pm_won else "lost"
                notify_kalshi = "not_traded"
            self.engine.notifier.notify_resolve(
                symbol=position.symbol,
                pm_result=notify_pm,
                kalshi_result=notify_kalshi,
                pnl=pnl,
                lock_valid=False,
            )

    def _print_status(self) -> None:
        self._resolve_one_legged_positions()
        self.engine.resolve()
        self.engine.print_status()
        open_cost = 0.0
        realized_pnl = 0.0
        try:
            c1 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(CASE "
                "WHEN execution_status='one_legged_kalshi' "
                "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
                "WHEN execution_status='one_legged_polymarket' "
                "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
                "ELSE total_cost END), 0) FROM positions WHERE status='open'"
            )
            open_cost = float(c1.fetchone()[0])
            c2 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(actual_pnl), 0) FROM positions WHERE status='resolved'"
            )
            realized_pnl = float(c2.fetchone()[0])
        except Exception:
            pass
        effective_budget = self.budget_usd + max(0.0, realized_pnl)
        kalshi_ws_status = (
            f"tickers={len(self._current_kalshi_tickers)}"
            if self._kalshi_ws is not None
            else "off"
        )
        print(
            f"[fast-arb][Status] watched={len(self.watch_by_pair_key)} "
            f"| live_tokens={len(self.live_books)} "
            f"| kalshi_ws={kalshi_ws_status} "
            f"| open=${open_cost:.2f}/budget=${effective_budget:.2f} "
            f"| realized_pnl=${realized_pnl:+.2f} (stop at -${self.max_realized_loss_usd:.0f})"
        )
