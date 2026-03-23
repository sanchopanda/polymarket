from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook

from cross_arb_bot.models import CrossVenueOpportunity, MatchedMarketPair

from real_arb_bot.engine import RealArbEngine


@dataclass
class WatchedPair:
    opportunity: CrossVenueOpportunity
    matched: MatchedMarketPair


class RealArbWatchRunner:
    UNIVERSE_REFRESH_SECONDS = 60
    STATUS_INTERVAL_SECONDS = 15
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, engine: RealArbEngine) -> None:
        self.engine = engine
        self.live_books: dict[str, TopOfBook] = {}
        self.live_books_kalshi: dict[str, KalshiTopOfBook] = {}
        self.watch_by_pair_key: dict[str, WatchedPair] = {}
        self.pairs_by_asset_id: dict[str, set[str]] = {}
        self.pairs_by_kalshi_ticker: dict[str, set[str]] = {}
        self._kalshi_ws: KalshiWebSocketClient | None = None
        self._signal_lock = threading.Lock()
        self._last_skip_log: dict[str, float] = {}
        self._SKIP_LOG_INTERVAL = 30.0

    def run(self) -> None:
        ws: MarketWebSocketClient | None = None
        last_refresh = 0.0
        last_status = 0.0

        try:
            while True:
                now = time.time()
                if ws is None or (now - last_refresh) >= self.UNIVERSE_REFRESH_SECONDS:
                    ws = self._refresh_watchlist(prev_ws=ws)
                    last_refresh = now

                if (now - last_status) >= self.STATUS_INTERVAL_SECONDS:
                    self._print_status()
                    last_status = now

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Watch] Stopped.")
        finally:
            if ws is not None:
                ws.stop()
            if self._kalshi_ws is not None:
                self._kalshi_ws.stop()

    def _refresh_watchlist(self, prev_ws: MarketWebSocketClient | None = None) -> MarketWebSocketClient | None:
        from cross_arb_bot.matcher import build_opportunities
        print("[Watch] Scanning markets...")
        self.engine.pm_clob._dead_tokens.clear()
        self.engine.scan(execute=False)
        pm_markets, kalshi_markets, matches, opportunities = self.engine.last_snapshot

        # Подписываемся на ВСЕ сматченные рынки, а не только на текущие opportunity
        max_edge = self.engine.trading["max_lock_edge"]
        all_candidates = build_opportunities(
            matches=matches,
            min_lock_edge=-1.0,
            max_lock_edge=max_edge,
            stake_per_pair_usd=self.engine.trading["stake_per_pair_usd"],
        )
        match_index = {
            f"{m.polymarket.market_id}:{m.kalshi.market_id}": m
            for m in matches
            if m.kalshi.venue == "kalshi"
        }

        # Логируем matched пары без кандидатов
        candidate_keys = {c.pair_key for c in all_candidates}
        for m in matches:
            pair_key = f"{m.polymarket.market_id}:{m.kalshi.market_id}"
            if pair_key not in candidate_keys:
                pm, ka = m.polymarket, m.kalshi
                best_edge = max(
                    1.0 - (pm.yes_ask + ka.no_ask),
                    1.0 - (ka.yes_ask + pm.no_ask),
                )
                reasons = []
                if pm.yes_ask > 0 and ka.yes_ask > 0:
                    pm_bullish = pm.yes_ask > 0.5
                    ka_bullish = ka.yes_ask > 0.5
                    price_diff = abs(pm.yes_ask - ka.yes_ask)
                    if pm_bullish != ka_bullish and price_diff > 0.40:
                        reasons.append(f"direction_mismatch(PM={'UP' if pm_bullish else 'DOWN'}, KA={'UP' if ka_bullish else 'DOWN'}, diff={price_diff:.2f})")
                if pm.yes_ask <= 0 or pm.no_ask <= 0:
                    reasons.append(f"pm_no_price(yes={pm.yes_ask},no={pm.no_ask})")
                if ka.yes_ask <= 0 or ka.no_ask <= 0:
                    reasons.append(f"kalshi_no_price(yes={ka.yes_ask},no={ka.no_ask})")
                is_edge_too_high = best_edge > max_edge
                if is_edge_too_high:
                    reasons.append(f"edge_too_high({best_edge:.4f}>{max_edge})")
                if not reasons:
                    reasons.append(f"edge_negative({best_edge:.4f})")
                print(
                    f"[Watch] Match не отслеживается: {pm.symbol}\n"
                    f"        PM: {pm.title} | id={pm.market_id} | yes={pm.yes_ask:.4f} no={pm.no_ask:.4f}\n"
                    f"        KA: {ka.title} | ticker={ka.market_id} | yes={ka.yes_ask:.4f} no={ka.no_ask:.4f}\n"
                    f"        причина: {', '.join(reasons)}"
                )
                # edge_too_high и нет других причин → записываем paper позицию для статистики
                if is_edge_too_high and len(reasons) == 1:
                    paper_opps = build_opportunities(
                        matches=[m],
                        min_lock_edge=-1.0,
                        max_lock_edge=999.0,
                        stake_per_pair_usd=self.engine.trading["stake_per_pair_usd"],
                    )
                    for p_opp in paper_opps:
                        if not self.engine.db.has_open_paper_position(p_opp.pair_key, p_opp.buy_yes_venue, p_opp.buy_no_venue):
                            self.engine.db.open_paper_position(p_opp)
                            print(f"[Watch][PAPER] {p_opp.symbol} | edge={p_opp.edge_per_share:.4f} | cost=${p_opp.total_cost:.2f} | exp_profit=${p_opp.expected_profit:.2f}")
                            if self.engine.notifier:
                                self.engine.notifier.notify_open(
                                    symbol=p_opp.symbol,
                                    yes_venue=p_opp.buy_yes_venue,
                                    no_venue=p_opp.buy_no_venue,
                                    yes_ask=p_opp.yes_ask,
                                    no_ask=p_opp.no_ask,
                                    ask_sum=p_opp.ask_sum,
                                    edge=p_opp.edge_per_share,
                                    cost=p_opp.total_cost,
                                    expected_profit=p_opp.expected_profit,
                                    execution_status="paper",
                                    is_paper=True,
                                )

        watched: dict[str, WatchedPair] = {}
        pairs_by_asset: dict[str, set[str]] = {}
        pairs_by_kalshi: dict[str, set[str]] = {}
        asset_ids: list[str] = []

        for opp in all_candidates:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if opp.buy_yes_venue not in {"polymarket", "kalshi"} or opp.buy_no_venue not in {"polymarket", "kalshi"}:
                continue

            watched[opp.pair_key] = WatchedPair(opportunity=opp, matched=matched)
            for token_id in [matched.polymarket.yes_token_id, matched.polymarket.no_token_id]:
                if not token_id:
                    continue
                asset_ids.append(token_id)
                pairs_by_asset.setdefault(token_id, set()).add(opp.pair_key)

            kalshi_ticker = matched.kalshi.market_id
            if kalshi_ticker:
                pairs_by_kalshi.setdefault(kalshi_ticker, set()).add(opp.pair_key)

        self.watch_by_pair_key = watched
        self.pairs_by_asset_id = pairs_by_asset
        self.pairs_by_kalshi_ticker = pairs_by_kalshi

        # Сразу пробуем открыть найденные возможности (REST execution pricing)
        self._try_immediate_open(opportunities, match_index)

        new_asset_ids = sorted(set(asset_ids))
        new_kalshi_tickers = sorted(set(pairs_by_kalshi.keys()))
        self._refresh_kalshi_ws(new_kalshi_tickers)

        if not new_asset_ids:
            self.live_books = {}
            print("[Watch] No candidates for live monitoring.")
            return None

        kept = set(new_asset_ids)
        self.live_books = {k: v for k, v in self.live_books.items() if k in kept}

        prev_asset_ids = getattr(self, "_current_asset_ids", [])
        if new_asset_ids == prev_asset_ids and prev_ws is not None:
            print(f"[Watch] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens (WS reused).")
            self._current_asset_ids = new_asset_ids
            return prev_ws

        if prev_ws is not None:
            prev_ws.stop()
        print(f"[Watch] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens.")
        self._current_asset_ids = new_asset_ids
        ws = MarketWebSocketClient(
            url=self.MARKET_WS_URL,
            asset_ids=new_asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

    def _try_immediate_open(self, opportunities, match_index: dict) -> None:
        """Попытка сразу открыть найденные при скане возможности (без WS)."""
        if not opportunities:
            return
        balances = self.engine.get_real_balances()
        if balances["polymarket"] is None or balances["kalshi"] is None:
            print(f"[Watch] Пропуск торговли: не удалось получить балансы")
            return
        for opp in opportunities:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                continue

            ok, reason = self.engine.safety.can_trade(opp, balances["polymarket"], balances["kalshi"])
            if not ok:
                print(f"[Watch][IMM-SKIP] {opp.symbol} | {reason}")
                continue

            executed, yes_leg, no_leg = self.engine._apply_execution_pricing(opp, matched)
            if executed is None:
                continue
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                print(f"[Watch][IMM-SKIP] {opp.symbol} | edge_disappeared ({executed.edge_per_share:.4f})")
                continue

            ok2, reason2 = self.engine.safety.can_trade(executed, balances["polymarket"], balances["kalshi"])
            if not ok2:
                print(f"[Watch][IMM-SKIP] {opp.symbol} | {reason2} (after exec pricing)")
                continue

            if not self.engine.safety.confirm_trade(executed):
                print(f"[Watch][IMM-SKIP] {opp.symbol} | not_confirmed")
                continue

            self.engine._execute_and_record(executed, matched, yes_leg, no_leg)
            balances = self.engine.get_real_balances()

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
            print(f"[Watch] Kalshi WS started for {len(new_tickers)} tickers.")
        except Exception as exc:
            print(f"[Watch] Kalshi WS unavailable: {exc}")
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

    def _skip_log(self, pair_key: str, msg: str) -> None:
        now = time.time()
        if now - self._last_skip_log.get(pair_key, 0.0) < self._SKIP_LOG_INTERVAL:
            return
        self._last_skip_log[pair_key] = now
        print(msg)

    def _maybe_open_pair(self, pair_key: str) -> None:
        watched = self.watch_by_pair_key.get(pair_key)
        if watched is None:
            return
        opp = watched.opportunity
        matched = watched.matched

        if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
            return
        open_count = len(self.engine.db.get_open_positions())
        if open_count >= self.engine.trading["max_open_pairs"]:
            self._skip_log(pair_key, f"[Watch][SKIP] {opp.symbol} | reason=max_open_pairs ({open_count})")
            return

        yes_book = self.live_books.get(matched.polymarket.yes_token_id or "")
        no_book = self.live_books.get(matched.polymarket.no_token_id or "")
        if yes_book is None or no_book is None:
            missing = [s for s, b in [("YES", yes_book), ("NO", no_book)] if b is None]
            self._skip_log(pair_key, f"[Watch][SKIP] {opp.symbol} | reason=missing_ws_book ({', '.join(missing)})")
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            self._skip_log(pair_key, f"[Watch][SKIP] {opp.symbol} | reason=invalid_ask (yes={yes_book.best_ask}, no={no_book.best_ask})")
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
            self._skip_log(
                pair_key,
                f"[Watch][SKIP] {opp.symbol} | reason=rough_edge_low ({rough_edge:.4f})"
                f" | yes={rough_yes:.4f} no={rough_no:.4f}",
            )
            return

        with self._signal_lock:
            if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                return

            balances = self.engine.get_real_balances()
            ok, reason = self.engine.safety.can_trade(opp, balances["polymarket"], balances["kalshi"])
            if not ok:
                self._skip_log(pair_key, f"[Watch][SKIP] {opp.symbol} | reason={reason}")
                return

            executed, yes_leg, no_leg = self.engine._apply_execution_pricing(opp, matched)
            if executed is None:
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=insufficient_liquidity\n"
                    f"              YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO  leg: {self.engine._format_leg_summary(no_leg)}",
                )
                return
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=edge_disappeared ({executed.edge_per_share:.4f} < {self.engine.trading['min_lock_edge']})\n"
                    f"              YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO  leg: {self.engine._format_leg_summary(no_leg)}",
                )
                return

            ok2, reason2 = self.engine.safety.can_trade(executed, balances["polymarket"], balances["kalshi"])
            if not ok2:
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason={reason2} (after execution pricing)\n"
                    f"              YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO  leg: {self.engine._format_leg_summary(no_leg)}",
                )
                return

            if not self.engine.safety.confirm_trade(executed):
                self._skip_log(pair_key, f"[Watch][SKIP] {opp.symbol} | reason=not_confirmed")
                return

            self.engine._execute_and_record(executed, matched, yes_leg, no_leg)

    def _print_status(self) -> None:
        self.engine.resolve()
        self.engine.print_status()
        kalshi_ws_status = (
            f"tickers={len(self._current_kalshi_tickers)}"
            if self._kalshi_ws is not None
            else "off"
        )
        print(
            f"[Watch][Status] watched_pairs={len(self.watch_by_pair_key)} "
            f"| live_tokens={len(self.live_books)} "
            f"| kalshi_ws={kalshi_ws_status}"
        )
