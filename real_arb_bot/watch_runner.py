from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook

from cross_arb_bot.matcher import kalshi_taker_fee, polymarket_crypto_taker_fee
from cross_arb_bot.models import CrossVenueOpportunity, MatchedMarketPair

from real_arb_bot.engine import RealArbEngine


@dataclass
class WatchedPair:
    opportunity: CrossVenueOpportunity
    matched: MatchedMarketPair


class RealArbWatchRunner:
    STATUS_INTERVAL_SECONDS = 15
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    HIGH_EDGE_THRESHOLD = 0.15
    UNIVERSE_REFRESH_MINUTES = {1, 16, 31, 46}

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
        self._last_refresh_slot: tuple[int, int, int, int, int] | None = None

    def run(self) -> None:
        ws: MarketWebSocketClient | None = None
        last_status = 0.0

        try:
            while True:
                now = time.time()
                if ws is None or self._should_refresh_universe():
                    ws = self._refresh_watchlist(prev_ws=ws)
                    self._mark_refresh_slot()

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

    def _current_refresh_slot(self) -> tuple[int, int, int, int, int] | None:
        now = datetime.now(timezone.utc)
        if now.minute not in self.UNIVERSE_REFRESH_MINUTES:
            return None
        return (now.year, now.month, now.day, now.hour, now.minute)

    def _should_refresh_universe(self) -> bool:
        slot = self._current_refresh_slot()
        return slot is not None and slot != self._last_refresh_slot

    def _mark_refresh_slot(self) -> None:
        self._last_refresh_slot = self._current_refresh_slot()

    def _refresh_watchlist(self, prev_ws: MarketWebSocketClient | None = None) -> MarketWebSocketClient | None:
        from cross_arb_bot.matcher import build_opportunities
        print("[Watch] Scanning markets...")
        self.engine.pm_clob._dead_tokens.clear()
        self.engine.scan(execute=False)
        pm_markets, kalshi_markets, matches, opportunities = self.engine.last_snapshot

        # Для watch подписываемся на все сматченные пары с валидными ценами.
        # Торговые фильтры применяем уже в момент входа, а не при построении подписки.
        max_edge = self.engine.trading["max_lock_edge"]
        tracking_candidates = self._build_tracking_candidates(matches)
        match_index = {
            f"{m.polymarket.market_id}:{m.kalshi.market_id}": m
            for m in matches
            if m.kalshi.venue == "kalshi"
        }

        # Логируем matched пары без немедленного trade-кандидата, но продолжаем их отслеживать.
        trade_candidate_keys = {c.pair_key for c in opportunities}
        min_edge = float(self.engine.trading["min_lock_edge"])
        for m in matches:
            pair_key = f"{m.polymarket.market_id}:{m.kalshi.market_id}"
            if pair_key not in trade_candidate_keys:
                pm, ka = m.polymarket, m.kalshi
                pm_yes_ka_no_edge = 1.0 - (pm.yes_ask + ka.no_ask)
                ka_yes_pm_no_edge = 1.0 - (ka.yes_ask + pm.no_ask)
                best_edge = max(pm_yes_ka_no_edge, ka_yes_pm_no_edge)
                if best_edge < min_edge:
                    continue
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
                    reasons.append(
                        "no_trade_candidate"
                        f"(pm_yes+ka_no={pm_yes_ka_no_edge:.4f}, ka_yes+pm_no={ka_yes_pm_no_edge:.4f})"
                    )
                print(
                    f"[Watch] Match пока не торгуем: {pm.symbol}\n"
                    f"        {self._snapshot_summary(m)}\n"
                    f"        причина: {', '.join(reasons)}\n"
                    f"        стороны: "
                    f"{self._snapshot_side_summary('PM YES + KA NO', pm.yes_ask, ka.no_ask, min(pm.yes_depth, ka.no_depth))}; "
                    f"{self._snapshot_side_summary('KA YES + PM NO', ka.yes_ask, pm.no_ask, min(ka.yes_depth, pm.no_depth))}"
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
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason={reason}\n"
                    f"                  snapshot: ask_sum={opp.ask_sum:.4f} edge={opp.edge_per_share:.4f} "
                    f"shares={opp.shares:.2f} cost=${opp.total_cost:.2f}"
                )
                continue

            executed, yes_leg, no_leg = self.engine._apply_execution_pricing(opp, matched)
            if executed is None:
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason=insufficient_liquidity\n"
                    f"                  YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"                  NO  leg: {self.engine._format_leg_summary(no_leg)}"
                )
                continue
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason=edge_disappeared ({executed.edge_per_share:.4f} < {self.engine.trading['min_lock_edge']:.4f})\n"
                    f"                  YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"                  NO  leg: {self.engine._format_leg_summary(no_leg)}"
                )
                continue
            if not self._passes_high_edge_price_filter(executed):
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason=high_edge_same_side "
                    f"(edge={executed.edge_per_share:.4f}, yes={executed.yes_ask:.4f}, no={executed.no_ask:.4f})\n"
                    f"                  YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"                  NO  leg: {self.engine._format_leg_summary(no_leg)}"
                )
                continue

            ok2, reason2 = self.engine.safety.can_trade(executed, balances["polymarket"], balances["kalshi"])
            if not ok2:
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason={reason2} (after execution pricing)\n"
                    f"                  executed: ask_sum={executed.ask_sum:.4f} edge={executed.edge_per_share:.4f} "
                    f"shares={executed.shares:.2f} cost=${executed.total_cost:.2f}\n"
                    f"                  YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"                  NO  leg: {self.engine._format_leg_summary(no_leg)}"
                )
                continue

            if not self.engine.safety.confirm_trade(executed):
                print(
                    f"[Watch][IMM-SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"                  reason=not_confirmed\n"
                    f"                  executed: ask_sum={executed.ask_sum:.4f} edge={executed.edge_per_share:.4f} "
                    f"shares={executed.shares:.2f} cost=${executed.total_cost:.2f}"
                )
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

    def _watch_key(self, opp: CrossVenueOpportunity) -> str:
        return f"{opp.pair_key}|{opp.buy_yes_venue}|{opp.buy_no_venue}"

    def _paired_watch_items(self, pair_key: str) -> list[WatchedPair]:
        watched = self.watch_by_pair_key.get(pair_key)
        if watched is None:
            return []
        base_pair_key = watched.opportunity.pair_key
        items = [
            item
            for item in self.watch_by_pair_key.values()
            if item.opportunity.pair_key == base_pair_key
        ]
        items.sort(key=lambda item: (item.opportunity.buy_yes_venue, item.opportunity.buy_no_venue))
        return items

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

    def _snapshot_summary(self, matched: MatchedMarketPair) -> str:
        pm = matched.polymarket
        ka = matched.kalshi
        return (
            f"PM: {pm.title} | id={pm.market_id} | yes={pm.yes_ask:.4f} no={pm.no_ask:.4f} "
            f"| depth_yes={pm.yes_depth:.2f} depth_no={pm.no_depth:.2f}\n"
            f"        KA: {ka.title} | ticker={ka.market_id} | yes={ka.yes_ask:.4f} no={ka.no_ask:.4f} "
            f"| depth_yes={ka.yes_depth:.2f} depth_no={ka.no_depth:.2f}"
        )

    def _snapshot_side_summary(self, label: str, yes_ask: float, no_ask: float, max_shares: float) -> str:
        ask_sum = yes_ask + no_ask
        edge = 1.0 - ask_sum
        return f"{label}: yes={yes_ask:.4f} no={no_ask:.4f} ask_sum={ask_sum:.4f} edge={edge:.4f} max_shares={max_shares:.2f}"

    def _rough_side_summary(
        self,
        opp: CrossVenueOpportunity,
        rough_yes: float,
        rough_no: float,
        yes_book: TopOfBook,
        no_book: TopOfBook,
        kalshi_live: KalshiTopOfBook | None,
    ) -> str:
        rough_edge = 1.0 - (rough_yes + rough_no)
        kalshi_yes = f"{kalshi_live.best_yes_ask:.4f}" if kalshi_live else "n/a"
        kalshi_no = f"{kalshi_live.best_no_ask:.4f}" if kalshi_live else "n/a"
        return (
            f"{opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO | "
            f"rough_yes={rough_yes:.4f} rough_no={rough_no:.4f} rough_edge={rough_edge:.4f}\n"
            f"              PM live: yes={yes_book.best_ask:.4f} no={no_book.best_ask:.4f}\n"
            f"              KA live: yes={kalshi_yes} no={kalshi_no}"
        )

    def _pair_live_side_summaries(
        self,
        pair_key: str,
        yes_book: TopOfBook,
        no_book: TopOfBook,
        kalshi_live: KalshiTopOfBook | None,
    ) -> str:
        lines: list[str] = []
        for item in self._paired_watch_items(pair_key):
            opp = item.opportunity
            rough_yes = (
                yes_book.best_ask if opp.buy_yes_venue == "polymarket"
                else (kalshi_live.best_yes_ask if kalshi_live else item.matched.kalshi.yes_ask)
            )
            rough_no = (
                no_book.best_ask if opp.buy_no_venue == "polymarket"
                else (kalshi_live.best_no_ask if kalshi_live else item.matched.kalshi.no_ask)
            )
            lines.append(
                f"{opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO -> "
                f"rough_yes={rough_yes:.4f} rough_no={rough_no:.4f} rough_edge={1.0 - (rough_yes + rough_no):.4f}"
            )
        return "\n".join(lines)

    def _pair_snapshot_side_summaries(self, pair_key: str) -> str:
        lines: list[str] = []
        for item in self._paired_watch_items(pair_key):
            opp = item.opportunity
            lines.append(
                f"{opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO -> "
                f"ask_sum={opp.ask_sum:.4f} edge={opp.edge_per_share:.4f} "
                f"shares={opp.shares:.2f} cost=${opp.total_cost:.2f}"
            )
        return "\n".join(lines)

    def _passes_high_edge_price_filter(self, opp: CrossVenueOpportunity) -> bool:
        if opp.edge_per_share <= self.HIGH_EDGE_THRESHOLD:
            return True
        return (opp.yes_ask > 0.5) != (opp.no_ask > 0.5)

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
            self._skip_log(
                pair_key,
                f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                f"              reason=max_open_pairs ({open_count})",
            )
            return

        yes_book = self.live_books.get(matched.polymarket.yes_token_id or "")
        no_book = self.live_books.get(matched.polymarket.no_token_id or "")
        if yes_book is None or no_book is None:
            missing = [s for s, b in [("YES", yes_book), ("NO", no_book)] if b is None]
            self._skip_log(
                pair_key,
                f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                f"              reason=missing_ws_book ({', '.join(missing)})",
            )
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            self._skip_log(
                pair_key,
                f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                f"              reason=invalid_pm_live_ask (yes={yes_book.best_ask}, no={no_book.best_ask})",
            )
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
        rough_summary = self._rough_side_summary(opp, rough_yes, rough_no, yes_book, no_book, kalshi_live)
        if rough_edge < self.engine.trading["min_lock_edge"]:
            return
        if rough_edge > self.engine.trading["max_lock_edge"]:
            rough_polarized = (rough_yes > 0.5) != (rough_no > 0.5)
            if not rough_polarized:
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=high_edge_same_side "
                    f"(edge={rough_edge:.4f}, yes={rough_yes:.4f}, no={rough_no:.4f})\n"
                    f"              {rough_summary}\n"
                    f"              обе стороны:\n"
                    f"              {self._pair_live_side_summaries(pair_key, yes_book, no_book, kalshi_live).replace(chr(10), chr(10) + '              ')}",
                )
                return

        with self._signal_lock:
            if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
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
            if not self._passes_high_edge_price_filter(executed):
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=high_edge_same_side "
                    f"(edge={executed.edge_per_share:.4f}, yes={executed.yes_ask:.4f}, no={executed.no_ask:.4f})\n"
                    f"              YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO  leg: {self.engine._format_leg_summary(no_leg)}",
                )
                return

            balances = self.engine.get_real_balances()
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
                self._skip_log(
                    pair_key,
                    f"[Watch][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=not_confirmed\n"
                    f"              executed: ask_sum={executed.ask_sum:.4f} edge={executed.edge_per_share:.4f} "
                    f"shares={executed.shares:.2f} cost=${executed.total_cost:.2f}",
                )
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
