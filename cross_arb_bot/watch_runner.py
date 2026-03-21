from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from arb_bot.ws import MarketWebSocketClient, TopOfBook

from cross_arb_bot.engine import CrossArbEngine
from cross_arb_bot.models import CrossVenueOpportunity, MatchedMarketPair


@dataclass
class WatchedPair:
    opportunity: CrossVenueOpportunity
    matched: MatchedMarketPair


class CrossArbWatchRunner:
    UNIVERSE_REFRESH_SECONDS = 60
    STATUS_INTERVAL_SECONDS = 15
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, engine: CrossArbEngine) -> None:
        self.engine = engine
        self.live_books: dict[str, TopOfBook] = {}
        self.watch_by_pair_key: dict[str, WatchedPair] = {}
        self.pairs_by_asset_id: dict[str, set[str]] = {}
        self._signal_lock = threading.Lock()

    def run(self) -> None:
        ws: MarketWebSocketClient | None = None
        last_refresh = 0.0
        last_status = 0.0

        try:
            while True:
                now = time.time()
                if ws is None or (now - last_refresh) >= self.UNIVERSE_REFRESH_SECONDS:
                    if ws is not None:
                        ws.stop()
                    ws = self._refresh_watchlist()
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

    def _refresh_watchlist(self) -> MarketWebSocketClient | None:
        opportunities = self.engine.scan(open_positions=False)
        pm_markets, kalshi_markets, matches, _ = self.engine.last_snapshot
        match_index = {
            f"{item.polymarket.market_id}:{item.kalshi.market_id}": item
            for item in matches
            if item.kalshi.venue == "kalshi"
        }

        watched: dict[str, WatchedPair] = {}
        pairs_by_asset: dict[str, set[str]] = {}
        asset_ids: list[str] = []

        for opp in opportunities:
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

        self.watch_by_pair_key = watched
        self.pairs_by_asset_id = pairs_by_asset
        self.live_books = {}

        if not asset_ids:
            print("[Watch] No Polymarket/Kalshi candidates for live monitoring.")
            return None

        asset_ids = sorted(set(asset_ids))
        print(f"[Watch] Tracking {len(watched)} candidate pairs via {len(asset_ids)} Polymarket tokens.")
        ws = MarketWebSocketClient(
            url=self.MARKET_WS_URL,
            asset_ids=asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

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

    def _maybe_open_pair(self, pair_key: str) -> None:
        watched = self.watch_by_pair_key.get(pair_key)
        if watched is None:
            return
        opp = watched.opportunity
        matched = watched.matched

        if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
            return
        if len(self.engine.db.get_open_positions()) >= self.engine.trading["max_open_pairs"]:
            return

        yes_book = self.live_books.get(matched.polymarket.yes_token_id or "")
        no_book = self.live_books.get(matched.polymarket.no_token_id or "")
        if yes_book is None or no_book is None:
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            return

        rough_yes = yes_book.best_ask if opp.buy_yes_venue == "polymarket" else matched.kalshi.yes_ask
        rough_no = no_book.best_ask if opp.buy_no_venue == "polymarket" else matched.kalshi.no_ask
        rough_edge = 1.0 - (rough_yes + rough_no)
        if rough_edge < self.engine.trading["min_lock_edge"]:
            return

        with self._signal_lock:
            if self.engine.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                return
            if self.engine._can_open(opp) != "ok":
                return
            executed, yes_leg, no_leg = self.engine._apply_execution_pricing(opp, matched)
            if executed is None:
                return
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                return
            if self.engine._can_open(executed) != "ok":
                return

            self.engine.db.open_position(
                executed,
                polymarket_snapshot_open=self.engine._polymarket_snapshot_for_market(matched.polymarket.market_id, stage="open"),
                kalshi_snapshot_open=self.engine._kalshi_snapshot_for_market(matched.kalshi.market_id, stage="open"),
            )
            print(
                f"[Watch][OPEN] {executed.symbol} | {executed.buy_yes_venue}:YES + {executed.buy_no_venue}:NO\n"
                f"               ask_sum={executed.ask_sum:.4f} | capital_used=${executed.capital_used:.2f} "
                f"| fees=${executed.total_fee:.2f} | expected_profit=${executed.expected_profit:.2f}\n"
                f"               YES leg: {self.engine._format_leg_summary(yes_leg)}\n"
                f"               NO  leg: {self.engine._format_leg_summary(no_leg)}"
            )

    def _print_status(self) -> None:
        self.engine.resolve()
        self.engine.print_status()
        print(
            f"[Watch][Status] watched_pairs={len(self.watch_by_pair_key)} "
            f"| live_tokens={len(self.live_books)}"
        )
