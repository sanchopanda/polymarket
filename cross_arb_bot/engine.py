from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import datetime, timezone

from src.api.clob import ClobClient, OrderLevel

from cross_arb_bot.db import CrossArbDB
from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.matcher import build_opportunities, match_markets
from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo, MatchedMarketPair, NormalizedMarket, OpportunityDecision
from cross_arb_bot.polymarket_feed import PolymarketFeed


class CrossArbEngine:
    def __init__(self, config: dict, db: CrossArbDB) -> None:
        self.config = config
        self.db = db
        self.trading = config["trading"]
        self.market_filter = config["market_filter"]
        self.runtime = config["runtime"]
        self.pm_feed = PolymarketFeed(
            base_url=config["polymarket"]["gamma_base_url"],
            page_size=config["polymarket"]["page_size"],
            request_delay_ms=config["polymarket"]["request_delay_ms"],
            market_filter=self.market_filter,
        )
        self.kalshi_feed = KalshiFeed(
            base_url=config["kalshi"]["base_url"],
            page_size=config["kalshi"]["page_size"],
            max_pages=config["kalshi"]["max_pages"],
            request_timeout_seconds=config["kalshi"]["request_timeout_seconds"],
            market_filter=self.market_filter,
            series_tickers=config["kalshi"].get("series_tickers", []),
        )
        self.pm_clob = ClobClient(base_url="https://clob.polymarket.com", delay_ms=100)
        self.last_kalshi_error: str | None = None
        self.last_snapshot: tuple[list[NormalizedMarket], list[NormalizedMarket], list[MatchedMarketPair], list[CrossVenueOpportunity]] = ([], [], [], [])
        self.last_decisions: list[OpportunityDecision] = []

    def free_balance(self, venue: str) -> float:
        stats = self.db.stats()
        if venue == "polymarket":
            return (
                self.trading["starting_balance_polymarket"]
                + stats["realized_pnl_polymarket"]
                + stats["transfer_net_polymarket"]
                - stats["locked_polymarket"]
            )
        return (
            self.trading["starting_balance_kalshi"]
            + stats["realized_pnl_kalshi"]
            + stats["transfer_net_kalshi"]
            - stats["locked_kalshi"]
        )

    def rebalance(self, from_venue: str, to_venue: str, amount: float, note: str = "") -> str:
        from_venue = from_venue.lower().strip()
        to_venue = to_venue.lower().strip()
        if from_venue not in {"polymarket", "kalshi"} or to_venue not in {"polymarket", "kalshi"}:
            raise ValueError("venues must be 'polymarket' or 'kalshi'")
        if from_venue == to_venue:
            raise ValueError("from_venue and to_venue must differ")
        if amount <= 0:
            raise ValueError("amount must be positive")
        free_from = self.free_balance(from_venue)
        if free_from + 1e-9 < amount:
            raise ValueError(
                f"insufficient free balance on {from_venue}: available ${free_from:.2f}, requested ${amount:.2f}"
            )
        return self.db.record_transfer(from_venue=from_venue, to_venue=to_venue, amount=amount, note=note)

    def should_stop_for_balance(self) -> bool:
        threshold = float(self.trading["rebalance_threshold_usd"])
        return self.free_balance("polymarket") <= threshold and self.free_balance("kalshi") <= threshold

    def auto_rebalance(self) -> None:
        """Автоматическая ребалансировка: выравнивает балансы если разница > threshold."""
        threshold = float(self.trading["rebalance_threshold_usd"])
        pm_free = self.free_balance("polymarket")
        kalshi_free = self.free_balance("kalshi")
        diff = pm_free - kalshi_free
        if abs(diff) < threshold:
            return
        transfer = abs(diff) / 2.0
        if diff > 0:
            from_v, to_v = "polymarket", "kalshi"
        else:
            from_v, to_v = "kalshi", "polymarket"
        free_from = self.free_balance(from_v)
        if free_from < transfer:
            transfer = free_from * 0.5
        if transfer < 10.0:
            return
        self.rebalance(from_v, to_v, round(transfer, 2), note="auto-rebalance")
        print(
            f"[Rebalance] ${transfer:.2f} {from_v} → {to_v}"
            f" | PM=${self.free_balance('polymarket'):.2f} KA=${self.free_balance('kalshi'):.2f}"
        )

    def scan(self, open_positions: bool = True) -> list[CrossVenueOpportunity]:
        pm_markets = self.pm_feed.fetch_markets()
        kalshi_markets, kalshi_error = self.kalshi_feed.fetch_markets()
        self.last_kalshi_error = kalshi_error

        matches = match_markets(
            polymarket_markets=pm_markets,
            kalshi_markets=kalshi_markets,
            expiry_tolerance_seconds=self.market_filter["expiry_tolerance_seconds"],
        )
        opportunities = build_opportunities(
            matches=matches,
            min_lock_edge=self.trading["min_lock_edge"],
            max_lock_edge=self.trading["max_lock_edge"],
            stake_per_pair_usd=self.trading["stake_per_pair_usd"],
        )
        self.last_snapshot = (pm_markets, kalshi_markets, matches, opportunities)

        if open_positions:
            self._open_positions(opportunities)
        return opportunities

    def simulate_execution_cycle(self) -> list[OpportunityDecision]:
        initial_opportunities = self.scan(open_positions=False)
        if not initial_opportunities:
            self.last_decisions = []
            return []

        delay = float(self.runtime["recheck_delay_seconds"])
        if delay > 0:
            time.sleep(delay)

        recheck_opportunities = self.scan(open_positions=False)
        recheck_index = {
            (item.pair_key, item.buy_yes_venue, item.buy_no_venue): item
            for item in recheck_opportunities
        }
        match_index = {
            f"{item.polymarket.market_id}:{item.kalshi.market_id}": item
            for item in self.last_snapshot[2]
        }

        decisions: list[OpportunityDecision] = []
        for initial in initial_opportunities:
            key = (initial.pair_key, initial.buy_yes_venue, initial.buy_no_venue)
            updated = recheck_index.get(key)
            matched = match_index.get(initial.pair_key)
            if matched is None:
                continue
            initial_edge = initial.edge_per_share
            recheck_edge = updated.edge_per_share if updated else -1.0
            ask_sum_recheck = updated.ask_sum if updated else 9.999
            decision = "edge_disappeared"
            expected_profit = 0.0
            yes_leg_summary = ""
            no_leg_summary = ""

            if updated is not None and self._can_open(updated) == "ok":
                executed, yes_leg, no_leg = self._apply_execution_pricing(updated, matched)
                yes_leg_summary = self._format_leg_summary(yes_leg)
                no_leg_summary = self._format_leg_summary(no_leg)
                if executed is None:
                    decision = "insufficient_liquidity"
                    expected_profit = 0.0
                elif executed.edge_per_share < self.trading["min_lock_edge"]:
                    decision = "edge_disappeared"
                    expected_profit = 0.0
                elif self._can_open(executed) != "ok":
                    decision = self._can_open(executed)
                    expected_profit = 0.0
                else:
                    self.db.open_position(
                        executed,
                        polymarket_snapshot_open=self._polymarket_snapshot_for_market(matched.polymarket.market_id, stage="open"),
                        kalshi_snapshot_open=self._kalshi_snapshot_for_market(matched.kalshi.market_id, stage="open"),
                        yes_leg=yes_leg,
                        no_leg=no_leg,
                    )
                    decision = "opened"
                    expected_profit = executed.expected_profit
            elif updated is not None:
                decision = self._can_open(updated)

            decisions.append(
                OpportunityDecision(
                    pair_key=initial.pair_key,
                    symbol=initial.symbol,
                    buy_yes_venue=initial.buy_yes_venue,
                    buy_no_venue=initial.buy_no_venue,
                    polymarket_yes=matched.polymarket.yes_ask,
                    polymarket_no=matched.polymarket.no_ask,
                    kalshi_yes=matched.kalshi.yes_ask,
                    kalshi_no=matched.kalshi.no_ask,
                    ask_sum_initial=initial.ask_sum,
                    ask_sum_recheck=ask_sum_recheck,
                    edge_initial=initial_edge,
                    edge_recheck=recheck_edge,
                    shares=initial.shares,
                    decision=decision,
                    expected_profit=expected_profit,
                    yes_leg_summary=yes_leg_summary,
                    no_leg_summary=no_leg_summary,
                    was_raw_opportunity=True,
                )
            )

        self.last_decisions = decisions
        return decisions

    def _open_positions(self, opportunities: list[CrossVenueOpportunity]) -> None:
        current_open = self.db.get_open_positions()
        remaining_slots = self.trading["max_open_pairs"] - len(current_open)
        if remaining_slots <= 0:
            return

        opened = 0
        for opp in opportunities:
            if opened >= remaining_slots:
                break
            if self.db.has_open_position(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue):
                continue
            if self._can_open(opp) != "ok":
                continue

            matched = next((item for item in self.last_snapshot[2] if f"{item.polymarket.market_id}:{item.kalshi.market_id}" == opp.pair_key), None)
            if matched is None:
                continue
            executed, yes_leg, no_leg = self._apply_execution_pricing(opp, matched)
            if executed is None:
                print(
                    f"[Cross][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=insufficient_liquidity\n"
                    f"              YES leg: {self._format_leg_summary(yes_leg)}\n"
                    f"              NO  leg: {self._format_leg_summary(no_leg)}"
                )
                continue
            if executed.edge_per_share < self.trading["min_lock_edge"]:
                continue
            if self._can_open(executed) != "ok":
                continue

            position = self.db.open_position(
                executed,
                polymarket_snapshot_open=self._polymarket_snapshot_for_market(matched.polymarket.market_id, stage="open"),
                kalshi_snapshot_open=self._kalshi_snapshot_for_market(matched.kalshi.market_id, stage="open"),
                yes_leg=yes_leg,
                no_leg=no_leg,
            )
            opened += 1
            print(
                f"[Cross][OPEN] {executed.symbol} | {executed.buy_yes_venue}:YES + {executed.buy_no_venue}:NO\n"
                f"              ask_sum={executed.ask_sum:.4f} | capital_used=${executed.capital_used:.2f} "
                f"| fees=${executed.total_fee:.2f} | expected_payout=${executed.expected_payout:.2f} "
                f"| expected_profit=${executed.expected_profit:.2f}\n"
                f"              YES leg: {self._format_leg_summary(yes_leg)}\n"
                f"              NO  leg: {self._format_leg_summary(no_leg)}"
            )

    def _can_open(self, opp: CrossVenueOpportunity) -> str:
        pm_needed = (
            (opp.yes_ask * opp.shares if opp.buy_yes_venue == "polymarket" else 0.0)
            + (opp.no_ask * opp.shares if opp.buy_no_venue == "polymarket" else 0.0)
            + opp.polymarket_fee
        )
        kalshi_needed = (
            (opp.yes_ask * opp.shares if opp.buy_yes_venue == "kalshi" else 0.0)
            + (opp.no_ask * opp.shares if opp.buy_no_venue == "kalshi" else 0.0)
            + opp.kalshi_fee
        )
        pm_free = self.free_balance("polymarket")
        kalshi_free = self.free_balance("kalshi")
        threshold = float(self.trading["rebalance_threshold_usd"])

        if pm_free <= threshold and kalshi_free <= threshold:
            return "bot_stopped"

        if self.free_balance("polymarket") < pm_needed:
            return "insufficient_balance"
        if self.free_balance("kalshi") < kalshi_needed:
            return "insufficient_balance"
        if self.db.count_positions_for_pair(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue) >= self.trading["max_entries_per_pair"]:
            return "pair_limit_reached"
        if len(self.db.get_open_positions()) >= self.trading["max_open_pairs"]:
            return "max_open_pairs"

        # If one venue is already drained, only allow a new trade when that venue
        # funds the higher-priced leg of the pair.
        depleted_venue: str | None = None
        if pm_free <= threshold < kalshi_free:
            depleted_venue = "polymarket"
        elif kalshi_free <= threshold < pm_free:
            depleted_venue = "kalshi"

        if depleted_venue is not None:
            higher_prob_venue = opp.buy_yes_venue if opp.yes_ask >= opp.no_ask else opp.buy_no_venue
            if depleted_venue != higher_prob_venue:
                return "rebalance_block"
        return "ok"

    def _apply_execution_pricing(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
    ) -> tuple[CrossVenueOpportunity | None, ExecutionLegInfo | None, ExecutionLegInfo | None]:
        yes_market = matched.polymarket if opp.buy_yes_venue == "polymarket" else matched.kalshi
        no_market = matched.polymarket if opp.buy_no_venue == "polymarket" else matched.kalshi

        yes_leg = self._execution_leg_info(opp.buy_yes_venue, yes_market, "yes", opp.shares)
        no_leg = self._execution_leg_info(opp.buy_no_venue, no_market, "no", opp.shares)
        if yes_leg is None or no_leg is None:
            return None, yes_leg, no_leg
        if yes_leg.filled_shares + 1e-6 < opp.shares or no_leg.filled_shares + 1e-6 < opp.shares:
            return None, yes_leg, no_leg

        yes_ask = yes_leg.avg_price
        no_ask = no_leg.avg_price
        ask_sum = yes_ask + no_ask
        edge_per_share = 1.0 - ask_sum
        capital_used = yes_leg.total_cost + no_leg.total_cost
        polymarket_fee = 0.0
        kalshi_fee = 0.0
        if opp.buy_yes_venue == "polymarket":
            polymarket_fee += self._polymarket_fee(opp.shares, yes_ask)
        else:
            kalshi_fee += self._kalshi_fee(opp.shares, yes_ask)
        if opp.buy_no_venue == "polymarket":
            polymarket_fee += self._polymarket_fee(opp.shares, no_ask)
        else:
            kalshi_fee += self._kalshi_fee(opp.shares, no_ask)
        total_fee = polymarket_fee + kalshi_fee
        total_cost = capital_used + total_fee
        return (
            replace(
                opp,
                yes_ask=yes_ask,
                no_ask=no_ask,
                ask_sum=ask_sum,
                edge_per_share=edge_per_share,
                capital_used=capital_used,
                polymarket_fee=polymarket_fee,
                kalshi_fee=kalshi_fee,
                total_fee=total_fee,
                total_cost=total_cost,
                expected_profit=opp.shares - total_cost,
            ),
            yes_leg,
            no_leg,
        )

    def _execution_leg_info(self, venue: str, market: NormalizedMarket, side: str, shares: float) -> ExecutionLegInfo | None:
        asks: list[OrderLevel] | None = None
        if venue == "polymarket":
            token_id = market.yes_token_id if side == "yes" else market.no_token_id
            if not token_id:
                return None
            book = self.pm_clob.get_orderbook(token_id)
            if not book or not book.asks:
                return None
            asks = book.asks
        else:
            asks, _ = self.kalshi_feed.fetch_side_asks(market.market_id, side)
            if not asks:
                return None

        available_shares = sum(level.size for level in asks)
        best_ask = asks[0].price if asks else 0.0
        remaining = shares
        total_cost = 0.0
        for level in asks:
            take = min(level.size, remaining)
            if take <= 0:
                continue
            total_cost += take * level.price
            remaining -= take
            if remaining <= 1e-9:
                break
        filled_shares = shares - remaining
        avg_price = (total_cost / filled_shares) if filled_shares > 1e-9 else 0.0
        return ExecutionLegInfo(
            venue=venue,
            market_id=market.market_id,
            side=side,
            requested_shares=shares,
            filled_shares=filled_shares,
            available_shares=available_shares,
            avg_price=avg_price,
            total_cost=total_cost,
            best_ask=best_ask,
            remaining_shares_after_fill=max(0.0, available_shares - filled_shares),
        )

    def _format_leg_summary(self, leg: ExecutionLegInfo | None) -> str:
        if leg is None:
            return "book unavailable"
        status = "ok" if leg.filled_shares + 1e-6 >= leg.requested_shares else "short"
        slippage = leg.avg_price - leg.best_ask if leg.filled_shares > 1e-9 else 0.0
        missing = max(0.0, leg.requested_shares - leg.filled_shares)
        return (
            f"{leg.venue}:{leg.side.upper()} need {leg.requested_shares:.2f}sh | "
            f"book has {leg.available_shares:.2f}sh | "
            f"filled {leg.filled_shares:.2f}sh | "
            f"best {leg.best_ask:.4f} -> avg {leg.avg_price:.4f} "
            f"(slippage {slippage:+.4f}) | "
            f"cost ${leg.total_cost:.2f} | "
            f"left in book {leg.remaining_shares_after_fill:.2f}sh"
            + (f" | missing {missing:.2f}sh" if missing > 1e-6 else "")
            + f" | {status}"
        )

    def _polymarket_fee(self, shares: float, price: float) -> float:
        fee_rate = 0.25
        exponent = 2
        return shares * price * fee_rate * ((price * (1 - price)) ** exponent)

    def _kalshi_fee(self, shares: float, price: float) -> float:
        raw = 0.07 * shares * price * (1 - price)
        cents = int(raw * 100)
        if raw * 100 > cents:
            cents += 1
        return cents / 100.0

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for position in self.db.get_open_positions():
            if position.expiry > now:
                continue
            pm_result, pm_snapshot_resolved = self._resolve_polymarket_leg(position)
            kalshi_result, kalshi_snapshot_resolved = self._resolve_kalshi_leg(position)
            if pm_result is None or kalshi_result is None:
                continue

            yes_wins = self._leg_wins(position.venue_yes, "yes", pm_result, kalshi_result)
            no_wins = self._leg_wins(position.venue_no, "no", pm_result, kalshi_result)
            payout = (position.shares if yes_wins else 0.0) + (position.shares if no_wins else 0.0)
            pnl = payout - position.total_cost
            lock_valid = (yes_wins + no_wins) == 1
            winning_side = "yes" if yes_wins and not no_wins else ("no" if no_wins and not yes_wins else "mismatch")
            self.db.resolve_position(
                position.id,
                winning_side=winning_side,
                pnl=pnl,
                polymarket_result=pm_result,
                kalshi_result=kalshi_result,
                lock_valid=lock_valid,
                polymarket_snapshot_resolved=pm_snapshot_resolved,
                kalshi_snapshot_resolved=kalshi_snapshot_resolved,
            )
            stored = self.db.get_position(position.id)
            print(
                f"[Cross][RESOLVE] {position.symbol} | pm={pm_result} | kalshi={kalshi_result} "
                f"| lock_valid={lock_valid} | pnl=${stored.pnl:+.2f}"
            )

    def _resolve_polymarket_leg(self, position) -> tuple[str | None, str | None]:
        market_id = position.market_yes if position.venue_yes == "polymarket" else position.market_no
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None, None
        snapshot = self._serialize_snapshot(
            {
                "stage": "resolve",
                "venue": "polymarket",
                "market_id": market.id,
                "title": market.question,
                "description": market.description,
                "resolution_source": market.resolution_source,
                "outcomes": market.outcomes,
                "outcome_prices": market.outcome_prices,
                "end_date": market.end_date.isoformat() if market.end_date else None,
            }
        )
        try:
            up_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "up")
            down_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "down")
        except StopIteration:
            return None, snapshot
        up_price = market.outcome_prices[up_idx]
        down_price = market.outcome_prices[down_idx]
        if up_price >= 0.9:
            return "yes", snapshot
        if down_price >= 0.9:
            return "no", snapshot
        return None, snapshot

    def _resolve_kalshi_leg(self, position) -> tuple[str | None, str | None]:
        market_id = position.market_yes if position.venue_yes == "kalshi" else position.market_no
        payload, error = self.kalshi_feed.fetch_market(market_id)
        if payload is None:
            return None, None
        snapshot = self._kalshi_snapshot_from_payload(payload, stage="resolve")
        result = str(payload.get("result") or "").lower()
        if result in {"yes", "no"}:
            return result, snapshot
        status = str(payload.get("status") or "").lower()
        if status not in {"closed", "determined", "finalized"}:
            return None, snapshot
        return None, snapshot

    def _leg_wins(self, venue: str, side: str, polymarket_result: str, kalshi_result: str) -> bool:
        if venue == "polymarket":
            return polymarket_result == side
        return kalshi_result == side

    def _polymarket_snapshot_for_market(self, market_id: str, stage: str) -> str | None:
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None:
            return None
        return self._serialize_snapshot(
            {
                "stage": stage,
                "venue": "polymarket",
                "market_id": market.id,
                "title": market.question,
                "description": market.description,
                "resolution_source": market.resolution_source,
                "outcomes": market.outcomes,
                "outcome_prices": market.outcome_prices,
                "end_date": market.end_date.isoformat() if market.end_date else None,
            }
        )

    def _kalshi_snapshot_for_market(self, market_id: str, stage: str) -> str | None:
        payload, error = self.kalshi_feed.fetch_market(market_id)
        if payload is None:
            return None
        return self._kalshi_snapshot_from_payload(payload, stage=stage)

    def _kalshi_snapshot_from_payload(self, payload: dict, stage: str) -> str:
        return self._serialize_snapshot(
            {
                "stage": stage,
                "venue": "kalshi",
                "ticker": payload.get("ticker"),
                "title": payload.get("title"),
                "subtitle": payload.get("subtitle"),
                "status": payload.get("status"),
                "result": payload.get("result"),
                "yes_ask_dollars": payload.get("yes_ask_dollars"),
                "no_ask_dollars": payload.get("no_ask_dollars"),
                "yes_bid_dollars": payload.get("yes_bid_dollars"),
                "no_bid_dollars": payload.get("no_bid_dollars"),
                "floor_strike": payload.get("floor_strike"),
                "cap_strike": payload.get("cap_strike"),
                "open_time": payload.get("open_time"),
                "close_time": payload.get("close_time"),
                "expiration_time": payload.get("expiration_time"),
                "rules_primary": payload.get("rules_primary"),
                "rules_secondary": payload.get("rules_secondary"),
                "settlement_sources": payload.get("settlement_sources"),
            }
        )

    def _serialize_snapshot(self, payload: dict) -> str:
        return json.dumps(payload, ensure_ascii=True, sort_keys=True)

    def print_status(self) -> None:
        stats = self.db.stats()
        pm_free = self.free_balance("polymarket")
        kalshi_free = self.free_balance("kalshi")
        print(
            f"Polymarket free: ${pm_free:.2f}\n"
            f"Kalshi free: ${kalshi_free:.2f}\n"
            f"Open pairs: {stats['open']}\n"
            f"Resolved pairs: {stats['resolved']}\n"
            f"Realized P&L: ${stats['realized_pnl']:+.2f}\n"
            f"Transfer net: PM ${stats['transfer_net_polymarket']:+.2f} | KA ${stats['transfer_net_kalshi']:+.2f} "
            f"| count={stats['transfer_count']}"
        )
        if self.last_kalshi_error:
            print(f"Kalshi status: {self.last_kalshi_error}")

        pm_markets, kalshi_markets, matches, opportunities = self.last_snapshot
        print(
            f"Last snapshot: polymarket={len(pm_markets)} markets | "
            f"kalshi={len(kalshi_markets)} markets | matches={len(matches)} | "
            f"opportunities={len(opportunities)}"
        )
        if not self.last_kalshi_error and not kalshi_markets:
            print("Kalshi status: API reachable, but no matching markets found for current filter.")
        if self.last_decisions:
            print("Raw opportunities and outcomes:")
            for item in self.last_decisions[:10]:
                print(
                    f"  {item.symbol} | {item.buy_yes_venue}:YES + {item.buy_no_venue}:NO "
                    f"| PM up/down={item.polymarket_yes:.3f}/{item.polymarket_no:.3f} "
                    f"| KA yes/no={item.kalshi_yes:.3f}/{item.kalshi_no:.3f}\n"
                    f"    raw_t0={item.ask_sum_initial:.4f} -> recheck_t1={item.ask_sum_recheck:.4f} "
                    f"| raw_edge=${item.edge_initial:.4f} recheck_edge=${item.edge_recheck:.4f} "
                    f"| decision={item.decision}\n"
                    f"    YES leg: {item.yes_leg_summary}\n"
                    f"    NO  leg: {item.no_leg_summary}"
                )
        elif opportunities:
            for opp in opportunities[:5]:
                print(
                    f"\n  [{opp.symbol}] {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO"
                    f" | ask_sum={opp.ask_sum:.4f} | edge={opp.edge_per_share:.4f}"
                    f" | cost=${opp.total_cost:.2f} | exp_profit=${opp.expected_profit:.2f}"
                    f"\n    PM:     {opp.polymarket_title}"
                    f"\n    Kalshi: {opp.kalshi_title}"
                    f"\n    Expiry: {opp.expiry.strftime('%Y-%m-%d %H:%M')} UTC"
                    f" | expiry_delta={opp.expiry_delta_seconds:.0f}s"
                    f"\n    YES ask: {opp.yes_ask:.4f} ({opp.buy_yes_venue})"
                    f" | NO ask: {opp.no_ask:.4f} ({opp.buy_no_venue})"
                )
