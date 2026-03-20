from __future__ import annotations

import time
from datetime import datetime, timezone

from src.api.clob import ClobClient, OrderLevel

from cross_arb_bot.db import CrossArbDB
from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.matcher import build_opportunities, match_markets
from cross_arb_bot.models import CrossVenueOpportunity, MatchedMarketPair, NormalizedMarket, OpportunityDecision
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
            return self.trading["starting_balance_polymarket"] + stats["realized_pnl"] - stats["locked_polymarket"]
        return self.trading["starting_balance_kalshi"] - stats["locked_kalshi"]

    def should_stop_for_balance(self) -> bool:
        threshold = float(self.trading["rebalance_threshold_usd"])
        return self.free_balance("polymarket") <= threshold and self.free_balance("kalshi") <= threshold

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

            if updated is not None and self._can_open(updated) == "ok":
                liquidity_decision = self._check_liquidity(updated, matched)
                if liquidity_decision != "ok":
                    decision = liquidity_decision
                    expected_profit = 0.0
                else:
                    self.db.open_position(updated)
                    decision = "opened"
                    expected_profit = updated.expected_profit
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
            if not self._can_open(opp):
                continue

            position = self.db.open_position(opp)
            opened += 1
            print(
                f"[Cross][OPEN] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                f"              ask_sum={opp.ask_sum:.4f} | capital_used=${opp.capital_used:.2f} "
                f"| fees=${opp.total_fee:.2f} | expected_payout=${opp.expected_payout:.2f} "
                f"| expected_profit=${opp.expected_profit:.2f}"
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

    def _check_liquidity(self, opp: CrossVenueOpportunity, matched: MatchedMarketPair) -> str:
        yes_market = matched.polymarket if opp.buy_yes_venue == "polymarket" else matched.kalshi
        no_market = matched.polymarket if opp.buy_no_venue == "polymarket" else matched.kalshi

        yes_cost = self._execution_cost(opp.buy_yes_venue, yes_market, "yes", opp.shares)
        if yes_cost is None:
            return "insufficient_liquidity"
        no_cost = self._execution_cost(opp.buy_no_venue, no_market, "no", opp.shares)
        if no_cost is None:
            return "insufficient_liquidity"
        return "ok"

    def _execution_cost(self, venue: str, market: NormalizedMarket, side: str, shares: float) -> float | None:
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
        if remaining > 1e-6:
            return None
        return total_cost

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for position in self.db.get_open_positions():
            if position.expiry > now:
                continue
            pm_result = self._resolve_polymarket_leg(position)
            kalshi_result = self._resolve_kalshi_leg(position)
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
            )
            stored = self.db.get_position(position.id)
            print(
                f"[Cross][RESOLVE] {position.symbol} | pm={pm_result} | kalshi={kalshi_result} "
                f"| lock_valid={lock_valid} | pnl=${stored.pnl:+.2f}"
            )

    def _resolve_polymarket_leg(self, position) -> str | None:
        market_id = position.market_yes if position.venue_yes == "polymarket" else position.market_no
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None
        try:
            up_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "up")
            down_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "down")
        except StopIteration:
            return None
        up_price = market.outcome_prices[up_idx]
        down_price = market.outcome_prices[down_idx]
        if up_price >= 0.9:
            return "yes"
        if down_price >= 0.9:
            return "no"
        return None

    def _resolve_kalshi_leg(self, position) -> str | None:
        market_id = position.market_yes if position.venue_yes == "kalshi" else position.market_no
        payload, error = self.kalshi_feed.fetch_market(market_id)
        if payload is None:
            return None
        result = str(payload.get("result") or "").lower()
        if result in {"yes", "no"}:
            return result
        status = str(payload.get("status") or "").lower()
        if status not in {"closed", "determined", "finalized"}:
            return None
        return None

    def _leg_wins(self, venue: str, side: str, polymarket_result: str, kalshi_result: str) -> bool:
        if venue == "polymarket":
            return polymarket_result == side
        return kalshi_result == side

    def print_status(self) -> None:
        stats = self.db.stats()
        pm_free = self.free_balance("polymarket")
        kalshi_free = self.free_balance("kalshi")
        print(
            f"Polymarket free: ${pm_free:.2f}\n"
            f"Kalshi free: ${kalshi_free:.2f}\n"
            f"Open pairs: {stats['open']}\n"
            f"Resolved pairs: {stats['resolved']}\n"
            f"Realized P&L: ${stats['realized_pnl']:+.2f}"
        )
        if self.last_kalshi_error:
            print(f"Kalshi status: {self.last_kalshi_error}")

        pm_markets, kalshi_markets, matches, opportunities = self.last_snapshot
        print(
            f"Last snapshot: polymarket={len(pm_markets)} markets | "
            f"kalshi={len(kalshi_markets)} markets | matches={len(matches)} | opportunities={len(opportunities)}"
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
                    f"| decision={item.decision}"
                )
        elif opportunities:
            for opp in opportunities[:5]:
                print(
                    f"  {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO "
                    f"| ask_sum={opp.ask_sum:.4f} | capital_used=${opp.capital_used:.2f} "
                    f"| fees=${opp.total_fee:.2f} | expected_payout=${opp.expected_payout:.2f} "
                    f"| expected_profit=${opp.expected_profit:.2f}"
                )
