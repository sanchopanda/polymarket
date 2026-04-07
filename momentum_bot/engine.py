from __future__ import annotations

import time
from datetime import datetime, timezone

from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.matcher import kalshi_taker_fee, match_markets, polymarket_crypto_taker_fee
from cross_arb_bot.models import MatchedMarketPair, NormalizedMarket
from cross_arb_bot.polymarket_feed import PolymarketFeed

from momentum_bot.db import MomentumDB
from momentum_bot.models import MomentumPosition, SpikeSignal
from momentum_bot.telegram_notify import MomentumTelegramNotifier


class MomentumEngine:
    def __init__(
        self,
        config: dict,
        db: MomentumDB,
        notifier: MomentumTelegramNotifier | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.strategy = config["strategy"]
        self.market_filter = config["market_filter"]
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
        self.last_discovery_stats: dict[str, int | str | None] = {
            "pm_markets": 0,
            "kalshi_markets": 0,
            "matches": 0,
            "kalshi_error": None,
            "same_symbol_pairs": 0,
            "symbol_only_pairs": 0,
            "kind_mismatch": 0,
            "rule_mismatch": 0,
            "interval_mismatch": 0,
            "expiry_mismatch": 0,
        }

    def discover_pairs(self) -> list[MatchedMarketPair]:
        pm_markets = self.pm_feed.fetch_markets()
        kalshi_markets, kalshi_error = self.kalshi_feed.fetch_markets()
        diag = self._diagnose_matching(pm_markets, kalshi_markets)
        matches = match_markets(
            pm_markets,
            kalshi_markets,
            self.market_filter["expiry_tolerance_seconds"],
        )
        self.last_discovery_stats = {
            "pm_markets": len(pm_markets),
            "kalshi_markets": len(kalshi_markets),
            "matches": len(matches),
            "kalshi_error": kalshi_error,
            **diag,
        }
        return matches

    def _diagnose_matching(
        self,
        pm_markets: list[NormalizedMarket],
        kalshi_markets: list[NormalizedMarket],
    ) -> dict[str, int]:
        same_symbol_pairs = 0
        symbol_only_pairs = 0
        kind_mismatch = 0
        rule_mismatch = 0
        interval_mismatch = 0
        expiry_mismatch = 0
        expiry_tolerance_seconds = self.market_filter["expiry_tolerance_seconds"]

        for pm in pm_markets:
            for kalshi in kalshi_markets:
                if pm.symbol != kalshi.symbol:
                    continue
                same_symbol_pairs += 1

                if pm.market_kind != kalshi.market_kind:
                    kind_mismatch += 1
                    continue
                if pm.rule_family != kalshi.rule_family:
                    rule_mismatch += 1
                    continue
                if pm.interval_minutes != kalshi.interval_minutes:
                    interval_mismatch += 1
                    continue

                symbol_only_pairs += 1
                delta = abs((pm.expiry - kalshi.expiry).total_seconds())
                if delta > expiry_tolerance_seconds:
                    expiry_mismatch += 1

        return {
            "same_symbol_pairs": same_symbol_pairs,
            "symbol_only_pairs": symbol_only_pairs,
            "kind_mismatch": kind_mismatch,
            "rule_mismatch": rule_mismatch,
            "interval_mismatch": interval_mismatch,
            "expiry_mismatch": expiry_mismatch,
        }

    def evaluate_signal(self, signal: SpikeSignal) -> bool:
        strat = self.strategy
        matched = signal.matched_pair

        # 0. Только последняя треть рынка (последние 5 минут из 15)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if now.minute % 15 < 9:
            return False

        # 1. Spike not too large (near-resolution markets)
        max_spike = strat.get("max_spike_cents", 9999)
        if signal.spike_magnitude > max_spike:
            return False

        # 2. Leader price must be above threshold
        if signal.leader_price < strat.get("min_leader_price", 0.0):
            return False

        # 3. Follower price not too high (near-resolution, tiny upside)
        max_entry = strat.get("max_entry_price", 1.0)
        if signal.follower_price > max_entry:
            return False

        # 3. Gap not too large (structural divergence, not momentum)
        max_gap = strat.get("max_price_gap_cents", 9999)
        if signal.price_gap * 100 > max_gap:
            return False

        # 4. Follower price must be > 0 (market active)
        if signal.follower_price <= 0:
            return False

        # 4. No duplicate open position
        if self.db.has_open_position(signal.pair_key, signal.side, signal.follower_venue):
            return False

        # 5. No opposite side open on same pair (any venue) — guaranteed loss
        if self.db.has_open_opposite_side(signal.pair_key, signal.side):
            return False

        # 5. Cooldown check
        last_trade = self.db.last_trade_time(signal.pair_key, signal.side)
        if last_trade is not None:
            elapsed = time.time() - last_trade
            if elapsed < strat["cooldown_seconds"]:
                return False

        # 6. Max open positions
        if len(self.db.get_open_positions()) >= strat["max_open_positions"]:
            return False

        # 7. Sufficient balance
        if self.free_balance() < strat["stake_per_trade_usd"]:
            return False

        return True

    def open_paper_position(self, signal: SpikeSignal) -> MomentumPosition | None:
        matched = signal.matched_pair
        strat = self.strategy
        entry_price = signal.follower_price
        if entry_price <= 0:
            return None

        shares = strat["stake_per_trade_usd"] / entry_price

        # Compute fee on the bet leg
        if signal.follower_venue == "polymarket":
            fee = polymarket_crypto_taker_fee(shares, entry_price)
        else:
            fee = kalshi_taker_fee(shares, entry_price)
        total_cost = shares * entry_price + fee

        # Title from matched pair
        title = f"{matched.polymarket.title} <> {matched.kalshi.title}"
        expiry = min(matched.polymarket.expiry, matched.kalshi.expiry)

        return self.db.open_position(
            signal=signal,
            shares=shares,
            entry_price=entry_price,
            total_cost=total_cost,
            title=title,
            expiry=expiry,
        )

    def notify_open(self, position: MomentumPosition, signal: SpikeSignal, signal_type: str) -> None:
        if not self.notifier:
            return
        self.notifier.notify_open(
            symbol=position.symbol,
            side=position.side,
            signal_type=signal_type,
            leader_venue=signal.leader_venue,
            follower_venue=signal.follower_venue,
            leader_price=signal.leader_price,
            follower_price=position.entry_price,
            gap_cents=(signal.leader_price - position.entry_price) * 100.0,
            spike_cents=signal.spike_magnitude,
            total_cost=position.total_cost,
        )

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for position in self.db.get_open_positions():
            if position.expiry > now:
                continue

            # Resolve based on bet_venue
            if position.bet_venue == "polymarket":
                result = self._resolve_polymarket(position)
            else:
                result = self._resolve_kalshi(position)

            if result is None:
                continue

            # PnL: single-side bet
            if result == position.side:
                pnl = position.shares * 1.0 - position.total_cost
            else:
                pnl = -position.total_cost

            self.db.resolve_position(position.id, outcome=result, pnl=pnl)
            print(
                f"[Momentum][RESOLVE] {position.symbol} {position.side.upper()} @ {position.bet_venue}"
                f" | outcome={result} | pnl=${pnl:+.2f}"
            )
            if self.notifier:
                self.notifier.notify_resolve(
                    position.symbol,
                    position.side,
                    position.bet_venue,
                    result,
                    pnl,
                )

    def _resolve_polymarket(self, position: MomentumPosition) -> str | None:
        # Find the PM market_id from pair_key (format: pm_id:kalshi_ticker)
        pm_market_id = position.pair_key.split(":")[0]
        market = self.pm_feed.client.fetch_market(pm_market_id)
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None
        try:
            up_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "up")
            down_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "down")
        except StopIteration:
            return None
        if market.outcome_prices[up_idx] >= 0.9:
            return "yes"
        if market.outcome_prices[down_idx] >= 0.9:
            return "no"
        return None

    def _resolve_kalshi(self, position: MomentumPosition) -> str | None:
        kalshi_ticker = position.pair_key.split(":")[-1]
        payload, _ = self.kalshi_feed.fetch_market(kalshi_ticker)
        if payload is None:
            return None
        result = str(payload.get("result") or "").lower()
        if result in {"yes", "no"}:
            return result
        return None

    def free_balance(self) -> float:
        stats = self.db.stats()
        return (
            self.strategy["starting_balance"]
            + stats["realized_pnl"]
            - stats["locked"]
        )

    def print_status(self) -> None:
        print(self.get_status_text())
        open_positions = self.db.get_open_positions()
        if open_positions:
            print("[Momentum][Open Positions]")
            for p in open_positions:
                print(
                    f"  {p.symbol} {p.side.upper()} @ {p.bet_venue}"
                    f" | leader={p.leader_venue} spike={p.spike_magnitude:.1f}¢"
                    f" | entry={p.entry_price:.4f} shares={p.shares:.2f}"
                    f" | cost=${p.total_cost:.2f}"
                    f" | expiry={p.expiry.strftime('%H:%M')}"
                )

    def get_status_text(self) -> str:
        stats = self.db.stats()
        balance = self.free_balance()
        lines = [
            f"[Momentum][Status] balance=${balance:.2f}"
            f" | realized_pnl=${stats['realized_pnl']:+.2f}"
            f" | open={stats['open_count']}"
            f" | resolved={stats['resolved_count']}"
            f" | won={stats['won_count']} lost={stats['lost_count']}"
        ]
        open_positions = self.db.get_open_positions()
        if open_positions:
            for p in open_positions:
                lines.append(
                    f"{p.symbol} {p.side.upper()} @ {p.bet_venue}"
                    f" | leader={p.leader_venue} spike={p.spike_magnitude:.1f}¢"
                    f" | entry={p.entry_price:.4f} shares={p.shares:.2f}"
                    f" | cost=${p.total_cost:.2f}"
                    f" | expiry={p.expiry.strftime('%H:%M')}"
                )
        return "\n".join(lines)
