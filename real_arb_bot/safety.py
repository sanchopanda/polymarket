from __future__ import annotations

import time

from cross_arb_bot.models import CrossVenueOpportunity

from real_arb_bot.db import RealArbDB


class SafetyGuard:
    def __init__(self, config: dict, db: RealArbDB) -> None:
        s = config["safety"]
        self.max_trade_size_usd: float = float(s["max_trade_size_usd"])
        self.min_total_balance_usd: float = float(s.get("min_total_balance_usd", 0.0))
        self.min_leg_price: float = float(s.get("min_leg_price", 0.0))
        self.max_leg_price: float = float(s.get("max_leg_price", 1.0))
        self.min_balance_polymarket: float = float(s["min_balance_polymarket"])
        self.min_balance_kalshi: float = float(s["min_balance_kalshi"])
        self.cooldown_seconds: float = float(s["cooldown_seconds"])
        self.require_confirmation: bool = bool(s.get("require_confirmation", True))
        self.dry_run: bool = bool(s.get("dry_run", False))
        self.balance_divergence_threshold: float = float(s.get("balance_divergence_threshold", 5.0))
        self.min_partial_fill_pct: float = float(s.get("min_partial_fill_pct", 50))
        self.kalshi_slippage_cents: int = int(s.get("kalshi_slippage_cents", 2))
        self.polymarket_price_buffer_cents: int = int(s.get("polymarket_price_buffer_cents", 1))
        self.trading_min_lock_edge: float = float(config["trading"]["min_lock_edge"])
        self.min_profit_pct: float = float(config["trading"].get("min_profit_pct", 3.0))
        self.max_open_pairs: int = int(config["trading"]["max_open_pairs"])
        self.max_entries_per_pair: int = int(config["trading"]["max_entries_per_pair"])
        self.db = db

    def can_trade(
        self,
        opp: CrossVenueOpportunity,
        pm_balance: float,
        kalshi_balance: float,
    ) -> tuple[bool, str]:
        if self.dry_run:
            return False, "dry_run"

        if opp.total_cost < 5.0:
            return False, f"trade_too_small (${opp.total_cost:.2f} < $5)"

        profit_pct = (opp.expected_profit / opp.total_cost) * 100 if opp.total_cost > 0 else 0.0
        if profit_pct < self.min_profit_pct:
            return False, f"profit_too_low ({profit_pct:.1f}% < {self.min_profit_pct:.1f}%)"

        if opp.total_cost > self.max_trade_size_usd:
            return False, f"trade_too_large (${opp.total_cost:.2f} > ${self.max_trade_size_usd})"

        if opp.yes_ask < self.min_leg_price or opp.no_ask < self.min_leg_price:
            return False, (
                f"leg_price_too_low "
                f"(yes={opp.yes_ask:.4f}, no={opp.no_ask:.4f}, min={self.min_leg_price:.4f})"
            )
        if opp.yes_ask > self.max_leg_price or opp.no_ask > self.max_leg_price:
            return False, (
                f"leg_price_too_high "
                f"(yes={opp.yes_ask:.4f}, no={opp.no_ask:.4f}, max={self.max_leg_price:.4f})"
            )

        total_balance = pm_balance + kalshi_balance
        if total_balance < self.min_total_balance_usd:
            return False, f"total_balance_low (${total_balance:.2f} < ${self.min_total_balance_usd:.2f})"

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

        if pm_balance < pm_needed + self.min_balance_polymarket:
            return False, f"pm_balance_low (${pm_balance:.2f}, need ${pm_needed + self.min_balance_polymarket:.2f})"
        if kalshi_balance < kalshi_needed + self.min_balance_kalshi:
            return False, f"kalshi_balance_low (${kalshi_balance:.2f}, need ${kalshi_needed + self.min_balance_kalshi:.2f})"

        last_trade = self.db.last_trade_time()
        if last_trade is not None and (time.time() - last_trade) < self.cooldown_seconds:
            remaining = self.cooldown_seconds - (time.time() - last_trade)
            return False, f"cooldown ({remaining:.0f}s remaining)"

        open_count = len(self.db.get_open_positions())
        if open_count >= self.max_open_pairs:
            return False, f"max_open_pairs ({open_count})"

        if self.db.count_positions_for_pair(opp.pair_key, opp.buy_yes_venue, opp.buy_no_venue) >= self.max_entries_per_pair:
            return False, "pair_limit_reached"

        return True, "ok"

    def confirm_trade(self, opp: CrossVenueOpportunity) -> bool:
        if self.dry_run:
            print(f"[dry-run] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO | edge={opp.edge_per_share:.4f} | cost=${opp.total_cost:.2f}")
            return False

        if not self.require_confirmation:
            return True

        print(
            f"\n[confirm] Арбитраж: {opp.symbol}\n"
            f"  YES @ {opp.buy_yes_venue}: {opp.yes_ask:.4f}\n"
            f"  NO  @ {opp.buy_no_venue}: {opp.no_ask:.4f}\n"
            f"  ask_sum={opp.ask_sum:.4f} | edge={opp.edge_per_share:.4f}\n"
            f"  capital=${opp.capital_used:.2f} | fees=${opp.total_fee:.2f} | cost=${opp.total_cost:.2f}\n"
            f"  expected_profit=${opp.expected_profit:.2f}"
        )
        try:
            answer = input("Ставить? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return False
        return answer == "y"

    def check_balance_divergence(self, real_balance: float, computed_balance: float, venue: str) -> bool:
        diff = abs(real_balance - computed_balance)
        if diff > self.balance_divergence_threshold:
            print(
                f"[safety] ВНИМАНИЕ: расхождение баланса {venue}: "
                f"реальный=${real_balance:.2f}, расчётный=${computed_balance:.2f}, diff=${diff:.2f}"
            )
            return False
        return True
