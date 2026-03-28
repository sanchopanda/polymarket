from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from real_arb_bot.clients import KalshiTrader, OrderResult, PolymarketTrader


@dataclass
class RealExecResult:
    status: str  # "both_filled" | "pending_pm" | "failed"
    ka_order_id: str = ""
    ka_fill_price: float = 0.0
    ka_fill_shares: float = 0.0
    pm_order_id: str = ""
    pm_fill_price: float = 0.0
    pm_fill_shares: float = 0.0


class SportsRealExecutor:
    """
    Kalshi-first execution for sports arb.
    If the PM leg fails, returns pending_pm — never unwinds Kalshi.
    """

    def __init__(
        self,
        pm_trader: "PolymarketTrader",
        kalshi_trader: "KalshiTrader",
        slippage_cents: int = 2,
        pm_buffer: float = 0.01,
        min_fill_pct: float = 0.80,
    ) -> None:
        self.pm_trader = pm_trader
        self.kalshi_trader = kalshi_trader
        self.slippage_cents = slippage_cents
        self.pm_buffer = pm_buffer
        self.min_fill_pct = min_fill_pct

    def execute(
        self,
        ka_ticker: str,
        pm_token_id: str,
        shares: int,
        ka_ask: float,
        pm_ask: float,
    ) -> RealExecResult:
        """
        1. Buy YES on Kalshi at ka_ask + slippage
        2. If Kalshi fills: buy YES on PM via FOK at pm_ask + buffer
        3. If PM fails: return pending_pm (don't unwind Kalshi)
        """
        # ── Kalshi leg ──────────────────────────────────────────────────
        ka_price_cents = int(ka_ask * 100) + self.slippage_cents
        try:
            ka_result = self.kalshi_trader.place_limit_order(
                ticker=ka_ticker,
                side="yes",
                count=shares,
                price_cents=ka_price_cents,
                action="buy",
            )
        except Exception as e:
            print(f"[real-exec] Kalshi order error: {e}")
            return RealExecResult(status="failed")

        if ka_result.status.startswith("error"):
            return RealExecResult(status="failed", ka_order_id=ka_result.order_id)

        # Wait briefly for fill to register, then refresh
        filled_ka = ka_result.shares_matched
        if filled_ka < shares * self.min_fill_pct and ka_result.order_id:
            time.sleep(1.0)
            refreshed = self.kalshi_trader.get_order(ka_result.order_id)
            if refreshed:
                filled_ka = refreshed.shares_matched

        if filled_ka < shares * self.min_fill_pct:
            if ka_result.order_id:
                self.kalshi_trader.cancel_order(ka_result.order_id)
            print(f"[real-exec] Kalshi fill too low: {filled_ka:.1f}/{shares} — aborting")
            return RealExecResult(status="failed", ka_order_id=ka_result.order_id)

        actual_shares = int(filled_ka)
        ka_fill_price = ka_result.fill_price
        print(
            f"[real-exec] Kalshi filled {actual_shares}@{ka_fill_price:.4f} "
            f"(order {ka_result.order_id[:16]}...)"
        )

        # ── PM leg ──────────────────────────────────────────────────────
        amount_usd = actual_shares * (pm_ask + self.pm_buffer)
        try:
            pm_result = self.pm_trader.place_fok_order(
                token_id=pm_token_id,
                amount_usd=amount_usd,
            )
        except Exception as e:
            print(f"[real-exec] PM order error: {e}")
            return RealExecResult(
                status="pending_pm",
                ka_order_id=ka_result.order_id,
                ka_fill_price=ka_fill_price,
                ka_fill_shares=float(actual_shares),
            )

        if pm_result.shares_matched >= actual_shares * self.min_fill_pct:
            print(
                f"[real-exec] PM filled {pm_result.shares_matched:.1f}@{pm_result.fill_price:.4f}"
            )
            return RealExecResult(
                status="both_filled",
                ka_order_id=ka_result.order_id,
                ka_fill_price=ka_fill_price,
                ka_fill_shares=float(actual_shares),
                pm_order_id=pm_result.order_id,
                pm_fill_price=pm_result.fill_price,
                pm_fill_shares=pm_result.shares_matched,
            )
        else:
            print(
                f"[real-exec] PM fill too low ({pm_result.shares_matched:.1f}/{actual_shares}) "
                f"— queuing for retry (Kalshi position kept)"
            )
            return RealExecResult(
                status="pending_pm",
                ka_order_id=ka_result.order_id,
                ka_fill_price=ka_fill_price,
                ka_fill_shares=float(actual_shares),
            )

    def retry_pm(
        self,
        pm_token_id: str,
        shares: int,
        pm_price_limit: float,
    ) -> "Optional[OrderResult]":
        """
        Single FOK retry for the PM leg. Returns OrderResult or None on error.
        Spends at most shares * pm_price_limit USD.
        """
        amount_usd = shares * pm_price_limit
        try:
            return self.pm_trader.place_fok_order(
                token_id=pm_token_id,
                amount_usd=amount_usd,
            )
        except Exception as e:
            print(f"[real-exec] retry_pm error: {e}")
            return None
