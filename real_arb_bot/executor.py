from __future__ import annotations

import math
from dataclasses import dataclass

from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo

from real_arb_bot.clients import KalshiTrader, OrderResult, PolymarketTrader
from real_arb_bot.db import RealArbDB
from real_arb_bot.safety import SafetyGuard


@dataclass
class ExecutionResult:
    success: bool
    kalshi_order: OrderResult | None
    polymarket_order: OrderResult | None
    execution_status: str  # both_filled | orphaned_kalshi | orphaned_polymarket | failed
    route: str
    reason: str = ""
    unwind_order: OrderResult | None = None
    realized_pnl: float | None = None


class OrderExecutor:
    PM_LIMIT_WAIT_SECONDS = 1.5
    PM_SHARE_EPSILON = 0.01

    def __init__(
        self,
        pm_trader: PolymarketTrader,
        kalshi_trader: KalshiTrader,
        safety: SafetyGuard,
        db: RealArbDB,
        first_leg: str = "kalshi",
    ) -> None:
        self.pm = pm_trader
        self.kalshi = kalshi_trader
        self.safety = safety
        self.db = db
        self.first_leg = first_leg if first_leg in {"kalshi", "polymarket"} else "kalshi"
        self.min_partial_fill_pct = safety.min_partial_fill_pct
        self.kalshi_slippage_cents = safety.kalshi_slippage_cents
        self.pm_price_buffer = safety.polymarket_price_buffer_cents / 100.0
        self.pm_first_kalshi_slippage_cents = 2
        self.pm_first_pm_price_buffer = 0.0

    def execute_pair(
        self,
        opp: CrossVenueOpportunity,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> ExecutionResult:
        kalshi_leg = yes_leg if opp.buy_yes_venue == "kalshi" else no_leg
        pm_leg = yes_leg if opp.buy_yes_venue == "polymarket" else no_leg

        if kalshi_leg is None or pm_leg is None:
            return self._failed("missing_leg_info")

        if self.first_leg == "polymarket":
            return self._execute_polymarket_first(opp, kalshi_leg, pm_leg)
        return self._execute_kalshi_first(opp, kalshi_leg, pm_leg)

    def _execute_kalshi_first(
        self,
        opp: CrossVenueOpportunity,
        kalshi_leg: ExecutionLegInfo,
        pm_leg: ExecutionLegInfo,
    ) -> ExecutionResult:
        kalshi_side = "yes" if opp.buy_yes_venue == "kalshi" else "no"
        kalshi_ticker = opp.kalshi_market_id
        kalshi_price_cents = round(kalshi_leg.best_ask * 100) + self.kalshi_slippage_cents
        kalshi_price_cents = min(kalshi_price_cents, 99)
        kalshi_count = max(1, math.floor(kalshi_leg.requested_shares))
        kalshi_balance_before = self._safe_kalshi_balance()

        edge_after_slippage = 1.0 - ((kalshi_price_cents / 100.0) + pm_leg.avg_price)
        if edge_after_slippage < self.safety.trading_min_lock_edge:
            return self._failed(f"edge_negative_after_slippage ({edge_after_slippage:.4f})")

        kalshi_order = self._place_kalshi_order(
            ticker=kalshi_ticker,
            side=kalshi_side,
            count=kalshi_count,
            price_cents=kalshi_price_cents,
        )
        if kalshi_order is None:
            return self._failed("kalshi_no_fill")

        pm_token_id = pm_leg.market_id
        max_pm_price = 1.0 - kalshi_order.fill_price - self.safety.trading_min_lock_edge
        if max_pm_price <= 0:
            return self._failed_with_orders(
                "failed",
                f"pm_price_cap_non_positive ({max_pm_price:.4f})",
                kalshi_order,
                None,
            )

        pm_limit_price = math.floor(min(max_pm_price, pm_leg.best_ask + self.pm_price_buffer) * 100) / 100.0
        pm_min_notional = kalshi_order.shares_matched * pm_limit_price
        if pm_min_notional < 1.0:
            unwind = self._try_unwind_kalshi(
                kalshi_ticker,
                kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            return self._unwind_result(
                unwind_status="unwound_kalshi",
                orphan_status="orphaned_kalshi",
                reason=f"pm_amount_too_small (${pm_min_notional:.2f})",
                kalshi_order=kalshi_order,
                polymarket_order=_empty_order("pm_amount_too_small"),
                unwind_order=unwind,
                realized_pnl=self._realized_kalshi_unwind_pnl(kalshi_balance_before) if unwind else None,
            )

        pm_order = self._place_pm_buy(
            token_id=pm_token_id,
            price=pm_limit_price,
            size=kalshi_order.shares_matched,
            kalshi_order=kalshi_order,
        )
        if pm_order is None:
            unwind = self._try_unwind_kalshi(
                kalshi_ticker,
                kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            return self._unwind_result(
                unwind_status="unwound_kalshi",
                orphan_status="orphaned_kalshi",
                reason="polymarket_exception",
                kalshi_order=kalshi_order,
                polymarket_order=_empty_order("pm_exception"),
                unwind_order=unwind,
                realized_pnl=self._realized_kalshi_unwind_pnl(kalshi_balance_before) if unwind else None,
            )

        pm_missing = kalshi_order.shares_matched - pm_order.shares_matched
        if pm_order.order_id and pm_missing > self.PM_SHARE_EPSILON:
            self.pm.cancel_order(pm_order.order_id)

        if pm_order.shares_matched <= 0:
            unwind = self._try_unwind_kalshi(
                kalshi_ticker,
                kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            return self._unwind_result(
                unwind_status="unwound_kalshi",
                orphan_status="orphaned_kalshi",
                reason=f"polymarket_not_filled: {pm_order.status}",
                kalshi_order=kalshi_order,
                polymarket_order=pm_order,
                unwind_order=unwind,
                realized_pnl=self._realized_kalshi_unwind_pnl(kalshi_balance_before) if unwind else None,
            )

        if pm_missing > self.PM_SHARE_EPSILON:
            unwind = self._try_unwind_kalshi(
                kalshi_ticker,
                kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            return self._unwind_result(
                unwind_status="unwound_kalshi",
                orphan_status="orphaned_kalshi",
                reason=(
                    f"polymarket_partial_fill: {pm_order.shares_matched:.4f}/"
                    f"{kalshi_order.shares_matched:.4f}"
                ),
                kalshi_order=kalshi_order,
                polymarket_order=pm_order,
                unwind_order=unwind,
                realized_pnl=self._realized_kalshi_unwind_pnl(kalshi_balance_before) if unwind else None,
            )

        return ExecutionResult(True, kalshi_order, pm_order, "both_filled", route="kalshi_first")

    def _execute_polymarket_first(
        self,
        opp: CrossVenueOpportunity,
        kalshi_leg: ExecutionLegInfo,
        pm_leg: ExecutionLegInfo,
    ) -> ExecutionResult:
        pm_token_id = pm_leg.market_id
        pm_limit_price = math.floor((pm_leg.best_ask + self.pm_first_pm_price_buffer) * 100) / 100.0
        if pm_limit_price <= 0:
            return self._failed(f"pm_price_non_positive ({pm_limit_price:.4f})")

        pm_size = pm_leg.requested_shares
        pm_notional = pm_leg.total_cost if pm_leg.total_cost > 0 else pm_size * pm_limit_price
        if pm_notional < 1.0:
            return self._failed(f"pm_amount_too_small (${pm_notional:.2f})")

        pm_order = self._place_pm_fok_buy(
            token_id=pm_token_id,
            amount_usd=pm_notional,
            kalshi_order=None,
        )
        if pm_order is None:
            return self._failed("polymarket_exception")

        if pm_order.shares_matched <= 0:
            return self._failed_with_orders(
                "failed",
                f"polymarket_not_filled: {pm_order.status}",
                None,
                pm_order,
            )

        kalshi_side = "yes" if opp.buy_yes_venue == "kalshi" else "no"
        kalshi_ticker = opp.kalshi_market_id
        max_kalshi_price = 1.0 - pm_order.fill_price - self.safety.trading_min_lock_edge
        if max_kalshi_price <= 0:
            unwind = self._try_unwind_polymarket(pm_token_id, pm_order)
            return self._unwind_result(
                unwind_status="unwound_polymarket",
                orphan_status="orphaned_polymarket",
                reason=f"kalshi_price_cap_non_positive ({max_kalshi_price:.4f})",
                kalshi_order=None,
                polymarket_order=pm_order,
                unwind_order=unwind,
            )

        kalshi_price_cents = round(
            min(
                max_kalshi_price,
                kalshi_leg.best_ask + self.pm_first_kalshi_slippage_cents / 100.0,
            ) * 100
        )
        kalshi_price_cents = min(max(kalshi_price_cents, 1), 99)
        kalshi_count = max(1, math.floor(pm_order.shares_matched))

        kalshi_order = self._place_kalshi_order(
            ticker=kalshi_ticker,
            side=kalshi_side,
            count=kalshi_count,
            price_cents=kalshi_price_cents,
        )
        if kalshi_order is None:
            unwind = self._try_unwind_polymarket(pm_token_id, pm_order)
            return self._unwind_result(
                unwind_status="unwound_polymarket",
                orphan_status="orphaned_polymarket",
                reason="kalshi_exception_or_no_fill",
                kalshi_order=None,
                polymarket_order=pm_order,
                unwind_order=unwind,
            )

        fill_pct = (kalshi_order.shares_matched / kalshi_order.shares_requested) * 100
        if fill_pct < self.min_partial_fill_pct:
            if kalshi_order.order_id:
                self.kalshi.cancel_order(kalshi_order.order_id)
            unwind = self._try_unwind_polymarket(pm_token_id, pm_order)
            return self._unwind_result(
                unwind_status="unwound_polymarket",
                orphan_status="orphaned_polymarket",
                reason=f"kalshi_partial_fill_too_small: {fill_pct:.1f}%",
                kalshi_order=kalshi_order,
                polymarket_order=pm_order,
                unwind_order=unwind,
            )

        kalshi_missing = pm_order.shares_matched - kalshi_order.shares_matched
        if kalshi_missing > self.PM_SHARE_EPSILON:
            if kalshi_order.order_id:
                self.kalshi.cancel_order(kalshi_order.order_id)
            unwind = self._try_unwind_polymarket(pm_token_id, pm_order)
            return self._unwind_result(
                unwind_status="unwound_polymarket",
                orphan_status="orphaned_polymarket",
                reason=(
                    f"kalshi_partial_fill: {kalshi_order.shares_matched:.4f}/"
                    f"{pm_order.shares_matched:.4f}"
                ),
                kalshi_order=kalshi_order,
                polymarket_order=pm_order,
                unwind_order=unwind,
            )

        return ExecutionResult(True, kalshi_order, pm_order, "both_filled", route="polymarket_first")

    def _place_kalshi_order(
        self,
        ticker: str,
        side: str,
        count: int,
        price_cents: int,
    ) -> OrderResult | None:
        self.db.audit("order_attempt", None, {
            "venue": "kalshi",
            "ticker": ticker,
            "side": side,
            "count": count,
            "price_cents": price_cents,
        })
        order = self.kalshi.place_limit_order(
            ticker=ticker,
            side=side,
            count=count,
            price_cents=price_cents,
        )
        self.db.audit("order_result", None, {
            "venue": "kalshi",
            "order_id": order.order_id,
            "status": order.status,
            "fill": order.shares_matched,
            "fee": order.fee,
        })
        if order.shares_matched <= 0 or order.status.startswith("error"):
            if order.order_id and order.status == "resting":
                self.kalshi.cancel_order(order.order_id)
            return None
        return order

    def _place_pm_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        kalshi_order: OrderResult | None,
    ) -> OrderResult | None:
        self.db.audit("order_attempt", None, {
            "venue": "polymarket",
            "token_id": token_id,
            "size": size,
            "limit_price": price,
            "kalshi_shares": kalshi_order.shares_matched if kalshi_order else None,
        })
        try:
            order = self.pm.place_limit_buy_order(
                token_id=token_id,
                price=price,
                size=size,
                wait_seconds=self.PM_LIMIT_WAIT_SECONDS,
            )
        except Exception as e:
            self.db.audit("order_error", None, {
                "venue": "polymarket",
                "error": str(e),
                "kalshi_order_id": kalshi_order.order_id if kalshi_order else None,
                "kalshi_fill": kalshi_order.shares_matched if kalshi_order else None,
            })
            return None
        self.db.audit("order_result", None, {
            "venue": "polymarket",
            "order_id": order.order_id,
            "status": order.status,
            "fill": order.shares_matched,
            "fill_price": order.fill_price,
            "fee": order.fee,
            "shares_requested": order.shares_requested,
        })
        return order

    def _place_pm_fok_buy(
        self,
        token_id: str,
        amount_usd: float,
        kalshi_order: OrderResult | None,
    ) -> OrderResult | None:
        self.db.audit("order_attempt", None, {
            "venue": "polymarket",
            "token_id": token_id,
            "amount_usd": amount_usd,
            "order_type": "fok_market_buy",
            "kalshi_shares": kalshi_order.shares_matched if kalshi_order else None,
        })
        try:
            order = self.pm.place_fok_order(
                token_id=token_id,
                amount_usd=amount_usd,
            )
        except Exception as e:
            self.db.audit("order_error", None, {
                "venue": "polymarket",
                "error": str(e),
                "order_type": "fok_market_buy",
                "kalshi_order_id": kalshi_order.order_id if kalshi_order else None,
                "kalshi_fill": kalshi_order.shares_matched if kalshi_order else None,
            })
            return None
        self.db.audit("order_result", None, {
            "venue": "polymarket",
            "order_id": order.order_id,
            "status": order.status,
            "fill": order.shares_matched,
            "fill_price": order.fill_price,
            "fee": order.fee,
            "shares_requested": order.shares_requested,
            "order_type": "fok_market_buy",
        })
        return order

    def _try_unwind_kalshi(
        self,
        ticker: str,
        side: str,
        count: int,
        buy_price_cents: int,
    ) -> OrderResult | None:
        sell_price_cents = 1
        try:
            sell_order = self.kalshi.place_limit_order(
                ticker=ticker,
                side=side,
                count=count,
                price_cents=sell_price_cents,
                action="sell",
            )
            self.db.audit("unwind_attempt", None, {
                "venue": "kalshi",
                "ticker": ticker,
                "side": side,
                "count": count,
                "buy_price_cents": buy_price_cents,
                "sell_price_cents": sell_price_cents,
                "order_id": sell_order.order_id,
                "status": sell_order.status,
                "fill": sell_order.shares_matched,
                "fill_price": sell_order.fill_price,
            })
            if sell_order.shares_matched > 0:
                if sell_order.shares_matched < count and sell_order.order_id:
                    self.kalshi.cancel_order(sell_order.order_id)
                return sell_order
            if sell_order.order_id:
                self.kalshi.cancel_order(sell_order.order_id)
            return None
        except Exception as e:
            self.db.audit("order_error", None, {"venue": "kalshi", "error": f"unwind_error: {e}"})
            return None

    def _try_unwind_polymarket(
        self,
        token_id: str,
        buy_order: OrderResult,
    ) -> OrderResult | None:
        sell_price = 0.01
        try:
            sell_order = self.pm.place_limit_sell_order(
                token_id=token_id,
                price=sell_price,
                size=buy_order.shares_matched,
                wait_seconds=self.PM_LIMIT_WAIT_SECONDS,
            )
            self.db.audit("unwind_attempt", None, {
                "venue": "polymarket",
                "token_id": token_id,
                "count": buy_order.shares_matched,
                "buy_price": buy_order.fill_price,
                "sell_price": sell_price,
                "order_id": sell_order.order_id,
                "status": sell_order.status,
                "fill": sell_order.shares_matched,
                "fill_price": sell_order.fill_price,
            })
            if sell_order.shares_matched > 0:
                missing = buy_order.shares_matched - sell_order.shares_matched
                if missing > self.PM_SHARE_EPSILON and sell_order.order_id:
                    self.pm.cancel_order(sell_order.order_id)
                return sell_order
            if sell_order.order_id:
                self.pm.cancel_order(sell_order.order_id)
            return None
        except Exception as e:
            self.db.audit("order_error", None, {"venue": "polymarket", "error": f"unwind_error: {e}"})
            return None

    def _safe_kalshi_balance(self) -> float | None:
        try:
            return self.kalshi.get_balance()
        except Exception:
            return None

    def _realized_kalshi_unwind_pnl(self, balance_before: float | None) -> float | None:
        if balance_before is None:
            return None
        balance_after = self._safe_kalshi_balance()
        if balance_after is None:
            return None
        return round(balance_after - balance_before, 6)

    def _failed(self, reason: str) -> ExecutionResult:
        return ExecutionResult(False, None, None, "failed", route=f"{self.first_leg}_first", reason=reason)

    def _failed_with_orders(
        self,
        status: str,
        reason: str,
        kalshi_order: OrderResult | None,
        polymarket_order: OrderResult | None,
    ) -> ExecutionResult:
        return ExecutionResult(
            False,
            kalshi_order,
            polymarket_order,
            status,
            route=f"{self.first_leg}_first",
            reason=reason,
        )

    def _unwind_result(
        self,
        unwind_status: str,
        orphan_status: str,
        reason: str,
        kalshi_order: OrderResult | None,
        polymarket_order: OrderResult | None,
        unwind_order: OrderResult | None,
        realized_pnl: float | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            success=False,
            kalshi_order=kalshi_order,
            polymarket_order=polymarket_order,
            execution_status=unwind_status if unwind_order else orphan_status,
            route=f"{self.first_leg}_first",
            reason=reason + (f" | {unwind_status}" if unwind_order else ""),
            unwind_order=unwind_order,
            realized_pnl=realized_pnl,
        )


def _empty_order(status: str) -> OrderResult:
    return OrderResult(
        order_id="",
        status=status,
        fill_price=0.0,
        shares_matched=0.0,
        shares_requested=0.0,
        fee=0.0,
        latency_ms=0.0,
        raw_response={},
    )
