from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from fast_arb_bot.executor import _normalize_pm_buy_order

if TYPE_CHECKING:
    from real_arb_bot.clients import KalshiTrader, OrderResult, PolymarketTrader


@dataclass
class RealExecResult:
    status: str  # "both_filled" | "one_legged_kalshi" | "one_legged_polymarket" | "failed"
    # Kalshi leg
    ka_order_id: str = ""
    ka_fill_price: float = 0.0
    ka_fill_shares: float = 0.0
    ka_status: str = ""   # "filled" | "resting" | "failed" | "error"
    # PM leg
    pm_order_id: str = ""
    pm_fill_price: float = 0.0
    pm_fill_shares: float = 0.0
    pm_status: str = ""   # "filled" | "not_filled" | "failed"


class SportsRealExecutor:
    """
    Параллельный исполнитель арбитража.
    Kalshi + PM-ордера выставляются одновременно. Нет анвинда:
    - Kalshi заполнен, PM нет  → one_legged_kalshi (retry PM при WS-обновлении)
    - PM заполнен, Kalshi resting → one_legged_polymarket (polling Kalshi каждые N сек)
    """

    def __init__(
        self,
        pm_trader: "PolymarketTrader",
        kalshi_trader: "KalshiTrader",
        slippage_cents: int = 2,
        pm_buffer: float = 0.02,
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
        Оба ордера параллельно.
        Ka: limit YES buy at ka_ask + slippage (остаётся живым если resting).
        PM: limit FOK at pm_ask + buffer (заполняется или нет немедленно).
        """
        ka_price_cents = min(int(ka_ask * 100) + self.slippage_cents, 99)
        pm_price = pm_ask + self.pm_buffer
        pm_price_norm, pm_size_norm = _normalize_pm_buy_order(pm_price, float(shares))

        with ThreadPoolExecutor(max_workers=2) as pool:
            ka_future = pool.submit(
                self._place_kalshi_order,
                ka_ticker, shares, ka_price_cents,
            )
            pm_future = pool.submit(
                self._place_pm_limit_fok,
                pm_token_id, pm_price_norm, pm_size_norm,
            )
            try:
                ka_result = ka_future.result(timeout=20)
            except Exception as e:
                print(f"[real-exec] Kalshi future error: {e}")
                ka_result = None
            try:
                pm_result = pm_future.result(timeout=20)
            except Exception as e:
                print(f"[real-exec] PM future error: {e}")
                pm_result = None

        ka_filled = ka_result is not None and ka_result.shares_matched >= shares * self.min_fill_pct
        ka_resting = ka_result is not None and ka_result.status == "resting" and ka_result.order_id
        pm_filled = pm_result is not None and pm_result.shares_matched >= shares * self.min_fill_pct

        if ka_filled and pm_filled:
            return RealExecResult(
                status="both_filled",
                ka_order_id=ka_result.order_id,
                ka_fill_price=ka_result.fill_price,
                ka_fill_shares=ka_result.shares_matched,
                ka_status="filled",
                pm_order_id=pm_result.order_id,
                pm_fill_price=pm_result.fill_price,
                pm_fill_shares=pm_result.shares_matched,
                pm_status="filled",
            )

        elif ka_filled and not pm_filled:
            # Kalshi заполнен, PM FOK не заполнился — retry PM по WS
            return RealExecResult(
                status="one_legged_kalshi",
                ka_order_id=ka_result.order_id,
                ka_fill_price=ka_result.fill_price,
                ka_fill_shares=ka_result.shares_matched,
                ka_status="filled",
                pm_order_id=pm_result.order_id if pm_result else "",
                pm_status="not_filled",
            )

        elif pm_filled and (ka_resting or ka_filled):
            # PM заполнен, Kalshi в рестинге — ждём пока заполнится
            return RealExecResult(
                status="one_legged_polymarket",
                ka_order_id=ka_result.order_id if ka_result else "",
                ka_fill_shares=ka_result.shares_matched if ka_result else 0.0,
                ka_status="resting" if ka_resting else "filled",
                pm_order_id=pm_result.order_id,
                pm_fill_price=pm_result.fill_price,
                pm_fill_shares=pm_result.shares_matched,
                pm_status="filled",
            )

        elif pm_filled and not ka_filled and not ka_resting:
            # PM заполнен, Kalshi полностью не удался
            return RealExecResult(
                status="one_legged_polymarket",
                ka_order_id=ka_result.order_id if ka_result else "",
                ka_status="failed",
                pm_order_id=pm_result.order_id,
                pm_fill_price=pm_result.fill_price,
                pm_fill_shares=pm_result.shares_matched,
                pm_status="filled",
            )

        else:
            # Ни один не заполнился — отменяем Kalshi resting если есть
            if ka_resting:
                self.kalshi_trader.cancel_order(ka_result.order_id)
            return RealExecResult(
                status="failed",
                ka_status="failed" if ka_result is None else ka_result.status,
                pm_status="failed" if pm_result is None else pm_result.status,
            )

    def retry_pm(
        self,
        pm_token_id: str,
        shares: int,
        pm_price_limit: float,
    ) -> "Optional[OrderResult]":
        """Одна попытка limit FOK на PM по цене pm_price_limit."""
        price_norm, size_norm = _normalize_pm_buy_order(pm_price_limit, float(shares))
        if price_norm <= 0 or size_norm <= 0:
            return None
        return self._place_pm_limit_fok(pm_token_id, price_norm, size_norm)

    def check_kalshi_order(self, order_id: str) -> "Optional[OrderResult]":
        """Проверяет статус Kalshi ордера через REST."""
        try:
            return self.kalshi_trader.get_order(order_id)
        except Exception as e:
            print(f"[real-exec] get_order error: {e}")
            return None

    # ── Internal helpers ───────────────────────────────────────────────

    def _place_kalshi_order(
        self,
        ticker: str,
        count: int,
        price_cents: int,
    ) -> "Optional[OrderResult]":
        try:
            order = self.kalshi_trader.place_limit_order(
                ticker=ticker,
                side="yes",
                count=count,
                price_cents=price_cents,
                action="buy",
            )
            # Kalshi: resting order is valid (оставляем живым)
            if order.status == "resting":
                print(
                    f"[real-exec] Kalshi order RESTING {order.order_id[:16]}... "
                    f"(waiting for fill)"
                )
                return order
            if order.status.startswith("error"):
                print(f"[real-exec] Kalshi order error: {order.status}")
                return None
            return order
        except Exception as e:
            print(f"[real-exec] Kalshi order exception: {e}")
            return None

    def _place_pm_limit_fok(
        self,
        token_id: str,
        price: float,
        size: float,
    ) -> "Optional[OrderResult]":
        """Limit FOK на PM: OrderArgs + OrderType.FOK. Заполняется или нет, без остатка."""
        from py_clob_client.clob_types import OrderArgs, OrderType
        from real_arb_bot.clients import OrderResult, _polymarket_fee

        if price <= 0 or size <= 0:
            return None
        try:
            args = OrderArgs(token_id=token_id, price=price, size=size, side="BUY")
            t0 = time.time()
            signed = self.pm_trader._client.create_order(args)
            resp = self.pm_trader._client.post_order(signed, orderType=OrderType.FOK)
            latency_ms = (time.time() - t0) * 1000

            # FOK резолвится мгновенно — короткий poll
            time.sleep(0.1)

            status = resp.get("status", "") if isinstance(resp, dict) else str(resp)
            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            shares_matched = 0.0
            fill_price = price
            fee = 0.0

            if order_id:
                try:
                    info = self.pm_trader._client.get_order(order_id)
                    status = info.get("status", status)
                    shares_matched = float(info.get("size_matched", 0))
                    fill_price = float(info.get("price", price))
                    fee = _polymarket_fee(shares_matched, fill_price)
                except Exception as poll_err:
                    print(f"[real-exec] PM order poll error: {poll_err}")

            print(
                f"[real-exec] PM limit-FOK {latency_ms:.0f}ms | "
                f"status={status} | fill={shares_matched:.4f}@{fill_price:.4f}"
            )
            return OrderResult(
                order_id=order_id,
                status=status,
                fill_price=fill_price,
                shares_matched=shares_matched,
                shares_requested=size,
                fee=fee,
                latency_ms=round(latency_ms, 1),
                raw_response=resp if isinstance(resp, dict) else {},
            )
        except Exception as e:
            print(f"[real-exec] PM order exception: {e}")
            return None
