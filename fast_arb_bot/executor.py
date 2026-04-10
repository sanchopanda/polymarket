from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN

from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo

from real_arb_bot.clients import KalshiTrader, OrderResult, PolymarketTrader, _polymarket_fee
from real_arb_bot.db import RealArbDB


@dataclass
class FastExecutionResult:
    success: bool
    kalshi_order: OrderResult | None
    polymarket_order: OrderResult | None
    execution_status: str  # both_filled | one_legged_kalshi | one_legged_polymarket | failed
    reason: str = ""


_PM_PRICE_QUANT = Decimal("0.01")
_PM_SIZE_QUANT = Decimal("0.01")
_PM_NOTIONAL_QUANT = Decimal("0.01")


def _normalize_pm_buy_order(price: float, size: float) -> tuple[float, float]:
    """Приводит PM BUY order к точности, которую принимает CLOB.

    Для limit BUY Polymarket сервер сейчас ожидает:
    - size (taker amount): максимум 2 знака
    - notional = price * size (maker amount): максимум 2 знака

    Простого round(size, 2) недостаточно: при дробной цене произведение всё ещё
    может иметь лишние знаки и сервер вернёт 400 invalid amounts.
    """
    price_dec = Decimal(str(price)).quantize(_PM_PRICE_QUANT, rounding=ROUND_DOWN)
    size_dec = Decimal(str(size)).quantize(_PM_SIZE_QUANT, rounding=ROUND_DOWN)
    if price_dec <= 0 or size_dec <= 0:
        return 0.0, 0.0

    while size_dec > 0:
        notional = (price_dec * size_dec).quantize(_PM_NOTIONAL_QUANT, rounding=ROUND_DOWN)
        if price_dec * size_dec == notional:
            return float(price_dec), float(size_dec)
        size_dec = (size_dec - _PM_SIZE_QUANT).quantize(_PM_SIZE_QUANT, rounding=ROUND_DOWN)

    return float(price_dec), 0.0


class FastArbExecutor:
    """Параллельный исполнитель: оба ордера выставляются одновременно.

    Нет анвинда — одноногие позиции остаются как есть.
    PM ордер: limit buy (не FOK), конкретная цена и количество контрактов.
    """

    PM_LIMIT_WAIT_SECONDS = 0.5  # vs 1.5s в real_arb_bot

    def __init__(
        self,
        pm_trader: PolymarketTrader,
        kalshi_trader: KalshiTrader,
        db: RealArbDB,
        kalshi_slippage_cents: int = 1,
        pm_price_buffer: float = 0.02,
        completion_min_edge: float = 0.05,
    ) -> None:
        self.pm = pm_trader
        self.kalshi = kalshi_trader
        self.db = db
        self.kalshi_slippage_cents = kalshi_slippage_cents
        self.pm_price_buffer = pm_price_buffer
        self.completion_min_edge = completion_min_edge

    def execute_pair_parallel(
        self,
        opp: CrossVenueOpportunity,
        yes_leg: ExecutionLegInfo,
        no_leg: ExecutionLegInfo,
    ) -> FastExecutionResult:
        kalshi_leg = yes_leg if opp.buy_yes_venue == "kalshi" else no_leg
        pm_leg = yes_leg if opp.buy_yes_venue == "polymarket" else no_leg

        kalshi_side = "yes" if opp.buy_yes_venue == "kalshi" else "no"
        kalshi_ticker = opp.kalshi_market_id
        kalshi_price_cents = round(kalshi_leg.best_ask * 100) + self.kalshi_slippage_cents
        kalshi_price_cents = min(kalshi_price_cents, 99)
        kalshi_count = max(1, math.floor(kalshi_leg.requested_shares))

        pm_token_id = pm_leg.market_id
        pm_price = math.floor((pm_leg.best_ask + self.pm_price_buffer) * 100) / 100.0
        if pm_price <= 0:
            return FastExecutionResult(False, None, None, "failed", f"pm_price_non_positive ({pm_price:.4f})")
        pm_size = pm_leg.requested_shares
        if pm_size <= 0:
            return FastExecutionResult(False, None, None, "failed", "pm_size_zero")

        # Логируем намерение
        self.db.audit("order_attempt", None, {
            "route": "parallel",
            "kalshi_ticker": kalshi_ticker,
            "kalshi_side": kalshi_side,
            "kalshi_count": kalshi_count,
            "kalshi_price_cents": kalshi_price_cents,
            "pm_token_id": pm_token_id,
            "pm_price": pm_price,
            "pm_size": pm_size,
        })

        # Оба ордера — одновременно
        with ThreadPoolExecutor(max_workers=2) as pool:
            k_future = pool.submit(
                self._place_kalshi_order,
                kalshi_ticker, kalshi_side, kalshi_count, kalshi_price_cents,
            )
            p_future = pool.submit(
                self._place_pm_limit_order,
                pm_token_id, pm_price, pm_size,
            )
            try:
                k_result = k_future.result(timeout=20)
            except Exception as e:
                k_result = None
                self.db.audit("order_error", None, {"venue": "kalshi", "error": str(e)})
            try:
                p_result = p_future.result(timeout=20)
            except Exception as e:
                p_result = None
                self.db.audit("order_error", None, {"venue": "polymarket", "error": str(e)})

        # Limit FOK на PM — частичных заполнений не бывает, auto-cancel если не заполнился

        k_filled = k_result is not None and k_result.shares_matched > 0
        p_filled = p_result is not None and p_result.shares_matched > 0

        if k_filled and p_filled:
            # Если Kalshi рестингует — часть акций ещё не куплена, позиция несбалансирована.
            # Записываем как one_legged_polymarket чтобы sync loop отслеживал рестинг-ордер
            # и дозаполнил до both_filled когда Kalshi исполнится полностью.
            if k_result.status == "resting":
                k_fill_pct = k_result.shares_matched / kalshi_count if kalshi_count > 0 else 1.0
                self.db.audit("order_partial_resting", None, {
                    "venue": "kalshi",
                    "filled": k_result.shares_matched,
                    "requested": kalshi_count,
                    "fill_pct": round(k_fill_pct, 4),
                    "action": "downgrade_to_one_legged_polymarket",
                })
                print(
                    f"[executor] Kalshi partial resting: {k_result.shares_matched}/{kalshi_count} "
                    f"({k_fill_pct:.0%}) → one_legged_polymarket"
                )
                return FastExecutionResult(
                    False, k_result, p_result, "one_legged_polymarket",
                    f"kalshi_partial_resting: {k_result.shares_matched}/{kalshi_count} ({k_fill_pct:.0%})",
                )
            return FastExecutionResult(True, k_result, p_result, "both_filled")

        elif k_filled and not p_filled:
            # Kalshi заполнился, PM FOK не заполнился (уже авто-отменён).
            # Kalshi уже исполнен — отменить нельзя. Одноногая позиция.
            return FastExecutionResult(
                False, k_result, p_result, "one_legged_kalshi",
                f"pm_fok_not_filled: {p_result.status if p_result else 'exception'}",
            )

        elif p_filled and not k_filled:
            replaced = self._replace_kalshi_resting_for_completion(
                ticker=kalshi_ticker,
                side=kalshi_side,
                count=kalshi_count,
                pm_fill_price=p_result.fill_price,
                current_order=k_result,
            )
            replacement_filled = replaced is not None and replaced.shares_matched > 0
            if replacement_filled:
                return FastExecutionResult(
                    True, replaced, p_result, "both_filled",
                    f"kalshi_resting_replaced_and_filled: {replaced.status}",
                )
            return FastExecutionResult(
                False, replaced or k_result, p_result, "one_legged_polymarket",
                f"kalshi_resting_replaced: {(replaced.status if replaced else (k_result.status if k_result else 'exception'))}",
            )

        else:
            # Ни один не заполнился — отменяем рестинг Kalshi на всякий случай
            self._try_cancel_kalshi_resting(k_result)
            return FastExecutionResult(False, k_result, p_result, "failed", "neither_filled")

    def _try_cancel_kalshi_resting(self, k_result: OrderResult | None) -> bool:
        """Пробует отменить Kalshi ордер если он в статусе resting. Возвращает True если отменили."""
        if k_result is None or not k_result.order_id:
            return False
        if k_result.shares_matched > 0:
            return False  # уже заполнен — нечего отменять
        try:
            ok = self.kalshi.cancel_order(k_result.order_id)
            return ok
        except Exception as e:
            self.db.audit("order_error", None, {"venue": "kalshi", "error": f"cancel_error: {e}"})
            return False

    def _replace_kalshi_resting_for_completion(
        self,
        ticker: str,
        side: str,
        count: int,
        pm_fill_price: float,
        current_order: OrderResult | None,
    ) -> OrderResult | None:
        if count <= 0 or pm_fill_price <= 0:
            return current_order
        if current_order is not None and current_order.order_id:
            self._try_cancel_kalshi_resting(current_order)

        max_price = 1.0 - pm_fill_price - self.completion_min_edge
        price_cents = max(1, min(99, math.floor(max_price * 100)))
        if price_cents <= 0:
            return current_order
        replacement = self._place_kalshi_order(ticker, side, count, price_cents)
        if replacement is not None:
            return replacement
        return current_order

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
        if order.status == "resting":
            return order
        if order.shares_matched <= 0 or order.status.startswith("error"):
            return None
        return order

    def _place_pm_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
    ) -> OrderResult | None:
        """Лимитный FOK на Polymarket.

        OrderArgs (конкретная цена и количество) + OrderType.FOK:
        заполняется целиком по нашей цене или не заполняется вообще.
        FOK резолвится немедленно — sleep/poll минимальный (0.1s).
        """
        import time as _time
        from py_clob_client.clob_types import OrderArgs, OrderType

        rounded_price, size = _normalize_pm_buy_order(price, size)
        if rounded_price <= 0 or size <= 0:
            return None

        self.db.audit("order_attempt", None, {
            "venue": "polymarket",
            "token_id": token_id,
            "price": rounded_price,
            "size": size,
            "order_type": "limit_fok",
        })
        try:
            args = OrderArgs(token_id=token_id, price=rounded_price, size=size, side="BUY")
            t0 = _time.time()
            signed = self.pm._client.create_order(args)
            resp = self.pm._client.post_order(signed, orderType=OrderType.FOK)
            post_ms = (_time.time() - t0) * 1000

            # FOK резолвится на сервере мгновенно — короткий wait чтобы индекс успел обновиться
            _time.sleep(0.1)

            status = resp.get("status", "") if isinstance(resp, dict) else str(resp)
            order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
            shares_matched = 0.0
            fill_price = rounded_price
            fee = 0.0

            if order_id:
                try:
                    info = self.pm._client.get_order(order_id)
                    status = info.get("status", status)
                    shares_matched = float(info.get("size_matched", 0))
                    fill_price = float(info.get("price", rounded_price))
                    fee = _polymarket_fee(shares_matched, fill_price)
                except Exception as poll_err:
                    self.db.audit("order_error", None, {
                        "venue": "polymarket", "error": f"poll_error: {poll_err}",
                    })

            print(
                f"[pm-limit-fok] {post_ms:.0f}ms | "
                f"status={status} | req={size:.4f} | fill={shares_matched:.4f}@{fill_price:.4f} | "
                f"limit={rounded_price:.4f} | fee=${fee:.4f}"
            )
            order = OrderResult(
                order_id=order_id,
                status=status,
                fill_price=fill_price,
                shares_matched=shares_matched,
                shares_requested=size,
                fee=fee,
                latency_ms=round(post_ms, 1),
                raw_response=resp if isinstance(resp, dict) else {},
            )
        except Exception as e:
            print(f"[pm-limit-fok] EXCEPTION: {e}")
            self.db.audit("order_error", None, {"venue": "polymarket", "error": str(e)})
            return None

        self.db.audit("order_result", None, {
            "venue": "polymarket",
            "order_id": order.order_id,
            "status": order.status,
            "fill": order.shares_matched,
            "fill_price": order.fill_price,
            "fee": order.fee,
            "order_type": "limit_fok",
        })
        return order


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
