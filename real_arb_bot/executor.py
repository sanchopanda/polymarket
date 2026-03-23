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
    execution_status: str  # both_filled | orphaned_kalshi | failed
    reason: str = ""
    unwind_order: OrderResult | None = None  # заполняется при unwound_kalshi


class OrderExecutor:
    def __init__(
        self,
        pm_trader: PolymarketTrader,
        kalshi_trader: KalshiTrader,
        safety: SafetyGuard,
        db: RealArbDB,
    ) -> None:
        self.pm = pm_trader
        self.kalshi = kalshi_trader
        self.safety = safety
        self.db = db
        self.min_partial_fill_pct = safety.min_partial_fill_pct
        self.kalshi_slippage_cents = safety.kalshi_slippage_cents

    def execute_pair(
        self,
        opp: CrossVenueOpportunity,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> ExecutionResult:
        """
        Размещаем две ноги арбитражной пары.

        Порядок:
        1. Kalshi первым (limit order — контроль цены, чёткий ответ)
        2. Polymarket вторым (FOK — всё или ничего)

        Если Kalshi наполнился, а Polymarket нет → orphaned_kalshi (нужен ручной разбор).
        """
        kalshi_leg = yes_leg if opp.buy_yes_venue == "kalshi" else no_leg
        pm_leg = yes_leg if opp.buy_yes_venue == "polymarket" else no_leg

        if kalshi_leg is None or pm_leg is None:
            return ExecutionResult(
                success=False,
                kalshi_order=None,
                polymarket_order=None,
                execution_status="failed",
                reason="missing_leg_info",
            )

        # ── Шаг 1: Kalshi ───────────────────────────────────────────────
        kalshi_side = "yes" if opp.buy_yes_venue == "kalshi" else "no"
        kalshi_ticker = opp.kalshi_market_id
        kalshi_price_cents = round(kalshi_leg.best_ask * 100) + self.kalshi_slippage_cents
        kalshi_price_cents = min(kalshi_price_cents, 99)  # не больше $0.99
        kalshi_count = max(1, math.floor(kalshi_leg.requested_shares))

        # Проверяем что edge всё ещё положительный с учётом slippage
        kalshi_price_with_slippage = kalshi_price_cents / 100.0
        pm_price = pm_leg.avg_price
        edge_after_slippage = 1.0 - (kalshi_price_with_slippage + pm_price)
        if edge_after_slippage < self.safety.trading_min_lock_edge:
            return ExecutionResult(
                success=False,
                kalshi_order=None,
                polymarket_order=None,
                execution_status="failed",
                reason=f"edge_negative_after_slippage ({edge_after_slippage:.4f})",
            )

        self.db.audit("order_attempt", None, {
            "venue": "kalshi",
            "ticker": kalshi_ticker,
            "side": kalshi_side,
            "count": kalshi_count,
            "price_cents": kalshi_price_cents,
        })

        kalshi_order = self.kalshi.place_limit_order(
            ticker=kalshi_ticker,
            side=kalshi_side,
            count=kalshi_count,
            price_cents=kalshi_price_cents,
        )

        self.db.audit("order_result", None, {
            "venue": "kalshi",
            "order_id": kalshi_order.order_id,
            "status": kalshi_order.status,
            "fill": kalshi_order.shares_matched,
            "fee": kalshi_order.fee,
        })

        # Проверяем Kalshi fill
        if kalshi_order.shares_matched <= 0 or kalshi_order.status.startswith("error"):
            # Отменяем resting ордер чтобы не висел в стакане
            if kalshi_order.order_id and kalshi_order.status == "resting":
                self.kalshi.cancel_order(kalshi_order.order_id)
            return ExecutionResult(
                success=False,
                kalshi_order=kalshi_order,
                polymarket_order=None,
                execution_status="failed",
                reason=f"kalshi_no_fill: {kalshi_order.status}",
            )

        fill_pct = (kalshi_order.shares_matched / kalshi_order.shares_requested) * 100
        if fill_pct < self.min_partial_fill_pct:
            if kalshi_order.order_id:
                self.kalshi.cancel_order(kalshi_order.order_id)
            return ExecutionResult(
                success=False,
                kalshi_order=kalshi_order,
                polymarket_order=None,
                execution_status="failed",
                reason=f"kalshi_partial_fill_too_small: {fill_pct:.1f}%",
            )

        # ── Шаг 2: Polymarket ───────────────────────────────────────────
        pm_side = "yes" if opp.buy_yes_venue == "polymarket" else "no"
        pm_token_id = (
            opp.polymarket_market_id  # рынок, но нам нужен token_id
        )
        # token_id хранится в ExecutionLegInfo через matched.polymarket.yes/no_token_id
        # передаём через leg.market_id который == token_id в нашем случае
        pm_token_id = pm_leg.market_id

        # PM нога должна купить ровно столько контрактов, сколько заполнил Kalshi
        # (не масштабировать по долларам — разные цены дают разные кол-ва контрактов)
        pm_amount_usd = kalshi_order.shares_matched * pm_leg.best_ask

        # Polymarket min order size = $1
        if pm_amount_usd < 1.0:
            print(f"[executor] PM amount ${pm_amount_usd:.2f} < $1 min → unwind Kalshi")
            unwind = self._try_unwind_kalshi(
                kalshi_ticker, kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            empty = OrderResult(
                order_id="", status="pm_amount_too_small", fill_price=0.0,
                shares_matched=0.0, shares_requested=0.0, fee=0.0,
                latency_ms=0.0, raw_response={},
            )
            return ExecutionResult(
                success=False,
                kalshi_order=kalshi_order,
                polymarket_order=empty,
                execution_status="unwound_kalshi" if unwind else "orphaned_kalshi",
                reason=f"pm_amount_too_small (${pm_amount_usd:.2f})",
                unwind_order=unwind,
            )

        self.db.audit("order_attempt", None, {
            "venue": "polymarket",
            "token_id": pm_token_id,
            "amount_usd": pm_amount_usd,
            "kalshi_shares": kalshi_order.shares_matched,
        })

        try:
            pm_order = self.pm.place_fok_order(
                token_id=pm_token_id,
                amount_usd=pm_amount_usd,
            )
        except Exception as e:
            print(
                f"[executor] КРИТ: Kalshi заполнен, Polymarket EXCEPTION!\n"
                f"           kalshi: {kalshi_order.shares_matched} контрактов @ {kalshi_order.fill_price}\n"
                f"           error: {e}"
            )
            self.db.audit("order_error", None, {
                "venue": "polymarket", "error": str(e),
                "kalshi_order_id": kalshi_order.order_id,
                "kalshi_fill": kalshi_order.shares_matched,
            })
            # Пробуем сразу продать Kalshi контракты
            unwind = self._try_unwind_kalshi(
                kalshi_ticker, kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            empty = OrderResult(
                order_id="", status=f"exception: {e}", fill_price=0.0,
                shares_matched=0.0, shares_requested=0.0, fee=0.0,
                latency_ms=0.0, raw_response={},
            )
            return ExecutionResult(
                success=False,
                kalshi_order=kalshi_order,
                polymarket_order=empty,
                execution_status="unwound_kalshi" if unwind else "orphaned_kalshi",
                reason=f"polymarket_exception: {e}" + (f" | kalshi_unwound" if unwind else ""),
                unwind_order=unwind,
            )

        self.db.audit("order_result", None, {
            "venue": "polymarket",
            "order_id": pm_order.order_id,
            "status": pm_order.status,
            "fill": pm_order.shares_matched,
            "fill_price": pm_order.fill_price,
            "fee": pm_order.fee,
        })

        if pm_order.shares_matched <= 0 or pm_order.status not in {"matched", "MATCHED"}:
            print(
                f"[executor] ВНИМАНИЕ: Kalshi заполнен, Polymarket не исполнился!\n"
                f"           kalshi: {kalshi_order.shares_matched} контрактов @ {kalshi_order.fill_price}\n"
                f"           polymarket: status={pm_order.status}"
            )
            # Пробуем сразу продать Kalshi контракты
            unwind = self._try_unwind_kalshi(
                kalshi_ticker, kalshi_side,
                int(kalshi_order.shares_matched),
                kalshi_price_cents,
            )
            return ExecutionResult(
                success=False,
                kalshi_order=kalshi_order,
                polymarket_order=pm_order,
                execution_status="unwound_kalshi" if unwind else "orphaned_kalshi",
                reason=f"polymarket_not_filled: {pm_order.status}" + (f" | kalshi_unwound" if unwind else ""),
                unwind_order=unwind,
            )

        return ExecutionResult(
            success=True,
            kalshi_order=kalshi_order,
            polymarket_order=pm_order,
            execution_status="both_filled",
        )

    def _try_unwind_kalshi(
        self, ticker: str, side: str, count: int, buy_price_cents: int,
    ) -> OrderResult | None:
        """Пробуем продать Kalshi контракты сразу после неудачи PM.

        Ставим sell limit @ 1¢ — Kalshi заполнит по лучшему bid (price improvement).
        Это фактически market sell. Потеря = спред, но лучше чем orphaned.
        """
        sell_price_cents = 1  # минимальная цена — заполнится по лучшему bid
        print(
            f"[executor] Попытка unwind Kalshi: sell {count}x {side} @ {sell_price_cents}¢ (market sell)"
        )
        try:
            sell_order = self.kalshi.place_limit_order(
                ticker=ticker,
                side=side,
                count=count,
                price_cents=sell_price_cents,
                action="sell",
            )
            self.db.audit("unwind_attempt", None, {
                "ticker": ticker, "side": side, "count": count,
                "buy_price_cents": buy_price_cents,
                "sell_price_cents": sell_price_cents,
                "order_id": sell_order.order_id,
                "status": sell_order.status,
                "fill": sell_order.shares_matched,
                "fill_price": sell_order.fill_price,
            })
            if sell_order.shares_matched > 0:
                actual_sell_cents = round(sell_order.fill_price * 100)
                loss = count * (buy_price_cents - actual_sell_cents) / 100.0 + sell_order.fee
                print(f"[executor] Unwind OK: продано {sell_order.shares_matched}x @ {actual_sell_cents}¢ | потеря ~${loss:.2f}")
                # Отменяем остаток если частично заполнился
                if sell_order.shares_matched < count and sell_order.order_id:
                    self.kalshi.cancel_order(sell_order.order_id)
                return sell_order
            else:
                if sell_order.order_id:
                    self.kalshi.cancel_order(sell_order.order_id)
                print(f"[executor] Unwind FAILED: нет bids в книге, orphaned позиция остаётся")
                return None
        except Exception as e:
            print(f"[executor] Unwind ERROR: {e}")
            return None
