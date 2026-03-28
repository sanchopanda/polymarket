from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional

from volatility_bot.models import VolatilityMarket


@dataclass
class BetResult:
    success: bool
    order_id: str
    order_status: str
    fill_price: float
    shares: float
    fee: float
    latency_ms: float
    is_paper: bool
    error: str = ""


class BetExecutor:
    def __init__(
        self,
        pm_trader=None,       # PolymarketTrader | None
        kalshi_trader=None,   # KalshiTrader | None
        stake_usd: float = 25.0,
        dry_run: bool = False,
    ) -> None:
        self._pm = pm_trader
        self._kalshi = kalshi_trader
        self._stake_usd = stake_usd
        self._dry_run = dry_run

    def place_bet(
        self,
        market: VolatilityMarket,
        side: str,
        best_ask: float,
        stake_usd: Optional[float] = None,
    ) -> BetResult:
        usd = stake_usd if stake_usd is not None else self._stake_usd
        if self._dry_run:
            return self._paper_result(best_ask, usd)

        if market.venue == "polymarket":
            return self._place_pm(market, side, usd)
        return self._place_kalshi(market, side, usd, best_ask)

    def _place_pm(self, market: VolatilityMarket, side: str, stake_usd: float) -> BetResult:
        token_id = market.yes_token_id if side == "yes" else market.no_token_id
        if not token_id:
            return BetResult(
                success=False, order_id="", order_status="error",
                fill_price=0.0, shares=0.0, fee=0.0, latency_ms=0.0,
                is_paper=False, error="missing token_id",
            )
        try:
            t0 = time.time()
            result = self._pm.place_fok_order(token_id, stake_usd)
            latency_ms = (time.time() - t0) * 1000
            success = result.shares_matched > 0
            return BetResult(
                success=success,
                order_id=result.order_id,
                order_status=result.status,
                fill_price=result.fill_price,
                shares=result.shares_matched,
                fee=result.fee,
                latency_ms=result.latency_ms or latency_ms,
                is_paper=False,
            )
        except Exception as exc:
            print(f"[executor] PM order error: {exc}")
            return BetResult(
                success=False, order_id="", order_status="error",
                fill_price=0.0, shares=0.0, fee=0.0, latency_ms=0.0,
                is_paper=False, error=str(exc),
            )

    def _place_kalshi(
        self, market: VolatilityMarket, side: str, stake_usd: float, best_ask: float
    ) -> BetResult:
        count = max(1, math.floor(stake_usd / best_ask))
        price_cents = min(99, round(best_ask * 100) + 1)
        try:
            t0 = time.time()
            result = self._kalshi.place_limit_order(
                ticker=market.market_id,
                side=side,
                count=count,
                price_cents=price_cents,
            )
            latency_ms = (time.time() - t0) * 1000
            success = result.shares_matched > 0
            return BetResult(
                success=success,
                order_id=result.order_id,
                order_status=result.status,
                fill_price=result.fill_price,
                shares=result.shares_matched,
                fee=result.fee,
                latency_ms=result.latency_ms or latency_ms,
                is_paper=False,
            )
        except Exception as exc:
            print(f"[executor] Kalshi order error: {exc}")
            return BetResult(
                success=False, order_id="", order_status="error",
                fill_price=0.0, shares=0.0, fee=0.0, latency_ms=0.0,
                is_paper=False, error=str(exc),
            )

    def _paper_result(self, best_ask: float, stake_usd: float) -> BetResult:
        shares = stake_usd / best_ask if best_ask > 0 else 0.0
        return BetResult(
            success=True,
            order_id="",
            order_status="paper",
            fill_price=best_ask,
            shares=shares,
            fee=0.0,
            latency_ms=0.0,
            is_paper=True,
        )
