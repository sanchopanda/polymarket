from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from volatility_bot.strategy import compute_position_pct, compute_market_minute

from oracle_arb_bot.models import OracleMarket


@dataclass
class SignalResult:
    should_bet: bool
    side: str           # "yes" | "no" | ""
    delta_pct: float
    position_pct: float
    market_minute: int
    reason: str         # для логов


def evaluate_oracle_signal(
    market: OracleMarket,
    current_price: float,
    now: datetime,
    delta_threshold_pct: float,
    max_entry_price: float,
    last_bet_side: Optional[str] = None,
) -> SignalResult:
    """
    Crossing-based signal: ставим при каждом пересечении pm_open_price.

    - delta_pct = (current_price - pm_open_price) / pm_open_price * 100
    - Если delta > +threshold и last_bet_side != "yes" → YES
    - Если delta < -threshold и last_bet_side != "no" → NO
    - Проверяем что PM цена нужной стороны < max_entry_price
    - current_price — Chainlink или Binance в зависимости от price_source конфига
    """
    market_minute = compute_market_minute(now, market.market_start)
    position_pct = compute_position_pct(now, market.market_start, market.interval_minutes)

    if not market.pm_open_price:
        return SignalResult(False, "", 0.0, position_pct, market_minute, "no_open_price")

    delta_pct = (current_price - market.pm_open_price) / market.pm_open_price * 100

    # Binance выше pm_open_price → YES
    if delta_pct > delta_threshold_pct and 0 < market.yes_ask < max_entry_price:
        if last_bet_side == "yes":
            return SignalResult(False, "", delta_pct, position_pct, market_minute, "same_side")
        return SignalResult(True, "yes", delta_pct, position_pct, market_minute, "crossing_yes")

    # Binance ниже pm_open_price → NO
    if delta_pct < -delta_threshold_pct and 0 < market.no_ask < max_entry_price:
        if last_bet_side == "no":
            return SignalResult(False, "", delta_pct, position_pct, market_minute, "same_side")
        return SignalResult(True, "no", delta_pct, position_pct, market_minute, "crossing_no")

    if abs(delta_pct) > delta_threshold_pct:
        return SignalResult(False, "", delta_pct, position_pct, market_minute, "price_not_stale")

    return SignalResult(False, "", delta_pct, position_pct, market_minute, "threshold_not_met")


def evaluate_cl_contradiction_signal(
    market: OracleMarket,
    cl_price: float,
    cl_prev_price: Optional[float],
    binance_price: Optional[float],
    now: datetime,
    last_bet_side: Optional[str] = None,
    min_cl_delta_pct: float = 0.0,
) -> SignalResult:
    """
    Сигнал "Binance противоречит CL тику":
      - CL тикнул вниз (cl_price < cl_prev_price) И Binance > CL → покупаем NO
      - CL тикнул вверх (cl_price > cl_prev_price) И Binance < CL → покупаем YES

    min_cl_delta_pct: минимальный |Δ%| CL тика (фильтр шумовых тиков).
    Нет ограничения по max_entry_price — проверка ликвидности снаружи.
    """
    market_minute = compute_market_minute(now, market.market_start)
    position_pct = compute_position_pct(now, market.market_start, market.interval_minutes)

    if cl_prev_price is None:
        return SignalResult(False, "", 0.0, position_pct, market_minute, "no_prev_price")
    if binance_price is None:
        return SignalResult(False, "", 0.0, position_pct, market_minute, "no_binance_price")

    cl_delta_pct = (cl_price - cl_prev_price) / cl_prev_price * 100

    if min_cl_delta_pct > 0 and abs(cl_delta_pct) < min_cl_delta_pct:
        return SignalResult(False, "", cl_delta_pct, position_pct, market_minute, "delta_too_small")

    # CL тикнул вниз, Binance выше CL → покупаем NO
    if cl_price < cl_prev_price and binance_price > cl_price:
        if last_bet_side == "no":
            return SignalResult(False, "no", cl_delta_pct, position_pct, market_minute, "same_side")
        return SignalResult(True, "no", cl_delta_pct, position_pct, market_minute, "cl_contradiction_no")

    # CL тикнул вверх, Binance ниже CL → покупаем YES
    if cl_price > cl_prev_price and binance_price < cl_price:
        if last_bet_side == "yes":
            return SignalResult(False, "yes", cl_delta_pct, position_pct, market_minute, "same_side")
        return SignalResult(True, "yes", cl_delta_pct, position_pct, market_minute, "cl_contradiction_yes")

    return SignalResult(False, "", cl_delta_pct, position_pct, market_minute, "no_contradiction")
