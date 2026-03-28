from __future__ import annotations

from datetime import datetime
from typing import Optional

from volatility_bot.models import VolatilityMarket


def compute_position_pct(now: datetime, market_start: datetime, interval_minutes: int) -> float:
    elapsed = (now - market_start).total_seconds()
    total = interval_minutes * 60.0
    return max(0.0, min(1.0, elapsed / total))


def compute_market_quarter(position_pct: float) -> int:
    return min(4, int(position_pct * 4) + 1)


def compute_market_minute(now: datetime, market_start: datetime) -> int:
    return max(0, int((now - market_start).total_seconds() // 60))


def evaluate_signal(
    market: VolatilityMarket,
    side: str,
    best_ask: float,
    now: datetime,
    buckets: list[dict],
) -> Optional[str]:
    """Return trigger_bucket name if a bet should be placed, else None.

    buckets: list of dicts with keys: name, lo, hi, timing
      timing: "first_three_quarters" | "last_quarter" | "any"
    """
    if best_ask <= 0:
        return None

    position_pct = compute_position_pct(now, market.market_start, market.interval_minutes)
    quarter = compute_market_quarter(position_pct)

    for bucket in buckets:
        lo = bucket["lo"]
        hi = bucket["hi"]
        timing = bucket["timing"]

        if not (lo <= best_ask <= hi):
            continue
        if timing == "first_three_quarters" and quarter >= 4:
            continue
        if timing == "last_quarter" and quarter < 4:
            continue
        return bucket["name"]

    return None
