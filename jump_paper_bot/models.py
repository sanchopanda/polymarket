from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class JumpPosition:
    id: str
    market_id: str
    symbol: str
    title: str
    interval_minutes: int
    side: str
    signal_bucket_seconds: int
    signal_level: float
    signal_price: float
    signal_avg_prev_10s: float
    limit_price: float
    entry_price: float
    filled_shares: float
    total_cost: float
    depth_usd: float
    opened_at: datetime
    market_end: datetime
    status: str
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
    telegram_message_id: Optional[int] = None


@dataclass
class JumpSignalRecord:
    id: int
    market_id: str
    symbol: str
    interval_minutes: int
    side: str
    signal_bucket_seconds: int
    signal_level: float
    signal_price: float
    signal_avg_prev_10s: float
    limit_price: float
    status: str
    skip_reason: Optional[str]
    created_at: datetime
    position_id: Optional[str] = None


@dataclass
class PricePoint:
    timestamp: datetime
    price: float


@dataclass
class TrackedSideState:
    prices: deque[PricePoint] = field(default_factory=deque)

