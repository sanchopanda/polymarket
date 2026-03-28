from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class VolatilityMarket:
    venue: str                       # "polymarket" | "kalshi"
    market_id: str                   # PM gamma ID or Kalshi ticker
    title: str
    symbol: str                      # "BTC", "ETH", ...
    interval_minutes: int            # 5, 15, or 60
    expiry: datetime                 # market window end (UTC, naive)
    market_start: datetime           # expiry - timedelta(minutes=interval_minutes)
    volume: float
    yes_ask: float                   # top-of-book ask for YES
    no_ask: float                    # top-of-book ask for NO
    yes_token_id: Optional[str] = None   # Polymarket only
    no_token_id: Optional[str] = None    # Polymarket only
    subscribed_at: Optional[datetime] = None


@dataclass
class Bet:
    id: str
    venue: str
    market_id: str
    symbol: str
    interval_minutes: int
    market_start: datetime
    market_end: datetime
    opened_at: datetime
    market_minute: int               # 0-based minute within the interval window
    market_quarter: int              # 1, 2, 3, or 4
    position_pct: float              # 0.0–1.0 fraction through market window
    side: str                        # "yes" | "no"
    entry_price: float
    trigger_bucket: str              # "0-0.1" | "0.2-0.4" | "0.85-0.95"
    shares: float
    total_cost: float
    order_id: str = ""
    order_status: str = ""
    order_fill_price: float = 0.0
    order_fee: float = 0.0
    order_latency_ms: float = 0.0
    status: str = "open"             # "open" | "resolved" | "paper"
    resolved_at: Optional[datetime] = None
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
    is_paper: int = 0
    is_legacy: int = 0
    legacy_source: Optional[str] = None
    legacy_pair_key: Optional[str] = None
