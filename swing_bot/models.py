from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class SwingState(str, Enum):
    WATCHING = "watching"
    PENDING_ENTRY = "pending_entry"
    HOLDING = "holding"
    PENDING_SELL = "pending_sell"
    PENDING_ARB = "pending_arb"
    PENDING_FLIP = "pending_flip"
    SOLD = "sold"
    ARBED = "arbed"
    FLIPPED = "flipped"
    RESOLVED = "resolved"


@dataclass
class SwingPosition:
    id: str
    market_id: str
    symbol: str
    interval_minutes: int
    market_start: datetime
    market_end: datetime
    yes_token_id: str
    no_token_id: str
    state: SwingState = SwingState.WATCHING
    # entry
    entry_side: str = "yes"                  # yes=Up, no=Down
    entry_price: Optional[float] = None       # WS price at signal
    entry_price_rest: Optional[float] = None  # REST-verified price (1s)
    stake_usd: float = 0.0
    shares: float = 0.0
    opened_at: Optional[datetime] = None
    # exit
    exit_type: Optional[str] = None           # sell / arb / flip
    exit_price: Optional[float] = None        # WS price at exit signal
    exit_price_rest: Optional[float] = None   # REST price at exit (1s)
    exited_at: Optional[datetime] = None
    hold_reason: Optional[str] = None
    # flip
    flip_shares: Optional[float] = None
    # resolution
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
    resolved_at: Optional[datetime] = None
