from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PriceTick:
    timestamp: float
    price: float


@dataclass
class SpikeSignal:
    leader_venue: str       # "polymarket" or "kalshi"
    follower_venue: str
    pair_key: str
    symbol: str
    side: str               # "yes" or "no"
    leader_price: float
    follower_price: float
    spike_magnitude: float  # cents
    price_gap: float        # leader_price - follower_price
    detected_at: float
    matched_pair: object    # MatchedMarketPair, avoid circular import


@dataclass
class MomentumPosition:
    id: str
    pair_key: str
    symbol: str
    title: str
    expiry: datetime
    side: str               # "yes" or "no"
    bet_venue: str          # follower
    leader_venue: str
    entry_price: float
    leader_price_at_entry: float
    shares: float
    total_cost: float
    spike_magnitude: float
    opened_at: datetime
    status: str = "open"
    resolved_at: Optional[datetime] = None
    outcome: Optional[str] = None
    pnl: Optional[float] = None
