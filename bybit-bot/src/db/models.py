from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class Series:
    """Серия Мартингейла."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "active"       # active | won | lost | abandoned
    symbol: str = "XRPUSDT"
    current_depth: int = 0
    initial_margin: float = 0.10
    total_invested: float = 0.0
    total_pnl: float = 0.0
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None


@dataclass
class Trade:
    """Одна сделка внутри серии."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    series_id: str = ""
    series_depth: int = 0
    symbol: str = "XRPUSDT"
    side: str = "Buy"            # Buy | Sell
    order_id: str = ""
    margin_usdt: float = 0.0
    qty: float = 0.0
    entry_price: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0
    status: str = "open"         # open | won | lost
    exit_price: float = 0.0
    pnl: float = 0.0
    opened_at: datetime = field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
