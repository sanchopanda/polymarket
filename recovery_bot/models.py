from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RecoveryConfig:
    name: str
    interval_minutes: int
    bottom_price: float
    entry_price: float
    top_price: float
    activation_delay_seconds: int
    paper_stake_usd: float
    real_stake_usd: float
    paper_only: bool = False
    real_only: bool = False
    max_seconds_to_expiry: int | None = None


@dataclass
class RecoveryPosition:
    id: str
    market_id: str
    symbol: str
    title: str
    interval_minutes: int
    market_start: datetime
    market_end: datetime
    side: str
    mode: str
    strategy_name: str
    touch_ts: datetime
    armed_ts: datetime
    opened_at: datetime
    touch_price: float
    trigger_price: float
    entry_price: float
    requested_shares: float
    filled_shares: float
    total_cost: float
    fee: float
    status: str
    pm_token_id: Optional[str] = None
    pm_order_id: Optional[str] = None
    note: Optional[str] = None
    resolved_at: Optional[datetime] = None
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
    pending_redeem_tx: Optional[str] = None
    tg_open_message_id: Optional[int] = None


@dataclass
class TrackedRecovery:
    market_id: str
    config_name: str
    symbol: str
    interval_minutes: int
    side: str = "no"
    touch_ts: Optional[datetime] = None
    touch_price: Optional[float] = None
    armed_ts: Optional[datetime] = None
    orders_placed: bool = False
    last_ask_above_entry: bool = False
    done: bool = False
    note: Optional[str] = None
