from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class OracleMarket:
    venue: str                              # always "polymarket"
    market_id: str
    title: str
    symbol: str                             # "BTC", "ETH", ...
    interval_minutes: int                   # 5 or 15
    expiry: datetime                        # market_end (UTC, naive)
    market_start: datetime                  # expiry - timedelta(minutes=interval_minutes)
    volume: float
    yes_ask: float
    no_ask: float
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None
    pm_event_slug: Optional[str] = None
    pm_open_price: Optional[float] = None           # Chainlink openPrice from PM HTML
    binance_price_at_start: Optional[float] = None  # first Binance tick after market_start (analytics)
    binance_ref_side: Optional[str] = None          # "above" | "below" pm_open_price at penultimate minute
    subscribed_at: Optional[datetime] = None


@dataclass
class OracleSignal:
    """
    Записывается каждый раз когда дельта Binance пересекает порог —
    независимо от того, была ли размещена ставка.
    Позволяет видеть случаи когда PM уже успел переоценить.
    """
    id: str
    market_id: str
    symbol: str
    interval_minutes: int
    market_minute: int
    position_pct: float
    fired_at: datetime
    side: str               # "yes" (дельта > 0) | "no" (дельта < 0)
    delta_pct: float
    pm_open_price: float
    binance_price: float
    pm_yes_ask: float       # PM цена YES в момент сигнала
    pm_no_ask: float        # PM цена NO в момент сигнала
    bet_placed: bool        # поставили ли ставку


@dataclass
class RealBet:
    id: str
    market_id: str
    symbol: str
    interval_minutes: int
    market_start: datetime
    market_end: datetime
    placed_at: datetime
    market_minute: int
    side: str                        # "yes" | "no"
    requested_price: float           # PM ask at signal time
    fill_price: float                # actual fill from CLOB
    shares_requested: float
    shares_filled: float
    stake_usd: float                 # amount spent ($1)
    order_id: str
    order_status: str                # "filled" | etc
    delta_pct: float
    pm_open_price: float
    binance_price_at_bet: float
    pm_close_price: Optional[float] = None
    status: str = "open"             # "open" | "resolved"
    resolved_at: Optional[datetime] = None
    winning_side: Optional[str] = None
    pnl: Optional[float] = None


@dataclass
class OracleBet:
    id: str
    market_id: str
    symbol: str
    interval_minutes: int
    market_start: datetime
    market_end: datetime
    opened_at: datetime
    market_minute: int
    position_pct: float
    side: str                               # "yes" | "no"
    entry_price: float                      # PM ask at signal time
    shares: float
    total_cost: float
    binance_price_at_start: Optional[float] # analytics only
    binance_price_at_bet: float
    delta_pct: float                        # (binance_at_bet - pm_open_price) / pm_open_price * 100
    pm_open_price: float                    # Chainlink reference price (basis for delta)
    pm_close_price: Optional[float] = None  # filled on resolution
    pm_price_10s: Optional[float] = None    # ask 10s after bet (analytics)
    venue: str = "polymarket"              # "polymarket" | "kalshi"
    seconds_to_close: Optional[int] = None  # exact seconds to market_end at bet time
    opposite_ask: Optional[float] = None    # ask of the OTHER side at bet time
    depth_usd: Optional[float] = None       # orderbook liquidity available at max_price
    volume: Optional[float] = None          # market volume at bet time
    binance_price_at_close: Optional[float] = None  # Binance price at resolution time
    strategy: str = "crossing"               # "crossing" | "cl_contradiction"
    status: str = "open"
    resolved_at: Optional[datetime] = None
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
