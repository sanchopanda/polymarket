from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NormalizedMarket:
    venue: str
    market_id: str
    title: str
    symbol: str
    market_kind: str
    expiry: datetime
    yes_label: str
    no_label: str
    yes_ask: float
    no_ask: float
    yes_bid: float
    no_bid: float
    yes_depth: float
    no_depth: float
    volume: float
    liquidity: float
    interval_minutes: Optional[int] = None
    rule_family: str = ""
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None
    reference_price: Optional[float] = None
    rules_text: str = ""


@dataclass
class MatchedMarketPair:
    polymarket: NormalizedMarket
    kalshi: NormalizedMarket
    score: float


@dataclass
class CrossVenueOpportunity:
    pair_key: str
    polymarket_market_id: str
    kalshi_market_id: str
    symbol: str
    title: str
    expiry: datetime
    polymarket_title: str
    kalshi_title: str
    match_score: float
    expiry_delta_seconds: float
    polymarket_reference_price: Optional[float]
    kalshi_reference_price: Optional[float]
    polymarket_rules: str
    kalshi_rules: str
    buy_yes_venue: str
    buy_no_venue: str
    yes_ask: float
    no_ask: float
    ask_sum: float
    edge_per_share: float
    shares: float
    capital_used: float
    polymarket_fee: float
    kalshi_fee: float
    total_fee: float
    total_cost: float
    expected_payout: float
    expected_profit: float


@dataclass
class ExecutionLegInfo:
    venue: str
    market_id: str
    side: str
    requested_shares: float
    filled_shares: float
    available_shares: float   # total book depth (all price levels)
    avg_price: float
    total_cost: float
    best_ask: float
    remaining_shares_after_fill: float
    usable_shares: float = 0.0  # shares at price levels that preserve edge >= min_lock_edge


@dataclass
class CrossPosition:
    id: str
    pair_key: str
    symbol: str
    title: str
    expiry: datetime
    venue_yes: str
    market_yes: str
    venue_no: str
    market_no: str
    shares: float
    yes_ask: float
    no_ask: float
    ask_sum: float
    total_cost: float
    expected_profit: float
    opened_at: datetime
    yes_requested_shares: Optional[float] = None
    yes_filled_shares: Optional[float] = None
    yes_available_shares: Optional[float] = None
    yes_avg_price: Optional[float] = None
    yes_best_ask: Optional[float] = None
    yes_remaining_shares_after_fill: Optional[float] = None
    no_requested_shares: Optional[float] = None
    no_filled_shares: Optional[float] = None
    no_available_shares: Optional[float] = None
    no_avg_price: Optional[float] = None
    no_best_ask: Optional[float] = None
    no_remaining_shares_after_fill: Optional[float] = None
    polymarket_title: Optional[str] = None
    kalshi_title: Optional[str] = None
    match_score: Optional[float] = None
    expiry_delta_seconds: Optional[float] = None
    polymarket_reference_price: Optional[float] = None
    kalshi_reference_price: Optional[float] = None
    polymarket_rules: Optional[str] = None
    kalshi_rules: Optional[str] = None
    polymarket_snapshot_open: Optional[str] = None
    kalshi_snapshot_open: Optional[str] = None
    polymarket_snapshot_resolved: Optional[str] = None
    kalshi_snapshot_resolved: Optional[str] = None
    status: str = "open"
    resolved_at: Optional[datetime] = None
    winning_side: Optional[str] = None
    pnl: Optional[float] = None
    polymarket_result: Optional[str] = None
    kalshi_result: Optional[str] = None
    lock_valid: Optional[bool] = None


@dataclass
class OpportunityDecision:
    pair_key: str
    symbol: str
    buy_yes_venue: str
    buy_no_venue: str
    polymarket_yes: float
    polymarket_no: float
    kalshi_yes: float
    kalshi_no: float
    ask_sum_initial: float
    ask_sum_recheck: float
    edge_initial: float
    edge_recheck: float
    shares: float
    decision: str
    expected_profit: float
    yes_leg_summary: str = ""
    no_leg_summary: str = ""
    was_raw_opportunity: bool = True
