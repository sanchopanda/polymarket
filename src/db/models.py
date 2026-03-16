from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid


@dataclass
class BetSeries:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "active"     # "active" | "won" | "lost" | "abandoned"
    current_depth: int = 0
    initial_bet_size: float = 0.10
    total_invested: float = 0.0
    total_pnl: float = 0.0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    finished_at: Optional[datetime] = None


@dataclass
class SimulatedBet:
    market_id: str
    market_question: str
    outcome: str           # "Yes", "No", or named outcome
    token_id: str
    entry_price: float     # Цена на момент симулированной покупки
    amount_usd: float      # Потрачено долларов (без комиссии)
    fee_usd: float         # Комиссия Polymarket (2%)
    shares: float          # amount_usd / entry_price
    score: float           # Скор кандидата на момент ставки
    placed_at: datetime
    market_end_date: datetime
    status: str = "open"   # "open" | "won" | "lost"
    resolved_at: Optional[datetime] = None
    exit_price: Optional[float] = None   # 1.0 если выиграли, 0.0 если проиграли
    pnl: Optional[float] = None          # прибыль с учётом комиссии
    series_id: Optional[str] = None
    series_depth: int = 0
    order_id: str = ""   # ID реального ордера на Polymarket (пусто для paper trading)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def total_cost(self) -> float:
        """Полная стоимость включая комиссию."""
        return self.amount_usd + self.fee_usd

    @property
    def potential_return(self) -> float:
        """Потенциальный доход если выиграем, net of fee (в $)."""
        return self.shares - self.total_cost

    @property
    def multiplier(self) -> float:
        """Во сколько раз умножится ставка при победе (с учётом комиссии)."""
        if self.total_cost > 0:
            return self.shares / self.total_cost
        return 0.0


@dataclass
class RedeemRecord:
    bet_id: str
    market_id: str
    market_question: str
    amount_usd: float       # shares * exit_price (валовой возврат)
    tx_hash: str
    redeemed_at: datetime
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class WalletSnapshot:
    balance_usdc: float
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class ScanLog:
    scanned_at: datetime
    total_markets: int
    candidates_found: int
    bets_placed: int
    skipped_limit: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class PortfolioSnapshot:
    total_deployed: float = 0.0    # Сумма amount_usd открытых позиций
    total_fees_paid: float = 0.0   # Всего уплачено комиссий
    total_pnl_realized: float = 0.0
    open_positions: int = 0
    win_count: int = 0
    loss_count: int = 0
    active_series_count: int = 0

    @property
    def total_bets(self) -> int:
        return self.win_count + self.loss_count

    @property
    def win_rate(self) -> float:
        if self.total_bets == 0:
            return 0.0
        return self.win_count / self.total_bets

    @property
    def roi(self) -> float:
        """ROI от суммы потраченной на закрытые ставки (включая комиссию)."""
        if self.total_deployed == 0:
            return 0.0
        return self.total_pnl_realized / (self.total_deployed + self.total_fees_paid) * 100
