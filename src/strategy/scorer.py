from __future__ import annotations

from dataclasses import dataclass
from typing import List

from src.config import StrategyConfig
from src.strategy.scanner import Candidate


@dataclass
class ScoredCandidate:
    candidate: Candidate
    total_score: float
    liquidity_score: float
    time_score: float

    # Проксируем нужные поля для удобства
    @property
    def market(self):
        return self.candidate.market

    @property
    def outcome(self):
        return self.candidate.outcome

    @property
    def price(self):
        return self.candidate.price

    @property
    def token_id(self):
        return self.candidate.token_id

    @property
    def days_to_expiry(self):
        return self.candidate.days_to_expiry

    @property
    def multiplier(self):
        return 1.0 / self.price if self.price > 0 else 0.0


def _normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val <= min_val:
        return 0.5
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


class CandidateScorer:
    # Веса: время (75%), ликвидность (25%) — цена не влияет на приоритет
    W_TIME       = 0.75
    W_LIQUIDITY  = 0.25

    LIQUIDITY_MAX = 500_000   # $500k

    def __init__(self, strategy_config: StrategyConfig | None = None) -> None:
        cfg = strategy_config
        self.max_days_to_expiry = cfg.max_days_to_expiry if cfg else 1.0

    def score(self, candidate: Candidate) -> ScoredCandidate:
        m = candidate.market

        # Время: чем МЕНЬШЕ осталось — тем лучше (нормализуем в пределах окна)
        d = candidate.days_to_expiry
        time_score = max(0.0, 1.0 - d / self.max_days_to_expiry) if self.max_days_to_expiry > 0 else 0.5

        # Ликвидность: больше = лучше
        liq_score = _normalize(m.liquidity_num, 0, self.LIQUIDITY_MAX)

        total = (
            self.W_TIME      * time_score +
            self.W_LIQUIDITY * liq_score
        )

        return ScoredCandidate(
            candidate=candidate,
            total_score=round(total, 4),
            liquidity_score=round(liq_score, 4),
            time_score=round(time_score, 4),
        )

    def rank(self, candidates: List[Candidate], top_n: int = 20) -> List[ScoredCandidate]:
        scored = [self.score(c) for c in candidates]
        scored.sort(key=lambda s: s.total_score, reverse=True)
        return scored[:top_n]
