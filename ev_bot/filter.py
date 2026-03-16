"""EV-фильтр: анализ win rate по ценовым бакетам, фильтрация +EV рынков."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from src.backtest.fetcher import HistoricalMarket


PRICE_STEP = 0.10  # ширина ценового бина


@dataclass
class EVBucket:
    price_min: float
    price_max: float
    total: int
    won: int
    win_rate: float
    ev_per_dollar: float  # EV = wr/mid_price - 1 - fee
    breakeven_wr: float   # мин. win rate для +EV при данной цене


def _price_bin(price: float) -> float:
    """Нижняя граница ценового бина для данной цены."""
    return round(math.floor(price / PRICE_STEP + 1e-9) * PRICE_STEP, 6)


class EVFilter:
    """Анализирует win rate закрытых рынков по ценовым бакетам."""

    def __init__(self, taker_fee: float = 0.02, min_samples: int = 50, recalc_interval: int = 50):
        self.taker_fee = taker_fee
        self.min_samples = min_samples
        self.recalc_interval = recalc_interval
        self.markets: List[HistoricalMarket] = []
        self.buckets: List[EVBucket] = []
        self._positive_bins: set = set()  # {price_bin} с EV > 0
        self._new_since_recalc: int = 0

    def load_history(self, markets: List[HistoricalMarket]) -> None:
        self.markets = list(markets)
        self.analyze()

    def analyze(self) -> List[EVBucket]:
        """Пересчитать win rate и EV по ценовым бакетам."""
        stats: dict = {}  # price_bin -> [won, total]

        for m in self.markets:
            key = _price_bin(m.entry_price)
            if key not in stats:
                stats[key] = [0, 0]
            stats[key][1] += 1
            if m.won:
                stats[key][0] += 1

        self.buckets = []
        for p_bin, (won, total) in stats.items():
            if total < self.min_samples:
                continue
            wr = won / total
            mid = p_bin + PRICE_STEP / 2
            # EV per $1: wr/price - 1 - fee
            ev = wr / mid - 1.0 - self.taker_fee
            # Breakeven win rate: wr при котором EV = 0 → wr = (1+fee)*price
            breakeven = (1.0 + self.taker_fee) * mid
            self.buckets.append(EVBucket(
                price_min=p_bin,
                price_max=round(p_bin + PRICE_STEP, 6),
                total=total,
                won=won,
                win_rate=wr,
                ev_per_dollar=ev,
                breakeven_wr=breakeven,
            ))

        self.buckets.sort(key=lambda b: b.price_min)
        self._positive_bins = {b.price_min for b in self.buckets if b.ev_per_dollar > 0}
        self._new_since_recalc = 0

        n_pos = len(self._positive_bins)
        print(f"[EVFilter] Бакетов: {len(self.buckets)}, +EV: {n_pos}")
        return self.buckets

    def add_resolved(self, market: HistoricalMarket) -> None:
        self.markets.append(market)
        self._new_since_recalc += 1
        if self._new_since_recalc >= self.recalc_interval:
            print(f"[EVFilter] Пересчёт ({len(self.markets)} рынков)...")
            self.analyze()

    def passes(self, price: float, volume: float = 0) -> bool:
        """Проходит ли кандидат фильтр +EV."""
        if not self._positive_bins:
            return False
        return _price_bin(price) in self._positive_bins

    def summary(self) -> str:
        positive = [b for b in self.buckets if b.ev_per_dollar > 0]
        if not positive:
            return f"нет +EV бакетов (мин. выборка {self.min_samples})"
        ranges = ", ".join(f"{b.price_min:.2f}–{b.price_max:.2f}" for b in positive)
        return f"{len(positive)} +EV бакетов: {ranges}"
