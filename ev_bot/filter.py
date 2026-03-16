"""EV-фильтр: анализ win rate по бакетам (цена × объём), фильтрация +EV рынков."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from src.backtest.fetcher import HistoricalMarket


# Тиры объёма (нижние границы): [0, 1k), [1k, 5k), [5k, 20k), [20k, 100k), [100k, ∞)
VOLUME_TIERS = [0, 1_000, 5_000, 20_000, 100_000]
PRICE_STEP = 0.05  # ширина ценового бина


@dataclass
class EVBucket:
    price_min: float
    price_max: float
    volume_min: float
    total: int
    won: int
    win_rate: float
    ev_per_dollar: float  # EV = wr/mid_price - 1 - fee


def _price_bin(price: float) -> float:
    """Нижняя граница ценового бина для данной цены."""
    return round(math.floor(price / PRICE_STEP + 1e-9) * PRICE_STEP, 6)


def _volume_tier(volume: float) -> float:
    """Наибольший тир объёма, не превышающий данное значение."""
    tier = 0.0
    for t in VOLUME_TIERS:
        if volume >= t:
            tier = float(t)
        else:
            break
    return tier


class EVFilter:
    """Анализирует win rate закрытых рынков по бакетам и фильтрует кандидатов с +EV."""

    def __init__(self, taker_fee: float = 0.02, min_samples: int = 50, recalc_interval: int = 50):
        self.taker_fee = taker_fee
        self.min_samples = min_samples
        self.recalc_interval = recalc_interval
        self.markets: List[HistoricalMarket] = []
        self.buckets: List[EVBucket] = []
        self._positive_set: set = set()  # {(price_bin, volume_tier)} с EV > 0
        self._new_since_recalc: int = 0

    def load_history(self, markets: List[HistoricalMarket]) -> None:
        """Загрузить исторические рынки и пересчитать бакеты."""
        self.markets = list(markets)
        self.analyze()

    def analyze(self) -> List[EVBucket]:
        """Пересчитать win rate и EV по всем бакетам."""
        stats: dict = {}  # (price_bin, vol_tier) -> [won, total]

        for m in self.markets:
            key = (_price_bin(m.entry_price), _volume_tier(m.volume_num))
            if key not in stats:
                stats[key] = [0, 0]
            stats[key][1] += 1
            if m.won:
                stats[key][0] += 1

        self.buckets = []
        for (p_bin, vol_min), (won, total) in stats.items():
            if total < self.min_samples:
                continue
            wr = won / total
            # EV per $1 bet:
            #   pnl_win  = shares - amount - fee = 1/price - 1 - fee
            #   pnl_loss = -(1 + fee)
            #   EV = wr * pnl_win + (1-wr) * pnl_loss = wr/price - 1 - fee
            mid_price = p_bin + PRICE_STEP / 2
            ev = wr / mid_price - 1.0 - self.taker_fee
            self.buckets.append(EVBucket(
                price_min=p_bin,
                price_max=round(p_bin + PRICE_STEP, 6),
                volume_min=vol_min,
                total=total,
                won=won,
                win_rate=wr,
                ev_per_dollar=ev,
            ))

        self.buckets.sort(key=lambda b: b.ev_per_dollar, reverse=True)
        self._positive_set = {
            (b.price_min, b.volume_min)
            for b in self.buckets
            if b.ev_per_dollar > 0
        }
        self._new_since_recalc = 0

        n_pos = len(self._positive_set)
        print(f"[EVFilter] Бакетов: {len(self.buckets)} (с мин. выборкой {self.min_samples}), +EV: {n_pos}")
        return self.buckets

    def add_resolved(self, market: HistoricalMarket) -> None:
        """Добавить зарезолвленный рынок. Каждые recalc_interval — пересчёт."""
        self.markets.append(market)
        self._new_since_recalc += 1
        if self._new_since_recalc >= self.recalc_interval:
            print(f"[EVFilter] Пересчёт ({len(self.markets)} рынков в истории)...")
            self.analyze()

    def passes(self, price: float, volume: float) -> bool:
        """Проходит ли кандидат фильтр +EV."""
        if not self._positive_set:
            return False
        return (_price_bin(price), _volume_tier(volume)) in self._positive_set

    def summary(self) -> str:
        """Краткое описание активных +EV фильтров."""
        positive = [b for b in self.buckets if b.ev_per_dollar > 0]
        if not positive:
            return f"нет +EV бакетов (мин. выборка {self.min_samples})"
        max_price = max(b.price_max for b in positive)
        min_vol = min(b.volume_min for b in positive)
        return f"{len(positive)} +EV бакетов, entry < {max_price:.2f}, volume ≥ {min_vol:,.0f}"
