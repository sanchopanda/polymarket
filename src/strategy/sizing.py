from __future__ import annotations

import math

from src.config import MartingaleConfig


def compute_max_deep_slots(total_capital: float, initial_bet: float, max_depth: int) -> int:
    """
    Динамически вычисляет максимальное количество одновременных глубоких серий.

    Порог для N глубоких + 3 мелких (фиксировано):
      N * cost_deep + 3 * cost_shallow <= total_capital
      → n = floor((total_capital - 3 * cost_shallow) / cost_deep)
      → deep_slots = max(1, n - 1)  — буфер на одну дополнительную глубокую

    cost_deep    = initial_bet * (2^max_depth - 1)  — серия до максимальной глубины
    cost_shallow = initial_bet * 3                  — серия до глубины 2
    """
    if total_capital <= 0 or initial_bet <= 0 or max_depth <= 0:
        return 1
    cost_deep = initial_bet * (2 ** max_depth - 1)
    cost_shallow = initial_bet * 3
    n = int((total_capital - 3 * cost_shallow) / cost_deep)
    return max(1, n - 1)


def compute_dynamic_active_series(deep_slots: int) -> int:
    """
    Общее количество активных серий: N глубоких + 3 ждущих (фиксированно).
    deep=1 → 4, deep=2 → 5, deep=3 → 6, ...
    """
    return deep_slots + 3


def compute_dynamic_base_bet(total_deposit: float, max_depth: int, max_active_series: int = 14) -> float:
    """Вычисляет базовую ставку так, чтобы депозит покрывал все активные серии в худшем случае.

    30% серий — на максимальной глубине (max_depth ставок)
    70% серий — на глубине 3 (3 ставки)

    Стоимость серии глубины d (в единицах base_bet): 2^d - 1
    """
    import math as _math
    n_max   = _math.ceil(0.3 * max_active_series)
    n_depth3 = max_active_series - n_max
    units_max    = (2 ** max_depth) - 1
    units_depth3 = (2 ** 3) - 1  # = 7
    total_units  = n_max * units_max + n_depth3 * units_depth3
    if total_units <= 0:
        return 0.01
    raw = total_deposit / total_units
    return max(0.01, math.floor(raw * 100) / 100)


class PositionSizer:
    def __init__(self, config: MartingaleConfig, taker_fee: float = 0.0) -> None:
        self.config = config
        self.taker_fee = taker_fee

    def calculate(
        self,
        series_depth: int,
        entry_price: float = 0.5,
        series_total_invested: float = 0.0,
        initial_bet_size: float | None = None,
    ) -> float:
        """Возвращает размер ставки в $.

        Глубина 0: фиксированная начальная ставка.
        Глубина > 0: initial_bet * escalation_multiplier^depth.

        initial_bet_size — переопределяет config.initial_bet_size (для серий с динамической ставкой).
        """
        ib = initial_bet_size if initial_bet_size is not None else self.config.initial_bet_size
        m = (self.config.escalation_multiplier
             if self.config.escalation_multiplier is not None
             else 2.0 * (1.0 + self.taker_fee))
        return ib * (m ** series_depth)
