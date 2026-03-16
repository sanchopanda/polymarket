from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from src.api.gamma import GammaClient, Market
from src.config import StrategyConfig


@dataclass
class Candidate:
    market: Market
    outcome: str        # Название исхода (Yes/No/...)
    outcome_idx: int    # Индекс в списке исходов рынка
    price: float        # Текущая цена (~0.50)
    token_id: str       # CLOB token ID для этого исхода
    days_to_expiry: float


class MarketScanner:
    def __init__(self, gamma: GammaClient, config: StrategyConfig) -> None:
        self.gamma = gamma
        self.config = config

    def scan(self) -> List[Candidate]:
        """Сканирует все активные рынки и возвращает кандидатов в ценовом диапазоне."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        markets = self.gamma.fetch_all_active_markets()
        candidates: List[Candidate] = []

        if self.config.price_min is not None and self.config.price_max is not None:
            price_min = self.config.price_min
            price_max = self.config.price_max
        else:
            price_min = self.config.target_price - self.config.price_tolerance
            price_max = self.config.target_price + self.config.price_tolerance

        for market in markets:
            # Фильтр по категории
            if self.config.categories and market.category.lower() not in [
                c.lower() for c in self.config.categories
            ]:
                continue

            # Фильтр по типу комиссии (crypto_fees = крипто, none = без комиссий, "" = все)
            if self.config.fee_type == "crypto_fees" and market.fee_type != "crypto_fees":
                continue
            if self.config.fee_type == "none" and market.fee_type != "":
                continue

            # Фильтр по объёму
            if market.volume_num < self.config.min_volume_24h:
                continue

            # Фильтр по ликвидности
            if market.liquidity_num < self.config.min_liquidity:
                continue

            # Фильтр по сроку
            days_to_expiry = None
            if market.end_date:
                delta = (market.end_date - now).total_seconds() / 86400
                if delta < 0:
                    continue  # Уже истёк
                if delta > self.config.max_days_to_expiry:
                    continue
                days_to_expiry = delta
            else:
                continue  # Без даты окончания не рассматриваем

            # Перебираем исходы рынка
            for i, (outcome, price) in enumerate(
                zip(market.outcomes, market.outcome_prices)
            ):
                if not (price_min <= price <= price_max):
                    continue
                if i >= len(market.clob_token_ids):
                    continue

                token_id = market.clob_token_ids[i]
                if not token_id:
                    continue

                candidates.append(Candidate(
                    market=market,
                    outcome=outcome,
                    outcome_idx=i,
                    price=price,
                    token_id=token_id,
                    days_to_expiry=days_to_expiry,
                ))

        return candidates
