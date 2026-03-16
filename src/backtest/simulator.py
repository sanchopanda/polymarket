from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List

from src.backtest.fetcher import HistoricalMarket
from src.config import MartingaleConfig


@dataclass
class SeriesResult:
    depth_reached: int      # сколько ставок сделано (0-based первая ставка)
    total_invested: float
    pnl: float
    won: bool


@dataclass
class BacktestResult:
    total_series: int
    won_series: int
    abandoned_series: int
    total_invested: float
    total_pnl: float
    win_rate: float
    roi: float
    final_balance: float = 0.0
    series_results: List[SeriesResult] = field(default_factory=list)
    depth_distribution: dict = field(default_factory=dict)  # depth -> count


def simulate(
    markets: List[HistoricalMarket],
    cfg: MartingaleConfig,
    taker_fee: float,
    starting_balance: float,
    shuffle: bool = True,
) -> BacktestResult:
    """Чистая in-memory симуляция стратегии Мартингейла на исторических данных."""

    if not markets:
        return BacktestResult(
            total_series=0, won_series=0, abandoned_series=0,
            total_invested=0.0, total_pnl=0.0, win_rate=0.0, roi=0.0,
        )

    pool = list(markets)
    if shuffle:
        random.shuffle(pool)

    m = cfg.escalation_multiplier if cfg.escalation_multiplier is not None else 2.0 * (1.0 + taker_fee)
    initial_bet = cfg.initial_bet_size
    max_depth = cfg.max_series_depth

    series_results: List[SeriesResult] = []
    idx = 0
    balance = starting_balance

    while idx < len(pool):
        # Нет средств даже на начальную ставку — стоп
        initial_cost = initial_bet * (1 + taker_fee)
        if balance < initial_cost:
            break

        # Начинаем новую серию
        depth = 0
        bet_size = initial_bet
        total_invested = 0.0
        series_pnl = 0.0
        won = False
        abandoned = False
        bets_made = 0

        while depth < max_depth and idx < len(pool):
            cost = bet_size * (1 + taker_fee)

            # Нет средств на эскалацию — бросаем серию
            if balance < cost:
                abandoned = True
                break

            market = pool[idx]
            idx += 1

            balance -= cost
            total_invested += cost
            bets_made += 1

            if market.won:
                contracts = bet_size / market.entry_price
                gross = contracts * market.final_price
                fee_exit = gross * taker_fee
                net = gross - fee_exit - cost
                series_pnl = net
                balance += cost + net  # возвращаем вложенное + прибыль
                won = True
                break
            else:
                series_pnl -= cost
                depth += 1
                bet_size = initial_bet * (m ** depth)

        if not won and not abandoned:
            abandoned = True

        sr = SeriesResult(
            depth_reached=bets_made,
            total_invested=total_invested,
            pnl=series_pnl,
            won=won,
        )
        series_results.append(sr)

    # Статистика
    total_series = len(series_results)
    won_series = sum(1 for s in series_results if s.won)
    abandoned_series = sum(1 for s in series_results if not s.won)
    total_invested = sum(s.total_invested for s in series_results)
    total_pnl = sum(s.pnl for s in series_results)
    win_rate = won_series / total_series if total_series > 0 else 0.0
    roi = total_pnl / total_invested * 100 if total_invested > 0 else 0.0

    depth_dist: dict[int, int] = {}
    for s in series_results:
        depth_dist[s.depth_reached] = depth_dist.get(s.depth_reached, 0) + 1

    return BacktestResult(
        total_series=total_series,
        won_series=won_series,
        abandoned_series=abandoned_series,
        total_invested=total_invested,
        total_pnl=total_pnl,
        win_rate=win_rate,
        roi=roi,
        final_balance=balance,
        series_results=series_results,
        depth_distribution=depth_dist,
    )
