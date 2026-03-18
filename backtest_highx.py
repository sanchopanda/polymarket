"""
Бэктест стратегии HighX: ставки с множителем ≥ 100x (цена ≤ 0.01).

1. Загружает закрытые рынки через Gamma API
2. Для каждого бинарного рынка проверяет историю цен — был ли момент с ценой ≤ 0.01
3. Если да — виртуальная ставка фиксированного размера
4. Проверяет, выиграла ли ставка (outcome won)
5. Считает итоговый P&L портфеля
"""

from __future__ import annotations

import sys
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

# Подключаем src
sys.path.insert(0, os.path.dirname(__file__))
from src.api.gamma import GammaClient, Market


GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

MAX_PRICE = 0.01       # множитель ≥ 100x
BET_SIZE = 0.50        # $ на ставку
TAKER_FEE = 0.02       # 2%


@dataclass
class BacktestBet:
    market_id: str
    question: str
    outcome: str
    entry_price: float
    multiplier: float
    won: bool
    amount: float        # вложено
    fee: float
    pnl: float           # profit/loss
    volume: float
    liquidity: float
    end_date: Optional[datetime]


def _find_lowprice_entry(
    clob_base: str,
    market: Market,
    max_price: float,
    winner_idx: int,
    min_price: float = 0.0,
) -> list[BacktestBet]:
    """Для рынка ищет в истории моменты когда цена исхода была в диапазоне [min_price, max_price].

    Возвращает список ставок (может быть 0, 1 или 2 — по одной на исход).
    Для каждого исхода берём ПЕРВЫЙ момент когда цена попала в диапазон.
    """
    bets = []
    with httpx.Client(timeout=15.0) as http:
        for i, token_id in enumerate(market.clob_token_ids):
            if not token_id:
                continue
            try:
                resp = http.get(
                    f"{clob_base}/prices-history",
                    params={"market": token_id, "interval": "max", "fidelity": 10},
                )
                resp.raise_for_status()
                history = resp.json().get("history", [])
            except Exception:
                continue

            if not history:
                continue

            # Ищем первый момент с ценой в диапазоне [min_price, max_price]
            entry_price = None
            for point in history:
                try:
                    p = float(point["p"])
                except (KeyError, ValueError, TypeError):
                    continue
                if min_price <= p <= max_price and p > 0:
                    entry_price = p
                    break

            if entry_price is None:
                continue

            won = (i == winner_idx)
            shares = BET_SIZE / entry_price
            fee = BET_SIZE * TAKER_FEE
            if won:
                pnl = shares * 1.0 - BET_SIZE - fee  # выигрыш: shares * $1
            else:
                pnl = -BET_SIZE - fee  # проигрыш

            bets.append(BacktestBet(
                market_id=market.id,
                question=market.question,
                outcome=market.outcomes[i],
                entry_price=entry_price,
                multiplier=1.0 / entry_price,
                won=won,
                amount=BET_SIZE,
                fee=fee,
                pnl=pnl,
                volume=market.volume_num,
                liquidity=market.liquidity_num,
                end_date=market.end_date,
            ))

    return bets


def run_backtest(
    limit: int = 5000,
    max_price: float = MAX_PRICE,
    min_price: float = 0.0,
    min_volume: float = 0.0,
    min_liquidity: float = 0.0,
    closed_after_days: float = 30.0,
    workers: int = 20,
    bet_size: float = BET_SIZE,
) -> list[BacktestBet]:
    global BET_SIZE
    BET_SIZE = bet_size

    gamma = GammaClient(base_url=GAMMA_URL)
    closed_after = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=closed_after_days)

    price_label = f"{min_price:.4f}–{max_price:.4f}" if min_price > 0 else f"≤ {max_price}"
    print(f"=== Бэктест HighX (цена {price_label}, множитель {1/max_price:.0f}x–{1/min_price:.0f}x) ===" if min_price > 0
          else f"=== Бэктест HighX (множитель ≥ {1/max_price:.0f}x, цена ≤ {max_price}) ===")
    print(f"Размер ставки: ${bet_size:.2f} | Комиссия: {TAKER_FEE*100:.0f}%")
    print(f"Загрузка закрытых рынков (лимит {limit}, за последние {closed_after_days:.0f} дней)...\n")

    raw_markets = gamma.fetch_closed_markets(
        limit=limit,
        min_volume=min_volume,
        min_liquidity=min_liquidity,
        closed_after=closed_after,
    )
    print(f"Получено {len(raw_markets)} рынков.\n")

    # Фильтрация: бинарные + зарезолвленные
    candidates: list[Market] = []
    winner_map: dict[str, int] = {}

    for m in raw_markets:
        if len(m.outcomes) != 2 or len(m.clob_token_ids) != 2:
            continue
        if not m.end_date:
            continue
        winner_idx = next((i for i, p in enumerate(m.outcome_prices) if p >= 0.9), None)
        if winner_idx is None:
            continue
        candidates.append(m)
        winner_map[m.id] = winner_idx

    print(f"Бинарных зарезолвленных: {len(candidates)}")
    print(f"Ищем моменты с ценой ≤ {max_price} в истории ({workers} потоков)...\n")

    all_bets: list[BacktestBet] = []
    processed = 0

    futures = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for m in candidates:
            f = pool.submit(_find_lowprice_entry, CLOB_URL, m, max_price, winner_map[m.id], min_price)
            futures[f] = m

        for future in as_completed(futures):
            processed += 1
            if processed % 200 == 0:
                print(f"  {processed}/{len(candidates)} обработано... (найдено {len(all_bets)} ставок)")

            bets = future.result()
            all_bets.extend(bets)

    print(f"\n  Всего обработано: {processed}/{len(candidates)}")
    return all_bets


def print_report(bets: list[BacktestBet], bet_size: float = BET_SIZE):
    if not bets:
        print("\n❌ Не найдено ни одной ставки с ценой ≤ порога в истории.")
        return

    total = len(bets)
    won = sum(1 for b in bets if b.won)
    lost = total - won
    win_rate = won / total * 100

    total_invested = sum(b.amount for b in bets)
    total_fees = sum(b.fee for b in bets)
    total_pnl = sum(b.pnl for b in bets)
    total_won_pnl = sum(b.pnl for b in bets if b.won)
    total_lost_pnl = sum(b.pnl for b in bets if not b.won)
    roi = total_pnl / total_invested * 100 if total_invested > 0 else 0

    avg_multiplier = sum(b.multiplier for b in bets) / total
    avg_win_mult = (sum(b.multiplier for b in bets if b.won) / won) if won > 0 else 0

    print(f"\n{'='*70}")
    print(f"  РЕЗУЛЬТАТЫ БЭКТЕСТА HighX")
    print(f"{'='*70}")
    print(f"\n  Ставок:         {total}")
    print(f"  Выигрышей:      {won} ({win_rate:.2f}%)")
    print(f"  Проигрышей:     {lost}")
    print(f"  Вложено:        ${total_invested:,.2f}")
    print(f"  Комиссии:       ${total_fees:,.2f}")
    print(f"  P&L:            ${total_pnl:,.2f}")
    print(f"  ROI:            {roi:+.1f}%")
    print(f"  Ср. множитель:  {avg_multiplier:.0f}x")
    if won > 0:
        print(f"  Ср. множ. побед: {avg_win_mult:.0f}x")

    # Безубыток: сколько выигрышей нужно на N ставок?
    cost_per_bet = bet_size * (1 + TAKER_FEE)
    breakeven_wins = total * cost_per_bet / (avg_multiplier * bet_size) if avg_multiplier > 0 else 0
    print(f"\n  Для безубытка нужно: {breakeven_wins:.1f} побед из {total}")
    print(f"  Фактически:         {won} побед")

    # Выигрыши
    if won > 0:
        print(f"\n{'='*70}")
        print(f"  ВЫИГРАВШИЕ СТАВКИ ({won})")
        print(f"{'='*70}")
        won_bets = sorted([b for b in bets if b.won], key=lambda x: -x.pnl)
        for b in won_bets[:30]:
            print(f"\n  💰 {b.multiplier:.0f}x | P&L: ${b.pnl:,.2f} | Цена: ${b.entry_price:.4f}")
            print(f"     vol=${b.volume:,.0f} | liq=${b.liquidity:,.0f}")
            print(f"     {b.question}")
            print(f"     Исход: {b.outcome}")
            if b.end_date:
                print(f"     Экспирация: {b.end_date.strftime('%Y-%m-%d')}")

    # Распределение по множителям
    print(f"\n{'='*70}")
    print(f"  РАСПРЕДЕЛЕНИЕ ПО МНОЖИТЕЛЯМ")
    print(f"{'='*70}\n")

    buckets = [
        ("100-200x", 100, 200),
        ("200-500x", 200, 500),
        ("500-1000x", 500, 1000),
        ("1000x+", 1000, float("inf")),
    ]
    for label, lo, hi in buckets:
        bucket_bets = [b for b in bets if lo <= b.multiplier < hi]
        if not bucket_bets:
            continue
        n = len(bucket_bets)
        w = sum(1 for b in bucket_bets if b.won)
        wr = w / n * 100
        pnl = sum(b.pnl for b in bucket_bets)
        print(f"  {label:12s}: {n:5d} ставок | {w:3d} побед ({wr:5.2f}%) | P&L: ${pnl:>+10,.2f}")

    # Кривая капитала
    print(f"\n{'='*70}")
    print(f"  КРИВАЯ КАПИТАЛА (хронологически)")
    print(f"{'='*70}\n")

    # Сортируем по дате экспирации
    sorted_bets = sorted(bets, key=lambda b: b.end_date or datetime.min)
    balance = 0.0
    peak = 0.0
    max_dd = 0.0

    step = max(1, len(sorted_bets) // 20)
    for i, b in enumerate(sorted_bets):
        balance += b.pnl
        if balance > peak:
            peak = balance
        dd = peak - balance
        if dd > max_dd:
            max_dd = dd
        if (i + 1) % step == 0 or i == len(sorted_bets) - 1:
            date_str = b.end_date.strftime('%Y-%m-%d') if b.end_date else '???'
            bar = "█" * max(0, int((balance + total_invested) / total_invested * 20))
            print(f"  [{date_str}] Ставка #{i+1:4d} | P&L: ${balance:>+10,.2f} | {bar}")

    print(f"\n  Итого P&L:      ${balance:,.2f}")
    print(f"  Макс. просадка: ${max_dd:,.2f}")
    print(f"  Пик:            ${peak:,.2f}")


def main():
    parser = argparse.ArgumentParser(description="Бэктест HighX — ставки с множителем ≥100x")
    parser.add_argument("--limit", type=int, default=5000, help="Лимит рынков для загрузки")
    parser.add_argument("--max-price", type=float, default=MAX_PRICE, help="Макс. цена исхода")
    parser.add_argument("--min-price", type=float, default=0.0, help="Мин. цена исхода")
    parser.add_argument("--min-volume", type=float, default=0, help="Мин. объём рынка")
    parser.add_argument("--min-liquidity", type=float, default=0, help="Мин. ликвидность")
    parser.add_argument("--days", type=float, default=30, help="Закрытые за последние N дней")
    parser.add_argument("--bet-size", type=float, default=BET_SIZE, help="Размер ставки ($)")
    parser.add_argument("--workers", type=int, default=20, help="Потоков для загрузки")
    args = parser.parse_args()

    bets = run_backtest(
        limit=args.limit,
        max_price=args.max_price,
        min_price=args.min_price,
        min_volume=args.min_volume,
        min_liquidity=args.min_liquidity,
        closed_after_days=args.days,
        workers=args.workers,
        bet_size=args.bet_size,
    )

    print_report(bets, bet_size=args.bet_size)


if __name__ == "__main__":
    main()
