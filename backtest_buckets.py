"""
Бэктест по бакетам цен (шаг $0.10).

Загружает закрытые рынки, для каждого исхода находит первый момент
когда цена попала в бакет, делает виртуальную ставку, считает P&L по бакетам.
Все бакеты обрабатываются за один проход по истории цен.

Режимы:
  --data data/markets_180d.json   # офлайн из кеша (быстро)
  без --data                       # онлайн из API (медленно)
"""

from __future__ import annotations

import sys
import os
import json
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

sys.path.insert(0, os.path.dirname(__file__))
from src.api.gamma import GammaClient, Market

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

BET_SIZE = 0.50
TAKER_FEE = 0.02

# Бакеты: (label, lo, hi) — lo включительно, hi не включительно
BUCKETS = [
    ("$0.00–0.10", 0.001, 0.10),
    ("$0.10–0.20", 0.10, 0.20),
    ("$0.20–0.30", 0.20, 0.30),
    ("$0.30–0.40", 0.30, 0.40),
    ("$0.40–0.50", 0.40, 0.50),
    ("$0.50–0.60", 0.50, 0.60),
    ("$0.60–0.70", 0.60, 0.70),
    ("$0.70–0.80", 0.70, 0.80),
    ("$0.80–0.90", 0.80, 0.90),
    ("$0.90–1.00", 0.90, 1.00),
]


@dataclass
class BucketBet:
    entry_price: float
    won: bool
    pnl: float


@dataclass
class BucketStats:
    label: str
    bets: list[BucketBet] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.bets)

    @property
    def wins(self) -> int:
        return sum(1 for b in self.bets if b.won)

    @property
    def win_rate(self) -> float:
        return self.wins / self.total * 100 if self.total > 0 else 0

    @property
    def pnl(self) -> float:
        return sum(b.pnl for b in self.bets)

    @property
    def invested(self) -> float:
        return self.total * BET_SIZE

    @property
    def roi(self) -> float:
        return self.pnl / self.invested * 100 if self.invested > 0 else 0


def _process_market(
    clob_base: str,
    market: Market,
    winner_idx: int,
) -> list[tuple[int, float, bool]]:
    """Для каждого исхода рынка ищет ВСЕ бакеты, в которые попала цена (онлайн)."""
    results = []
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

            won = (i == winner_idx)
            results.extend(_scan_history_buckets(history, won))

    return results


def _process_market_cached(
    market_data: dict,
) -> list[tuple[int, float, bool]]:
    """Для каждого исхода рынка ищет ВСЕ бакеты (офлайн из кеша)."""
    results = []
    winner_idx = market_data["winner_idx"]

    for i, token_id in enumerate(market_data["clob_token_ids"]):
        history = market_data.get("history", {}).get(token_id, [])
        if not history:
            continue
        won = (i == winner_idx)
        results.extend(_scan_history_buckets(history, won))

    return results


def _scan_history_buckets(
    history: list[dict],
    won: bool,
) -> list[tuple[int, float, bool]]:
    """Общая логика: проход по истории, поиск первого попадания в каждый бакет."""
    results = []
    found_buckets: set[int] = set()

    for point in history:
        try:
            p = float(point["p"])
        except (KeyError, ValueError, TypeError):
            continue
        if p <= 0:
            continue

        for bi, (_, lo, hi) in enumerate(BUCKETS):
            if bi not in found_buckets and lo <= p < hi:
                found_buckets.add(bi)
                results.append((bi, p, won))

        if len(found_buckets) == len(BUCKETS):
            break

    return results


def _load_cached_markets(data_path: str, days: float, min_volume: float) -> list[dict]:
    """Загружает рынки из кеша с фильтрацией по периоду и объёму."""
    with open(data_path) as f:
        data = json.load(f)

    markets = data["markets"]
    meta = data.get("meta", {})
    print(f"Загружено из кеша: {len(markets)} рынков (файл создан {meta.get('fetched_at', '?')})")

    # Фильтр по периоду (end_date за последние N дней)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(days=days)
    filtered = []
    for m in markets:
        if not m.get("end_date"):
            continue
        try:
            end_date = datetime.fromisoformat(m["end_date"]).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
        if end_date < cutoff:
            continue
        if end_date > now:
            continue
        if min_volume > 0 and m.get("volume_num", 0) < min_volume:
            continue
        filtered.append(m)

    print(f"После фильтрации (период {days:.0f}д, volume≥{min_volume}): {len(filtered)} рынков\n")
    return filtered


def main():
    parser = argparse.ArgumentParser(description="Бэктест по бакетам цен ($0.10 шаг)")
    parser.add_argument("--data", type=str, default="", help="Путь к кешу (data/markets_180d.json)")
    parser.add_argument("--limit", type=int, default=5000, help="Лимит рынков (только для онлайн)")
    parser.add_argument("--days", type=float, default=30, help="За последние N дней")
    parser.add_argument("--bet-size", type=float, default=BET_SIZE, help="Размер ставки ($)")
    parser.add_argument("--min-volume", type=float, default=0, help="Мин. объём")
    parser.add_argument("--workers", type=int, default=20, help="Потоков (только для онлайн)")
    args = parser.parse_args()

    bet_size = args.bet_size

    print(f"=== Бэктест по бакетам цен (шаг $0.10) ===")
    print(f"Ставка: ${bet_size:.2f} | Комиссия: {TAKER_FEE*100:.0f}% | Период: {args.days:.0f} дней\n")

    # Инициализация бакетов
    bucket_stats = [BucketStats(label=label) for label, _, _ in BUCKETS]

    if args.data:
        # === ОФЛАЙН: из кеша ===
        cached_markets = _load_cached_markets(args.data, args.days, args.min_volume)
        print(f"Обработка {len(cached_markets)} рынков (офлайн)...\n")

        for idx, m in enumerate(cached_markets):
            if (idx + 1) % 2000 == 0:
                total_bets = sum(bs.total for bs in bucket_stats)
                print(f"  {idx+1}/{len(cached_markets)} обработано... ({total_bets} ставок)")

            for bi, entry_price, won in _process_market_cached(m):
                shares = bet_size / entry_price
                fee = bet_size * TAKER_FEE
                if won:
                    pnl = shares * 1.0 - bet_size - fee
                else:
                    pnl = -bet_size - fee
                bucket_stats[bi].bets.append(BucketBet(entry_price=entry_price, won=won, pnl=pnl))
    else:
        # === ОНЛАЙН: из API ===
        gamma = GammaClient(base_url=GAMMA_URL)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        closed_after = now - timedelta(days=args.days)

        print(f"Загрузка рынков из API (лимит {args.limit})...\n")
        raw_markets = gamma.fetch_closed_markets(
            limit=args.limit,
            min_volume=args.min_volume,
            min_liquidity=0.0,
            closed_after=closed_after,
            end_date_max=now,
            order_by="closedTime",
        )
        print(f"Получено {len(raw_markets)} рынков.\n")

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
        print(f"Обработка истории цен ({args.workers} потоков)...\n")

        processed = 0
        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for m in candidates:
                f = pool.submit(_process_market, CLOB_URL, m, winner_map[m.id])
                futures[f] = m

            for future in as_completed(futures):
                processed += 1
                if processed % 200 == 0:
                    total_bets = sum(bs.total for bs in bucket_stats)
                    print(f"  {processed}/{len(candidates)} обработано... ({total_bets} ставок)")

                for bi, entry_price, won in future.result():
                    shares = bet_size / entry_price
                    fee = bet_size * TAKER_FEE
                    if won:
                        pnl = shares * 1.0 - bet_size - fee
                    else:
                        pnl = -bet_size - fee
                    bucket_stats[bi].bets.append(BucketBet(entry_price=entry_price, won=won, pnl=pnl))

    # Отчёт
    print(f"\n{'='*85}")
    print(f"  РЕЗУЛЬТАТЫ ПО БАКЕТАМ")
    print(f"{'='*85}\n")

    header = f"  {'Бакет':14s} | {'Ставок':>7s} | {'Побед':>6s} | {'Win%':>7s} | {'Implied':>7s} | {'P&L':>12s} | {'ROI':>8s}"
    print(header)
    print(f"  {'-'*14}-+-{'-'*7}-+-{'-'*6}-+-{'-'*7}-+-{'-'*7}-+-{'-'*12}-+-{'-'*8}")

    total_pnl = 0.0
    total_bets = 0
    total_wins = 0
    profitable_buckets = []

    for bi, bs in enumerate(bucket_stats):
        if bs.total == 0:
            continue

        _, lo, hi = BUCKETS[bi]
        mid = (lo + hi) / 2
        implied = mid * 100  # implied probability %

        marker = " ✅" if bs.pnl > 0 else ""
        print(f"  {bs.label:14s} | {bs.total:7d} | {bs.wins:6d} | {bs.win_rate:6.2f}% | {implied:6.1f}% | ${bs.pnl:>+10,.2f} | {bs.roi:>+7.1f}%{marker}")

        total_pnl += bs.pnl
        total_bets += bs.total
        total_wins += bs.wins
        if bs.pnl > 0:
            profitable_buckets.append(bs.label)

    print(f"  {'-'*14}-+-{'-'*7}-+-{'-'*6}-+-{'-'*7}-+-{'-'*7}-+-{'-'*12}-+-{'-'*8}")
    total_wr = total_wins / total_bets * 100 if total_bets > 0 else 0
    total_roi = total_pnl / (total_bets * bet_size) * 100 if total_bets > 0 else 0
    print(f"  {'ИТОГО':14s} | {total_bets:7d} | {total_wins:6d} | {total_wr:6.2f}% | {'':7s} | ${total_pnl:>+10,.2f} | {total_roi:>+7.1f}%")

    # Вывод: какие бакеты в плюсе
    print(f"\n{'='*85}")
    if profitable_buckets:
        print(f"  ✅ Прибыльные бакеты: {', '.join(profitable_buckets)}")
    else:
        print(f"  ❌ Ни один бакет не вышел в плюс")
    print(f"{'='*85}")

    # Детальнее по каждому бакету: win rate vs implied probability
    print(f"\n{'='*85}")
    print(f"  EDGE (win rate - implied probability)")
    print(f"{'='*85}\n")

    for bi, bs in enumerate(bucket_stats):
        if bs.total == 0:
            continue
        _, lo, hi = BUCKETS[bi]
        mid = (lo + hi) / 2
        implied = mid * 100
        edge = bs.win_rate - implied
        bar_len = int(abs(edge) * 2)
        if edge > 0:
            bar = "+" * bar_len
            print(f"  {bs.label:14s} | WR {bs.win_rate:6.2f}% vs implied {implied:5.1f}% | edge {edge:+6.2f}% |{bar}")
        else:
            bar = "-" * bar_len
            print(f"  {bs.label:14s} | WR {bs.win_rate:6.2f}% vs implied {implied:5.1f}% | edge {edge:+6.2f}% |{bar}")


if __name__ == "__main__":
    main()
