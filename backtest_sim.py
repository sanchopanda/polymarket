"""
Хронологическая симуляция портфеля на исторических данных.

- Виртуальный депозит (--deposit)
- Фиксированная ставка $1
- Ставить можно только если свободный баланс >= bet_size
- Деньги возвращаются только после экспирации рынка
- Хронологический порядок: события размещения и резолюции идут по времени

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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

sys.path.insert(0, os.path.dirname(__file__))
from src.api.gamma import GammaClient, Market

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
TAKER_FEE = 0.02


@dataclass
class OpportunityEvent:
    """Момент когда цена исхода попала в целевой диапазон."""
    timestamp: float         # unix ts — когда цена попала в диапазон
    market_id: str
    question: str
    outcome: str
    outcome_idx: int
    entry_price: float
    won: bool                # выиграл ли этот исход в итоге
    end_date: datetime       # когда рынок экспирирует (= резолюция)
    volume: float
    liquidity: float


@dataclass
class PlacedBet:
    """Размещённая ставка."""
    market_id: str
    question: str
    outcome: str
    entry_price: float
    amount: float
    fee: float
    shares: float
    won: bool
    placed_at: float         # unix ts
    resolves_at: datetime    # end_date


def _scan_market_history(
    clob_base: str,
    market: Market,
    winner_idx: int,
    price_min: float,
    price_max: float,
    max_expiry_days: float = 0,
    min_expiry_days: float = 0,
) -> list[OpportunityEvent]:
    """Для каждого исхода ищет ПЕРВЫЙ момент попадания цены в диапазон (онлайн)."""
    end_ts = market.end_date.replace(tzinfo=timezone.utc).timestamp() if market.end_date.tzinfo is None else market.end_date.timestamp()

    events = []
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

            ev = _scan_history_for_entry(
                history, i, market, winner_idx, end_ts,
                price_min, price_max, max_expiry_days, min_expiry_days,
            )
            if ev:
                events.append(ev)

    return events


def _scan_market_cached(
    market_data: dict,
    price_min: float,
    price_max: float,
    max_expiry_days: float = 0,
    min_expiry_days: float = 0,
) -> list[OpportunityEvent]:
    """Для каждого исхода ищет ПЕРВЫЙ момент попадания цены в диапазон (офлайн)."""
    winner_idx = market_data["winner_idx"]
    end_date_str = market_data.get("end_date")
    if not end_date_str:
        return []
    try:
        end_date = datetime.fromisoformat(end_date_str)
    except (ValueError, TypeError):
        return []

    end_ts = end_date.replace(tzinfo=timezone.utc).timestamp() if end_date.tzinfo is None else end_date.timestamp()

    # Собираем Market-подобный объект для _scan_history_for_entry
    class _M:
        pass
    m = _M()
    m.id = market_data["id"]
    m.question = market_data["question"]
    m.outcomes = market_data["outcomes"]
    m.end_date = end_date
    m.volume_num = market_data.get("volume_num", 0)
    m.liquidity_num = market_data.get("liquidity_num", 0)

    events = []
    for i, token_id in enumerate(market_data["clob_token_ids"]):
        history = market_data.get("history", {}).get(token_id, [])
        if not history:
            continue

        ev = _scan_history_for_entry(
            history, i, m, winner_idx, end_ts,
            price_min, price_max, max_expiry_days, min_expiry_days,
        )
        if ev:
            events.append(ev)

    return events


def _scan_history_for_entry(
    history: list[dict],
    outcome_idx: int,
    market,
    winner_idx: int,
    end_ts: float,
    price_min: float,
    price_max: float,
    max_expiry_days: float,
    min_expiry_days: float,
) -> Optional[OpportunityEvent]:
    """Общая логика: поиск первого попадания цены в диапазон."""
    for point in history:
        try:
            p = float(point["p"])
            t = float(point["t"])
        except (KeyError, ValueError, TypeError):
            continue

        # Фильтр по дням до экспирации
        if max_expiry_days > 0 or min_expiry_days > 0:
            days_to_expiry = (end_ts - t) / 86400
            if max_expiry_days > 0 and days_to_expiry > max_expiry_days:
                continue
            if min_expiry_days > 0 and days_to_expiry < min_expiry_days:
                continue

        if price_min <= p <= price_max and p > 0:
            return OpportunityEvent(
                timestamp=t,
                market_id=market.id,
                question=market.question,
                outcome=market.outcomes[outcome_idx],
                outcome_idx=outcome_idx,
                entry_price=p,
                won=(outcome_idx == winner_idx),
                end_date=market.end_date,
                volume=market.volume_num,
                liquidity=market.liquidity_num,
            )

    return None


def fetch_opportunities_online(
    limit: int,
    days: float,
    price_min: float,
    price_max: float,
    min_volume: float,
    workers: int,
    max_expiry_days: float = 0,
    min_expiry_days: float = 0,
) -> list[OpportunityEvent]:
    """Загрузка возможностей из API (онлайн)."""
    gamma = GammaClient(base_url=GAMMA_URL)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    closed_after = now - timedelta(days=days)

    print(f"Загрузка закрытых рынков (лимит {limit}, за {days:.0f} дней)...")
    raw_markets = gamma.fetch_closed_markets(
        limit=limit,
        min_volume=min_volume,
        min_liquidity=0.0,
        closed_after=closed_after,
        end_date_max=now,
        order_by="closedTime",
    )
    print(f"Получено {len(raw_markets)} рынков.")

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
    print(f"Сканирование истории цен ({workers} потоков)...\n")

    all_events: list[OpportunityEvent] = []
    processed = 0

    futures = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for m in candidates:
            f = pool.submit(_scan_market_history, CLOB_URL, m, winner_map[m.id], price_min, price_max, max_expiry_days, min_expiry_days)
            futures[f] = m

        for future in as_completed(futures):
            processed += 1
            if processed % 200 == 0:
                print(f"  {processed}/{len(candidates)} обработано... ({len(all_events)} возможностей)")
            all_events.extend(future.result())

    print(f"\nНайдено {len(all_events)} возможностей для ставок.")
    return all_events


def fetch_opportunities_cached(
    data_path: str,
    days: float,
    price_min: float,
    price_max: float,
    min_volume: float,
    max_expiry_days: float = 0,
    min_expiry_days: float = 0,
) -> list[OpportunityEvent]:
    """Загрузка возможностей из кеша (офлайн)."""
    with open(data_path) as f:
        data = json.load(f)

    markets = data["markets"]
    meta = data.get("meta", {})
    print(f"Загружено из кеша: {len(markets)} рынков (файл создан {meta.get('fetched_at', '?')})")

    # Фильтр по периоду
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(days=days)
    filtered = []
    for m in markets:
        if not m.get("end_date"):
            continue
        try:
            end_date = datetime.fromisoformat(m["end_date"])
        except (ValueError, TypeError):
            continue
        if end_date < cutoff or end_date > now:
            continue
        if min_volume > 0 and m.get("volume_num", 0) < min_volume:
            continue
        filtered.append(m)

    print(f"После фильтрации (период {days:.0f}д, volume≥{min_volume}): {len(filtered)} рынков")
    print(f"Сканирование истории цен (офлайн)...\n")

    all_events: list[OpportunityEvent] = []
    for idx, m in enumerate(filtered):
        if (idx + 1) % 2000 == 0:
            print(f"  {idx+1}/{len(filtered)} обработано... ({len(all_events)} возможностей)")
        all_events.extend(_scan_market_cached(m, price_min, price_max, max_expiry_days, min_expiry_days))

    print(f"\nНайдено {len(all_events)} возможностей для ставок.")
    return all_events


def simulate(
    events: list[OpportunityEvent],
    starting_balance: float = 100.0,
) -> None:
    """Хронологическая симуляция портфеля."""

    events.sort(key=lambda e: e.timestamp)

    free_balance = starting_balance
    invested = 0.0
    placed_bets: list[PlacedBet] = []
    pending_resolves: list[PlacedBet] = []
    resolved_bets: list[PlacedBet] = []

    # Дедупликация: не ставить на один и тот же market+outcome дважды
    bet_keys: set[str] = set()

    # Статистика
    total_won = 0
    total_lost = 0
    total_pnl = 0.0
    peak_capital = starting_balance
    max_drawdown = 0.0
    skipped_no_balance = 0
    skipped_duplicate = 0

    # Статистика по дням
    daily_available: dict[str, int] = defaultdict(int)
    daily_placed: dict[str, int] = defaultdict(int)
    daily_skipped_balance: dict[str, int] = defaultdict(int)

    for ev in events:
        # Сначала резолвим все ставки с end_date <= текущему моменту
        still_pending = []
        for bet in pending_resolves:
            resolve_ts = bet.resolves_at.timestamp() if bet.resolves_at.tzinfo else \
                bet.resolves_at.replace(tzinfo=timezone.utc).timestamp()
            if resolve_ts <= ev.timestamp:
                invested -= bet.amount
                if bet.won:
                    payout = bet.shares * 1.0
                    free_balance += payout
                    pnl = payout - bet.amount - bet.fee
                    total_won += 1
                else:
                    pnl = -bet.amount - bet.fee
                    total_lost += 1
                total_pnl += pnl
                resolved_bets.append(bet)
            else:
                still_pending.append(bet)
        pending_resolves = still_pending

        # Дедупликация
        key = f"{ev.market_id}:{ev.outcome_idx}"
        if key in bet_keys:
            skipped_duplicate += 1
            continue
        bet_keys.add(key)

        day_key = datetime.fromtimestamp(ev.timestamp, tz=timezone.utc).strftime('%Y-%m-%d')
        daily_available[day_key] += 1

        # Размер ставки — фиксированный $1
        bet_size = 1.0

        # Проверяем баланс
        if free_balance < bet_size:
            skipped_no_balance += 1
            daily_skipped_balance[day_key] += 1
            continue

        # Размещаем ставку
        fee = bet_size * TAKER_FEE
        shares = bet_size / ev.entry_price
        free_balance -= (bet_size + fee)
        invested += bet_size

        bet = PlacedBet(
            market_id=ev.market_id,
            question=ev.question,
            outcome=ev.outcome,
            entry_price=ev.entry_price,
            amount=bet_size,
            fee=fee,
            shares=shares,
            won=ev.won,
            placed_at=ev.timestamp,
            resolves_at=ev.end_date,
        )
        placed_bets.append(bet)
        pending_resolves.append(bet)
        daily_placed[day_key] += 1

        # Снапшот
        total_capital = free_balance + invested
        if total_capital > peak_capital:
            peak_capital = total_capital
        dd = peak_capital - total_capital
        if dd > max_drawdown:
            max_drawdown = dd

    # Финальная резолюция всех оставшихся ставок
    for bet in pending_resolves:
        invested -= bet.amount
        if bet.won:
            payout = bet.shares * 1.0
            free_balance += payout
            pnl = payout - bet.amount - bet.fee
            total_won += 1
        else:
            pnl = -bet.amount - bet.fee
            total_lost += 1
        total_pnl += pnl
        resolved_bets.append(bet)

    final_capital = free_balance + invested

    # === ОТЧЁТ ===
    total_bets = len(placed_bets)
    total_invested = sum(b.amount for b in placed_bets)
    total_fees = sum(b.fee for b in placed_bets)
    win_rate = total_won / total_bets * 100 if total_bets > 0 else 0
    roi = (final_capital - starting_balance) / starting_balance * 100

    print(f"\n{'='*70}")
    print(f"  РЕЗУЛЬТАТЫ СИМУЛЯЦИИ")
    print(f"{'='*70}")
    print(f"\n  Начальный депозит:  ${starting_balance:,.2f}")
    print(f"  Итоговый капитал:   ${final_capital:,.2f}")
    print(f"  P&L:                ${total_pnl:>+,.2f}")
    print(f"  ROI:                {roi:+.1f}%")
    print(f"  Пик капитала:       ${peak_capital:,.2f}")
    print(f"  Макс. просадка:     ${max_drawdown:,.2f}")

    print(f"\n  Всего ставок:       {total_bets}")
    print(f"  Выигрышей:          {total_won} ({win_rate:.2f}%)")
    print(f"  Проигрышей:         {total_lost}")
    print(f"  Вложено всего:      ${total_invested:,.2f}")
    print(f"  Комиссии:           ${total_fees:,.2f}")
    print(f"  Пропущено (баланс): {skipped_no_balance}")
    print(f"  Пропущено (дупл.):  {skipped_duplicate}")

    if total_bets > 0:
        avg_bet = total_invested / total_bets
        avg_mult = sum(1/b.entry_price for b in placed_bets) / total_bets
        print(f"\n  Ср. размер ставки:  ${avg_bet:.2f}")
        print(f"  Ср. множитель:      {avg_mult:.0f}x")

    # Кривая капитала
    print(f"\n{'='*70}")
    print(f"  КРИВАЯ КАПИТАЛА")
    print(f"{'='*70}\n")

    step = max(1, len(placed_bets) // 20)
    running_pnl = 0.0
    for i, bet in enumerate(placed_bets):
        if bet.won:
            running_pnl += bet.shares * 1.0 - bet.amount - bet.fee
        else:
            running_pnl += -bet.amount - bet.fee

        if (i + 1) % step == 0 or i == len(placed_bets) - 1:
            dt = datetime.fromtimestamp(bet.placed_at, tz=timezone.utc)
            capital = starting_balance + running_pnl
            bar_val = capital / starting_balance * 20
            bar = "█" * max(0, int(bar_val))
            print(f"  [{dt.strftime('%Y-%m-%d')}] #{i+1:4d} | ${capital:>8,.2f} | {bar}")

    print(f"\n  Финал: ${final_capital:,.2f} (было ${starting_balance:,.2f})")

    # Графики по дням
    all_days = sorted(set(list(daily_available.keys()) + list(daily_placed.keys())))
    if all_days:
        max_avail = max(daily_available.get(d, 0) for d in all_days) or 1
        bar_width = 40

        print(f"\n{'='*70}")
        print(f"  СТАВКИ ПО ДНЯМ (доступные / сделанные)")
        print(f"{'='*70}\n")
        print(f"  {'Дата':12s} | {'Доступно':>8s} | {'Сделано':>7s} | {'Пропущ.':>7s} | График")
        print(f"  {'-'*12}-+-{'-'*8}-+-{'-'*7}-+-{'-'*7}-+-{'-'*40}")
        for d in all_days:
            avail = daily_available.get(d, 0)
            placed = daily_placed.get(d, 0)
            skipped = daily_skipped_balance.get(d, 0)
            bar_a = "░" * int(avail / max_avail * bar_width)
            bar_p = "█" * int(placed / max_avail * bar_width)
            print(f"  {d:12s} | {avail:8d} | {placed:7d} | {skipped:7d} | {bar_p}{bar_a[len(bar_p):]}")

        print(f"\n  █ = сделанные | ░ = доступные но не сделанные")


def main():
    parser = argparse.ArgumentParser(description="Симуляция портфеля на исторических данных")
    parser.add_argument("--data", type=str, default="", help="Путь к кешу (data/markets_180d.json)")
    parser.add_argument("--limit", type=int, default=5000, help="Лимит рынков (только для онлайн)")
    parser.add_argument("--days", type=float, default=30, help="Период (дней назад)")
    parser.add_argument("--min-price", type=float, default=0.001, help="Мин. цена исхода")
    parser.add_argument("--max-price", type=float, default=0.01, help="Макс. цена исхода")
    parser.add_argument("--min-volume", type=float, default=0, help="Мин. объём рынка")
    parser.add_argument("--deposit", type=float, default=100.0, help="Начальный депозит ($)")
    parser.add_argument("--max-expiry", type=float, default=0, help="Макс. дней до экспирации (0=без ограничения)")
    parser.add_argument("--min-expiry", type=float, default=0, help="Мин. дней до экспирации")
    parser.add_argument("--workers", type=int, default=20, help="Потоков (только для онлайн)")
    args = parser.parse_args()

    expiry_label = ""
    if args.max_expiry > 0 or args.min_expiry > 0:
        parts = []
        if args.min_expiry > 0:
            parts.append(f"≥{args.min_expiry:.1f}д")
        if args.max_expiry > 0:
            parts.append(f"≤{args.max_expiry:.1f}д")
        expiry_label = f" | Экспирация: {' и '.join(parts)}"

    source = f"кеш: {args.data}" if args.data else "API (онлайн)"
    print(f"=== Симуляция портфеля ===")
    print(f"Депозит: ${args.deposit:.0f} | Цена: {args.min_price}–{args.max_price} | Период: {args.days:.0f} дней{expiry_label}")
    print(f"Источник: {source}\n")

    if args.data:
        events = fetch_opportunities_cached(
            data_path=args.data,
            days=args.days,
            price_min=args.min_price,
            price_max=args.max_price,
            min_volume=args.min_volume,
            max_expiry_days=args.max_expiry,
            min_expiry_days=args.min_expiry,
        )
    else:
        events = fetch_opportunities_online(
            limit=args.limit,
            days=args.days,
            price_min=args.min_price,
            price_max=args.max_price,
            min_volume=args.min_volume,
            workers=args.workers,
            max_expiry_days=args.max_expiry,
            min_expiry_days=args.min_expiry,
        )

    if not events:
        print("\n❌ Не найдено возможностей для ставок.")
        return

    simulate(events, starting_balance=args.deposit)


if __name__ == "__main__":
    main()
