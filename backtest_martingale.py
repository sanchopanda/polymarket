"""
Симуляция портфеля с Мартингейлом на исторических данных.

Логика:
- Начальные ставки по $1 на исходы в заданном ценовом диапазоне (--min-price / --max-price)
- Если ставка проигрывает → эскалация: удвоение ставки на новом рынке
  в фиксированном диапазоне 0.40–0.50 (мультипликатор ~2x)
- Серия заканчивается при выигрыше (профит) или достижении --max-depth (потеря)
- Деньги залочены до экспирации рынка

Запуск:
  python backtest_martingale.py --data data/markets_180d.json --days 30 --deposit 500 --max-depth 4
"""

from __future__ import annotations

import sys
import os
import json
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

TAKER_FEE = 0.02
ESCALATION_PRICE_MIN = 0.40
ESCALATION_PRICE_MAX = 0.50


@dataclass
class Opportunity:
    """Момент когда цена исхода попала в целевой диапазон."""
    timestamp: float
    market_id: str
    question: str
    outcome: str
    outcome_idx: int
    entry_price: float
    won: bool
    end_date: datetime
    volume: float
    liquidity: float
    is_escalation: bool = False  # True = диапазон 0.40-0.50


@dataclass
class Bet:
    market_id: str
    outcome: str
    entry_price: float
    amount: float
    fee: float
    shares: float
    won: bool
    placed_at: float
    resolves_at: datetime
    series_id: int
    depth: int


@dataclass
class Series:
    id: int
    bets: list[Bet] = field(default_factory=list)
    status: str = "active"  # active, waiting, won, abandoned
    current_depth: int = 0
    next_bet_size: float = 0.0  # размер следующей ставки при эскалации

    @property
    def total_invested(self) -> float:
        return sum(b.amount + b.fee for b in self.bets)

    @property
    def pnl(self) -> float:
        total = 0.0
        for b in self.bets:
            if b.won:
                total += b.shares * 1.0 - b.amount - b.fee
            else:
                total += -b.amount - b.fee
        return total


def _scan_opportunities_cached(
    data_path: str,
    days: float,
    initial_price_min: float,
    initial_price_max: float,
    min_volume: float,
    max_expiry_days: float,
) -> list[Opportunity]:
    """Загружает возможности из кеша для обоих диапазонов цен."""
    with open(data_path) as f:
        data = json.load(f)

    markets = data["markets"]
    meta = data.get("meta", {})
    print(f"Загружено из кеша: {len(markets)} рынков (файл создан {meta.get('fetched_at', '?')})")

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
        if end_date < cutoff or end_date > now:
            continue
        if min_volume > 0 and m.get("volume_num", 0) < min_volume:
            continue
        filtered.append(m)

    print(f"После фильтрации (период {days:.0f}д, volume≥{min_volume}): {len(filtered)} рынков")
    print(f"Сканирование историй цен...\n")

    all_opps: list[Opportunity] = []

    for idx, m in enumerate(filtered):
        if (idx + 1) % 2000 == 0:
            print(f"  {idx+1}/{len(filtered)} обработано... ({len(all_opps)} возможностей)")

        winner_idx = m["winner_idx"]
        try:
            end_date = datetime.fromisoformat(m["end_date"])
        except (ValueError, TypeError):
            continue

        end_ts = end_date.replace(tzinfo=timezone.utc).timestamp() if end_date.tzinfo is None else end_date.timestamp()

        for i, token_id in enumerate(m["clob_token_ids"]):
            history = m.get("history", {}).get(token_id, [])
            if not history:
                continue

            won = (i == winner_idx)
            found_initial = False
            found_escalation = False

            for point in history:
                try:
                    p = float(point["p"])
                    t = float(point["t"])
                except (KeyError, ValueError, TypeError):
                    continue

                if p <= 0:
                    continue

                # Фильтр по дням до экспирации
                if max_expiry_days > 0:
                    days_to_expiry = (end_ts - t) / 86400
                    if days_to_expiry > max_expiry_days:
                        continue

                # Начальный диапазон
                if not found_initial and initial_price_min <= p <= initial_price_max:
                    found_initial = True
                    all_opps.append(Opportunity(
                        timestamp=t, market_id=m["id"], question=m["question"],
                        outcome=m["outcomes"][i], outcome_idx=i,
                        entry_price=p, won=won, end_date=end_date,
                        volume=m.get("volume_num", 0), liquidity=m.get("liquidity_num", 0),
                        is_escalation=False,
                    ))

                # Диапазон эскалации (0.40-0.50)
                if not found_escalation and ESCALATION_PRICE_MIN <= p <= ESCALATION_PRICE_MAX:
                    found_escalation = True
                    all_opps.append(Opportunity(
                        timestamp=t, market_id=m["id"], question=m["question"],
                        outcome=m["outcomes"][i], outcome_idx=i,
                        entry_price=p, won=won, end_date=end_date,
                        volume=m.get("volume_num", 0), liquidity=m.get("liquidity_num", 0),
                        is_escalation=True,
                    ))

                if found_initial and found_escalation:
                    break

    initial_count = sum(1 for o in all_opps if not o.is_escalation)
    escalation_count = sum(1 for o in all_opps if o.is_escalation)
    print(f"\nНайдено возможностей: {len(all_opps)} (начальные: {initial_count}, эскалация: {escalation_count})")
    return all_opps


def simulate(
    opportunities: list[Opportunity],
    starting_balance: float,
    initial_bet: float,
    max_depth: int,
) -> None:
    """Хронологическая симуляция с Мартингейлом."""

    opportunities.sort(key=lambda e: e.timestamp)

    free_balance = starting_balance
    invested = 0.0
    all_series: list[Series] = []
    pending_bets: list[Bet] = []  # ожидают резолюции
    waiting_series: list[Series] = []  # ждут эскалации

    bet_keys: set[str] = set()  # дедупликация
    series_counter = 0

    # Статистика
    peak_capital = starting_balance
    max_drawdown = 0.0
    skipped_no_balance = 0
    skipped_duplicate = 0

    daily_new_series: dict[str, int] = defaultdict(int)
    daily_escalations: dict[str, int] = defaultdict(int)

    for opp in opportunities:
        # Резолвим все ставки с end_date <= текущему моменту
        still_pending = []
        for bet in pending_bets:
            resolve_ts = bet.resolves_at.replace(tzinfo=timezone.utc).timestamp() \
                if bet.resolves_at.tzinfo is None else bet.resolves_at.timestamp()
            if resolve_ts <= opp.timestamp:
                invested -= bet.amount
                if bet.won:
                    payout = bet.shares * 1.0
                    free_balance += payout
                else:
                    pass  # потеря — amount уже вычтен

                # Обновляем серию
                series = all_series[bet.series_id]
                if bet.won:
                    series.status = "won"
                else:
                    # Проигрыш — нужна эскалация?
                    if series.current_depth < max_depth - 1:
                        series.status = "waiting"
                        series.next_bet_size = bet.amount * 2
                        waiting_series.append(series)
                    else:
                        series.status = "abandoned"
            else:
                still_pending.append(bet)
        pending_bets = still_pending

        # Дедупликация
        key = f"{opp.market_id}:{opp.outcome_idx}"
        if key in bet_keys:
            skipped_duplicate += 1
            continue
        bet_keys.add(key)

        day_key = datetime.fromtimestamp(opp.timestamp, tz=timezone.utc).strftime('%Y-%m-%d')

        if opp.is_escalation:
            # Ищем серию, ожидающую эскалации
            if not waiting_series:
                continue

            series = waiting_series[0]
            bet_size = series.next_bet_size

            if free_balance < bet_size + bet_size * TAKER_FEE:
                skipped_no_balance += 1
                continue

            # Эскалация
            waiting_series.pop(0)
            series.current_depth += 1
            series.status = "active"

            fee = bet_size * TAKER_FEE
            shares = bet_size / opp.entry_price
            free_balance -= (bet_size + fee)
            invested += bet_size

            bet = Bet(
                market_id=opp.market_id, outcome=opp.outcome,
                entry_price=opp.entry_price, amount=bet_size, fee=fee,
                shares=shares, won=opp.won, placed_at=opp.timestamp,
                resolves_at=opp.end_date, series_id=series.id,
                depth=series.current_depth,
            )
            series.bets.append(bet)
            pending_bets.append(bet)
            daily_escalations[day_key] += 1

        else:
            # Начальная ставка — новая серия
            bet_size = initial_bet

            if free_balance < bet_size + bet_size * TAKER_FEE:
                skipped_no_balance += 1
                continue

            series = Series(id=series_counter, current_depth=0)
            series_counter += 1
            all_series.append(series)

            fee = bet_size * TAKER_FEE
            shares = bet_size / opp.entry_price
            free_balance -= (bet_size + fee)
            invested += bet_size

            bet = Bet(
                market_id=opp.market_id, outcome=opp.outcome,
                entry_price=opp.entry_price, amount=bet_size, fee=fee,
                shares=shares, won=opp.won, placed_at=opp.timestamp,
                resolves_at=opp.end_date, series_id=series.id,
                depth=0,
            )
            series.bets.append(bet)
            pending_bets.append(bet)
            daily_new_series[day_key] += 1

        # Снапшот
        total_capital = free_balance + invested
        if total_capital > peak_capital:
            peak_capital = total_capital
        dd = peak_capital - total_capital
        if dd > max_drawdown:
            max_drawdown = dd

    # Финальная резолюция
    for bet in pending_bets:
        invested -= bet.amount
        if bet.won:
            payout = bet.shares * 1.0
            free_balance += payout
            all_series[bet.series_id].status = "won"
        else:
            series = all_series[bet.series_id]
            if series.status == "active":
                if series.current_depth < max_depth - 1:
                    series.status = "abandoned"  # нет больше возможностей
                else:
                    series.status = "abandoned"

    # Серии что так и остались в waiting
    for s in waiting_series:
        if s.status == "waiting":
            s.status = "abandoned"

    final_capital = free_balance + invested

    # === ОТЧЁТ ===
    total_bets = sum(len(s.bets) for s in all_series)
    total_series = len(all_series)
    won_series = sum(1 for s in all_series if s.status == "won")
    abandoned_series = sum(1 for s in all_series if s.status == "abandoned")
    total_invested = sum(s.total_invested for s in all_series)
    total_pnl = final_capital - starting_balance
    roi = total_pnl / starting_balance * 100

    print(f"\n{'='*70}")
    print(f"  РЕЗУЛЬТАТЫ СИМУЛЯЦИИ (МАРТИНГЕЙЛ)")
    print(f"{'='*70}")
    print(f"\n  Начальный депозит:  ${starting_balance:,.2f}")
    print(f"  Итоговый капитал:   ${final_capital:,.2f}")
    print(f"  P&L:                ${total_pnl:>+,.2f}")
    print(f"  ROI:                {roi:+.1f}%")
    print(f"  Пик капитала:       ${peak_capital:,.2f}")
    print(f"  Макс. просадка:     ${max_drawdown:,.2f}")

    print(f"\n  Всего серий:        {total_series}")
    print(f"  Выиграно:           {won_series} ({won_series/total_series*100:.1f}%)" if total_series > 0 else "")
    print(f"  Заброшено:          {abandoned_series}")
    print(f"  Всего ставок:       {total_bets}")
    print(f"  Вложено всего:      ${total_invested:,.2f}")
    print(f"  Пропущено (баланс): {skipped_no_balance}")
    print(f"  Пропущено (дупл.):  {skipped_duplicate}")

    # Статистика по глубинам
    depth_stats: dict[int, dict] = defaultdict(lambda: {"total": 0, "won": 0})
    for s in all_series:
        d = s.current_depth
        depth_stats[d]["total"] += 1
        if s.status == "won":
            depth_stats[d]["won"] += 1

    print(f"\n{'='*70}")
    print(f"  СТАТИСТИКА ПО ГЛУБИНЕ СЕРИЙ")
    print(f"{'='*70}\n")
    print(f"  {'Глубина':>8s} | {'Серий':>6s} | {'Выигр.':>6s} | {'Win%':>7s} | {'Ставка':>8s}")
    print(f"  {'-'*8}-+-{'-'*6}-+-{'-'*6}-+-{'-'*7}-+-{'-'*8}")
    for d in sorted(depth_stats.keys()):
        st = depth_stats[d]
        wr = st["won"] / st["total"] * 100 if st["total"] > 0 else 0
        bet_at_depth = initial_bet * (2 ** d)
        print(f"  {d:8d} | {st['total']:6d} | {st['won']:6d} | {wr:6.1f}% | ${bet_at_depth:>7.2f}")

    # Кривая капитала
    print(f"\n{'='*70}")
    print(f"  КРИВАЯ КАПИТАЛА")
    print(f"{'='*70}\n")

    all_bets = []
    for s in all_series:
        all_bets.extend(s.bets)
    all_bets.sort(key=lambda b: b.placed_at)

    step = max(1, len(all_bets) // 20)
    running_pnl = 0.0
    for i, bet in enumerate(all_bets):
        if bet.won:
            running_pnl += bet.shares * 1.0 - bet.amount - bet.fee
        else:
            running_pnl += -bet.amount - bet.fee

        if (i + 1) % step == 0 or i == len(all_bets) - 1:
            dt = datetime.fromtimestamp(bet.placed_at, tz=timezone.utc)
            capital = starting_balance + running_pnl
            bar_val = capital / starting_balance * 20
            bar = "█" * max(0, int(bar_val))
            print(f"  [{dt.strftime('%Y-%m-%d')}] #{i+1:4d} | ${capital:>8,.2f} | d{bet.depth} | {bar}")

    print(f"\n  Финал: ${final_capital:,.2f} (было ${starting_balance:,.2f})")

    # По дням
    all_days = sorted(set(list(daily_new_series.keys()) + list(daily_escalations.keys())))
    if all_days:
        print(f"\n{'='*70}")
        print(f"  СЕРИИ ПО ДНЯМ")
        print(f"{'='*70}\n")
        print(f"  {'Дата':12s} | {'Новые':>6s} | {'Эскал.':>6s}")
        print(f"  {'-'*12}-+-{'-'*6}-+-{'-'*6}")
        for d in all_days:
            ns = daily_new_series.get(d, 0)
            esc = daily_escalations.get(d, 0)
            if ns > 0 or esc > 0:
                print(f"  {d:12s} | {ns:6d} | {esc:6d}")


def main():
    parser = argparse.ArgumentParser(description="Симуляция портфеля с Мартингейлом")
    parser.add_argument("--data", type=str, required=True, help="Путь к кешу (data/markets_180d.json)")
    parser.add_argument("--days", type=float, default=30, help="Период (дней назад)")
    parser.add_argument("--min-price", type=float, default=0.60, help="Мин. цена начальной ставки")
    parser.add_argument("--max-price", type=float, default=0.70, help="Макс. цена начальной ставки")
    parser.add_argument("--min-volume", type=float, default=0, help="Мин. объём рынка")
    parser.add_argument("--deposit", type=float, default=500.0, help="Начальный депозит ($)")
    parser.add_argument("--bet", type=float, default=1.0, help="Начальная ставка ($)")
    parser.add_argument("--max-depth", type=int, default=4, help="Макс. глубина Мартингейла")
    parser.add_argument("--max-expiry", type=float, default=0, help="Макс. дней до экспирации (0=без ограничения)")
    args = parser.parse_args()

    esc_label = f"{ESCALATION_PRICE_MIN}–{ESCALATION_PRICE_MAX}"
    print(f"=== Симуляция портфеля с Мартингейлом ===")
    print(f"Депозит: ${args.deposit:.0f} | Ставка: ${args.bet:.2f} | Глубина: {args.max_depth}")
    print(f"Начальный диапазон: {args.min_price}–{args.max_price} | Эскалация: {esc_label}")
    print(f"Период: {args.days:.0f} дней" + (f" | Экспирация: ≤{args.max_expiry:.0f}д" if args.max_expiry > 0 else ""))
    print()

    opps = _scan_opportunities_cached(
        data_path=args.data,
        days=args.days,
        initial_price_min=args.min_price,
        initial_price_max=args.max_price,
        min_volume=args.min_volume,
        max_expiry_days=args.max_expiry,
    )

    if not opps:
        print("\nНе найдено возможностей для ставок.")
        return

    simulate(opps, starting_balance=args.deposit, initial_bet=args.bet, max_depth=args.max_depth)


if __name__ == "__main__":
    main()
