"""
research_bot/backtest.py

Бэктест стратегии "Binance противоречит CL тику → ставим ЗА CL тик, HOLD до исхода".

Запуск:
  python3 -m research_bot.backtest [путь_к_csv]
  python3 -m research_bot.backtest                  # берёт correlate_all.csv
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from statistics import mean, median

from research_bot.analyze_correlate import enrich_with_ref_price


DEFAULT_CSV = Path("research_bot/data/correlate_all.csv")


def _f(val: str | None) -> float | None:
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _load(path: Path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def _is_signal(row: dict) -> bool:
    """Binance противоречит CL тику:
       CL up   + Binance < CL  (delta < 0)
       CL down + Binance > CL  (delta > 0)
    """
    cl_dir = row.get("cl_direction")
    delta = _f(row.get("binance_cl_delta_pct"))
    if cl_dir not in ("up", "down") or delta is None:
        return False
    return (cl_dir == "up" and delta < 0) or (cl_dir == "down" and delta > 0)


def _backtest_hold(signals: list[dict], bet: float, label: str) -> None:
    """Покупаем ЗА CL тик по pm_ask_before, держим до исхода рынка."""
    print(f"\n{'─'*60}")
    print(f"  HOLD до исхода | ставка ${bet:.2f} | {label}")
    print(f"{'─'*60}")

    total_cost = 0.0
    total_payout = 0.0
    wins = losses = 0
    entry_prices = []
    pnls = []

    for r in signals:
        cl_dir = r["cl_direction"]
        ask_before = _f(r["pm_ask_before"])
        outcome = r.get("market_outcome")
        if outcome not in ("yes", "no"):
            continue

        # Цена входа — pm_entry_t1 (через 1s после сигнала, реалистичная)
        # Если t1 спайковая (отклонилась от ask_before больше чем на 30c) — берём ask_before
        entry_t1 = _f(r.get("pm_entry_t1"))
        if entry_t1 is not None and abs(entry_t1 - ask_before) <= 0.30:
            entry = entry_t1
        else:
            entry = ask_before

        # CL up → покупаем YES по entry
        # CL down → покупаем NO по (1 - entry)
        unit_cost = entry if cl_dir == "up" else 1.0 - entry
        if unit_cost <= 0.05 or unit_cost >= 0.95:
            continue  # нереалистичные цены / нет ликвидности

        n_shares = bet / unit_cost
        won = (cl_dir == "up" and outcome == "yes") or \
              (cl_dir == "down" and outcome == "no")
        payout = n_shares * 1.0 if won else 0.0
        pnl = payout - bet

        total_cost += bet
        total_payout += payout
        entry_prices.append(unit_cost)
        pnls.append(pnl)
        if won:
            wins += 1
        else:
            losses += 1

    n = wins + losses
    if n == 0:
        print("  Нет данных")
        return

    total_pnl = total_payout - total_cost
    win_pnls = [p for p in pnls if p > 0]
    loss_pnls = [p for p in pnls if p < 0]

    print(f"  Сделок:           {n}")
    print(f"  Цена входа:       avg={mean(entry_prices)*100:.1f}c  median={median(entry_prices)*100:.1f}c")
    print(f"  Win rate:         {wins}/{n} = {wins/n*100:.1f}%")
    print(f"  Вложено:          ${total_cost:.2f}")
    print(f"  Получено:         ${total_payout:.2f}")
    print(f"  PnL:              ${total_pnl:+.2f}  (ROI {total_pnl/total_cost*100:+.1f}%)")
    print(f"  Avg PnL/сделку:   ${total_pnl/n:+.3f}")
    if win_pnls:
        print(f"  Avg win:          ${mean(win_pnls):+.3f}")
    if loss_pnls:
        print(f"  Avg loss:         ${mean(loss_pnls):+.3f}")


def _filter_signals(rows: list[dict], active_only: bool = False,
                    min_cl_delta: float = 0.0) -> list[dict]:
    result = []
    for r in rows:
        if not _is_signal(r):
            continue
        ask = _f(r.get("pm_ask_before"))
        if ask is None:
            continue
        if active_only and not (0.15 <= ask <= 0.85):
            continue
        if min_cl_delta > 0:
            cd = _f(r.get("cl_delta_pct"))
            if cd is None or abs(cd) < min_cl_delta:
                continue
        result.append(r)
    return result


def _ticks_per_market_stats(rows: list[dict]) -> None:
    """Считает количество тиков на рынок для 5m и 15m."""
    from collections import defaultdict
    from datetime import datetime, timedelta

    def market_key(row: dict):
        ts_str = row.get("ts_utc", "").strip()
        mins_rem = _f(row.get("minutes_remaining"))
        sym = row.get("symbol", "").strip()
        if not ts_str or mins_rem is None or not sym:
            return None
        try:
            ts = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        interval = 15 if mins_rem > 5 else 5
        market_end = ts + timedelta(minutes=mins_rem)
        me = market_end.replace(second=0, microsecond=0)
        remainder = me.minute % 5
        if remainder:
            me = me + timedelta(minutes=(5 - remainder))
        return f"{sym}_{interval}m_{me.strftime('%Y%m%d_%H%M')}"

    markets_5m: dict[str, int] = defaultdict(int)
    markets_15m: dict[str, int] = defaultdict(int)
    for row in rows:
        key = market_key(row)
        if key is None:
            continue
        if "_5m_" in key:
            markets_5m[key] += 1
        else:
            markets_15m[key] += 1

    print("\n── Тиков на рынок ───────────────────────────────────")
    for label, d in [("5m", markets_5m), ("15m", markets_15m)]:
        if not d:
            print(f"  {label}: нет данных")
            continue
        counts = sorted(d.values())
        n = len(counts)
        print(f"  {label}: рынков={n}  "
              f"min={counts[0]}  median={counts[n//2]}  "
              f"max={counts[-1]}  avg={sum(counts)/n:.1f}")


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    if not path.exists():
        print(f"Файл не найден: {path}")
        return

    rows = _load(path)
    print(f"Файл: {path}")
    print(f"Строк: {len(rows)}")
    _ticks_per_market_stats(rows)

    print("Загружаем исходы рынков (closePrice)...")
    enrich_with_ref_price(rows, path)
    outcomes = sum(1 for r in rows if r.get("market_outcome") in ("yes", "no"))
    print(f"С известным исходом: {outcomes}/{len(rows)}")

    BET = 1.00

    configs = [
        ("Все события", False, 0.0),
        ("Активная зона 0.15–0.85", True, 0.0),
        ("Активная зона + |cl_delta|>=0.03%", True, 0.030),
        ("Активная зона + |cl_delta|>=0.05%", True, 0.050),
    ]

    for label, active, min_delta in configs:
        sigs = _filter_signals(rows, active_only=active, min_cl_delta=min_delta)

        print(f"\n{'='*60}")
        print(f"  {label} — {len(sigs)} сигналов")
        print(f"{'='*60}")

        if not sigs:
            print("  Нет сигналов")
            continue

        _backtest_hold(sigs, BET, label)


if __name__ == "__main__":
    main()
