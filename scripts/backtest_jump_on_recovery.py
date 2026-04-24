"""
scripts/backtest_jump_on_recovery.py

Бэктест стратегии jump_paper_bot по данным market_price_history из recovery_bot.db.

Особенности:
  - Данные: тики цен из market_price_history (записывается при seconds_left <= 300)
  - Сигнал: прыжок >= jump_cents от среднего за lookback_seconds до текущего тика
  - Уровень: цена попадает в зону [level, level + offset]
  - Тайм-баккет: seconds_left <= bucket (первый сигнал на каждый bucket)
  - Исполнение: цена СЛЕДУЮЩЕГО тика после сигнала (без стакана)
  - Разрешение: winning_side из positions; если рынка нет в positions —
    выводится из последнего тика price_history (YES>=0.9 → yes, NO>=0.9 → no)
  - market_end: из positions; если нет — max(ts) по тикам рынка

Запуск:
    python3 scripts/backtest_jump_on_recovery.py
    python3 scripts/backtest_jump_on_recovery.py --jump 0.03 --stake 10 --detail
    python3 scripts/backtest_jump_on_recovery.py --levels 0.55,0.60,0.65,0.70 --buckets 60,45,30
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime

RECOVERY_DB = "data/recovery_bot.db"

LOOKBACK_SECONDS = 10.0
JUMP_CENTS = 0.05
DEPTH_LIMIT_OFFSET = 0.05
SIGNAL_LEVELS = [0.60, 0.65, 0.70]
TIME_BUCKETS = [60, 40, 30]
PAPER_STAKE_USD = 5.0


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _matching_level(price: float, levels: list[float], offset: float) -> float | None:
    for level in levels:
        if level < price <= level + offset + 1e-9:
            return level
    return None


def _active_buckets(seconds_left: float, buckets: list[int]) -> list[int]:
    return [b for b in sorted(buckets, reverse=True) if seconds_left <= b + 1e-9]


def _infer_winner(last_prices: dict[str, float], threshold: float = 0.9) -> str | None:
    """Определяет победителя по последним ценам сторон (YES/NO → yes/no)."""
    yes_p = last_prices.get("yes", 0.0)
    no_p = last_prices.get("no", 0.0)
    if yes_p >= threshold:
        return "yes"
    if no_p >= threshold:
        return "no"
    return None


def run_backtest(args: argparse.Namespace) -> None:
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # winning_side из positions (торгуемые рынки)
    winning: dict[str, str] = {}
    for row in conn.execute(
        "SELECT DISTINCT market_id, winning_side FROM positions WHERE winning_side IS NOT NULL"
    ):
        winning[row["market_id"]] = row["winning_side"]

    # market_end из positions
    market_end: dict[str, datetime] = {}
    for row in conn.execute(
        "SELECT DISTINCT market_id, market_end FROM positions WHERE market_end IS NOT NULL"
    ):
        try:
            market_end[row["market_id"]] = _dt(row["market_end"])
        except Exception:
            pass

    # Все тики, сгруппированные по (market_id, side)
    symbols_filter: set[str] | None = (
        {s.strip().upper() for s in args.symbol.split(",")} if args.symbol else None
    )
    groups: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    sym_map: dict[tuple[str, str], str] = {}
    for row in conn.execute(
        "SELECT market_id, symbol, side, ts, price "
        "FROM market_price_history ORDER BY market_id, side, ts"
    ):
        if symbols_filter and row["symbol"].upper() not in symbols_filter:
            continue
        key = (row["market_id"], row["side"])
        groups[key].append((_dt(row["ts"]), float(row["price"])))
        sym_map[key] = row["symbol"]

    # Для рынков без записи в positions: выводим winner из последнего тика,
    # market_end — из max(ts) по всем сторонам рынка.
    last_price_by_market: dict[str, dict[str, float]] = defaultdict(dict)
    last_ts_by_market: dict[str, datetime] = {}
    for (market_id, side), ticks in groups.items():
        if ticks:
            last_ts, last_price = ticks[-1]
            last_price_by_market[market_id][side] = last_price
            if market_id not in last_ts_by_market or last_ts > last_ts_by_market[market_id]:
                last_ts_by_market[market_id] = last_ts

    inferred_count = 0
    for market_id, last_prices in last_price_by_market.items():
        if market_id in winning:
            continue
        winner = _infer_winner(last_prices, threshold=args.resolve_threshold)
        if winner is None:
            continue
        winning[market_id] = winner
        inferred_count += 1
        if market_id not in market_end and market_id in last_ts_by_market:
            market_end[market_id] = last_ts_by_market[market_id]

    print(f"Рынки: {len(last_price_by_market)} в price_history  "
          f"| из positions={len(last_price_by_market) - inferred_count}  "
          f"| выведено из тиков={inferred_count}  "
          f"| без исхода={len(last_price_by_market) - len([m for m in last_price_by_market if m in winning])}")

    jump_cents: float = args.jump
    stake: float = args.stake
    lookback_s: float = args.lookback
    offset: float = args.offset
    signal_levels: list[float] = [float(x) for x in args.levels.split(",")]
    buckets: list[int] = sorted([int(x) for x in args.buckets.split(",")], reverse=True)

    Trade = dict  # {symbol, side, bucket, level, signal_price, avg_prev, entry_price, won, pnl, seconds_left}
    trades: list[Trade] = []

    for (market_id, side), ticks in groups.items():
        if market_id not in winning or market_id not in market_end:
            continue

        winner = winning[market_id]
        end_dt = market_end[market_id]
        sym = sym_map.get((market_id, side), "?")

        fired_buckets: set[int] = set()
        # sliding window: список (ts, price) за последние lookback_s секунд
        window: list[tuple[datetime, float]] = []

        for i, (ts, price) in enumerate(ticks):
            seconds_left = (end_dt - ts).total_seconds()
            if seconds_left <= 0:
                break

            # Выкидываем из окна всё старше lookback_s
            window = [(t, p) for t, p in window if (ts - t).total_seconds() <= lookback_s]

            # avg_prev = среднее цен в окне (до текущего тика)
            if window:
                avg_prev = sum(p for _, p in window) / len(window)
            else:
                # Добавляем текущий тик в окно и переходим к следующему
                window.append((ts, price))
                continue

            # Добавляем текущий тик в окно
            window.append((ts, price))

            # Проверяем сигнал
            if price - avg_prev + 1e-9 < jump_cents:
                continue

            level = _matching_level(price, signal_levels, offset)
            if level is None:
                continue

            applicable = [b for b in _active_buckets(seconds_left, buckets) if b not in fired_buckets]
            if not applicable:
                continue

            # Исполнение по следующему тику
            if i + 1 >= len(ticks):
                continue
            next_ts, next_price = ticks[i + 1]
            if (end_dt - next_ts).total_seconds() <= 0:
                continue
            if not (0.0 < next_price < 1.0):
                continue

            entry_price = next_price
            filled_shares = stake / entry_price
            total_cost = stake
            won = (winner == side)
            pnl = filled_shares * 0.98 - total_cost if won else -total_cost

            for bucket in applicable:
                fired_buckets.add(bucket)
                trades.append({
                    "symbol": sym,
                    "side": side,
                    "bucket": bucket,
                    "level": level,
                    "signal_price": price,
                    "avg_prev": avg_prev,
                    "jump": price - avg_prev,
                    "entry_price": entry_price,
                    "won": won,
                    "pnl": pnl,
                    "seconds_left": seconds_left,
                    "market_id": market_id,
                })

    # ── Вывод ─────────────────────────────────────────────────────────────────

    if not trades:
        print("Нет сигналов. Попробуй снизить --jump или изменить --levels.")
        return

    n = len(trades)
    n_won = sum(1 for t in trades if t["won"])
    total_pnl = sum(t["pnl"] for t in trades)
    wr = n_won / n * 100

    print(f"\n{'='*62}")
    print("JUMP BACKTEST  (данные: market_price_history из recovery_bot.db)")
    print(f"  jump >= {jump_cents:.3f}  lookback={lookback_s:.0f}s  offset={offset:.2f}")
    print(f"  levels={signal_levels}  buckets={buckets}  stake=${stake:.1f}")
    print(f"{'='*62}")
    print(f"Сделок : {n}")
    print(f"WR     : {wr:.1f}%  ({n_won}W / {n - n_won}L)")
    print(f"PnL    : ${total_pnl:+.2f}  (avg ${total_pnl/n:+.3f}/сделку)")

    # По символу
    by_sym: dict[str, list[Trade]] = defaultdict(list)
    for t in trades:
        by_sym[t["symbol"]].append(t)
    print("\nПо символу:")
    for sym, ts_ in sorted(by_sym.items()):
        nw = sum(1 for t in ts_ if t["won"])
        pnl = sum(t["pnl"] for t in ts_)
        avg_e = sum(t["entry_price"] for t in ts_) / len(ts_)
        print(f"  {sym:<6}  n={len(ts_):>3}  WR={nw/len(ts_)*100:>5.1f}%  "
              f"avg_entry={avg_e:.3f}  PnL=${pnl:+.2f}")

    # По bucket
    by_bucket: dict[int, list[Trade]] = defaultdict(list)
    for t in trades:
        by_bucket[t["bucket"]].append(t)
    print("\nПо bucket (seconds_left <=):")
    for bucket, ts_ in sorted(by_bucket.items()):
        nw = sum(1 for t in ts_ if t["won"])
        pnl = sum(t["pnl"] for t in ts_)
        print(f"  <={bucket:>2}s  n={len(ts_):>3}  WR={nw/len(ts_)*100:>5.1f}%  PnL=${pnl:+.2f}")

    # По уровню
    by_level: dict[float, list[Trade]] = defaultdict(list)
    for t in trades:
        by_level[t["level"]].append(t)
    print("\nПо signal level:")
    for level, ts_ in sorted(by_level.items()):
        nw = sum(1 for t in ts_ if t["won"])
        pnl = sum(t["pnl"] for t in ts_)
        avg_e = sum(t["entry_price"] for t in ts_) / len(ts_)
        avg_j = sum(t["jump"] for t in ts_) / len(ts_)
        print(f"  level={level:.2f}  n={len(ts_):>3}  WR={nw/len(ts_)*100:>5.1f}%  "
              f"avg_entry={avg_e:.3f}  avg_jump={avg_j:+.3f}  PnL=${pnl:+.2f}")

    # По side
    by_side: dict[str, list[Trade]] = defaultdict(list)
    for t in trades:
        by_side[t["side"]].append(t)
    print("\nПо side:")
    for side, ts_ in sorted(by_side.items()):
        nw = sum(1 for t in ts_ if t["won"])
        pnl = sum(t["pnl"] for t in ts_)
        print(f"  {side:<4}  n={len(ts_):>3}  WR={nw/len(ts_)*100:>5.1f}%  PnL=${pnl:+.2f}")

    if args.detail:
        print(f"\n{'─'*80}")
        print(f"  {'sym':<6} {'side':<4} {'bkt':>4} {'lev':>5} {'sig':>6} {'avg':>6} {'jmp':>5} "
              f"{'ent':>6} {'sec':>6} {'W':>2} {'pnl':>8}")
        print(f"{'─'*80}")
        for t in sorted(trades, key=lambda x: (x["symbol"], x["side"], x["seconds_left"])):
            print(
                f"  {t['symbol']:<6} {t['side']:<4} {t['bucket']:>4} {t['level']:>5.2f}"
                f" {t['signal_price']:>6.3f} {t['avg_prev']:>6.3f} {t['jump']:>+5.3f}"
                f" {t['entry_price']:>6.3f} {t['seconds_left']:>6.1f}"
                f" {'✓' if t['won'] else '✗':>2} {t['pnl']:>+8.3f}"
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest jump strategy on recovery_bot price data")
    ap.add_argument("--db",                default=RECOVERY_DB)
    ap.add_argument("--jump",             type=float, default=JUMP_CENTS,         help="Мин. прыжок от avg_prev")
    ap.add_argument("--lookback",         type=float, default=LOOKBACK_SECONDS,   help="Окно avg_prev, секунды")
    ap.add_argument("--offset",           type=float, default=DEPTH_LIMIT_OFFSET, help="Ширина зоны сигнала")
    ap.add_argument("--levels",           default=",".join(str(x) for x in SIGNAL_LEVELS), help="Уровни через запятую")
    ap.add_argument("--buckets",          default=",".join(str(x) for x in TIME_BUCKETS),  help="Баккеты секунд")
    ap.add_argument("--stake",            type=float, default=PAPER_STAKE_USD,    help="Ставка USD")
    ap.add_argument("--resolve-threshold", type=float, default=0.9,               help="Порог цены для вывода победителя из тиков")
    ap.add_argument("--symbol",            default=None,                           help="Фильтр символов через запятую, напр. BTC,ETH")
    ap.add_argument("--detail",           action="store_true",                    help="Вывести все сделки")
    args = ap.parse_args()
    run_backtest(args)


if __name__ == "__main__":
    main()
