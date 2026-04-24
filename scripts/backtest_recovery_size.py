#!/usr/bin/env python3
"""
scripts/backtest_recovery_size.py

Бэктест recovery стратегии с анализом зависимости WR от размера сигнального трейда.
Использует trade-based сигналы (как signal_source=trade в боте).

Запуск:
    python3 scripts/backtest_recovery_size.py
    python3 scripts/backtest_recovery_size.py --symbol BTC --days 7
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone

DB = "data/backtest.db"

FIXED_BUCKETS = [
    (0,    2,    "<2"),
    (2,    5,    "2–5"),
    (5,    10,   "5–10"),
    (10,   25,   "10–25"),
    (25,   100,  "25–100"),
    (100,  500,  "100–500"),
    (500,  None, "≥500"),
]


def ts_from_str(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def pnl_per_fill(won: int, n: int, stake: float, entry: float, fee: float) -> float:
    if n == 0:
        return 0.0
    gross = (stake / entry) * (1 - fee) - stake
    return (won * gross + (n - won) * (-stake)) / n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--bottom", type=float, default=0.38)
    parser.add_argument("--entry", type=float, default=0.65)
    parser.add_argument("--top", type=float, default=0.68)
    parser.add_argument("--min-expiry", type=float, default=20.0)
    parser.add_argument("--max-expiry", type=float, default=240.0)
    parser.add_argument("--confirm-delay", type=float, default=0.2)
    parser.add_argument("--cutoff", type=int, default=30)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--fee", type=float, default=0.02)
    parser.add_argument("--full-coverage", action="store_true",
                        help="только рынки где все трейды имеют size")
    opts = parser.parse_args()

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    since_ts = datetime.utcnow().timestamp() - opts.days * 86400
    since_str = datetime.utcfromtimestamp(since_ts).strftime("%Y-%m-%d %H:%M:%S")

    extra = ""
    if opts.full_coverage:
        extra = " AND market_id NOT IN (SELECT DISTINCT market_id FROM pm_trades WHERE size IS NULL)"
    markets = conn.execute(
        f"""SELECT market_id, market_end, winning_side
           FROM markets
           WHERE symbol=? AND interval_minutes=? AND winning_side IS NOT NULL
             AND market_end >= ?{extra}""",
        (opts.symbol, opts.interval, since_str),
    ).fetchall()

    print(f"symbol={opts.symbol} interval={opts.interval}m days={opts.days}")
    print(f"bottom={opts.bottom} entry={opts.entry} top={opts.top}")
    print(f"min_expiry={opts.min_expiry}s max_expiry={opts.max_expiry}s delay={opts.confirm_delay}s")
    print(f"markets={len(markets)}")

    records: list[tuple[float | None, bool]] = []  # (signal_size, won)
    touched = 0

    for mkt in markets:
        market_end_unix = ts_from_str(mkt["market_end"])
        cutoff_ts = market_end_unix - opts.cutoff
        winning_side = mkt["winning_side"]

        for outcome_label, side in [("Down", "no"), ("Up", "yes")]:
            rows = conn.execute(
                "SELECT ts, price, size FROM pm_trades "
                "WHERE market_id=? AND outcome=? AND ts < ? ORDER BY ts",
                (mkt["market_id"], outcome_label, cutoff_ts),
            ).fetchall()
            if not rows:
                continue

            trades = [(r["ts"], r["price"], r["size"]) for r in rows]

            touch = next(((ts, p) for ts, p, _ in trades if p <= opts.bottom), None)
            if touch is None:
                continue
            touch_ts = touch[0]
            touched += 1

            if market_end_unix - touch_ts > opts.max_expiry:
                continue

            trigger_ts = touch_ts + opts.confirm_delay
            for ts, p, sz in trades:
                if ts < trigger_ts:
                    continue
                if p > opts.top:
                    break
                if opts.entry <= p <= opts.top:
                    if market_end_unix - ts < opts.min_expiry:
                        break
                    records.append((sz, winning_side == side))
                    break

    conn.close()

    total = len(records)
    if total == 0:
        print("Нет сигналов.")
        return

    won_total = sum(1 for _, w in records if w)
    with_size = [(sz, w) for sz, w in records if sz is not None]
    print(f"\ntouched={touched}  filled={total}  WR={won_total/total*100:.1f}% ({won_total}/{total})")
    print(f"with_size={len(with_size)}  without_size={total - len(with_size)}")

    if not with_size:
        print("Нет записей с size — анализ невозможен.")
        return

    stake, entry, fee = opts.stake, opts.entry, opts.fee

    # ── Фиксированные бакеты ──────────────────────────────────────────────────
    print(f"\n── По бакетам size ──────────────────────────────────────────────")
    print(f"  {'bucket':<10}  {'n':>5}  {'WR':>7}  {'PnL/fill':>10}  {'size range':>20}")
    print(f"  {'-'*60}")
    for lo, hi, label in FIXED_BUCKETS:
        if hi is None:
            bucket = [(sz, w) for sz, w in with_size if sz >= lo]
        else:
            bucket = [(sz, w) for sz, w in with_size if lo <= sz < hi]
        if not bucket:
            continue
        bn = len(bucket)
        bwon = sum(1 for _, w in bucket if w)
        bpnl = pnl_per_fill(bwon, bn, stake, entry, fee)
        bsizes = [sz for sz, _ in bucket]
        print(
            f"  {label:<10}  {bn:>5}  {bwon/bn*100:>6.1f}%  ${bpnl:>+9.3f}"
            f"  {min(bsizes):>8.1f}–{max(bsizes):<8.1f}"
        )
    # итого with_size
    n_ws = len(with_size)
    won_ws = sum(1 for _, w in with_size if w)
    print(
        f"  {'ALL':<10}  {n_ws:>5}  {won_ws/n_ws*100:>6.1f}%"
        f"  ${pnl_per_fill(won_ws, n_ws, stake, entry, fee):>+9.3f}"
    )

    # ── Кумулятивный фильтр (min_size) ────────────────────────────────────────
    sizes_sorted = sorted(sz for sz, _ in with_size)
    n_ws = len(sizes_sorted)
    # Порог = значение на каждом дециле
    thresholds = sorted({
        round(sizes_sorted[int(n_ws * p / 100)], 2)
        for p in range(0, 91, 10)
        if int(n_ws * p / 100) < n_ws
    })

    print(f"\n── Кумулятивный фильтр (≥ min_size) ────────────────────────────")
    print(f"  {'min_size':>10}  {'n':>5}  {'WR':>7}  {'PnL/fill':>10}  {'% fills':>8}")
    print(f"  {'-'*50}")
    for thr in thresholds:
        subset = [(sz, w) for sz, w in with_size if sz >= thr]
        sn = len(subset)
        if sn < 5:
            continue
        swon = sum(1 for _, w in subset if w)
        spnl = pnl_per_fill(swon, sn, stake, entry, fee)
        print(
            f"  ≥{thr:>9.2f}  {sn:>5}  {swon/sn*100:>6.1f}%  ${spnl:>+9.3f}"
            f"  {sn/total*100:>7.1f}%"
        )


if __name__ == "__main__":
    main()
