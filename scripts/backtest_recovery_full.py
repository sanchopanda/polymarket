#!/usr/bin/env python3
"""
scripts/backtest_recovery_full.py

Backtest recovery_bot strategy (trade-based signals) over last N days:
  Run A: Base strategy — one bet per market, first valid signal wins
  Run B: Repeat opposite — hedge bet on opposite side when it also fires
  Comparison.

Data: pm_trades from data/backtest.db.
Signal detection: touch <= 0.38, then armed in [0.65, 0.68].
Filters: confirm_delay + confirm_min_price (per symbol), min_seconds_to_expiry.
Repeat opposite: no confirm, no min_expiry; top_price 0.70 (0.80 in last 20s).
"""
from __future__ import annotations

import argparse
import sqlite3
import time as _time
from collections import defaultdict
from datetime import datetime, timezone

DB = "data/backtest.db"

BOTTOM, ENTRY, TOP = 0.38, 0.65, 0.68
MAX_EXPIRY = 240

CONFIRM_DELAY = {"BTC": 0.2, "SOL": 2.0, "DOGE": 2.0, "BNB": 2.0, "XRP": 2.0}
CONFIRM_DELAY_DEF = 1.0
CONFIRM_MIN = {"BTC": 0.60, "DOGE": 0.65, "BNB": 0.65, "XRP": 0.66}
CONFIRM_MIN_DEF = 0.63
MIN_EXPIRY = {"ETH": 120, "BTC": 20}

REPEAT_TOP = 0.70
REPEAT_TOP_FINAL = 0.80
REPEAT_FINAL_WINDOW = 20
REPEAT_TOUCH_MAX = 0.39
REPEAT_GAP_MIN = 10

FEE = 0.02


def find_signal(
    trades: list[tuple[float, float]],
    mkt_end: float,
    sym: str,
    for_repeat: bool = False,
) -> dict | None:
    """Find first recovery signal: touch <= BOTTOM then price in [ENTRY, TOP].

    for_repeat=True skips confirm delay and min_expiry (repeat bets bypass these).
    Returns dict with signal info, or None.
    """
    delay = 0 if for_repeat else CONFIRM_DELAY.get(sym, CONFIRM_DELAY_DEF)
    minp = 0 if for_repeat else CONFIRM_MIN.get(sym, CONFIRM_MIN_DEF)
    minexp = None if for_repeat else MIN_EXPIRY.get(sym)

    touched = False
    touch_ts = touch_price = 0.0

    for i, (ts, price) in enumerate(trades):
        sl = mkt_end - ts
        if sl <= 0:
            break

        if not touched:
            if price <= BOTTOM and sl <= MAX_EXPIRY:
                touched, touch_ts, touch_price = True, ts, price
            continue

        # Overshot → reset (allows new touch later)
        if price > TOP:
            touched = False
            continue

        if price >= ENTRY:
            if minexp is not None and sl < minexp:
                continue

            # Confirm delay: read latest trade price at armed_ts + delay
            if delay > 0:
                tgt = ts + delay
                cp = price
                for j in range(i + 1, len(trades)):
                    if trades[j][0] > tgt:
                        break
                    cp = trades[j][1]
                if cp < minp:
                    return dict(skip=True, armed_ts=ts, touch_ts=touch_ts,
                                touch_price=touch_price, sl=sl, confirm=cp)

            return dict(skip=False, armed_ts=ts, touch_ts=touch_ts,
                        touch_price=touch_price, sl=sl)

    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Recovery bot full backtest")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--interval", type=int, default=5)
    ap.add_argument("--symbol", default=None, help="Filter by symbol, e.g. BTC")
    ap.add_argument("--stake", type=float, default=1.0)
    ap.add_argument("--entry-price", type=float, default=None,
                    help="Override fill price (default: top_price, conservative)")
    ap.add_argument("--detail", action="store_true")
    o = ap.parse_args()

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("Ensuring index on pm_trades(market_id, outcome, ts)...")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pt_mo_ts "
        "ON pm_trades(market_id, outcome, ts)"
    )
    conn.commit()

    since = datetime.utcfromtimestamp(
        datetime.utcnow().timestamp() - o.days * 86400
    ).strftime("%Y-%m-%d %H:%M:%S")

    q = (
        "SELECT market_id, symbol, market_end, winning_side FROM markets "
        "WHERE interval_minutes=? AND winning_side IS NOT NULL AND market_end>=?"
    )
    p: list = [o.interval, since]
    if o.symbol:
        q += " AND symbol=?"
        p.append(o.symbol)
    mkts = conn.execute(q, p).fetchall()

    print(f"Markets: {len(mkts)}  interval={o.interval}m  days={o.days}")
    if o.symbol:
        print(f"Symbol: {o.symbol}")
    fill_price = o.entry_price or TOP
    repeat_fill = o.entry_price or REPEAT_TOP
    repeat_fill_final = o.entry_price or REPEAT_TOP_FINAL
    print(f"Config: bottom={BOTTOM} entry={ENTRY} top={TOP} max_expiry={MAX_EXPIRY}s")
    print(f"Fill price: {fill_price:.3f} (base)  {repeat_fill:.3f}/{repeat_fill_final:.3f} (repeat)")
    print(f"Repeat: top={REPEAT_TOP} top_final={REPEAT_TOP_FINAL}@{REPEAT_FINAL_WINDOW}s "
          f"touch_max={REPEAT_TOUCH_MAX} gap_min={REPEAT_GAP_MIN}s")

    base_bets: list[dict] = []
    repeat_bets: list[dict] = []
    skipped_fake_drop = 0
    t0 = _time.monotonic()

    for idx, m in enumerate(mkts):
        if idx and idx % 5000 == 0:
            print(f"  {idx}/{len(mkts)} ({_time.monotonic() - t0:.0f}s)")

        mid = m["market_id"]
        sym = m["symbol"]
        ws = m["winning_side"]
        me = datetime.strptime(
            m["market_end"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc).timestamp()
        cutoff = me - MAX_EXPIRY - 60

        sides: dict[str, list[tuple[float, float]]] = {}
        for out, sd in [("Down", "no"), ("Up", "yes")]:
            rows = conn.execute(
                "SELECT ts, price FROM pm_trades "
                "WHERE market_id=? AND outcome=? AND ts>? ORDER BY ts",
                (mid, out, cutoff),
            ).fetchall()
            sides[sd] = [(r["ts"], r["price"]) for r in rows]

        # Find base signals (with confirm + min_expiry)
        sigs: dict[str, dict] = {}
        for sd in ("yes", "no"):
            if sides[sd]:
                sigs[sd] = find_signal(sides[sd], me, sym)

        valid = {s: g for s, g in sigs.items() if g and not g.get("skip")}
        if not valid:
            if any(g and g.get("skip") for g in sigs.values()):
                skipped_fake_drop += 1
            continue

        # Base = first armed signal
        first_s, first_g = min(valid.items(), key=lambda x: x[1]["armed_ts"])
        won = ws == first_s
        sh = o.stake / fill_price
        base_bets.append(dict(
            sym=sym, side=first_s, won=won, sl=first_g["sl"],
            tp=first_g["touch_price"], armed=first_g["armed_ts"],
            pnl=sh * (1 - FEE) - o.stake if won else -o.stake,
            entry=fill_price, mid=mid,
        ))

        # Repeat opposite: look for signal on other side (no confirm, no min_expiry)
        opp = "yes" if first_s == "no" else "no"
        if not sides[opp]:
            continue
        og = find_signal(sides[opp], me, sym, for_repeat=True)
        if not og or og.get("skip"):
            continue
        gap = og["armed_ts"] - first_g["armed_ts"]
        if gap < REPEAT_GAP_MIN or og["touch_price"] >= REPEAT_TOUCH_MAX:
            continue

        rfill = repeat_fill_final if og["sl"] <= REPEAT_FINAL_WINDOW else repeat_fill
        rwon = ws == opp
        rsh = o.stake / rfill
        repeat_bets.append(dict(
            sym=sym, side=opp, won=rwon, sl=og["sl"],
            tp=og["touch_price"], armed=og["armed_ts"],
            pnl=rsh * (1 - FEE) - o.stake if rwon else -o.stake,
            entry=rfill, mid=mid, gap=gap, orig=first_s,
        ))

    elapsed = _time.monotonic() - t0
    print(f"Done in {elapsed:.0f}s  (fake_drop skips: {skipped_fake_drop})\n")

    show("BASE STRATEGY (one bet per market)", base_bets, o)
    print()
    show("REPEAT OPPOSITE ONLY (hedge bet on other side)", repeat_bets, o)
    print()
    show("COMBINED (base + repeat opposite)", base_bets + repeat_bets, o)

    if repeat_bets:
        print(f"\n{'─'*70}")
        print("COMPARISON")
        print(f"{'─'*70}")
        bn, rn = len(base_bets), len(repeat_bets)
        bw = sum(b["won"] for b in base_bets)
        rw = sum(b["won"] for b in repeat_bets)
        bp = sum(b["pnl"] for b in base_bets)
        rp = sum(b["pnl"] for b in repeat_bets)
        be_base = fill_price / (1 - FEE) * 100
        be_rpt = repeat_fill / (1 - FEE) * 100
        print(f"  Base:   {bn:>5} bets  WR={bw / bn * 100:.1f}%  PnL=${bp:+.2f}  "
              f"(${bp / bn:+.3f}/bet)  breakeven={be_base:.1f}%")
        print(f"  Repeat: {rn:>5} bets  WR={rw / rn * 100:.1f}%  PnL=${rp:+.2f}  "
              f"(${rp / rn:+.3f}/bet)  breakeven={be_rpt:.1f}%")
        cp = bp + rp
        cn = bn + rn
        cw = bw + rw
        print(f"  Combo:  {cn:>5} bets  WR={cw / cn * 100:.1f}%  PnL=${cp:+.2f}  "
              f"(${cp / cn:+.3f}/bet)")
        print(f"\n  Repeat adds: +{rn} bets, ${rp:+.2f} PnL")
        print(f"  Avg gap (repeat): {sum(b.get('gap', 0) for b in repeat_bets) / rn:.1f}s")


def show(title: str, bets: list[dict], o) -> None:
    print(f"{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    if not bets:
        print("  Нет ставок.")
        return

    n = len(bets)
    w = sum(b["won"] for b in bets)
    p = sum(b["pnl"] for b in bets)
    print(f"  Ставок: {n}  WR: {w / n * 100:.1f}% ({w}W/{n - w}L)"
          f"  PnL: ${p:+.2f} (${p / n:+.3f}/bet)")

    by_sym: dict[str, list] = defaultdict(list)
    for b in bets:
        by_sym[b["sym"]].append(b)
    print(f"\n  По символу:")
    for s, bs in sorted(by_sym.items()):
        bw = sum(b["won"] for b in bs)
        bp = sum(b["pnl"] for b in bs)
        ae = sum(b["entry"] for b in bs) / len(bs)
        at = sum(b["tp"] for b in bs) / len(bs)
        print(f"    {s:<5} n={len(bs):>5}  WR={bw / len(bs) * 100:>5.1f}%"
              f"  entry={ae:.3f}  avg_touch={at:.3f}  PnL=${bp:+.2f}")

    by_side: dict[str, list] = defaultdict(list)
    for b in bets:
        by_side[b["side"]].append(b)
    print(f"\n  По стороне:")
    for s, bs in sorted(by_side.items()):
        bw = sum(b["won"] for b in bs)
        bp = sum(b["pnl"] for b in bs)
        print(f"    {s:<4} n={len(bs):>5}  WR={bw / len(bs) * 100:>5.1f}%  PnL=${bp:+.2f}")

    bkts = [(0, 30), (30, 60), (60, 90), (90, 120), (120, 180), (180, 240)]
    by_bkt: dict[tuple, list] = defaultdict(list)
    for b in bets:
        for lo, hi in bkts:
            if lo <= b["sl"] < hi:
                by_bkt[(lo, hi)].append(b)
                break
    print(f"\n  По времени до экспирации:")
    for (lo, hi), bs in sorted(by_bkt.items()):
        bw = sum(b["won"] for b in bs)
        bp = sum(b["pnl"] for b in bs)
        print(f"    {lo:>3}-{hi:<3}s  n={len(bs):>5}  WR={bw / len(bs) * 100:>5.1f}%  PnL=${bp:+.2f}")

    if o.detail and len(bets) < 300:
        print(f"\n  {'sym':<5} {'side':<4} {'touch':>6} {'sec':>5} "
              f"{'gap':>5} {'W':>2} {'pnl':>+8}")
        for b in sorted(bets, key=lambda x: x["armed"]):
            gap = f"{b['gap']:>5.0f}" if "gap" in b else "    -"
            print(f"    {b['sym']:<5} {b['side']:<4} {b['tp']:>6.3f} "
                  f"{b['sl']:>5.0f} {gap} "
                  f"{'W' if b['won'] else 'L':>2} {b['pnl']:>+8.3f}")


if __name__ == "__main__":
    main()
