#!/usr/bin/env python3
"""
Backtest: profit from PM lag behind Binance.

Signal: Binance is already >X% from strike, but PM ask for the winning
side is still cheap (< max_ask). This means PM hasn't caught up.

Entry: buy the side Binance points to at PM ask.
Exit: $1 payout if correct, $0 if wrong.

We take only the FIRST signal per position to avoid repeated counting.
"""

import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "fast_arb_bot.db"
STAKE_USD = 5.0  # bet size per trade


def load_data():
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()

    cur.execute("""
        SELECT p.id, p.symbol, p.winning_side, p.kalshi_reference_price
        FROM positions p
        WHERE p.winning_side IN ('yes', 'no')
          AND p.kalshi_reference_price IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM edge_ticks et
              WHERE et.position_id = p.id AND et.binance_price IS NOT NULL
          )
    """)
    positions = {}
    for pid, sym, ws, ref in cur.fetchall():
        positions[pid] = {"symbol": sym, "winning_side": ws, "ref": ref}

    pos_ids = list(positions.keys())
    ph = ",".join("?" * len(pos_ids))
    cur.execute(f"""
        SELECT position_id, seconds_to_expiry, binance_price,
               pm_yes_ask, pm_no_ask
        FROM edge_ticks
        WHERE position_id IN ({ph})
          AND binance_price IS NOT NULL
        ORDER BY position_id, seconds_to_expiry DESC
    """, pos_ids)

    ticks = defaultdict(list)
    for pid, ste, bp, pya, pna in cur.fetchall():
        ticks[pid].append({"ste": ste, "bp": bp, "pya": pya, "pna": pna})

    con.close()
    return positions, ticks


def scan_mispricing(pos, tick_list, min_dist_pct, max_ask, min_ste=0):
    """Find first tick where Binance is directional but PM is cheap.

    Returns one trade dict or None.
    """
    ref = pos["ref"]
    # Ticks are STE desc (earliest = furthest from expiry comes first)
    # Scan from earliest tick to latest (chronological)
    for tick in tick_list:  # already STE desc = chronological
        if tick["ste"] < min_ste:
            continue

        dist = (tick["bp"] - ref) / ref  # signed

        if abs(dist) < min_dist_pct / 100:
            continue

        above = dist > 0
        if above:
            ask = tick["pya"]
            side = "yes"
        else:
            ask = tick["pna"]
            side = "no"

        if ask is None or ask <= 0 or ask >= 1:
            continue
        if ask > max_ask:
            continue

        correct = pos["winning_side"] == side
        shares = STAKE_USD / ask
        pnl = (shares - STAKE_USD) if correct else (-STAKE_USD)

        return {
            "symbol": pos["symbol"],
            "side": side,
            "ask": ask,
            "shares": shares,
            "correct": correct,
            "pnl": pnl,
            "ste": tick["ste"],
            "bp": tick["bp"],
            "ref": ref,
            "dist_pct": dist,
            "winning_side": pos["winning_side"],
        }

    return None


def fmt_row(trades, label=""):
    if not trades:
        return f"  {label}--"
    n = len(trades)
    tp = sum(t["pnl"] for t in trades)
    wr = sum(1 for t in trades if t["correct"]) / n
    wins = [t for t in trades if t["correct"]]
    losses = [t for t in trades if not t["correct"]]
    aa = statistics.mean(t["ask"] for t in trades)
    aw = statistics.mean(t["pnl"] for t in wins) if wins else 0
    al = statistics.mean(t["pnl"] for t in losses) if losses else 0
    invested = n * STAKE_USD
    return (f"  {label}n={n:>3} winR={wr:>5.1%} pnl=${tp:>+8.2f} "
            f"avg=${tp/n:>+7.2f} ask={aa:.3f} "
            f"w=+${aw:.2f} l=-${abs(al):.2f} invested=${invested:.0f}")


def pr(title):
    print(f"\n{'='*78}")
    print(f"  {title}")
    print(f"{'='*78}")


def main():
    positions, ticks = load_data()
    print(f"Loaded {len(positions)} positions with Binance ticks + resolution\n")

    # ═══════════════════════════════════════════════════════════════════════
    # 1. MAIN SWEEP
    # ═══════════════════════════════════════════════════════════════════════
    pr("PM LAG SIGNAL: Binance |dist|>X% from strike, PM ask < Y")
    print(f"\n  First matching tick per position. Stake=${STAKE_USD:.0f} per trade.\n")

    print(f"  {'dist%':>6} {'max_ask':>8} │ {'n':>4} {'winR':>6} {'totPnL':>10} "
          f"{'avgPnL':>9} {'avgAsk':>7} {'medSTE':>7} {'invested':>9}")
    print(f"  {'-'*75}")

    results_store = {}

    for min_dist in [0.02, 0.05, 0.08, 0.10, 0.15, 0.20]:
        for max_ask in [0.30, 0.40, 0.50, 0.60, 0.70, 0.80]:
            trades = []
            for pid, pos in positions.items():
                t = scan_mispricing(pos, ticks.get(pid, []), min_dist, max_ask)
                if t:
                    trades.append(t)

            key = (min_dist, max_ask)
            results_store[key] = trades

            if len(trades) < 3:
                continue

            n = len(trades)
            tp = sum(t["pnl"] for t in trades)
            wr = sum(1 for t in trades if t["correct"]) / n
            aa = statistics.mean(t["ask"] for t in trades)
            ms = statistics.median(t["ste"] for t in trades)

            print(f"  {min_dist:>5.2f}% ${max_ask:<6.2f} │ {n:>4} {wr:>5.1%} "
                  f"${tp:>+9.2f} ${tp/n:>+8.2f} ${aa:>5.3f} {ms:>5.0f}s ${n*STAKE_USD:>7.0f}")

    # ═══════════════════════════════════════════════════════════════════════
    # 2. DEEP DIVE: best combos
    # ═══════════════════════════════════════════════════════════════════════
    # Find combos with best avg_pnl and n >= 10
    scored = []
    for (md, ma), trades in results_store.items():
        if len(trades) >= 5:
            tp = sum(t["pnl"] for t in trades)
            scored.append((tp / len(trades), md, ma, trades))

    scored.sort(reverse=True)

    for avg_pnl, md, ma, trades in scored[:5]:
        pr(f"DEEP DIVE: dist>{md:.2f}%, ask<${ma:.2f} "
           f"(n={len(trades)}, avg_pnl=${avg_pnl:+.4f})")

        # By symbol
        by_sym = defaultdict(list)
        for t in trades:
            by_sym[t["symbol"]].append(t)

        print(f"\n  By symbol:")
        for sym in sorted(by_sym.keys()):
            print(fmt_row(by_sym[sym], f"{sym:5} "))

        # STE distribution
        stes = [t["ste"] for t in trades]
        print(f"\n  STE: min={min(stes):.0f}s median={statistics.median(stes):.0f}s max={max(stes):.0f}s")

        # Individual trades
        if len(trades) <= 40:
            print(f"\n  {'Sym':<5} {'Side':<4} {'Ask':>6} {'Shares':>7} {'PnL':>9} {'Dist%':>8} "
                  f"{'STE':>6} {'Won':>4}")
            print(f"  {'-'*58}")
            for t in sorted(trades, key=lambda x: x["pnl"]):
                print(f"  {t['symbol']:<5} {t['side']:<4} ${t['ask']:.3f} "
                      f"{t['shares']:>6.1f} ${t['pnl']:>+8.2f} {t['dist_pct']:>+7.3%} "
                      f"{t['ste']:>5.0f}s {t['winning_side']:>4}")

        # ROI
        invested = len(trades) * STAKE_USD
        tp = sum(t["pnl"] for t in trades)
        print(f"\n  ROI: {len(trades)} trades × ${STAKE_USD:.0f} = ${invested:.0f} invested → pnl ${tp:>+.2f} = {tp/invested:+.1%}")

    # ═══════════════════════════════════════════════════════════════════════
    # 3. TIME-RESTRICTED: only enter when STE > X (enough time to execute)
    # ═══════════════════════════════════════════════════════════════════════
    pr("TIME-RESTRICTED: only enter if STE > 60s (realistic execution)")

    print(f"\n  {'dist%':>6} {'max_ask':>8} │ {'n':>4} {'winR':>6} {'totPnL':>10} "
          f"{'avgPnL':>9} {'avgAsk':>7} {'medSTE':>7} {'invested':>9}")
    print(f"  {'-'*75}")

    for min_dist in [0.02, 0.05, 0.08, 0.10, 0.15, 0.20]:
        for max_ask in [0.30, 0.40, 0.50, 0.60, 0.70]:
            trades = []
            for pid, pos in positions.items():
                t = scan_mispricing(pos, ticks.get(pid, []),
                                    min_dist, max_ask, min_ste=60)
                if t:
                    trades.append(t)

            if len(trades) < 3:
                continue

            n = len(trades)
            tp = sum(t["pnl"] for t in trades)
            wr = sum(1 for t in trades if t["correct"]) / n
            aa = statistics.mean(t["ask"] for t in trades)
            ms = statistics.median(t["ste"] for t in trades)

            print(f"  {min_dist:>5.2f}% ${max_ask:<6.2f} │ {n:>4} {wr:>5.1%} "
                  f"${tp:>+9.2f} ${tp/n:>+8.2f} ${aa:>5.3f} {ms:>5.0f}s ${n*STAKE_USD:>7.0f}")

    # ═══════════════════════════════════════════════════════════════════════
    # 4. THE KEY QUESTION: Is the PM lag diminishing over time?
    # ═══════════════════════════════════════════════════════════════════════
    pr("PM LAG OVER LIFE OF POSITION (dist>0.05%, ask<0.50)")

    print(f"\n  How does PM pricing evolve as we approach expiry?")
    print(f"\n  {'STE range':<15} {'n_ticks':>8} {'avg_ask':>8} {'med_ask':>8} {'pct<0.50':>9}")
    print(f"  {'-'*50}")

    # Gather all ticks where |dist| > 0.05%
    all_ticks_by_ste = defaultdict(list)
    for pid, pos in positions.items():
        ref = pos["ref"]
        for tick in ticks.get(pid, []):
            dist = (tick["bp"] - ref) / ref
            if abs(dist) < 0.0005:
                continue
            above = dist > 0
            ask = tick["pya"] if above else tick["pna"]
            if ask is None or ask <= 0 or ask >= 1:
                continue

            # Bucket by STE
            ste = tick["ste"]
            if ste < 600:
                bucket = int(ste // 60) * 60
                all_ticks_by_ste[bucket].append(ask)

    for bucket in sorted(all_ticks_by_ste.keys(), reverse=True):
        asks = all_ticks_by_ste[bucket]
        if len(asks) < 10:
            continue
        lo = bucket
        hi = bucket + 60
        avg = statistics.mean(asks)
        med = statistics.median(asks)
        cheap = sum(1 for a in asks if a < 0.50) / len(asks)
        print(f"  {lo:>3}-{hi:<3}s       {len(asks):>8} ${avg:>6.3f} ${med:>6.3f} {cheap:>8.1%}")


if __name__ == "__main__":
    main()
