#!/usr/bin/env python3
"""
Backtest on oracle_arb_bot price_ticks + orderbook_snapshots.

Mimics the live bot logic:
- Scans ALL ticks per market chronologically (STE desc = chronological).
- Signal fires when |delta_pct| > threshold AND side ask < max_ask.
- last_bet_side logic: skip if already bet on the same side for this market.
  After betting YES, next bet must be NO (and vice versa).
- Entry price from orderbook walk if available, else from tick ask.
- Outcome from market prices at close (yes_ask/no_ask converging to 0/1).
"""

import json
import sqlite3
import statistics
import sys
from collections import defaultdict
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "oracle_arb_bot.db"
STAKE_USD = 5.0


def load_data():
    con = sqlite3.connect(str(DB_PATH))

    # 1. Get all ticks with ref price, ordered for scanning
    ticks_raw = con.execute("""
        SELECT market_id, symbol, interval_minutes, seconds_to_expiry,
               binance_price, pm_open_price, pm_yes_ask, pm_no_ask,
               pm_yes_bid, pm_no_bid, delta_pct, ts
        FROM price_ticks
        WHERE pm_open_price IS NOT NULL AND pm_open_price > 0
        ORDER BY market_id, seconds_to_expiry DESC
    """).fetchall()

    # Group ticks by market
    ticks = defaultdict(list)
    for r in ticks_raw:
        ticks[r[0]].append({
            "symbol": r[1], "interval": r[2], "ste": r[3],
            "bp": r[4], "ref": r[5],
            "ya": r[6], "na": r[7], "yb": r[8], "nb": r[9],
            "delta": r[10], "ts": r[11],
        })

    # 2. Determine outcomes from last tick per market
    outcomes = {}
    for mid, market_ticks in ticks.items():
        last = market_ticks[-1]
        if last["ste"] is None or last["ste"] > 60:
            continue
        ya = last["ya"] or 0.5
        na = last["na"] or 0.5
        if ya >= 0.85 and na <= 0.20:
            outcomes[mid] = "yes"
        elif na >= 0.85 and ya <= 0.20:
            outcomes[mid] = "no"

    # 3. Load orderbook snapshots indexed by (market_id, ts)
    ob_data = {}
    for r in con.execute("""
        SELECT market_id, ts, yes_asks, yes_bids, no_asks, no_bids
        FROM orderbook_snapshots
    """):
        ob_data[(r[0], r[1])] = {
            "yes_asks": r[2], "yes_bids": r[3],
            "no_asks": r[4], "no_bids": r[5],
        }

    con.close()
    return ticks, outcomes, ob_data


def walk_orderbook(book_json: str | None, stake_usd: float) -> float | None:
    """Walk orderbook ask levels to fill $stake_usd. Returns avg fill price or None."""
    if not book_json:
        return None
    try:
        levels = json.loads(book_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not levels:
        return None

    remaining = stake_usd
    total_shares = 0.0
    for price_str, size_str in levels:
        price = float(price_str)
        size = float(size_str)
        if price <= 0 or price >= 1.0 or size <= 0:
            continue
        can_buy_usd = price * size
        fill_usd = min(remaining, can_buy_usd)
        fill_shares = fill_usd / price
        total_shares += fill_shares
        remaining -= fill_usd
        if remaining <= 0.001:
            break

    if total_shares <= 0 or remaining > stake_usd * 0.5:
        return None
    filled_usd = stake_usd - remaining
    return filled_usd / total_shares


def get_fill_price(tick, side, ob_data, market_id, max_ask):
    """Get fill price for a trade. Returns (fill_price, had_ob) or (None, False)."""
    ask = tick["ya"] if side == "yes" else tick["na"]
    if ask is None or ask <= 0 or ask >= 1.0:
        return None, False
    if ask > max_ask:
        return None, False

    ob_key = (market_id, tick["ts"])
    ob = ob_data.get(ob_key)
    if ob:
        book_key = "yes_asks" if side == "yes" else "no_asks"
        ob_fill = walk_orderbook(ob[book_key], STAKE_USD)
        if ob_fill is not None:
            fill_price = ob_fill
        else:
            fill_price = ask
    else:
        fill_price = ask

    if fill_price > max_ask:
        return None, ob is not None
    return fill_price, ob is not None


def scan_market(market_ticks, outcome, min_delta_pct, max_ask, ob_data, market_id):
    """
    Scan all ticks for a market and return ALL trades (like the live bot).
    Respects last_bet_side: after betting YES, only NO can fire next (and vice versa).
    """
    ref = market_ticks[0]["ref"]
    symbol = market_ticks[0]["symbol"]
    interval = market_ticks[0]["interval"]

    trades = []
    last_bet_side = None

    for tick in market_ticks:  # STE desc = chronological
        ste = tick["ste"]
        if ste is None:
            continue

        delta = tick["delta"]
        if delta is None:
            continue
        if abs(delta) < min_delta_pct:
            continue

        # Determine which side to buy
        if delta > 0:
            side = "yes"
        else:
            side = "no"

        # Skip if same side as last bet (bot logic)
        if side == last_bet_side:
            continue

        fill_price, had_ob = get_fill_price(tick, side, ob_data, market_id, max_ask)
        if fill_price is None:
            continue

        correct = outcome == side
        shares = STAKE_USD / fill_price
        pnl = (shares - STAKE_USD) if correct else -STAKE_USD

        trades.append({
            "symbol": symbol, "interval": interval,
            "side": side, "ask": fill_price, "shares": shares,
            "correct": correct, "pnl": pnl,
            "ste": ste, "delta": delta, "ref": ref,
            "outcome": outcome, "had_ob": had_ob,
            "ts": tick["ts"],
        })
        last_bet_side = side

    return trades


def print_header():
    print(f"  {'delta%':>6} {'max_ask':>8} │ {'n':>4} {'winR':>6} {'totPnL':>10} "
          f"{'avgPnL':>9} {'avgAsk':>7} {'medSTE':>7} {'invested':>9}")
    print(f"  {'-'*75}")


def print_row(min_delta, max_ask, trades):
    if len(trades) < 2:
        return
    n = len(trades)
    tp = sum(t["pnl"] for t in trades)
    wr = sum(1 for t in trades if t["correct"]) / n
    aa = statistics.mean(t["ask"] for t in trades)
    ms = statistics.median(t["ste"] for t in trades)
    print(f"  {min_delta:>5.2f}% ${max_ask:<6.2f} │ {n:>4} {wr:>5.1%} "
          f"${tp:>+9.2f} ${tp/n:>+8.2f} ${aa:>5.3f} {ms:>5.0f}s ${n*STAKE_USD:>7.0f}")


def deep_dive(trades, md, ma):
    n = len(trades)
    if n < 2:
        return
    tp = sum(t["pnl"] for t in trades)
    wr = sum(1 for t in trades if t["correct"]) / n
    print(f"\n{'='*78}")
    print(f"  DEEP DIVE: delta>{md:.2f}%, ask<${ma:.2f} "
          f"(n={n}, pnl=${tp:+.2f}, winR={wr:.0%})")
    print(f"{'='*78}")

    # By symbol
    by_sym = defaultdict(list)
    for t in trades:
        by_sym[t["symbol"]].append(t)
    print(f"\n  By symbol:")
    for sym in sorted(by_sym):
        st = by_sym[sym]
        sn = len(st)
        stp = sum(t["pnl"] for t in st)
        swr = sum(1 for t in st if t["correct"]) / sn if sn else 0
        sa = statistics.mean(t["ask"] for t in st)
        print(f"    {sym:5s} n={sn:>3} winR={swr:>5.1%} pnl=${stp:>+8.2f} avgAsk={sa:.3f}")

    # By interval
    by_iv = defaultdict(list)
    for t in trades:
        by_iv[t["interval"]].append(t)
    print(f"\n  By interval:")
    for iv in sorted(by_iv):
        st = by_iv[iv]
        sn = len(st)
        stp = sum(t["pnl"] for t in st)
        swr = sum(1 for t in st if t["correct"]) / sn if sn else 0
        print(f"    {iv:>2d}m   n={sn:>3} winR={swr:>5.1%} pnl=${stp:>+8.2f}")

    # STE distribution
    stes = [t["ste"] for t in trades]
    print(f"\n  STE: min={min(stes):.0f}s med={statistics.median(stes):.0f}s max={max(stes):.0f}s")

    # Individual trades
    if n <= 60:
        print(f"\n  {'Sym':<5} {'IV':>3} {'Side':<4} {'Fill':>6} {'PnL':>9} "
              f"{'Delta%':>8} {'STE':>6} {'Won':>4} {'OB':>3}")
        print(f"  {'-'*60}")
        for t in sorted(trades, key=lambda x: x["pnl"]):
            tag = "✓" if t["correct"] else "✗"
            ob_tag = "OB" if t.get("had_ob") else "  "
            print(f"  {t['symbol']:<5} {t['interval']:>2}m {t['side']:<4} "
                  f"${t['ask']:.3f} ${t['pnl']:>+8.2f} "
                  f"{t['delta']:>+7.3f}% {t['ste']:>5.0f}s {tag}  {ob_tag}")

    invested = n * STAKE_USD
    print(f"\n  ROI: {n} × ${STAKE_USD:.0f} = ${invested:.0f} → pnl ${tp:>+.2f} = {tp/invested:+.1%}")


def main():
    ticks, outcomes, ob_data = load_data()
    print(f"Loaded {len(ticks)} markets, {len(outcomes)} with known outcome")
    print(f"Orderbook snapshots: {len(ob_data)} records\n")

    # ═══════════════════════════════════════════════════════════════
    # 1. PARAMETER SWEEP
    # ═══════════════════════════════════════════════════════════════
    print(f"{'='*78}")
    print(f"  SWEEP: delta threshold × max ask (stake=${STAKE_USD:.0f})")
    print(f"{'='*78}\n")
    print_header()

    best = []
    for min_delta in [0.02, 0.03, 0.05, 0.08, 0.10, 0.15]:
        for max_ask in [0.30, 0.40, 0.50, 0.60, 0.70]:
            trades = []
            for mid, market_ticks in ticks.items():
                if mid not in outcomes:
                    continue
                trades.extend(scan_market(market_ticks, outcomes[mid], min_delta, max_ask, ob_data, mid))
            print_row(min_delta, max_ask, trades)
            if len(trades) >= 3:
                tp = sum(t["pnl"] for t in trades)
                best.append((tp, min_delta, max_ask, trades))
    best.sort(reverse=True)

    # ═══════════════════════════════════════════════════════════════
    # 2. DEEP DIVE: top 3 combos
    # ═══════════════════════════════════════════════════════════════
    for tp, md, ma, trades in best[:3]:
        deep_dive(trades, md, ma)

    # ═══════════════════════════════════════════════════════════════
    # 3. SPECIFIC CONFIG (from command line or default)
    # ═══════════════════════════════════════════════════════════════
    if len(sys.argv) >= 3:
        md = float(sys.argv[1])
        ma = float(sys.argv[2])
        trades = []
        for mid, market_ticks in ticks.items():
            if mid not in outcomes:
                continue
            trades.extend(scan_market(market_ticks, outcomes[mid], md, ma, ob_data, mid))
        deep_dive(trades, md, ma)


if __name__ == "__main__":
    main()
