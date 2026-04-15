#!/usr/bin/env python3
"""
Backtest: can you profit by betting on Binance direction on Polymarket?

Strategy: at time T before expiry, if Binance > strike → buy YES on PM at pm_yes_ask.
If Binance < strike → buy NO at pm_no_ask. Payout = $1 if correct, $0 if wrong.

PnL per trade = (1 - ask_price) if correct, (-ask_price) if wrong.
"""

import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "fast_arb_bot.db"


def load_data(con: sqlite3.Connection):
    """Load positions with binance ticks and resolution outcomes."""
    cur = con.cursor()
    cur.execute("""
        SELECT p.id, p.symbol, p.winning_side, p.kalshi_reference_price,
               p.title, p.is_paper
        FROM positions p
        WHERE p.winning_side IN ('yes', 'no')
          AND p.kalshi_reference_price IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM edge_ticks et
              WHERE et.position_id = p.id AND et.binance_price IS NOT NULL
          )
    """)
    positions = {}
    for row in cur.fetchall():
        positions[row[0]] = {
            "symbol": row[1],
            "winning_side": row[2],
            "reference_price": row[3],
            "title": row[4],
            "is_paper": row[5],
        }

    # Load ticks
    pos_ids = list(positions.keys())
    placeholders = ",".join("?" * len(pos_ids))
    cur.execute(f"""
        SELECT position_id, seconds_to_expiry, binance_price,
               pm_yes_ask, pm_no_ask, pm_yes_bid, pm_no_bid
        FROM edge_ticks
        WHERE position_id IN ({placeholders})
          AND binance_price IS NOT NULL
        ORDER BY position_id, seconds_to_expiry DESC
    """, pos_ids)

    ticks = defaultdict(list)
    for row in cur.fetchall():
        ticks[row[0]].append({
            "ste": row[1],
            "binance_price": row[2],
            "pm_yes_ask": row[3],
            "pm_no_ask": row[4],
            "pm_yes_bid": row[5],
            "pm_no_bid": row[6],
        })

    return positions, ticks


def find_tick_at(ticks_list, target_ste, tolerance=5):
    """Find tick closest to target seconds_to_expiry within tolerance."""
    best = None
    best_diff = float("inf")
    for t in ticks_list:
        diff = abs(t["ste"] - target_ste)
        if diff < best_diff and diff <= tolerance:
            best = t
            best_diff = diff
    return best


def simulate_trade(pos, tick):
    """Simulate a single trade at the given tick.

    Returns dict with trade details or None if no valid PM price.
    """
    ref = pos["reference_price"]
    bp = tick["binance_price"]

    above_strike = bp > ref
    distance_pct = (bp - ref) / ref  # signed

    if above_strike:
        # Buy YES
        ask = tick["pm_yes_ask"]
        if ask is None or ask <= 0 or ask >= 1:
            return None
        side = "yes"
        correct = pos["winning_side"] == "yes"
    else:
        # Buy NO
        ask = tick["pm_no_ask"]
        if ask is None or ask <= 0 or ask >= 1:
            return None
        side = "no"
        correct = pos["winning_side"] == "no"

    pnl = (1.0 - ask) if correct else (-ask)

    return {
        "symbol": pos["symbol"],
        "side": side,
        "ask": ask,
        "correct": correct,
        "pnl": pnl,
        "distance_pct": distance_pct,
        "binance_price": bp,
        "reference_price": ref,
        "winning_side": pos["winning_side"],
        "ste": tick["ste"],
    }


def print_section(title):
    print(f"\n{'='*78}")
    print(f"  {title}")
    print(f"{'='*78}")


def print_pnl_table(trades, label=""):
    """Print PnL summary for a list of trades."""
    if not trades:
        print(f"  {label}No trades")
        return

    total_pnl = sum(t["pnl"] for t in trades)
    wins = [t for t in trades if t["correct"]]
    losses = [t for t in trades if not t["correct"]]
    n = len(trades)
    win_rate = len(wins) / n

    avg_win = statistics.mean(t["pnl"] for t in wins) if wins else 0
    avg_loss = statistics.mean(t["pnl"] for t in losses) if losses else 0
    avg_ask = statistics.mean(t["ask"] for t in trades)

    print(f"  {label}Trades: {n}  |  Win rate: {win_rate:.1%}  |  "
          f"Total PnL: ${total_pnl:.2f}  |  Avg PnL/trade: ${total_pnl/n:.4f}")
    print(f"  {label}Avg ask paid: ${avg_ask:.3f}  |  "
          f"Avg win: +${avg_win:.3f}  |  Avg loss: -${abs(avg_loss):.3f}")


def main():
    con = sqlite3.connect(str(DB_PATH))
    positions, ticks = load_data(con)
    con.close()

    print(f"Loaded {len(positions)} positions with Binance ticks + resolution")

    # ── PnL at different entry times ─────────────────────────────────────
    print_section("PnL BY ENTRY TIME (no filters)")
    print()
    print(f"  Strategy: at T sec before expiry, Binance > strike → buy YES @ pm_yes_ask,")
    print(f"            Binance < strike → buy NO @ pm_no_ask. $1 payout if correct.")
    print()

    entry_times = [300, 240, 180, 120, 90, 60, 45, 30, 15]

    header = f"  {'Entry':<8} {'N':>5} {'WinR':>7} {'TotPnL':>9} {'Avg PnL':>9} {'AvgAsk':>8} {'AvgWin':>8} {'AvgLoss':>8}"
    print(header)
    print(f"  {'-'*len(header)}")

    all_trades_by_time = {}

    for entry_ste in entry_times:
        trades = []
        for pos_id, pos in positions.items():
            tick = find_tick_at(ticks.get(pos_id, []), entry_ste)
            if tick is None:
                continue
            trade = simulate_trade(pos, tick)
            if trade:
                trades.append(trade)

        all_trades_by_time[entry_ste] = trades
        if not trades:
            continue

        n = len(trades)
        total_pnl = sum(t["pnl"] for t in trades)
        wins = [t for t in trades if t["correct"]]
        losses = [t for t in trades if not t["correct"]]
        win_rate = len(wins) / n
        avg_ask = statistics.mean(t["ask"] for t in trades)
        avg_win = statistics.mean(t["pnl"] for t in wins) if wins else 0
        avg_loss = statistics.mean(t["pnl"] for t in losses) if losses else 0

        print(f"  {entry_ste:>4}s   {n:>5} {win_rate:>6.1%} ${total_pnl:>+8.2f} "
              f"${total_pnl/n:>+8.4f} ${avg_ask:>6.3f} +${avg_win:>5.3f} -${abs(avg_loss):>5.3f}")

    # ── PnL with distance filter ─────────────────────────────────────────
    print_section("PnL BY ENTRY TIME + MIN DISTANCE FROM STRIKE")
    print()
    print(f"  Only take trades when |Binance - strike| / strike > threshold")

    for min_dist in [0.02, 0.05, 0.1]:
        print(f"\n  --- |distance| > {min_dist}% ---")
        print(f"  {'Entry':<8} {'N':>5} {'WinR':>7} {'TotPnL':>9} {'Avg PnL':>9} {'AvgAsk':>8}")
        print(f"  {'-'*50}")

        for entry_ste in entry_times:
            trades = all_trades_by_time.get(entry_ste, [])
            filtered = [t for t in trades if abs(t["distance_pct"]) > min_dist / 100]
            if not filtered:
                continue

            n = len(filtered)
            total_pnl = sum(t["pnl"] for t in filtered)
            win_rate = sum(1 for t in filtered if t["correct"]) / n
            avg_ask = statistics.mean(t["ask"] for t in filtered)

            print(f"  {entry_ste:>4}s   {n:>5} {win_rate:>6.1%} ${total_pnl:>+8.2f} "
                  f"${total_pnl/n:>+8.4f} ${avg_ask:>6.3f}")

    # ── PnL by symbol ────────────────────────────────────────────────────
    print_section("PnL BY SYMBOL (entry at 120s)")

    trades_120 = all_trades_by_time.get(120, [])
    if trades_120:
        by_sym = defaultdict(list)
        for t in trades_120:
            by_sym[t["symbol"]].append(t)

        print(f"\n  {'Symbol':<8} {'N':>5} {'WinR':>7} {'TotPnL':>9} {'Avg PnL':>9} {'AvgAsk':>8}")
        print(f"  {'-'*50}")

        for sym in sorted(by_sym.keys()):
            st = by_sym[sym]
            n = len(st)
            total_pnl = sum(t["pnl"] for t in st)
            win_rate = sum(1 for t in st if t["correct"]) / n
            avg_ask = statistics.mean(t["ask"] for t in st)
            print(f"  {sym:<8} {n:>5} {win_rate:>6.1%} ${total_pnl:>+8.2f} "
                  f"${total_pnl/n:>+8.4f} ${avg_ask:>6.3f}")

    # ── Distribution of ask prices when signal fires ─────────────────────
    print_section("ASK PRICE DISTRIBUTION (what does PM charge when Binance knows?)")

    for entry_ste in [300, 120, 60, 30]:
        trades = all_trades_by_time.get(entry_ste, [])
        if not trades:
            continue

        asks = [t["ask"] for t in trades]
        correct_asks = [t["ask"] for t in trades if t["correct"]]
        wrong_asks = [t["ask"] for t in trades if not t["correct"]]

        print(f"\n  Entry at {entry_ste}s:")
        print(f"    All asks:    min=${min(asks):.3f}  median=${statistics.median(asks):.3f}  "
              f"max=${max(asks):.3f}  mean=${statistics.mean(asks):.3f}")
        if correct_asks:
            print(f"    Wins' asks:  min=${min(correct_asks):.3f}  median=${statistics.median(correct_asks):.3f}  "
                  f"max=${max(correct_asks):.3f}  mean=${statistics.mean(correct_asks):.3f}")
        if wrong_asks:
            print(f"    Loss' asks:  min=${min(wrong_asks):.3f}  median=${statistics.median(wrong_asks):.3f}  "
                  f"max=${max(wrong_asks):.3f}  mean=${statistics.mean(wrong_asks):.3f}")

    # ── Worst trades ─────────────────────────────────────────────────────
    print_section("WORST TRADES (biggest losses, entry at 120s)")

    if trades_120:
        worst = sorted(trades_120, key=lambda t: t["pnl"])[:15]
        print(f"\n  {'Symbol':<6} {'Side':<5} {'Ask':>6} {'PnL':>7} {'Dist%':>8} {'Binance':>12} {'Ref':>12} {'Won':>5} {'STE':>6}")
        print(f"  {'-'*75}")
        for t in worst:
            print(f"  {t['symbol']:<6} {t['side']:<5} ${t['ask']:.3f} ${t['pnl']:>+6.3f} "
                  f"{t['distance_pct']:>+7.4%} {t['binance_price']:>11.4f} "
                  f"{t['reference_price']:>11.4f} {t['winning_side']:>5} {t['ste']:>5.0f}s")

    # ── ROI per $1 invested ──────────────────────────────────────────────
    print_section("ROI: profit per $1 of capital deployed")

    for entry_ste in entry_times:
        trades = all_trades_by_time.get(entry_ste, [])
        if not trades:
            continue
        total_invested = sum(t["ask"] for t in trades)
        total_pnl = sum(t["pnl"] for t in trades)
        roi = total_pnl / total_invested if total_invested else 0
        print(f"  {entry_ste:>4}s: invested ${total_invested:.2f}, "
              f"pnl ${total_pnl:>+.2f}, ROI {roi:>+.1%}")


if __name__ == "__main__":
    main()
