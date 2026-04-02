"""
research_bot/backtest_historical.py

Исторический бэктест стратегии "CL contradiction" на данных из markets_cache.csv.
Для каждого рынка ищем все CL тики где Binance противоречит CL, записываем
результат (выиграли/нет) и детали сигнала.

Предварительно:
  python3 -m research_bot.fetch_markets --limit 5000
  python3 -m research_bot.fetch_cl_history --days 30

Запуск:
  python3 -m research_bot.backtest_historical
  python3 -m research_bot.backtest_historical --min-delta 0.03
"""
from __future__ import annotations

import argparse
import bisect
import csv
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

DATA_DIR = Path("research_bot/data")
MARKETS_CACHE = DATA_DIR / "markets_cache.csv"
TRADES_DIR = DATA_DIR / "trades"
SIGNALS_OUT = DATA_DIR / "backtest_signals.csv"

BINANCE_SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

SIGNALS_FIELDS = [
    "market_id", "symbol", "interval_minutes", "winning_side",
    "tick_ts", "minute_of_market",
    "cl_price", "cl_prev_price", "cl_direction", "cl_delta_pct",
    "binance_price", "binance_cl_delta_pct",
    "pm_entry_price",
    "is_signal", "signal_side", "won",
]


# ── loaders ───────────────────────────────────────────────────────────────────

def _parse_dt(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def load_markets(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    result = []
    for r in rows:
        if not r.get("winning_side") or r["winning_side"] not in ("yes", "no"):
            continue
        try:
            r["_market_start"] = _parse_dt(r["market_start"])
            r["_market_end"] = _parse_dt(r["market_end"])
            r["_interval"] = int(r["interval_minutes"])
        except Exception:
            continue
        result.append(r)
    return result


def load_market_trades(market_id: str) -> dict[str, list[tuple[int, float]]]:
    """
    Load trades for a market from trades/{market_id}.csv.
    Returns dict: outcome ("Up"/"Down") -> sorted list of (timestamp, price).
    Empty dict if file not found.
    """
    path = TRADES_DIR / f"{market_id}.csv"
    if not path.exists():
        return {}
    up: list[tuple[int, float]] = []
    down: list[tuple[int, float]] = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                ts = int(row["timestamp"])
                price = float(row["price"])
                outcome = row["outcome"]
            except Exception:
                continue
            if outcome == "Up":
                up.append((ts, price))
            elif outcome == "Down":
                down.append((ts, price))
    return {"Up": sorted(up), "Down": sorted(down)}


def _last_trade_price(lookup: dict[str, list[tuple[int, float]]],
                      outcome: str, ts: int) -> Optional[float]:
    """Last traded price for outcome at or before ts."""
    pts = lookup.get(outcome, [])
    if not pts:
        return None
    # pts sorted by timestamp; find rightmost with ts <= ts
    times = [p[0] for p in pts]
    idx = bisect.bisect_right(times, ts) - 1
    if idx < 0:
        return None
    return pts[idx][1]


def load_cl_history(symbol: str) -> list[tuple[int, float]]:
    """Returns list of (ts_unix, price) sorted by time."""
    path = DATA_DIR / f"cl_history_{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"CL история не найдена: {path}\n"
            f"Запустите: python3 -m research_bot.fetch_cl_history --days 30"
        )
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            try:
                ts = int(datetime.strptime(r["ts_utc"], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=timezone.utc).timestamp())
                price = float(r["price"])
                rows.append((ts, price))
            except Exception:
                continue
    return sorted(rows)


# ── Binance klines ────────────────────────────────────────────────────────────

def fetch_binance_klines(symbol: str, start_ts: int, end_ts: int,
                         http: httpx.Client) -> dict[int, float]:
    """Returns dict minute_ts → close_price. minute_ts = floor(open_time_ms / 60000) * 60."""
    binance_sym = BINANCE_SYMBOLS[symbol]
    result: dict[int, float] = {}
    cur = start_ts * 1000  # to ms

    while cur < end_ts * 1000:
        try:
            resp = http.get(BINANCE_KLINES_URL, params={
                "symbol": binance_sym,
                "interval": "1m",
                "startTime": cur,
                "endTime": end_ts * 1000,
                "limit": 1000,
            }, timeout=15.0)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            print(f"  [binance] {symbol} klines error: {exc}")
            break

        if not batch:
            break

        for candle in batch:
            open_ms = int(candle[0])
            close_price = float(candle[4])
            minute_ts = (open_ms // 60000) * 60
            result[minute_ts] = close_price

        last_open_ms = int(batch[-1][0])
        cur = last_open_ms + 60_000  # next minute

        if len(batch) < 1000:
            break
        time.sleep(0.1)

    return result


def get_binance_price(klines: dict[int, float], ts_unix: int) -> Optional[float]:
    """Get Binance price at the 1m candle containing ts_unix."""
    minute_ts = (ts_unix // 60) * 60
    return klines.get(minute_ts)


# ── simulation ────────────────────────────────────────────────────────────────

def simulate_market(market: dict, cl_ticks: list[tuple[int, float]],
                    binance_klines: dict[int, float],
                    min_cl_delta_pct: float,
                    trades: Optional[dict] = None) -> list[dict]:
    """Returns list of signal rows for this market."""
    market_start_ts = int(market["_market_start"].timestamp())
    market_end_ts = int(market["_market_end"].timestamp())
    winning_side = market["winning_side"]
    interval = market["_interval"]

    results = []

    for i in range(1, len(cl_ticks)):
        prev_ts, prev_price = cl_ticks[i - 1]
        curr_ts, curr_price = cl_ticks[i]

        # Only ticks within the market window (curr tick in [start, end])
        if curr_ts < market_start_ts or curr_ts > market_end_ts:
            continue

        if curr_price == prev_price:
            continue

        cl_delta_pct = (curr_price - prev_price) / prev_price * 100

        if min_cl_delta_pct > 0 and abs(cl_delta_pct) < min_cl_delta_pct:
            continue

        cl_direction = "up" if curr_price > prev_price else "down"

        binance_price = get_binance_price(binance_klines, curr_ts)

        if binance_price is None:
            is_signal = False
            signal_side = None
            binance_cl_delta_pct = None
        else:
            binance_cl_delta_pct = (binance_price - curr_price) / curr_price * 100
            # CL down + Binance > CL → NO
            # CL up + Binance < CL → YES
            if cl_direction == "down" and binance_price > curr_price:
                is_signal = True
                signal_side = "no"
            elif cl_direction == "up" and binance_price < curr_price:
                is_signal = True
                signal_side = "yes"
            else:
                is_signal = False
                signal_side = None

        minute_of_market = max(0, (curr_ts - market_start_ts) // 60)
        won = (signal_side == winning_side) if is_signal else None

        tick_ts_str = datetime.fromtimestamp(curr_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # PM entry price: last trade of the relevant outcome before this tick
        pm_entry_price = None
        if trades and is_signal and signal_side:
            pm_outcome = "Up" if signal_side == "yes" else "Down"
            pm_entry_price = _last_trade_price(trades, pm_outcome, curr_ts)

        results.append({
            "market_id": market["market_id"],
            "symbol": market["symbol"],
            "interval_minutes": interval,
            "winning_side": winning_side,
            "tick_ts": tick_ts_str,
            "minute_of_market": minute_of_market,
            "cl_price": round(curr_price, 6),
            "cl_prev_price": round(prev_price, 6),
            "cl_direction": cl_direction,
            "cl_delta_pct": round(cl_delta_pct, 6),
            "binance_price": round(binance_price, 6) if binance_price is not None else "",
            "binance_cl_delta_pct": round(binance_cl_delta_pct, 4) if binance_cl_delta_pct is not None else "",
            "pm_entry_price": round(pm_entry_price, 4) if pm_entry_price is not None else "",
            "is_signal": is_signal,
            "signal_side": signal_side or "",
            "won": "" if won is None else won,
        })

    return results


# ── stats ─────────────────────────────────────────────────────────────────────

def print_stats(all_rows: list[dict]) -> None:
    signals = [r for r in all_rows if r["is_signal"]]
    resolved = [r for r in signals if r["won"] != ""]

    print(f"\n{'='*60}")
    print(f"  Всего тиков:          {len(all_rows)}")
    print(f"  Сигналов (is_signal): {len(signals)}")
    print(f"  С исходом (won≠''):   {len(resolved)}")

    if not resolved:
        return

    wins = sum(1 for r in resolved if r["won"] is True or r["won"] == "True")
    losses = len(resolved) - wins
    print(f"  Win rate:             {wins}/{len(resolved)} = {wins/len(resolved)*100:.1f}%")

    # By symbol
    print(f"\n  По символам:")
    by_sym: dict[str, list] = defaultdict(list)
    for r in resolved:
        by_sym[r["symbol"]].append(r)
    for sym, rows in sorted(by_sym.items()):
        w = sum(1 for r in rows if r["won"] is True or r["won"] == "True")
        print(f"    {sym}: {w}/{len(rows)} = {w/len(rows)*100:.1f}%")

    # By interval
    print(f"\n  По интервалу:")
    by_int: dict[str, list] = defaultdict(list)
    for r in resolved:
        by_int[str(r["interval_minutes"])].append(r)
    for iv, rows in sorted(by_int.items()):
        w = sum(1 for r in rows if r["won"] is True or r["won"] == "True")
        print(f"    {iv}m: {w}/{len(rows)} = {w/len(rows)*100:.1f}%")

    # By side
    print(f"\n  По стороне сигнала:")
    by_side: dict[str, list] = defaultdict(list)
    for r in resolved:
        by_side[r["signal_side"]].append(r)
    for side, rows in sorted(by_side.items()):
        w = sum(1 for r in rows if r["won"] is True or r["won"] == "True")
        print(f"    {side}: {w}/{len(rows)} = {w/len(rows)*100:.1f}%")

    # By minute_of_market
    print(f"\n  По минуте рынка (топ):")
    by_min: dict[int, list] = defaultdict(list)
    for r in resolved:
        try:
            by_min[int(r["minute_of_market"])].append(r)
        except Exception:
            pass
    for minute in sorted(by_min.keys()):
        rows = by_min[minute]
        w = sum(1 for r in rows if r["won"] is True or r["won"] == "True")
        print(f"    min {minute:2d}: {w:3d}/{len(rows):3d} = {w/len(rows)*100:.1f}%")

    # PnL simulation using open_price as proxy entry price
    _print_pnl(resolved)

    print(f"{'='*60}")


def _print_pnl(resolved: list[dict]) -> None:
    """PnL simulation using actual last-trade pm_entry_price, $1 bet per signal."""

    def _f(v):
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    priced = [r for r in resolved if r.get("pm_entry_price") not in ("", None)]
    if not priced:
        print(f"\n  [PnL] pm_entry_price недоступен — сначала запустите fetch_trades")
        return

    BET = 1.0
    total_cost = total_payout = 0.0
    n = wins = 0

    for r in priced:
        entry = _f(r["pm_entry_price"])
        if entry is None or entry <= 0.05 or entry >= 0.95:
            continue
        won = r["won"] is True or r["won"] == "True"
        shares = BET / entry
        total_cost += BET
        total_payout += shares if won else 0.0
        n += 1
        if won:
            wins += 1

    if n == 0:
        return

    pnl = total_payout - total_cost
    print(f"\n  PnL (последняя сделка PM, ${BET:.2f}/сигнал):")
    print(f"    Сигналов с ценой: {n}")
    print(f"    Win rate:         {wins}/{n} = {wins/n*100:.1f}%")
    print(f"    Вложено:          ${total_cost:.2f}")
    print(f"    Получено:         ${total_payout:.2f}")
    print(f"    PnL:              ${pnl:+.2f}  (ROI {pnl/total_cost*100:+.1f}%)")

    # By price bucket: what was the entry price when signal fired?
    buckets: dict[str, list] = defaultdict(list)
    for r in priced:
        ep = _f(r["pm_entry_price"])
        if ep is None:
            continue
        if ep < 0.20:
            b = "<0.20"
        elif ep < 0.35:
            b = "0.20–0.35"
        elif ep < 0.50:
            b = "0.35–0.50"
        elif ep < 0.65:
            b = "0.50–0.65"
        elif ep < 0.80:
            b = "0.65–0.80"
        else:
            b = ">0.80"
        buckets[b].append(r)

    print(f"\n  PnL по цене входа (pm_entry_price):")
    for bk in ["<0.20", "0.20–0.35", "0.35–0.50", "0.50–0.65", "0.65–0.80", ">0.80"]:
        rows = buckets.get(bk, [])
        if not rows:
            continue
        bw = bl = 0
        bc = bp = 0.0
        for r in rows:
            ep = _f(r["pm_entry_price"])
            if ep is None or ep <= 0.05 or ep >= 0.95:
                continue
            won = r["won"] is True or r["won"] == "True"
            shares = BET / ep
            bc += BET
            bp += shares if won else 0.0
            if won:
                bw += 1
            else:
                bl += 1
        bn = bw + bl
        if bn == 0:
            continue
        bpnl = bp - bc
        print(f"    {bk:10s}: {bw:4d}/{bn:4d} = {bw/bn*100:.1f}%  "
              f"PnL ${bpnl:+.2f}  (ROI {bpnl/bc*100:+.1f}%)")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-delta", type=float, default=0.03,
                        help="минимальный |cl_delta_pct| для сигнала (default 0.03)")
    parser.add_argument("--markets", type=Path, default=MARKETS_CACHE)
    args = parser.parse_args()

    if not args.markets.exists():
        print(f"Файл рынков не найден: {args.markets}")
        print("Запустите: python3 -m research_bot.fetch_markets --limit 5000")
        return

    markets = load_markets(args.markets)
    print(f"Рынков загружено: {len(markets)}")
    if not markets:
        return

    trades_available = TRADES_DIR.exists() and any(TRADES_DIR.iterdir())
    if trades_available:
        print(f"Найдена папка trades/ — будет использована реальная цена входа")
    else:
        print(f"[!] trades/ не найдена — запустите fetch_trades для точного PnL")

    # Date range
    all_starts = [m["_market_start"] for m in markets]
    all_ends = [m["_market_end"] for m in markets]
    range_start = min(all_starts)
    range_end = max(all_ends)
    print(f"Диапазон: {range_start.strftime('%Y-%m-%d %H:%M')} – {range_end.strftime('%Y-%m-%d %H:%M')}")

    # Load CL history per symbol
    symbols = list({m["symbol"] for m in markets})
    cl_by_symbol: dict[str, list[tuple[int, float]]] = {}
    for sym in symbols:
        print(f"Загрузка CL истории {sym}...")
        cl_by_symbol[sym] = load_cl_history(sym)
        print(f"  {sym}: {len(cl_by_symbol[sym])} раундов")

    # Fetch Binance klines per symbol (cache in memory)
    print("\nЗагрузка Binance 1m klines...")
    http = httpx.Client(timeout=20.0)
    range_start_ts = int(range_start.timestamp()) - 120
    range_end_ts = int(range_end.timestamp()) + 120
    binance_by_symbol: dict[str, dict[int, float]] = {}
    for sym in symbols:
        print(f"  {sym}...", end=" ", flush=True)
        klines_path = DATA_DIR / f"binance_1m_{sym}.csv"
        if klines_path.exists():
            # Load from cache
            klines: dict[int, float] = {}
            with open(klines_path, newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        klines[int(row["minute_ts"])] = float(row["close_price"])
                    except Exception:
                        pass
            print(f"{len(klines)} свечей (из кэша)")
        else:
            klines = fetch_binance_klines(sym, range_start_ts, range_end_ts, http)
            # Save cache
            with open(klines_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["minute_ts", "close_price"])
                for mt, cp in sorted(klines.items()):
                    w.writerow([mt, cp])
            print(f"{len(klines)} свечей")
        binance_by_symbol[sym] = klines
    http.close()

    # Run simulation
    print(f"\nСимуляция (min_cl_delta={args.min_delta}%)...")
    all_rows: list[dict] = []
    processed = 0

    out_f = open(SIGNALS_OUT, "w", newline="")
    writer = csv.DictWriter(out_f, fieldnames=SIGNALS_FIELDS)
    writer.writeheader()

    # Group markets by symbol for efficient CL lookup
    markets_by_symbol: dict[str, list[dict]] = defaultdict(list)
    for m in markets:
        markets_by_symbol[m["symbol"]].append(m)

    for sym, sym_markets in markets_by_symbol.items():
        cl_ticks = cl_by_symbol.get(sym, [])
        binance_klines = binance_by_symbol.get(sym, {})

        if not cl_ticks:
            print(f"  [{sym}] Нет CL данных, пропускаем")
            continue

        for market in sym_markets:
            market_start_ts = int(market["_market_start"].timestamp())
            market_end_ts = int(market["_market_end"].timestamp())

            # Slice CL ticks relevant to this market (include one tick before start for prev_price)
            buffer = 300  # 5 min before market start to get prev_price
            relevant = [
                (ts, p) for ts, p in cl_ticks
                if market_start_ts - buffer <= ts <= market_end_ts
            ]

            if len(relevant) < 2:
                processed += 1
                continue

            trades = load_market_trades(market["market_id"]) if trades_available else None
            rows = simulate_market(market, relevant, binance_klines, args.min_delta,
                                   trades=trades)
            writer.writerows(rows)
            all_rows.extend(rows)
            processed += 1

            if processed % 500 == 0:
                signals = sum(1 for r in all_rows if r["is_signal"])
                print(f"  {processed}/{len(markets)} рынков | {len(all_rows)} тиков | {signals} сигналов")

    out_f.close()

    print(f"\nГотово! {processed} рынков, {len(all_rows)} тиков")
    print(f"Файл: {SIGNALS_OUT}")

    print_stats(all_rows)


if __name__ == "__main__":
    main()
