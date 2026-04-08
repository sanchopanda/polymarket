#!/usr/bin/env python3
"""
Анализ: дельта между Kalshi floor_strike и Binance 15m open
vs вероятность мисматча с Polymarket 15m.

Логика:
  - Kalshi KXBTC15M: floor_strike = BTC цена в начале 15m окна (Kalshi feed)
  - Polymarket 15m: резолюция по Chainlink BTC/USD (open vs close)
  - Binance 15m open = прокси для "реальной" цены в начале окна
  - delta = |Kalshi floor_strike - Binance open|
  Гипотеза: чем больше delta, тем выше вероятность мисматча.

Запуск: python3 data/kalshi_15m_delta.py [--days 7]
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import httpx

KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
BINANCE_BASE = "https://api.binance.com/api/v3"
DB_PATH      = "research_bot/data/backtest.db"


def fetch_kalshi_15m(http: httpx.Client, days_back: int) -> list[dict]:
    """Fetch settled KXBTC15M markets (result + floor_strike included directly)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    markets = []
    cursor = None

    while True:
        params: dict = {
            "series_ticker": "KXBTC15M",
            "status": "settled",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        resp = http.get(f"{KALSHI_BASE}/markets", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("markets", [])
        if not batch:
            break

        stop = False
        for m in batch:
            # close_time is the resolution time = window end
            close_raw = m.get("close_time") or m.get("expiration_time")
            if not close_raw:
                continue
            try:
                close_dt = datetime.fromisoformat(close_raw.replace("Z", "+00:00"))
            except ValueError:
                continue

            if close_dt < cutoff:
                stop = True
                break

            if m.get("result") not in ("yes", "no"):
                continue

            floor_strike = m.get("floor_strike")
            if floor_strike is None:
                continue

            markets.append({
                "ticker":       m["ticker"],
                "close_dt":     close_dt,
                "open_dt":      close_dt - timedelta(minutes=15),
                "floor_strike": float(floor_strike),
                "result":       m["result"],  # "yes" = BTC >= floor_strike at close
            })

        cursor = data.get("cursor")
        if stop or not cursor:
            break

    return markets


def fetch_binance_15m(http: httpx.Client, days_back: int) -> dict[int, float]:
    """Fetch BTC/USDT 15m candles. Returns {open_time_ms: open_price}."""
    now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - days_back * 24 * 3600 * 1000

    candles: dict[int, float] = {}
    limit = 1000
    current_start = start_ms

    while current_start < now_ms:
        resp = http.get(
            f"{BINANCE_BASE}/klines",
            params={"symbol": "BTCUSDT", "interval": "15m",
                    "startTime": current_start, "limit": limit},
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for row in rows:
            candles[int(row[0])] = float(row[1])
        current_start = int(rows[-1][0]) + 15 * 60 * 1000
        if len(rows) < limit:
            break
        time.sleep(0.05)

    return candles


def fetch_pm_markets(days_back: int) -> dict[str, str]:
    """
    Load Polymarket 15m BTC markets from backtest.db.
    Returns {market_end_str -> winning_side} where market_end_str = "YYYY-MM-DD HH:MM:SS".
    """
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        """SELECT market_end, winning_side FROM markets
           WHERE symbol='BTC' AND interval_minutes=15 AND market_end >= ?""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    http = httpx.Client(timeout=30)

    print(f"=== Kalshi KXBTC15M vs Polymarket 15m — delta анализ (последние {args.days} дней) ===\n")

    print("Fetching Binance BTCUSDT 15m candles...")
    candles = fetch_binance_15m(http, args.days + 1)
    print(f"  → {len(candles)} candles\n")

    print("Fetching Kalshi KXBTC15M settled markets...")
    ka_markets = fetch_kalshi_15m(http, args.days)
    print(f"  → {len(ka_markets)} markets\n")

    print("Loading Polymarket 15m BTC markets from DB...")
    pm_markets = fetch_pm_markets(args.days)
    print(f"  → {len(pm_markets)} markets\n")

    http.close()

    # Match: for each Kalshi market find PM market with same close_time
    rows = []
    for ka in ka_markets:
        close_utc = ka["close_dt"].replace(tzinfo=None)
        pm_key = close_utc.strftime("%Y-%m-%d %H:%M:%S")
        pm_res = pm_markets.get(pm_key)
        if pm_res is None:
            # Try rounding to nearest minute in case of 1-second drift
            for delta_s in (-1, 1, -2, 2, -30, 30):
                alt_key = (close_utc + timedelta(seconds=delta_s)).strftime("%Y-%m-%d %H:%M:%S")
                if alt_key in pm_markets:
                    pm_res = pm_markets[alt_key]
                    break
        if pm_res is None:
            continue

        # Binance 15m candle that OPENS at ka["open_dt"]
        open_ms = int(ka["open_dt"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        binance_open = candles.get(open_ms)
        if binance_open is None:
            if args.verbose:
                print(f"  [skip] no Binance candle for {ka['open_dt']}")
            continue

        delta = abs(ka["floor_strike"] - binance_open)
        agree = ka["result"] == pm_res

        rows.append({
            "close_utc":    close_utc,
            "floor_strike": ka["floor_strike"],
            "binance_open": binance_open,
            "delta":        delta,
            "ka_result":    ka["result"],
            "pm_result":    pm_res,
            "agree":        agree,
        })

        if args.verbose:
            ok = "✓" if agree else "✗"
            print(f"{ok} {close_utc} | FS={ka['floor_strike']:,.0f} | "
                  f"BN={binance_open:,.2f} | Δ={delta:.2f} | "
                  f"PM={pm_res.upper()} Kalshi={ka['result'].upper()}")

    if not rows:
        print("Нет данных для анализа.")
        return

    total   = len(rows)
    agreed  = sum(1 for r in rows if r["agree"])
    print(f"\n{'='*60}")
    print(f"Матчей:      {total}")
    print(f"Совпадений:  {agreed} ({100*agreed/total:.1f}%)")
    print(f"Расхождений: {total - agreed} ({100*(total-agreed)/total:.1f}%)")
    print(f"{'='*60}\n")

    # Delta correlation analysis
    # Group by delta buckets
    buckets = [0, 5, 10, 20, 50, 100, float("inf")]
    bucket_labels = ["0-5", "5-10", "10-20", "20-50", "50-100", "100+"]

    print("Связь дельты (|Kalshi floor_strike - Binance open|) с мисматчами:\n")
    header = f"{'Δ range ($)':<12} {'матчей':>8} {'мисматч':>9} {'rate':>7}"
    print(header)
    print("-" * 40)

    for i, label in enumerate(bucket_labels):
        lo, hi = buckets[i], buckets[i + 1]
        bucket_rows = [r for r in rows if lo <= r["delta"] < hi]
        n = len(bucket_rows)
        if n == 0:
            continue
        mismatches = sum(1 for r in bucket_rows if not r["agree"])
        rate = 100 * mismatches / n
        print(f"{label:<12} {n:>8} {mismatches:>9} {rate:>6.1f}%")

    print()

    # Show mismatches
    mismatches = [r for r in rows if not r["agree"]]
    if mismatches:
        print(f"Расхождения ({len(mismatches)}):")
        for r in sorted(mismatches, key=lambda x: x["close_utc"]):
            print(f"  {r['close_utc']} | FS={r['floor_strike']:,.0f} | "
                  f"BN={r['binance_open']:,.2f} | Δ={r['delta']:.2f} | "
                  f"PM={r['pm_result'].upper()} Kalshi={r['ka_result'].upper()}")

    # Show avg delta for matches vs mismatches
    match_rows    = [r for r in rows if r["agree"]]
    mismatch_rows = [r for r in rows if not r["agree"]]
    if match_rows:
        avg_match = sum(r["delta"] for r in match_rows) / len(match_rows)
        print(f"\nСредняя Δ для матчей:     ${avg_match:.2f}")
    if mismatch_rows:
        avg_mismatch = sum(r["delta"] for r in mismatch_rows) / len(mismatch_rows)
        print(f"Средняя Δ для мисматчей:  ${avg_mismatch:.2f}")

    # Threshold filter analysis
    print(f"\n{'='*60}")
    print("Фильтр: торгуем только рынки с Δ < $30")
    print(f"{'='*60}")
    below = [r for r in rows if r["delta"] < 30]
    above = [r for r in rows if r["delta"] >= 30]
    b_miss = sum(1 for r in below if not r["agree"])
    a_miss = sum(1 for r in above if not r["agree"])
    print(f"Δ < $30:  {len(below):>4} рынков, мисматчей: {b_miss} ({100*b_miss/len(below):.1f}%)")
    if above:
        print(f"Δ ≥ $30:  {len(above):>4} рынков, мисматчей: {a_miss} ({100*a_miss/len(above):.1f}%)")
        print(f"Отсеивается: {len(above)} рынков ({100*len(above)/total:.1f}% от всех), "
              f"из них мисматчей: {a_miss} ({100*a_miss/len(above):.1f}%)")


if __name__ == "__main__":
    main()
