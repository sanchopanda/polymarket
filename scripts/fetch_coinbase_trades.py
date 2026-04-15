#!/usr/bin/env python3
"""Download Coinbase historical trades and build 1-second close prices.

Usage: python3 scripts/fetch_coinbase_trades.py
"""

import httpx
import sqlite3
import time as _time
from datetime import datetime, timezone

DB_PATH = "research_bot/data/backtest.db"
SYMBOLS = [("BTC-USD", "BTC"), ("ETH-USD", "ETH"), ("SOL-USD", "SOL"), ("XRP-USD", "XRP")]

START = datetime(2026, 3, 29, 7, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 4, 3, 13, 9, 0, tzinfo=timezone.utc)
START_TS = int(START.timestamp())
END_TS = int(END.timestamp())

BASE = "https://api.exchange.coinbase.com"


def find_trade_id_near(client, product: str, target: datetime, hint_lo: int, hint_hi: int) -> int:
    """Binary search for trade_id closest to target time."""
    for _ in range(40):
        mid = (hint_lo + hint_hi) // 2
        for attempt in range(5):
            try:
                resp = client.get(f"{BASE}/products/{product}/trades",
                                  params={"limit": 1, "after": mid + 1})
                if resp.status_code == 429:
                    _time.sleep(3)
                    continue
                resp.raise_for_status()
                break
            except Exception:
                if attempt < 4:
                    _time.sleep(2)
                else:
                    raise
        data = resp.json()
        if not data:
            hint_hi = mid
            continue
        t = datetime.fromisoformat(data[0]["time"].replace("Z", "+00:00"))
        if t < target:
            hint_lo = mid
        else:
            hint_hi = mid
        if hint_hi - hint_lo < 200:
            break
        _time.sleep(0.15)
    return hint_hi


def fetch_trades_range(client, product: str, start_cursor: int, start_ts: int, end_ts: int):
    """Fetch trades from start_cursor backward until we pass start_ts.
    Returns dict {sec_ts: last_price} (1-second close prices)."""
    prices_1s = {}
    cursor = start_cursor
    pages = 0
    total_trades = 0
    t0 = _time.time()

    while True:
        params = {"limit": 1000, "after": cursor}
        for attempt in range(5):
            try:
                resp = client.get(f"{BASE}/products/{product}/trades", params=params)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 3))
                    print(f"\n  rate limited, waiting {wait}s", flush=True)
                    _time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except Exception:
                if attempt < 4:
                    _time.sleep(2)
                else:
                    raise

        trades = resp.json()
        if not trades:
            break

        cursor = resp.headers.get("cb-after")
        if not cursor:
            break
        cursor = int(cursor)

        done = False
        for t in trades:
            dt = datetime.fromisoformat(t["time"].replace("Z", "+00:00"))
            ts = int(dt.timestamp())
            if ts < start_ts:
                done = True
                break
            if ts <= end_ts:
                total_trades += 1
                # For 1s close: keep latest trade per second (trades come newest-first,
                # so first write = latest trade in that second — don't overwrite)
                if ts not in prices_1s:
                    prices_1s[ts] = float(t["price"])

        pages += 1
        if pages % 20 == 0:
            oldest = trades[-1]["time"][:19]
            elapsed = _time.time() - t0
            print(f"\r  page {pages}: {total_trades:,} trades, {len(prices_1s):,} seconds, at {oldest} ({elapsed:.0f}s)", end="", flush=True)

        if done:
            break

        if pages % 10 == 0:
            _time.sleep(0.1)

    return prices_1s, total_trades


def fill_gaps(prices_1s: dict[int, float]) -> list[tuple[int, float]]:
    """Fill missing seconds with previous close."""
    if not prices_1s:
        return []
    min_ts = min(prices_1s)
    max_ts = max(prices_1s)
    result = []
    last = None
    for ts in range(min_ts, max_ts + 1):
        if ts in prices_1s:
            last = prices_1s[ts]
        if last is not None:
            result.append((ts, last))
    return result


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS coinbase_1s (
            symbol TEXT NOT NULL,
            sec_ts INTEGER NOT NULL,
            close REAL NOT NULL,
            PRIMARY KEY (symbol, sec_ts)
        )
    """)
    conn.commit()

    client = httpx.Client(timeout=15)

    print("Starting download...")

    for product, symbol in SYMBOLS:
        existing = conn.execute(
            "SELECT COUNT(*) FROM coinbase_1s WHERE symbol=?", (symbol,)
        ).fetchone()[0]
        if existing > 100000:
            print(f"\n{symbol}: already have {existing:,} rows, skipping")
            continue

        print(f"\n{'='*60}")
        print(f"Fetching {product} ({symbol})...")
        _time.sleep(5)  # pause between symbols to avoid rate limit

        # Get latest trade_id for THIS symbol
        resp = client.get(f"{BASE}/products/{product}/trades", params={"limit": 1})
        latest_id = resp.json()[0]["trade_id"]

        # Find end cursor: trade_id near END time
        print(f"  finding end cursor...", end="", flush=True)
        end_cursor = find_trade_id_near(client, product, END, latest_id - 10_000_000, latest_id)
        print(f" id={end_cursor}")

        t0 = _time.time()
        prices_1s, total_trades = fetch_trades_range(client, product, end_cursor, START_TS, END_TS)

        filled = fill_gaps(prices_1s)
        elapsed = _time.time() - t0
        print(f"\n  {total_trades:,} trades -> {len(filled):,} 1s prices in {elapsed:.0f}s")

        conn.execute("DELETE FROM coinbase_1s WHERE symbol=?", (symbol,))
        conn.executemany(
            "INSERT INTO coinbase_1s (symbol, sec_ts, close) VALUES (?, ?, ?)",
            [(symbol, ts, price) for ts, price in filled]
        )
        conn.commit()
        print(f"  saved to DB")

    client.close()

    # Summary
    print(f"\n{'='*60}")
    print("Summary:")
    for _, symbol in SYMBOLS:
        cur = conn.execute(
            "SELECT COUNT(*), MIN(sec_ts), MAX(sec_ts) FROM coinbase_1s WHERE symbol=?", (symbol,)
        )
        cnt, mn, mx = cur.fetchone()
        if cnt:
            print(f"  {symbol}: {cnt:,} rows, {datetime.utcfromtimestamp(mn)} -> {datetime.utcfromtimestamp(mx)}")
    conn.close()


if __name__ == "__main__":
    main()
