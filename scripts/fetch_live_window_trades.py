#!/usr/bin/env python3
"""Download PM trades for the live-window markets (2026-04-14 12:30 -> 2026-04-15 08:00).

Fetches condition_ids from Gamma API, then trades from data-api.polymarket.com.
Stores everything in research_bot/data/backtest.db.
Uses parallel workers for speed.

Usage: python3 scripts/fetch_live_window_trades.py
"""

import sqlite3
import time as _time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import httpx

DB_PATH = "research_bot/data/backtest.db"
GAMMA_URL = "https://gamma-api.polymarket.com"
TRADES_URL = "https://data-api.polymarket.com/trades"

WINDOW_START = "2026-04-14 12:30"
WORKERS = 10


def fetch_condition_id(client: httpx.Client, market_id: str) -> str | None:
    for attempt in range(5):
        try:
            r = client.get(f"{GAMMA_URL}/markets/{market_id}", timeout=10)
            if r.status_code == 429:
                _time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                return data.get("conditionId") or None
        except Exception:
            if attempt < 4:
                _time.sleep(1)
    return None


def fetch_trades(condition_id: str, start_ts: int, end_ts: int) -> list[tuple[int, str, float]]:
    cutoff_lo = start_ts - 120
    cutoff_hi = end_ts + 30
    trades = []
    offset = 0
    client = httpx.Client(timeout=15)

    try:
        while True:
            for attempt in range(5):
                try:
                    r = client.get(TRADES_URL, params={
                        "market": condition_id,
                        "limit": 500,
                        "offset": offset,
                    }, timeout=15)
                    if r.status_code == 429:
                        _time.sleep(min(2 ** attempt, 30))
                        continue
                    r.raise_for_status()
                    batch = r.json()
                    break
                except Exception:
                    if attempt < 4:
                        _time.sleep(min(2 ** attempt, 10))
                    else:
                        return sorted(trades)

            if not isinstance(batch, list) or not batch:
                break

            for t in batch:
                ts = t.get("timestamp")
                if ts is None:
                    continue
                if cutoff_lo <= ts <= cutoff_hi:
                    trades.append((int(ts), t["outcome"], float(t["price"])))

            oldest_ts = min((t["timestamp"] for t in batch if t.get("timestamp")), default=0)
            if oldest_ts < cutoff_lo:
                break
            if len(batch) < 500:
                break
            offset += 500
    finally:
        client.close()

    return sorted(trades)


def process_market(mid, cid, ms_str, me_str):
    """Fetch trades for one market. Returns (mid, trades_list) or (mid, None) on error."""
    try:
        ms = int(datetime.strptime(ms_str, "%Y-%m-%d %H:%M:%S")
                 .replace(tzinfo=timezone.utc).timestamp())
        me = int(datetime.strptime(me_str, "%Y-%m-%d %H:%M:%S")
                 .replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return mid, None

    trades = fetch_trades(cid, ms, me)
    return mid, trades


def main():
    conn = sqlite3.connect(DB_PATH)
    http = httpx.Client(timeout=15)

    # Step 1: Fetch missing condition_ids (parallel)
    need_cid = conn.execute("""
        SELECT market_id FROM markets
        WHERE market_start >= ?
        AND (condition_id IS NULL OR condition_id = '')
    """, (WINDOW_START,)).fetchall()

    if need_cid:
        print(f"Fetching condition_ids for {len(need_cid)} markets ({WORKERS} workers)...")
        fetched = 0

        def _get_cid(mid):
            return mid, fetch_condition_id(http, mid)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_get_cid, mid): mid for (mid,) in need_cid}
            for i, fut in enumerate(as_completed(futures), 1):
                mid, cid = fut.result()
                if cid:
                    conn.execute("UPDATE markets SET condition_id=? WHERE market_id=?", (cid, mid))
                    fetched += 1
                if i % 100 == 0:
                    conn.commit()
                    print(f"  {i}/{len(need_cid)} fetched={fetched}", flush=True)
        conn.commit()
        print(f"  Done: {fetched}/{len(need_cid)} condition_ids")

    http.close()

    # Step 2: Fetch trades (parallel, each worker has own HTTP client)
    markets = conn.execute("""
        SELECT market_id, condition_id, market_start, market_end
        FROM markets
        WHERE market_start >= ?
        AND condition_id IS NOT NULL AND condition_id != ''
    """, (WINDOW_START,)).fetchall()

    has_trades = set()
    for (mid,) in conn.execute("""
        SELECT DISTINCT m.market_id FROM markets m
        JOIN pm_trades p ON m.market_id = p.market_id
        WHERE m.market_start >= ?
    """, (WINDOW_START,)):
        has_trades.add(mid)

    need_trades = [(m[0], m[1], m[2], m[3]) for m in markets if m[0] not in has_trades]
    print(f"\nFetching trades for {len(need_trades)} markets ({WORKERS} workers, skipping {len(has_trades)} existing)...")

    saved = errors = empty = 0
    t0 = _time.time()
    db_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(process_market, mid, cid, ms, me): mid
            for mid, cid, ms, me in need_trades
        }

        for i, fut in enumerate(as_completed(futures), 1):
            mid, trades = fut.result()

            if trades is None:
                errors += 1
            elif trades:
                conn.executemany(
                    "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                    [(mid, ts, outcome, price, size) for ts, outcome, price, size in trades],
                )
                saved += 1
            else:
                empty += 1

            if i % 50 == 0:
                conn.commit()
                elapsed = _time.time() - t0
                rate = i / elapsed
                remaining = (len(need_trades) - i) / rate
                print(f"  [{i}/{len(need_trades)}] saved={saved} empty={empty} err={errors} "
                      f"({elapsed:.0f}s, ~{remaining:.0f}s left)", flush=True)

    conn.commit()

    # Summary
    total_trades = conn.execute("""
        SELECT COUNT(*) FROM pm_trades p
        JOIN markets m ON p.market_id = m.market_id
        WHERE m.market_start >= ?
    """, (WINDOW_START,)).fetchone()[0]

    markets_with_trades = conn.execute("""
        SELECT COUNT(DISTINCT m.market_id) FROM markets m
        JOIN pm_trades p ON m.market_id = p.market_id
        WHERE m.market_start >= ?
    """, (WINDOW_START,)).fetchone()[0]

    elapsed = _time.time() - t0
    print(f"\nDone in {elapsed:.0f}s!")
    print(f"  Saved: {saved}, Empty: {empty}, Errors: {errors}")
    print(f"  Total trades in window: {total_trades:,}")
    print(f"  Markets with trades: {markets_with_trades}")
    conn.close()


if __name__ == "__main__":
    main()
