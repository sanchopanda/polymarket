"""
research_bot/fetch_trades.py

Скачивает историю сделок (data-api.polymarket.com/trades) для всех рынков
из backtest.db (таблица markets). Сохраняет в backtest.db (таблица pm_trades).

Запуск:
  python3 -m research_bot.fetch_trades
  python3 -m research_bot.fetch_trades --force      # перезаписать существующие
  python3 -m research_bot.fetch_trades --limit 100  # только первые N рынков
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import httpx

from research_bot.backtest_db import get_connection, has_trades

GAMMA_URL = "https://gamma-api.polymarket.com"
TRADES_URL = "https://data-api.polymarket.com/trades"
REQUEST_DELAY = 0.02  # seconds between requests


def _fetch_condition_id(market_id: str, http: httpx.Client) -> str | None:
    try:
        r = http.get(f"{GAMMA_URL}/markets/{market_id}", timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            data = data[0]
        if isinstance(data, dict):
            return data.get("conditionId") or None
    except Exception:
        pass
    return None


def enrich_condition_ids(conn, http: httpx.Client) -> int:
    """Fetch missing condition_ids and update DB."""
    rows = conn.execute(
        "SELECT market_id FROM markets WHERE condition_id IS NULL OR condition_id = ''"
    ).fetchall()
    if not rows:
        return 0

    print(f"Получаем condition_id для {len(rows)} рынков (Gamma API)...")
    fetched = 0
    for i, row in enumerate(rows):
        cid = _fetch_condition_id(row[0], http)
        if cid:
            conn.execute(
                "UPDATE markets SET condition_id = ? WHERE market_id = ?",
                (cid, row[0]),
            )
            fetched += 1
        time.sleep(REQUEST_DELAY)
        if (i + 1) % 200 == 0:
            conn.commit()
            print(f"  {i+1}/{len(rows)} | получено: {fetched}", flush=True)

    conn.commit()
    print(f"  condition_id получено: {fetched}/{len(rows)}")
    return fetched


def fetch_market_trades(
    condition_id: str,
    market_start_ts: int,
    market_end_ts: int,
    http: httpx.Client,
) -> list[tuple[int, str, float, float | None]]:
    """
    Fetch trades in window [market_start - 120s, market_end + 30s].
    Returns sorted list of (timestamp, outcome, price, size).
    """
    cutoff_lo = market_start_ts - 120
    cutoff_hi = market_end_ts + 30
    trades: list[tuple[int, str, float, float | None]] = []
    offset = 0

    while True:
        for attempt in range(5):
            try:
                r = http.get(TRADES_URL, params={
                    "market": condition_id,
                    "limit": 500,
                    "offset": offset,
                }, timeout=15)
                if r.status_code == 429:
                    delay = min(2 ** attempt, 30)
                    time.sleep(delay)
                    continue
                r.raise_for_status()
                batch = r.json()
                break
            except Exception:
                if attempt < 4:
                    time.sleep(min(2 ** attempt, 10))
                else:
                    return sorted(trades)

        if not isinstance(batch, list) or not batch:
            break

        for t in batch:
            ts = t.get("timestamp")
            if ts is None:
                continue
            if cutoff_lo <= ts <= cutoff_hi:
                raw_size = t.get("size")
                try:
                    size = float(raw_size) if raw_size is not None else None
                except (TypeError, ValueError):
                    size = None
                trades.append((int(ts), t["outcome"], float(t["price"]), size))

        oldest_ts = min((t["timestamp"] for t in batch if t.get("timestamp")), default=0)
        if oldest_ts < cutoff_lo:
            break

        if len(batch) < 500:
            break

        offset += 500

    return sorted(trades)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="перезаписать существующие трейды")
    parser.add_argument("--limit", type=int, default=None,
                        help="максимум рынков для обработки")
    args = parser.parse_args()

    conn = get_connection()

    http = httpx.Client(timeout=15.0)
    try:
        enrich_condition_ids(conn, http)

        markets = conn.execute(
            """SELECT market_id, condition_id, market_start, market_end
               FROM markets
               WHERE condition_id IS NOT NULL AND condition_id != ''
                 AND winning_side IN ('yes', 'no')"""
        ).fetchall()
        print(f"Рынков с condition_id: {len(markets)}")

        if args.limit:
            markets = markets[:args.limit]

        saved = skipped = errors = 0

        for i, row in enumerate(markets):
            mid = row[0]

            if not args.force and has_trades(conn, mid):
                null_count = conn.execute(
                    "SELECT COUNT(*) FROM pm_trades WHERE market_id=? AND size IS NULL", (mid,)
                ).fetchone()[0]
                if null_count == 0:
                    skipped += 1
                    continue

            try:
                ms = int(datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=timezone.utc).timestamp())
                me = int(datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                         .replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                errors += 1
                continue

            trades = fetch_market_trades(row[1], ms, me, http)

            if args.force:
                conn.execute("DELETE FROM pm_trades WHERE market_id = ?", (mid,))

            if trades:
                conn.executemany(
                    "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                    [(mid, ts, outcome, price, size) for ts, outcome, price, size in trades],
                )
                conn.commit()

            saved += 1
            time.sleep(REQUEST_DELAY)

            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(markets)}] saved={saved} skipped={skipped} err={errors}",
                      flush=True)

    finally:
        http.close()
        conn.close()

    print(f"\nГотово!")
    print(f"  Скачано:   {saved}")
    print(f"  Пропущено: {skipped} (уже есть)")
    print(f"  Ошибок:    {errors}")


if __name__ == "__main__":
    main()
