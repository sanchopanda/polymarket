"""Backfill исторических weather events в отдельную БД.

Для каждого closed US-event:
  1. Парсим через weather_bot.markets._parse_event (уже есть в коде).
  2. Берём actual daily high из ACIS (по station.icao).
  3. Для каждого bucket тянем CLOB prices-history для YES token'а — это mid/last
     ценовая траектория в UTC-timestamp'ах.
  4. Сохраняем всё в data/weather_history.db (отдельно от рабочей weather_bot.db).

Результат потом анализируется scripts/analyze_weather_history.py.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone

import httpx

sys.path.insert(0, ".")
from weather_bot.markets import _parse_event, GAMMA_BASE, TAG_SLUG
from weather_bot.stations import US_STATIONS


DB_PATH = "data/weather_history.db"
ACIS_URL = "https://data.rcc-acis.org/StnData"
CLOB_BASE = "https://clob.polymarket.com"
UA = "weather-history-backfill/0.1 (claude3@icons8.com)"

# prices-history возвращает ограниченное окно; просим конкретный startTs/endTs
# fidelity в минутах (1 min? нет — тестами выясняется что точки выходят нерегулярно).


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        slug TEXT,
        city_slug TEXT,
        station_icao TEXT,
        event_date TEXT,
        end_date_utc TEXT,
        total_volume REAL,
        actual_high_f REAL,
        winning_bucket_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS buckets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        title TEXT,
        lo REAL,
        hi REAL,
        yes_token_id TEXT,
        no_token_id TEXT,
        final_outcome_yes INTEGER  -- 0/1 из outcomePrices
    );
    CREATE INDEX IF NOT EXISTS ix_buckets_event ON buckets(event_id);
    CREATE TABLE IF NOT EXISTS prices (
        bucket_id INTEGER,
        ts INTEGER,           -- unix seconds
        yes_price REAL,       -- mid/last от CLOB
        PRIMARY KEY (bucket_id, ts)
    );
    """)


def fetch_closed_events(client: httpx.Client) -> list[dict]:
    out = []
    offset = 0
    while True:
        r = client.get(f"{GAMMA_BASE}/events", params={
            "limit": 500, "closed": "true", "tag_slug": TAG_SLUG, "offset": offset,
        }, timeout=60.0)
        r.raise_for_status()
        data = r.json()
        out.extend(data)
        if len(data) < 500:
            break
        offset += 500
        time.sleep(0.3)
    return out


def acis_daily_max(icao: str, event_date: str) -> float | None:
    body = {
        "sid": icao, "sdate": event_date, "edate": event_date,
        "elems": [{"name": "maxt", "interval": "dly", "units": "degreeF"}],
    }
    r = httpx.post(ACIS_URL, json=body, headers={"User-Agent": UA}, timeout=30.0)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return None
    v = data[0][1]
    if v in ("M", "T", "", None):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def fetch_prices(client: httpx.Client, token_id: str, start_ts: int, end_ts: int) -> list[tuple[int, float]]:
    try:
        r = client.get(f"{CLOB_BASE}/prices-history", params={
            "market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 60,
        }, timeout=30.0)
        if r.status_code != 200:
            return []
        h = r.json().get("history", [])
        return [(int(p["t"]), float(p["p"])) for p in h]
    except Exception:
        return []


def find_winning_bucket_id(conn: sqlite3.Connection, event_id: str, actual_f: float) -> int | None:
    rounded = round(actual_f)
    rows = conn.execute("SELECT id, lo, hi FROM buckets WHERE event_id=?", (event_id,)).fetchall()
    for bid, lo, hi in rows:
        lo_ = -1e18 if lo <= -1e8 else lo
        hi_ = 1e18 if hi >= 1e8 else hi
        if lo_ <= rounded <= hi_:
            return int(bid)
    return None


def main() -> int:
    max_events = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    client = httpx.Client(headers={"User-Agent": UA}, timeout=60.0)

    print("fetching closed events list from Gamma...")
    raw = fetch_closed_events(client)
    print(f"  got {len(raw)} raw events")

    # parse + filter to US stations
    parsed = []
    for item in raw:
        ev = _parse_event(item)
        if ev is None:
            continue
        if ev.city_slug not in US_STATIONS:
            continue
        parsed.append((ev, item))
    print(f"  US parsable: {len(parsed)}")
    parsed = parsed[:max_events]

    acis_cache: dict[tuple[str, str], float | None] = {}
    done = 0
    for ev, item in parsed:
        done += 1
        exist = conn.execute("SELECT 1 FROM events WHERE event_id=?", (ev.event_id,)).fetchone()
        if exist:
            continue

        key = (ev.station.icao, ev.event_date)
        if key not in acis_cache:
            try:
                acis_cache[key] = acis_daily_max(ev.station.icao, ev.event_date)
            except Exception as e:
                print(f"  [acis err] {ev.city_slug} {ev.event_date}: {e}")
                acis_cache[key] = None
            time.sleep(0.1)
        actual = acis_cache[key]

        conn.execute(
            "INSERT INTO events (event_id, slug, city_slug, station_icao, event_date, "
            "end_date_utc, total_volume, actual_high_f, winning_bucket_id) "
            "VALUES (?,?,?,?,?,?,?,?,NULL)",
            (ev.event_id, ev.slug, ev.city_slug, ev.station.icao, ev.event_date,
             ev.end_date_utc.isoformat(), ev.total_volume, actual),
        )

        # buckets + final outcome
        raw_markets = {m.get("groupItemTitle", "").strip(): m for m in item.get("markets", [])}
        for b in ev.buckets:
            final_yes = None
            rm = raw_markets.get(b.title)
            if rm:
                try:
                    op = json.loads(rm.get("outcomePrices") or "[]")
                    if len(op) == 2:
                        final_yes = 1 if float(op[0]) > 0.5 else 0
                except (json.JSONDecodeError, ValueError):
                    pass
            cur = conn.execute(
                "INSERT INTO buckets (event_id, title, lo, hi, yes_token_id, no_token_id, "
                "final_outcome_yes) VALUES (?,?,?,?,?,?,?)",
                (ev.event_id, b.title,
                 b.lo if b.lo != float("-inf") else -1e9,
                 b.hi if b.hi != float("inf") else 1e9,
                 b.yes_token_id, b.no_token_id, final_yes),
            )
            bucket_id = cur.lastrowid

            # prices: окно = [endDate - 7 days, endDate]
            end_ts = int(ev.end_date_utc.timestamp())
            start_ts = end_ts - 7 * 86400
            pts = fetch_prices(client, b.yes_token_id, start_ts, end_ts)
            if pts:
                conn.executemany(
                    "INSERT OR IGNORE INTO prices (bucket_id, ts, yes_price) VALUES (?,?,?)",
                    [(bucket_id, t, p) for (t, p) in pts],
                )
            time.sleep(0.05)  # be nice to CLOB

        if actual is not None:
            wb = find_winning_bucket_id(conn, ev.event_id, actual)
            if wb is not None:
                conn.execute("UPDATE events SET winning_bucket_id=? WHERE event_id=?", (wb, ev.event_id))

        conn.commit()
        if done % 25 == 0:
            print(f"  progress: {done}/{len(parsed)}  last={ev.city_slug} {ev.event_date}")

    # summary
    n_ev = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    n_ev_resolved = conn.execute("SELECT COUNT(*) FROM events WHERE winning_bucket_id IS NOT NULL").fetchone()[0]
    n_bk = conn.execute("SELECT COUNT(*) FROM buckets").fetchone()[0]
    n_pr = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    print(f"\ndone. events={n_ev} resolved={n_ev_resolved} buckets={n_bk} prices={n_pr}")

    conn.close()
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
