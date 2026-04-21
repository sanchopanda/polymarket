"""Заполняет outcomes в weather_bot.db официальным daily high через ACIS.

Источник: https://data.rcc-acis.org/StnData (RCC-ACIS, NOAA-backed). Это тот же
климатический summary (CF6), на который Polymarket резолвит US-маркеты.

Предыдущая версия использовала NWS `/observations` и брала max текущих наблюдений.
Проблема: ASOS 5-min SPECI часто репортят в целых °C, и ночной 22°C попадал как
"максимум дня" вместо реального daily high из CF6. Пример: KATL 2026-04-19
дал 71.6°F вместо официальных 69°F. См. docs/recovery/resolve_fix_2026-04-21.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import httpx

sys.path.insert(0, ".")
from weather_bot.db import WeatherDB
from weather_bot.stations import US_STATIONS


ACIS_URL = "https://data.rcc-acis.org/StnData"
UA = "weather-bot/0.2 (claude3@icons8.com)"
DB_PATH = "data/weather_bot.db"


def acis_daily_max_f(icao: str, event_date: str) -> tuple[float, int] | None:
    """Официальный daily max °F для станции на local day. (value_f, n_obs=1) или None.

    ACIS возвращает 'M' когда день отсутствует/ещё не резолвнут.
    """
    body = {
        "sid": icao,
        "sdate": event_date,
        "edate": event_date,
        "elems": [{"name": "maxt", "interval": "dly", "units": "degreeF"}],
    }
    r = httpx.post(ACIS_URL, json=body, headers={"User-Agent": UA}, timeout=30.0)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return None
    val = data[0][1]
    if val in ("M", "T", "", None):
        return None
    try:
        return float(val), 1
    except ValueError:
        return None


def find_winning_bucket(db: WeatherDB, event_id: str, actual_f: float) -> int | None:
    rounded = round(actual_f)
    rows = db._conn.execute(
        "SELECT id, lo, hi FROM buckets WHERE event_id=?", (event_id,)
    ).fetchall()
    for r in rows:
        lo, hi = r["lo"], r["hi"]
        lo_ = -1e18 if lo <= -1e8 else lo
        hi_ = 1e18 if hi >= 1e8 else hi
        if lo_ <= rounded <= hi_:
            return int(r["id"])
    return None


def main() -> int:
    db = WeatherDB(DB_PATH)
    from zoneinfo import ZoneInfo
    today_local_any_us = datetime.now(ZoneInfo("America/New_York")).date().isoformat()

    # --force: пере-резолвить существующие outcomes.
    force = "--force" in sys.argv

    rows = db._conn.execute(
        "SELECT event_id, city_slug, station_icao, event_date FROM markets "
        "WHERE event_date < ? ORDER BY event_date, city_slug",
        (today_local_any_us,),
    ).fetchall()

    print(f"resolving {len(rows)} past markets (today_local={today_local_any_us}, force={force})")
    done, skipped, missing = 0, 0, 0
    now = datetime.now(timezone.utc)
    for row in rows:
        eid = row["event_id"]
        existing = db._conn.execute(
            "SELECT actual_high_f FROM outcomes WHERE event_id=?", (eid,)
        ).fetchone()
        if existing and not force:
            skipped += 1
            continue
        station = US_STATIONS.get(row["city_slug"])
        if not station:
            continue
        try:
            res = acis_daily_max_f(station.icao, row["event_date"])
        except Exception as e:
            print(f"  [warn] {row['city_slug']} {row['event_date']}: {e}")
            continue
        if res is None:
            missing += 1
            print(f"  [skip] {row['city_slug']} {row['event_date']}: ACIS no data")
            continue
        high_f, n = res
        wb = find_winning_bucket(db, eid, high_f)
        if existing:
            old = existing["actual_high_f"]
            db.set_outcome(eid, high_f, wb, now)
            tag = "upd" if abs(old - high_f) >= 0.05 else "ok "
            print(f"  {tag} {row['city_slug']:>14} {row['event_date']}  {old:5.1f} → {high_f:5.1f}°F  bucket={wb}")
        else:
            db.set_outcome(eid, high_f, wb, now)
            print(f"  new {row['city_slug']:>14} {row['event_date']}  high={high_f:5.1f}°F  bucket_id={wb}")
        done += 1

    print(f"done: {done} written, {skipped} unchanged, {missing} missing")
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
