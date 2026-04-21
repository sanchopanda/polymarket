"""SQLite-схема weather_bot.

Пока бот работает в read-only режиме — пишет снимки (forecast + p_model + p_market
по каждому бакету). По этим данным потом считаем retrospective PnL и калибруем σ.

Таблицы:
  forecasts  — один прогноз на (city, event_date) в момент ts. Не привязан к рынку.
  markets    — каталог обнаруженных event'ов.
  buckets    — каталог бакетов внутри event (статичен за жизнь event'а).
  snapshots  — один снимок по (bucket_id, ts): p_model, yes_price, no_price, forecast_id.
  outcomes   — фактический high + winning bucket, заполняется после резолва.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional

from weather_bot.markets import Bucket, WeatherEvent


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class WeatherDB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def close(self) -> None:
        self._conn.close()

    def _migrate(self) -> None:
        c = self._conn
        c.executescript("""
        CREATE TABLE IF NOT EXISTS markets (
            event_id TEXT PRIMARY KEY,
            slug TEXT NOT NULL,
            title TEXT NOT NULL,
            city_slug TEXT NOT NULL,
            station_icao TEXT NOT NULL,
            event_date TEXT NOT NULL,
            end_date_utc TEXT NOT NULL,
            first_seen_ts TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_markets_city_date
            ON markets(city_slug, event_date);

        CREATE TABLE IF NOT EXISTS buckets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            title TEXT NOT NULL,
            lo REAL NOT NULL,       -- -1e9 для левого хвоста, +1e9 для правого
            hi REAL NOT NULL,
            yes_token_id TEXT NOT NULL,
            no_token_id TEXT NOT NULL,
            UNIQUE(event_id, title)
        );
        CREATE INDEX IF NOT EXISTS idx_buckets_event
            ON buckets(event_id);

        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            city_slug TEXT NOT NULL,
            event_date TEXT NOT NULL,
            mu_f REAL NOT NULL,          -- forecast high °F
            sigma_f REAL NOT NULL,       -- используемый RMSE
            lead_hours REAL NOT NULL,
            source TEXT NOT NULL,        -- "nws"
            hours_covered INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
            ON forecasts(city_slug, event_date, ts);

        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            bucket_id INTEGER NOT NULL,
            forecast_id INTEGER,
            p_model REAL NOT NULL,
            yes_price REAL NOT NULL,
            no_price REAL NOT NULL,
            edge_yes REAL NOT NULL       -- p_model - yes_price
        );
        CREATE INDEX IF NOT EXISTS idx_snapshots_bucket_ts
            ON snapshots(bucket_id, ts);

        CREATE TABLE IF NOT EXISTS outcomes (
            event_id TEXT PRIMARY KEY,
            actual_high_f REAL,
            winning_bucket_id INTEGER,
            resolved_ts TEXT
        );

        -- Intraday observations: max температура, наблюдённая к моменту ts.
        -- Один ряд per (station, event_date, ts). Используется при расчёте
        -- intraday-edge (факт уже отрезал часть бакетов).
        CREATE TABLE IF NOT EXISTS obs_max (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            station_icao TEXT NOT NULL,
            event_date TEXT NOT NULL,
            observed_max_f REAL,           -- null если нет наблюдений к этому моменту
            n_obs INTEGER NOT NULL,
            hours_remaining REAL NOT NULL, -- от ts до конца local day
            UNIQUE(station_icao, event_date, ts)
        );
        CREATE INDEX IF NOT EXISTS idx_obs_max_lookup
            ON obs_max(station_icao, event_date, ts);
        """)

        # Миграция: добавляем колонки в snapshots, если их ещё нет.
        existing_cols = {r[1] for r in c.execute("PRAGMA table_info(snapshots)").fetchall()}
        for col, decl in [
            ("yes_best_ask",   "REAL"),
            ("yes_best_bid",   "REAL"),
            ("yes_spread",     "REAL"),
            ("liquidity_num",  "REAL"),
            ("volume_24h",     "REAL"),
            ("last_trade_price", "REAL"),
            ("observed_max_f", "REAL"),
            ("hours_remaining", "REAL"),
        ]:
            if col not in existing_cols:
                c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {decl}")
        c.commit()

    def upsert_market(self, ev: WeatherEvent, now: datetime) -> None:
        c = self._conn
        c.execute("""
            INSERT OR IGNORE INTO markets
                (event_id, slug, title, city_slug, station_icao, event_date, end_date_utc, first_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ev.event_id, ev.slug, ev.title, ev.city_slug,
            ev.station.icao, ev.event_date, _iso(ev.end_date_utc), _iso(now),
        ))
        for b in ev.buckets:
            lo = b.lo if b.lo != float("-inf") else -1e9
            hi = b.hi if b.hi != float("inf") else 1e9
            c.execute("""
                INSERT OR IGNORE INTO buckets
                    (event_id, title, lo, hi, yes_token_id, no_token_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ev.event_id, b.title, lo, hi, b.yes_token_id, b.no_token_id))
        c.commit()

    def get_bucket_id(self, event_id: str, title: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT id FROM buckets WHERE event_id=? AND title=?",
            (event_id, title),
        ).fetchone()
        return int(row["id"]) if row else None

    def insert_forecast(
        self, *, ts: datetime, city_slug: str, event_date: str,
        mu_f: float, sigma_f: float, lead_hours: float,
        source: str, hours_covered: int,
    ) -> int:
        cur = self._conn.execute("""
            INSERT INTO forecasts
                (ts, city_slug, event_date, mu_f, sigma_f, lead_hours, source, hours_covered)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (_iso(ts), city_slug, event_date, mu_f, sigma_f, lead_hours, source, hours_covered))
        self._conn.commit()
        return int(cur.lastrowid)

    def insert_snapshot(
        self, *, ts: datetime, bucket_id: int, forecast_id: int,
        p_model: float, yes_price: float, no_price: float,
        yes_best_ask: float = 0.0, yes_best_bid: float = 0.0, yes_spread: float = 0.0,
        liquidity_num: float = 0.0, volume_24h: float = 0.0, last_trade_price: float = 0.0,
        observed_max_f: float | None = None, hours_remaining: float = 0.0,
    ) -> None:
        self._conn.execute("""
            INSERT INTO snapshots
                (ts, bucket_id, forecast_id, p_model, yes_price, no_price, edge_yes,
                 yes_best_ask, yes_best_bid, yes_spread,
                 liquidity_num, volume_24h, last_trade_price,
                 observed_max_f, hours_remaining)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            _iso(ts), bucket_id, forecast_id, p_model, yes_price, no_price, p_model - yes_price,
            yes_best_ask, yes_best_bid, yes_spread,
            liquidity_num, volume_24h, last_trade_price,
            observed_max_f, hours_remaining,
        ))
        self._conn.commit()

    def insert_obs_max(
        self, *, ts: datetime, station_icao: str, event_date: str,
        observed_max_f: float | None, n_obs: int, hours_remaining: float,
    ) -> None:
        self._conn.execute("""
            INSERT OR REPLACE INTO obs_max
                (ts, station_icao, event_date, observed_max_f, n_obs, hours_remaining)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (_iso(ts), station_icao, event_date, observed_max_f, n_obs, hours_remaining))
        self._conn.commit()

    def set_outcome(
        self, event_id: str, actual_high_f: float,
        winning_bucket_id: int | None, resolved_ts: datetime,
    ) -> None:
        self._conn.execute("""
            INSERT OR REPLACE INTO outcomes
                (event_id, actual_high_f, winning_bucket_id, resolved_ts)
            VALUES (?, ?, ?, ?)
        """, (event_id, actual_high_f, winning_bucket_id, _iso(resolved_ts)))
        self._conn.commit()

    def stats(self) -> dict:
        """Быстрая сводка — для CLI `status`."""
        markets_n = self._conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        forecasts_n = self._conn.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
        snapshots_n = self._conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        resolved_n = self._conn.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
        return {
            "markets": markets_n,
            "forecasts": forecasts_n,
            "snapshots": snapshots_n,
            "resolved": resolved_n,
        }
