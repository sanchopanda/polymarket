"""Клиент National Weather Service API (api.weather.gov).

Бесплатный, без ключа. Flow:
  1. GET /points/{lat},{lon} → gridId, gridX, gridY, forecast URLs (кешируется по станции)
  2. GET /gridpoints/{gridId}/{x},{y}/forecast/hourly → 156 часовых периодов
  3. max temperature за целевой локальный день = наш point-estimate для дневного high

NWS требует User-Agent с контактом.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

from weather_bot.stations import Station


NWS_BASE = "https://api.weather.gov"
DEFAULT_UA = "weather-bot/0.1 (claude3@icons8.com)"


@dataclass
class GridPoint:
    grid_id: str
    grid_x: int
    grid_y: int
    hourly_url: str
    daily_url: str


@dataclass
class DayForecast:
    event_date: str          # "2026-04-18"
    max_temp_f: float        # high за local day
    hours_covered: int       # сколько часов реально вошло в max (24 = полный день)
    source: str              # "hourly" | "daily"
    lead_hours: float        # время до начала целевого дня (может быть <=0 для today)


class NWSClient:
    def __init__(self, user_agent: str = DEFAULT_UA, timeout: float = 30.0) -> None:
        self._ua = user_agent
        self._client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/geo+json"},
        )
        self._grid_cache: dict[tuple[float, float], GridPoint] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NWSClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def get_grid(self, station: Station) -> GridPoint:
        key = (round(station.lat, 4), round(station.lon, 4))
        with self._lock:
            cached = self._grid_cache.get(key)
        if cached is not None:
            return cached
        url = f"{NWS_BASE}/points/{station.lat},{station.lon}"
        r = self._client.get(url)
        r.raise_for_status()
        props = r.json()["properties"]
        gp = GridPoint(
            grid_id=props["gridId"],
            grid_x=int(props["gridX"]),
            grid_y=int(props["gridY"]),
            hourly_url=props["forecastHourly"],
            daily_url=props["forecast"],
        )
        with self._lock:
            self._grid_cache[key] = gp
        return gp

    def forecast_high(
        self,
        station: Station,
        event_date: str,  # "YYYY-MM-DD" в LOCAL tz станции
    ) -> Optional[DayForecast]:
        """Возвращает прогнозный high на указанный local day. None если дата вне горизонта API."""
        gp = self.get_grid(station)
        r = self._client.get(gp.hourly_url)
        r.raise_for_status()
        periods = r.json()["properties"]["periods"]

        temps: list[float] = []
        for p in periods:
            start = p["startTime"]  # "2026-04-17T18:00:00-04:00"
            local_date = start[:10]  # первые 10 символов = YYYY-MM-DD в local tz
            if local_date == event_date:
                temps.append(float(p["temperature"]))

        if not temps:
            return None

        # Lead hours: delta от now_utc до начала локального дня event_date.
        # Достаточно грубо — через первый период того дня.
        first_period = next((p for p in periods if p["startTime"][:10] == event_date), None)
        lead_hours = 0.0
        if first_period is not None:
            start_dt = datetime.fromisoformat(first_period["startTime"])
            now_dt = datetime.now(start_dt.tzinfo)
            lead_hours = (start_dt - now_dt).total_seconds() / 3600.0

        return DayForecast(
            event_date=event_date,
            max_temp_f=max(temps),
            hours_covered=len(temps),
            source="hourly",
            lead_hours=lead_hours,
        )

    def observed_max_so_far(
        self,
        station: Station,
        event_date: str,   # local-day YYYY-MM-DD
        now_utc: datetime,
    ) -> tuple[Optional[float], int, float]:
        """Максимальная наблюдённая температура °F за local-day до now_utc.

        Возвращает (observed_max_f | None, n_obs, hours_remaining).
          - None если event_date в будущем или нет ни одного наблюдения.
          - hours_remaining — от now_utc до конца local day (может быть <=0 для прошедших дат).
        """
        tz = ZoneInfo(station.timezone)
        start_local = datetime.fromisoformat(f"{event_date}T00:00:00").replace(tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc_dt = start_local.astimezone(timezone.utc)
        end_utc_dt = end_local.astimezone(timezone.utc)

        hours_remaining = (end_utc_dt - now_utc).total_seconds() / 3600.0

        # День ещё не начался — наблюдений по нему быть не может.
        if now_utc < start_utc_dt:
            return (None, 0, hours_remaining)

        # День закончился — все наблюдения уже в прошлом; end — конец дня.
        query_end = min(now_utc, end_utc_dt)
        r = self._client.get(
            f"{NWS_BASE}/stations/{station.icao}/observations",
            params={
                "start": start_utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": query_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "limit": 500,
            },
        )
        r.raise_for_status()
        feats = r.json().get("features", [])

        temps_c: list[float] = []
        for f in feats:
            p = f.get("properties", {})
            t = p.get("temperature") or {}
            val = t.get("value")
            if val is None:
                continue
            if t.get("qualityControl", "") in ("X", "B", "F"):
                continue
            temps_c.append(float(val))

        if not temps_c:
            return (None, 0, hours_remaining)
        max_f = max(temps_c) * 9.0 / 5.0 + 32.0
        return (max_f, len(temps_c), hours_remaining)
