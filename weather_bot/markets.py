"""Discovery и парсинг weather-рынков Polymarket.

Event = «Highest temperature in {City} on {Date}» — 11 связанных negRisk-рынков
по бакетам температуры. Мы парсим бакет из `groupItemTitle` каждого рынка.

Пример groupItemTitle: "64-65°F", "53°F or below", "72°F or higher".
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

from weather_bot.stations import Station, get_station


GAMMA_BASE = "https://gamma-api.polymarket.com"
TAG_SLUG = "daily-temperature"


@dataclass
class Bucket:
    """Один бакет внутри event — [lo, hi] в °F, включительно.

    Хвосты: `77°F or below` → lo=-inf, hi=77.  `96°F or higher` → lo=96, hi=+inf.
    Средний: `64-65°F` → lo=64, hi=65.

    Цены:
      yes_price / no_price — outcomePrices из Gamma (≈ mid/last), не исполняемые.
      yes_best_ask / yes_best_bid — YES-side orderbook; исполняемые.
      no_best_ask ≈ 1 - yes_best_bid — оценка (NO-книга торгуется независимо на negRisk,
        но 1-yes_bid — безрисковая верхняя граница того, что дадут за NO).
      spread = yes_best_ask - yes_best_bid.
    """
    title: str               # "64-65°F"
    lo: float                # -inf для лев. хвоста
    hi: float                # +inf для прав. хвоста
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    yes_best_ask: float      # 0..1, 0 если книга пустая
    yes_best_bid: float      # 0..1
    yes_spread: float        # >=0
    liquidity_num: float     # $ notional в книге per market
    volume_24h: float
    last_trade_price: float  # 0..1

    @property
    def is_left_tail(self) -> bool:
        return self.lo == float("-inf")

    @property
    def is_right_tail(self) -> bool:
        return self.hi == float("inf")

    @property
    def no_best_ask_est(self) -> float:
        """Оценка bestAsk для NO-стороны = 1 - yes_best_bid. Верхняя граница цены NO."""
        return max(0.0, 1.0 - self.yes_best_bid)


@dataclass
class WeatherEvent:
    slug: str
    title: str
    city_slug: str            # "nyc"
    station: Station          # распарсенная станция резолва
    event_date: str           # "2026-04-18"
    end_date_utc: datetime    # endDate рынка — торги закрываются
    buckets: list[Bucket]
    total_volume: float
    event_id: str


_BUCKET_MID = re.compile(r"^(\d+)-(\d+)°F$")
_BUCKET_LOW = re.compile(r"^(\d+)°F or below$")
_BUCKET_HIGH = re.compile(r"^(\d+)°F or higher$")
_SLUG_RE = re.compile(r"^highest-temperature-in-(.+?)-on-([a-z]+)-(\d+)-(\d{4})$")

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def _parse_bucket_title(title: str) -> tuple[float, float] | None:
    t = title.strip()
    m = _BUCKET_MID.match(t)
    if m:
        return float(m.group(1)), float(m.group(2))
    m = _BUCKET_LOW.match(t)
    if m:
        return float("-inf"), float(m.group(1))
    m = _BUCKET_HIGH.match(t)
    if m:
        return float(m.group(1)), float("inf")
    return None


def _parse_slug(slug: str) -> tuple[str, str] | None:
    """Возвращает (city_slug, event_date_iso) или None."""
    m = _SLUG_RE.match(slug)
    if not m:
        return None
    city = m.group(1)
    month = _MONTHS.get(m.group(2))
    if month is None:
        return None
    day = int(m.group(3))
    year = int(m.group(4))
    return city, f"{year:04d}-{month:02d}-{day:02d}"


def _parse_float(raw) -> float:
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(raw: str | None) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_event(raw: dict) -> WeatherEvent | None:
    slug = raw.get("slug", "")
    parsed = _parse_slug(slug)
    if parsed is None:
        return None
    city_slug, event_date = parsed
    station = get_station(city_slug)
    if station is None:
        return None  # не US, пока скипаем

    end_dt = _parse_dt(raw.get("endDate"))
    if end_dt is None:
        return None

    buckets: list[Bucket] = []
    for m in raw.get("markets", []):
        title = (m.get("groupItemTitle") or "").strip()
        parsed_bucket = _parse_bucket_title(title)
        if parsed_bucket is None:
            continue
        lo, hi = parsed_bucket

        try:
            prices = json.loads(m.get("outcomePrices") or "[]")
            tokens = json.loads(m.get("clobTokenIds") or "[]")
        except json.JSONDecodeError:
            continue
        if len(prices) != 2 or len(tokens) != 2:
            continue

        yes_best_ask = _parse_float(m.get("bestAsk"))
        spread = _parse_float(m.get("spread"))
        yes_best_bid = max(0.0, yes_best_ask - spread) if yes_best_ask > 0 else 0.0
        buckets.append(Bucket(
            title=title,
            lo=lo, hi=hi,
            yes_token_id=str(tokens[0]),
            no_token_id=str(tokens[1]),
            yes_price=_parse_float(prices[0]),
            no_price=_parse_float(prices[1]),
            yes_best_ask=yes_best_ask,
            yes_best_bid=yes_best_bid,
            yes_spread=spread,
            liquidity_num=_parse_float(m.get("liquidityNum") or m.get("liquidity")),
            volume_24h=_parse_float(m.get("volume24hrClob") or m.get("volume24hr")),
            last_trade_price=_parse_float(m.get("lastTradePrice")),
        ))

    if not buckets:
        return None

    buckets.sort(key=lambda b: (b.lo if b.lo != float("-inf") else -1e9))

    return WeatherEvent(
        slug=slug,
        title=raw.get("title", ""),
        city_slug=city_slug,
        station=station,
        event_date=event_date,
        end_date_utc=end_dt,
        buckets=buckets,
        total_volume=_parse_float(raw.get("volume")),
        event_id=str(raw.get("id", "")),
    )


def fetch_active_events(
    *,
    limit: int = 500,
    cities: set[str] | None = None,
    timeout: float = 30.0,
) -> list[WeatherEvent]:
    """Тянет все активные daily-temperature events и парсит их.

    Если `cities` задан — фильтрует по city_slug.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(
            f"{GAMMA_BASE}/events",
            params={
                "limit": limit,
                "active": "true",
                "closed": "false",
                "tag_slug": TAG_SLUG,
                "end_date_min": now_iso,
            },
        )
        resp.raise_for_status()
        raw = resp.json()

    out: list[WeatherEvent] = []
    for item in raw:
        ev = _parse_event(item)
        if ev is None:
            continue
        if cities and ev.city_slug not in cities:
            continue
        out.append(ev)
    out.sort(key=lambda e: e.end_date_utc)
    return out
