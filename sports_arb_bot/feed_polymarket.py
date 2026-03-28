from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from sports_arb_bot.models import PMSportsEvent

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Slug-prefix → sport label для теннисных серий
SLUG_PREFIX_TO_SPORT: dict[str, str] = {
    "wta-": "wta",
    "atp-": "atp",
}

# tag_slug для спортов без серий (бокс, mma и т.д.)
TAG_TO_SPORT: dict[str, str] = {
    "boxing": "boxing",
    "mma": "mma",
}

# PM seriesSlug → internal sport label (для эспорта и других серий с длинным slug)
SERIES_SLUG_TO_SPORT: dict[str, str] = {
    "rainbow-six-siege": "r6",
    "indian-premier-league": "ipl",
}

DEFAULT_WINDOW_HOURS = 24


def _parse_dt(raw: str | None) -> Optional[datetime]:
    if not raw:
        return None
    raw = raw.strip()
    # Формат PM: "2026-03-27 10:00:00+00" или ISO с T
    raw = raw.replace(" ", "T")
    if raw.endswith("+00"):
        raw += ":00"
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _parse_json_field(raw) -> list:
    if isinstance(raw, list):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return []


def _sport_from_slug(slug: str) -> Optional[str]:
    for prefix, sport in SLUG_PREFIX_TO_SPORT.items():
        if slug.startswith(prefix):
            return sport
    return None


def _sport_from_tags(tags: list[dict]) -> Optional[str]:
    slugs = {t.get("slug", "") for t in tags}
    for tag_slug, sport in TAG_TO_SPORT.items():
        if tag_slug in slugs:
            return sport
    return None


def _sport_from_series_slug(series_slug: str) -> Optional[str]:
    return SERIES_SLUG_TO_SPORT.get(series_slug)


def _market_to_pm(raw: dict) -> Optional[PMSportsEvent]:
    """Преобразует сырой dict рынка из /markets в PMSportsEvent."""
    # Только moneyline рынки
    if raw.get("sportsMarketType") != "moneyline":
        return None

    slug = raw.get("slug") or ""
    sport = _sport_from_slug(slug)

    # Для эспорта и других серий с длинным seriesSlug
    if not sport:
        ev_list = raw.get("events") or []
        series_slug = (ev_list[0].get("seriesSlug") or "") if ev_list else ""
        sport = _sport_from_series_slug(series_slug)

    # Для бокса и других спортов без серий — ищем в tags
    if not sport:
        tags = raw.get("tags") or []
        if not isinstance(tags, list):
            tags = []
        sport = _sport_from_tags(tags)

    if not sport:
        return None

    outcomes = _parse_json_field(raw.get("outcomes") or [])
    if len(outcomes) < 2:
        return None

    prices_raw = _parse_json_field(raw.get("outcomePrices") or [])
    try:
        prices = [float(p) for p in prices_raw]
    except (TypeError, ValueError):
        prices = []

    token_ids = _parse_json_field(raw.get("clobTokenIds") or [])

    game_date = (
        _parse_dt(raw.get("gameStartTime"))
        or _parse_dt(raw.get("startDate"))
        or _parse_dt(raw.get("endDate"))
    )
    if not game_date:
        return None

    end_date = _parse_dt(raw.get("endDate")) or game_date

    # Турнир — из events если есть, иначе из title
    events = raw.get("events") or []
    league = ""
    if events:
        meta = events[0].get("eventMetadata") or {}
        league = meta.get("league") or ""
    if not league:
        # Пытаемся достать из question/title: "Dubrovnik: Player A vs Player B"
        title = raw.get("question") or raw.get("title") or ""
        if ":" in title:
            league = title.split(":")[0].strip()

    title = raw.get("question") or raw.get("title") or slug

    return PMSportsEvent(
        slug=slug,
        title=title,
        sport=sport,
        league=league,
        game_date=game_date,
        game_id=None,
        players=outcomes,
        prices=prices,
        token_ids=token_ids,
        market_id=str(raw.get("id") or ""),
        end_date=end_date,
    )


class PolymarketSportsFeed:
    def __init__(self, timeout: float = 15.0) -> None:
        self._http = httpx.Client(timeout=timeout)

    def fetch(
        self,
        sports: list[str],
        window_hours: float = DEFAULT_WINDOW_HOURS,
    ) -> list[PMSportsEvent]:
        """
        Логика 1-в-1 как fetch_pm_sports в scripts/dump_expiring_markets.py.
        Пагинирует по gameStartTime desc, фильтрует по seriesSlug и временному окну.
        """
        sports_set = set(sports)
        now = datetime.now(tz=timezone.utc)
        since = now - timedelta(hours=1, minutes=45)
        cutoff = now + timedelta(hours=5)

        result_raw: list[dict] = []
        offset = 0
        page_size = 500

        for _ in range(200):
            try:
                resp = self._http.get(
                    f"{GAMMA_BASE}/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "sportsMarketType": "moneyline",
                        "order": "gameStartTime",
                        "ascending": "false",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                )
                resp.raise_for_status()
                page = resp.json()
            except Exception as e:
                print(f"[pm-sports-feed] ошибка: {e}")
                break

            if not page:
                break

            earliest_on_page = None
            for m in page:
                dt = _parse_dt(m.get("gameStartTime"))
                if dt is None:
                    continue
                if earliest_on_page is None or dt < earliest_on_page:
                    earliest_on_page = dt
                ev = (m.get("events") or [{}])[0]
                series_slug = ev.get("seriesSlug") or ""
                # Нормализуем: "rainbow-six-siege" → "r6", иначе используем как есть
                sport = SERIES_SLUG_TO_SPORT.get(series_slug, series_slug)
                if (since <= dt <= cutoff
                        and m.get("sportsMarketType") == "moneyline"
                        and sport in sports_set):
                    result_raw.append(m)

            if earliest_on_page and earliest_on_page < since:
                break
            if len(page) < page_size:
                break

            offset += page_size

        seen_slugs: set[str] = set()
        results: list[PMSportsEvent] = []
        for raw in result_raw:
            ev = _market_to_pm(raw)
            if ev is None:
                continue
            if ev.slug in seen_slugs:
                continue
            seen_slugs.add(ev.slug)
            results.append(ev)
        return results

    def fetch_tags_report(self) -> dict[str, int]:
        """Скачивает первые 500 moneyline рынков, возвращает {tag_slug: count}."""
        try:
            resp = self._http.get(
                f"{GAMMA_BASE}/markets",
                params={"active": "true", "closed": "false",
                        "sportsMarketType": "moneyline",
                        "order": "gameStartTime", "ascending": "false",
                        "limit": "500"},
            )
            resp.raise_for_status()
            raw_markets = resp.json()
        except Exception as e:
            print(f"[pm-sports-feed] ошибка tags: {e}")
            return {}
        tag_counts: dict[str, int] = {}
        for m in raw_markets:
            if m.get("sportsMarketType") != "moneyline":
                continue
            for t in (m.get("tags") or []):
                slug = t.get("slug") or t.get("label") or ""
                if slug:
                    tag_counts[slug] = tag_counts.get(slug, 0) + 1
        return dict(sorted(tag_counts.items(), key=lambda x: -x[1]))
