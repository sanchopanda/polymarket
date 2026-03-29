from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from sports_arb_bot.models import KalshiMarket, KalshiMatchEvent

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Kalshi series_ticker → sport label
SERIES_TO_SPORT: dict[str, str] = {
    "KXWTACHALLENGERMATCH": "wta",
    "KXWTAMATCH": "wta",
    "KXATPCHALLENGERMATCH": "atp",
    "KXATPMATCH": "atp",
    "KXBOXING": "boxing",
    "KXMMA": "mma",
    "KXR6GAME": "r6",
    "KXIPLGAME": "ipl",
    "KXT20MATCH": "t20",
    "KXNCAAWBGAME": "cwbb",
    "KXNCAAMBGAME": "cbb",
    "KXNBAGAME": "nba",
}


def _parse_dt(raw: str | None) -> Optional[datetime]:
    if not raw:
        return None
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _to_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _parse_market(raw: dict) -> KalshiMarket:
    return KalshiMarket(
        ticker=raw.get("ticker", ""),
        player_name=raw.get("yes_sub_title") or raw.get("title", ""),
        yes_ask=_to_float(raw.get("yes_ask_dollars")),
        yes_bid=_to_float(raw.get("yes_bid_dollars")),
        no_ask=_to_float(raw.get("no_ask_dollars")),
        no_bid=_to_float(raw.get("no_bid_dollars")),
        volume=_to_float(raw.get("volume_fp")),
        open_interest=_to_float(raw.get("open_interest_fp")),
    )


def _group_by_event(markets: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for m in markets:
        et = m.get("event_ticker", "")
        groups.setdefault(et, []).append(m)
    return groups


class KalshiSportsFeed:
    def __init__(self, timeout: float = 15.0) -> None:
        self._http = httpx.Client(timeout=timeout)

    def fetch(self, series_tickers: list[str]) -> list[KalshiMatchEvent]:
        results: list[KalshiMatchEvent] = []
        for series in series_tickers:
            results.extend(self._fetch_series(series))
        return results

    def _fetch_series(self, series_ticker: str) -> list[KalshiMatchEvent]:
        markets_raw = self._get_markets(series_ticker)
        if not markets_raw:
            return []

        now = datetime.now(tz=timezone.utc)
        exp_since  = now - timedelta(hours=5)   # берём и уже идущие матчи
        exp_cutoff = now + timedelta(hours=10)  # и матчи на 10ч вперёд

        groups = _group_by_event(markets_raw)
        events: list[KalshiMatchEvent] = []
        for event_ticker, mkt_list in groups.items():
            # expected_expiration берём из первого маркета
            exp = _parse_dt(mkt_list[0].get("expected_expiration_time"))
            if not exp:
                continue
            if not (exp_since <= exp <= exp_cutoff):
                continue

            event_meta = self._get_event_meta(event_ticker)
            ka_markets = [_parse_market(m) for m in mkt_list]

            product_meta = (event_meta or {}).get("product_metadata") or {}
            events.append(KalshiMatchEvent(
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                title=(event_meta or {}).get("title") or mkt_list[0].get("title", ""),
                sub_title=(event_meta or {}).get("sub_title") or "",
                competition=product_meta.get("competition") or "",
                expected_expiration=exp,
                strike_type=mkt_list[0].get("strike_type", ""),
                markets=ka_markets,
            ))
        return events

    def _get_markets(self, series_ticker: str) -> list[dict]:
        results = []
        cursor = None
        page_size = 200
        for _ in range(20):  # макс 20 страниц
            params: dict = {"series_ticker": series_ticker, "status": "open", "limit": str(page_size)}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = self._http.get(f"{KALSHI_BASE}/markets", params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"[ka-sports-feed] ошибка {series_ticker}: {e}")
                break
            page = data.get("markets") or []
            results.extend(page)
            cursor = data.get("cursor")
            if not cursor or len(page) < page_size:
                break
        return results

    def _get_event_meta(self, event_ticker: str) -> Optional[dict]:
        try:
            resp = self._http.get(f"{KALSHI_BASE}/events/{event_ticker}")
            resp.raise_for_status()
            return resp.json().get("event")
        except Exception:
            return None
