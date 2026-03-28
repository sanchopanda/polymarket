"""
Скачивает активные R6 (Rainbow Six Siege) рынки с Polymarket и Kalshi.

Окно: [now-2h, now+72h] — захватывает игры, запланированные на несколько дней вперёд.

Сохраняет:
  data/pm_r6.json           — урезанные поля PM
  data/kalshi_r6.json       — урезанные поля Kalshi
  data/pm_r6_titles.json    — slug + question (для быстрого просмотра)
  data/kalshi_r6_titles.json— event_ticker + title
  data/pm_r6_example.json   — 2 полных сырых примера PM
  data/kalshi_r6_example.json—2 полных сырых примера Kalshi

Usage:
    python3 scripts/dump_r6_markets.py [--hours 72]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

PM_SERIES_SLUG = "rainbow-six-siege"
KALSHI_SERIES = "KXR6GAME"

OUT_DIR = Path(__file__).resolve().parents[1] / "data"


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    raw = raw.strip().replace(" ", "T")
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


def _slim_pm(m: dict) -> dict:
    ev = (m.get("events") or [{}])[0]
    meta = ev.get("eventMetadata") or {}
    return {
        "slug":             m.get("slug"),
        "question":         m.get("question"),
        "series_slug":      ev.get("seriesSlug"),
        "league":           meta.get("league"),
        "serie":            meta.get("serie"),
        "game_start_time":  m.get("gameStartTime"),
        "end_date":         m.get("endDate"),
        "outcomes":         m.get("outcomes"),
        "outcome_prices":   m.get("outcomePrices"),
        "clob_token_ids":   m.get("clobTokenIds"),
        "accepting_orders": m.get("acceptingOrders"),
        "best_bid":         m.get("bestBid"),
        "best_ask":         m.get("bestAsk"),
        "liquidity":        m.get("liquidityNum"),
        "url":              f"https://polymarket.com/esports/rainbow-six-siege/{m.get('slug','')}",
    }


def _slim_ka(m: dict) -> dict:
    return {
        "ticker":                   m.get("ticker"),
        "event_ticker":             m.get("event_ticker"),
        "series_ticker":            m.get("series_ticker"),
        "title":                    m.get("title"),
        "yes_sub_title":            m.get("yes_sub_title"),
        "no_sub_title":             m.get("no_sub_title"),
        "expected_expiration_time": m.get("expected_expiration_time"),
        "close_time":               m.get("close_time"),
        "status":                   m.get("status"),
        "yes_ask":                  m.get("yes_ask_dollars"),
        "yes_bid":                  m.get("yes_bid_dollars"),
        "no_ask":                   m.get("no_ask_dollars"),
        "no_bid":                   m.get("no_bid_dollars"),
        "last_price":               m.get("last_price_dollars"),
        "liquidity":                m.get("liquidity_dollars"),
        "open_interest":            m.get("open_interest_fp"),
        "url":                      f"https://kalshi.com/markets/{m.get('event_ticker','').lower()}",
    }


def fetch_pm_r6(hours_ahead: int) -> tuple[list[dict], list[dict]]:
    now = datetime.now(tz=timezone.utc)
    since = now - timedelta(hours=2)
    cutoff = now + timedelta(hours=hours_ahead)

    result_raw: list[dict] = []
    offset = 0

    with httpx.Client(timeout=30) as http:
        for _ in range(50):
            resp = http.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "sportsMarketType": "moneyline",
                    "order": "gameStartTime",
                    "ascending": "false",
                    "limit": "500",
                    "offset": str(offset),
                },
            )
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break

            earliest_on_page: datetime | None = None
            for m in page:
                dt = _parse_dt(m.get("gameStartTime"))
                if dt is None:
                    continue
                if earliest_on_page is None or dt < earliest_on_page:
                    earliest_on_page = dt
                ev = (m.get("events") or [{}])[0]
                if (since <= dt <= cutoff
                        and m.get("sportsMarketType") == "moneyline"
                        and ev.get("seriesSlug") == PM_SERIES_SLUG):
                    result_raw.append(m)

            label = earliest_on_page.strftime("%Y-%m-%d %H:%M") if earliest_on_page else "?"
            print(f"  PM offset={offset}: {len(page)} рынков, earliest gameStartTime={label}")

            if earliest_on_page and earliest_on_page < since:
                break
            if len(page) < 500:
                break
            offset += 500

    return [_slim_pm(m) for m in result_raw], result_raw


def fetch_kalshi_r6(hours_ahead: int) -> tuple[list[dict], list[dict]]:
    now = datetime.now(tz=timezone.utc)
    since = now - timedelta(hours=2)
    cutoff = now + timedelta(hours=hours_ahead)

    result_raw: list[dict] = []
    cursor: str | None = None

    with httpx.Client(timeout=20) as http:
        while True:
            params: dict = {
                "status": "open",
                "series_ticker": KALSHI_SERIES,
                "limit": "200",
            }
            if cursor:
                params["cursor"] = cursor

            resp = http.get(f"{KALSHI_BASE}/markets", params=params)
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("markets", [])

            filtered = [
                m for m in markets
                if (dt := _parse_dt(m.get("expected_expiration_time"))) and since <= dt <= cutoff
            ]
            result_raw.extend(filtered)
            print(f"  Kalshi {KALSHI_SERIES}: {len(filtered)}/{len(markets)} рынков в окне")

            cursor = data.get("cursor")
            if not cursor or not markets:
                break

    return [_slim_ka(m) for m in result_raw], result_raw


def main() -> None:
    parser = argparse.ArgumentParser(description="Дамп R6 рынков")
    parser.add_argument("--hours", type=int, default=72,
                        help="Горизонт вперёд в часах (default 72)")
    args = parser.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    now = datetime.now(tz=timezone.utc)
    print(f"Время: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Окно: [{(now - timedelta(hours=2)).strftime('%m-%d %H:%M')} — "
          f"{(now + timedelta(hours=args.hours)).strftime('%m-%d %H:%M')} UTC]\n")

    print("[PM] Скачиваем R6...")
    pm_slim, pm_raw = fetch_pm_r6(args.hours)
    (OUT_DIR / "pm_r6.json").write_text(json.dumps(pm_slim, ensure_ascii=False, indent=2))
    (OUT_DIR / "pm_r6_example.json").write_text(json.dumps(pm_raw[:2], ensure_ascii=False, indent=2))
    pm_titles = [{"slug": m.get("slug"), "question": m.get("question"),
                  "game_start_time": m.get("game_start_time"), "league": m.get("league")}
                 for m in pm_slim]
    (OUT_DIR / "pm_r6_titles.json").write_text(json.dumps(pm_titles, ensure_ascii=False, indent=2))
    print(f"[PM] {len(pm_slim)} рынков → pm_r6.json, pm_r6_titles.json")
    for m in sorted(pm_slim, key=lambda x: x.get("game_start_time") or ""):
        teams = " vs ".join(json.loads(m.get("outcomes") or "[]") or [])
        print(f"  {str(m.get('game_start_time') or '?')[:16]}  [{m.get('league')}]  {teams}")

    print(f"\n[Kalshi] Скачиваем {KALSHI_SERIES}...")
    ka_slim, ka_raw = fetch_kalshi_r6(args.hours)
    (OUT_DIR / "kalshi_r6.json").write_text(json.dumps(ka_slim, ensure_ascii=False, indent=2))
    (OUT_DIR / "kalshi_r6_example.json").write_text(json.dumps(ka_raw[:2], ensure_ascii=False, indent=2))

    seen_events: set[str] = set()
    ka_titles = []
    for m in sorted(ka_slim, key=lambda x: x.get("expected_expiration_time") or ""):
        ev = m.get("event_ticker", "")
        if ev not in seen_events:
            seen_events.add(ev)
            ka_titles.append({"event_ticker": ev, "title": m.get("title"),
                               "expected_expiration_time": m.get("expected_expiration_time")})
            print(f"  {str(m.get('expected_expiration_time') or '?')[:16]}  {ev}  {m.get('yes_sub_title')}")
    (OUT_DIR / "kalshi_r6_titles.json").write_text(json.dumps(ka_titles, ensure_ascii=False, indent=2))
    print(f"[Kalshi] {len(ka_slim)} рынков, {len(ka_titles)} событий → kalshi_r6.json, kalshi_r6_titles.json")


if __name__ == "__main__":
    main()
