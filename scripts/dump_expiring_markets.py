"""
Скачивает активные спортивные рынки с Polymarket и Kalshi.

Сохраняет:
  data/pm_sports.json         — урезанные поля PM
  data/kalshi_sports.json     — урезанные поля Kalshi
  data/pm_example_full.json   — 2 полных примера PM (для справки)
  data/kalshi_example_full.json — 2 полных примера Kalshi (для справки)

Usage:
    python3 scripts/dump_expiring_markets.py [--hours 4] [--limit 200]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Активные серии Kalshi по спортам.
# TODO: расширить список — см. docs/todo_kalshi_series.md
KALSHI_SERIES_ACTIVE = [
    # Теннис (активно)
    "KXWTACHALLENGERMATCH",   # WTA Challenger
    "KXATPCHALLENGERMATCH",   # ATP Challenger
    "KXATPMATCH",             # ATP Tour (основной тур)
    # Единоборства
    "KXBOXING",               # Бокс
    # Баскетбол
    "KXNBAGAME",              # NBA
    # Хоккей
    "KXNHLGAME",              # NHL
    # Бейсбол
    "KXMLBGAME",              # MLB
    # Киберспорт
    "KXDOTA2GAME",            # Dota 2
    "KXCS2GAME",              # CS2 / Counter-Strike
    "KXLOLGAME",              # League of Legends
]

# Текущий фильтр: только эти серии Kalshi
KALSHI_SPORTS_SERIES = [
    "KXWTACHALLENGERMATCH",
    "KXATPCHALLENGERMATCH",
    "KXATPMATCH",
]

# Текущий фильтр: только эти спорты PM (seriesSlug)
PM_SPORTS_FILTER = {"wta", "atp"}

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
    return {
        "slug":             m.get("slug"),
        "question":         m.get("question"),
        "sport":            ev.get("seriesSlug"),
        "game_start_time":  m.get("gameStartTime"),
        "end_date":         m.get("endDate"),
        "outcomes":         m.get("outcomes"),
        "outcome_prices":   m.get("outcomePrices"),
        "clob_token_ids":   m.get("clobTokenIds"),
        "accepting_orders": m.get("acceptingOrders"),
        "best_bid":         m.get("bestBid"),
        "best_ask":         m.get("bestAsk"),
        "liquidity":        m.get("liquidityNum"),
        "url":              f"https://polymarket.com/sports/{ev.get('seriesSlug','')}/{m.get('slug','')}",
    }


def _slim_ka(m: dict) -> dict:
    return {
        "ticker":                   m.get("ticker"),
        "event_ticker":             m.get("event_ticker"),
        "title":                    m.get("title"),
        "rules_primary":            m.get("rules_primary"),
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
        "volume_24h":               m.get("volume_24h_fp"),
    }


def fetch_pm_sports(limit: int) -> tuple[list[dict], list[dict]]:
    """
    Скачивает PM moneyline рынки с gameStartTime в окне [now-3h45m, now+3h].
    Логика: предполагаем макс. длительность матча 4ч, буфер 15мин.
    Захватывает текущие матчи (начались до 3ч45м назад) и ближайшие (через 3ч).
    Пагинирует по offset пока не выйдем за нижнюю границу окна.
    Возвращает (slim_list, raw_list).
    """
    now = datetime.now(tz=timezone.utc)
    since = now - timedelta(hours=1, minutes=45)
    cutoff = now + timedelta(hours=5)

    result_raw: list[dict] = []
    offset = 0
    page_size = 500

    max_pages = 200
    with httpx.Client(timeout=30) as http:
        for page_num in range(max_pages):
            resp = http.get(
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
            if not page:
                print(f"  PM offset={offset}: пусто, стоп")
                break

            earliest_on_page = None
            for m in page:
                dt = _parse_dt(m.get("gameStartTime"))
                if dt is None:
                    continue
                if earliest_on_page is None or dt < earliest_on_page:
                    earliest_on_page = dt
                ev = (m.get("events") or [{}])[0]
                sport = ev.get("seriesSlug") or ""
                if since <= dt <= cutoff and m.get("sportsMarketType") == "moneyline" and sport in PM_SPORTS_FILTER:
                    result_raw.append(m)

            print(f"  PM offset={offset}: {len(page)} рынков, "
                  f"gameStartTime до {earliest_on_page.strftime('%Y-%m-%d %H:%M') if earliest_on_page else '?'}")

            if earliest_on_page and earliest_on_page < since:
                break
            if len(page) < page_size:
                break

            offset += page_size

    result_raw = result_raw[:limit]
    return [_slim_pm(m) for m in result_raw], result_raw


def fetch_kalshi_sports(limit: int) -> tuple[list[dict], list[dict]]:
    """
    Скачивает Kalshi рынки по спортивным сериям.
    Фильтрует по expected_expiration_time в окне [now+15m, now+7h].
    Логика: матч заканчивается через 15мин..7ч (текущие и ближайшие).
    7ч = 3ч вперёд (горизонт) + 4ч (макс. длительность матча).
    Возвращает (slim_list, raw_list).
    """
    now = datetime.now(tz=timezone.utc)
    since = now + timedelta(minutes=15)
    cutoff = now + timedelta(hours=7)

    result_raw: list[dict] = []
    with httpx.Client(timeout=20) as http:
        for series in KALSHI_SPORTS_SERIES:
            try:
                resp = http.get(
                    f"{KALSHI_BASE}/markets",
                    params={
                        "status": "open",
                        "series_ticker": series,
                        "limit": "200",
                    },
                )
                resp.raise_for_status()
                markets = resp.json().get("markets", [])
                filtered = [
                    m for m in markets
                    if (dt := _parse_dt(m.get("expected_expiration_time"))) and since <= dt <= cutoff
                ]
                result_raw.extend(filtered)
                print(f"  {series}: {len(filtered)}/{len(markets)} рынков")
            except Exception as e:
                print(f"  [Kalshi] ошибка серии {series}: {e}")

    result_raw = result_raw[:limit]
    return [_slim_ka(m) for m in result_raw], result_raw


def main() -> None:
    parser = argparse.ArgumentParser(description="Дамп спортивных рынков")
    parser.add_argument("--limit", type=int, default=200, help="Макс. кол-во рынков (default 200)")
    args = parser.parse_args()

    OUT_DIR.mkdir(exist_ok=True)
    now = datetime.now(tz=timezone.utc)
    pm_since  = now - timedelta(hours=3, minutes=45)
    pm_cutoff = now + timedelta(hours=3)
    ka_since  = now + timedelta(minutes=15)
    ka_cutoff = now + timedelta(hours=7)
    print(f"PM окно:     gameStartTime {pm_since.strftime('%H:%M')} — {pm_cutoff.strftime('%H:%M UTC')} (1ч45м назад / 5ч вперёд)")
    print(f"Kalshi окно: exp_time      {ka_since.strftime('%H:%M')} — {ka_cutoff.strftime('%H:%M UTC')} (15м / 7ч)\n")

    print("[PM] Скачиваем...")
    pm_slim, pm_raw = fetch_pm_sports(args.limit)
    (OUT_DIR / "pm_sports.json").write_text(json.dumps(pm_slim, ensure_ascii=False, indent=2))
    (OUT_DIR / "pm_example_full.json").write_text(json.dumps(pm_raw[:2], ensure_ascii=False, indent=2))
    pm_titles = [{"slug": m.get("slug"), "question": m.get("question")} for m in pm_slim]
    (OUT_DIR / "pm_titles.json").write_text(json.dumps(pm_titles, ensure_ascii=False, indent=2))
    print(f"[PM] {len(pm_slim)} рынков → pm_sports.json, pm_titles.json")
    for m in sorted(pm_slim, key=lambda x: x.get("game_start_time") or ""):
        print(f"  {(m.get('game_start_time') or '?')[:16]}  {m['slug']}")

    print(f"\n[Kalshi] Скачиваем...")
    ka_slim, ka_raw = fetch_kalshi_sports(args.limit)
    (OUT_DIR / "kalshi_sports.json").write_text(json.dumps(ka_slim, ensure_ascii=False, indent=2))
    (OUT_DIR / "kalshi_example_full.json").write_text(json.dumps(ka_raw[:2], ensure_ascii=False, indent=2))
    seen_events: set[str] = set()
    ka_titles = []
    for m in sorted(ka_slim, key=lambda x: x.get("expected_expiration_time") or ""):
        ev = m.get("event_ticker", "")
        if ev not in seen_events:
            seen_events.add(ev)
            ka_titles.append({"event_ticker": ev, "title": m.get("title")})
            print(f"  {(m.get('expected_expiration_time') or '?')[:16]}  {ev}")
    (OUT_DIR / "kalshi_titles.json").write_text(json.dumps(ka_titles, ensure_ascii=False, indent=2))
    print(f"[Kalshi] {len(ka_slim)} рынков → kalshi_sports.json, kalshi_titles.json")


if __name__ == "__main__":
    main()
