"""
research_bot/fetch_markets.py

Скачивает историю закрытых 5m/15m BTC/ETH/SOL/XRP рынков с Polymarket
и сохраняет в backtest.db (таблица markets).

Запуск:
  python3 -m research_bot.fetch_markets            # 5000 рынков
  python3 -m research_bot.fetch_markets --limit 1000
"""
from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timedelta
from typing import Generator, Optional

import httpx

from src.api.gamma import _parse_end_date

GAMMA_URL = "https://gamma-api.polymarket.com"
PAGE_SIZE = 500
REQUEST_DELAY = 0.25  # seconds between pages

ALLOWED_SYMBOLS = {"BTC", "ETH", "SOL", "XRP"}
ALLOWED_INTERVALS = {5, 15, 60}

SYMBOL_MAP = {
    "BITCOIN": "BTC",
    "ETHEREUM": "ETH",
    "SOLANA": "SOL",
    "DOGECOIN": "DOGE",
    "HYPERLIQUID": "HYPE",
}
UPDOWN_RE = re.compile(r"^(?P<symbol>[A-Za-z]+)\s+Up or Down\s+-", re.IGNORECASE)
MINUTE_WINDOW_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}(?:AM|PM))-(?P<end>\d{1,2}:\d{2}(?:AM|PM))\s+ET",
    re.IGNORECASE,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _extract_interval(question: str) -> Optional[int]:
    m = MINUTE_WINDOW_RE.search(question)
    if m:
        start = datetime.strptime(m.group("start").upper(), "%I:%M%p")
        end = datetime.strptime(m.group("end").upper(), "%I:%M%p")
        delta = int((end - start).total_seconds() // 60)
        if delta <= 0:
            delta += 24 * 60
        return delta
    if re.search(r"\b15\s*Minutes\b", question, re.IGNORECASE):
        return 15
    if re.search(r"\b5\s*Minutes\b", question, re.IGNORECASE):
        return 5
    if re.search(r"\b1\s*Hour\b|\b60\s*Minutes\b", question, re.IGNORECASE):
        return 60
    return None


def _winning_side(outcomes: list, prices: list) -> Optional[str]:
    for name, price in zip(outcomes, prices):
        try:
            p = float(price)
        except (ValueError, TypeError):
            continue
        if name.lower() == "up" and p >= 0.95:
            return "yes"
        if name.lower() == "down" and p >= 0.95:
            return "no"
    return None


def _parse_outcomes(raw: dict) -> tuple[list, list]:
    import json
    def parse_field(v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        try:
            return json.loads(v)
        except Exception:
            return []
    return parse_field(raw.get("outcomes")), parse_field(raw.get("outcomePrices"))


def _iter_closed_pages(
    http: httpx.Client,
    end_date_min: str | None = None,
    end_date_max: str | None = None,
    ascending: bool = False,
) -> Generator[dict, None, None]:
    """Yields raw market dicts from Gamma API.

    end_date_min/end_date_max — server-side filter (ISO date 'YYYY-MM-DD').
    ascending=True → walk oldest→newest, полезно когда общий каталог
    превышает 250k offset-лимит Gamma.
    """
    offset = 0
    while True:
        params: dict = {
            "closed": "true",
            "resolved": "true",
            "order": "endDate",
            "ascending": "true" if ascending else "false",
            "limit": PAGE_SIZE,
            "offset": offset,
            "feeType": "crypto_fees",
        }
        if end_date_min:
            params["end_date_min"] = end_date_min
        if end_date_max:
            params["end_date_max"] = end_date_max
        try:
            resp = http.get(f"{GAMMA_URL}/markets", params=params, timeout=30.0)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            print(f"[fetch] API error at offset={offset}: {exc}")
            break

        if not batch:
            break

        yield from batch

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)



# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch resolved Polymarket markets to DB")
    parser.add_argument("--limit", type=int, default=5000, help="max markets to fetch (default 5000)")
    args = parser.parse_args()

    from research_bot.backtest_db import get_connection

    conn = get_connection()
    existing = {r[0] for r in conn.execute("SELECT market_id FROM markets").fetchall()}
    print(f"Уже в DB: {len(existing)} рынков")
    print(f"Цель: ещё {args.limit} рынков (type=5m/15m, BTC/ETH/SOL/XRP)")
    print()

    http = httpx.Client(timeout=30.0)

    saved = 0
    skipped_dupe = 0
    skipped_filter = 0
    raw_processed = 0

    try:
        for raw in _iter_closed_pages(http):
            raw_processed += 1

            mid = str(raw.get("id", ""))
            if not mid:
                continue

            if mid in existing:
                skipped_dupe += 1
                continue

            outcomes, prices = _parse_outcomes(raw)
            if not outcomes or not prices:
                skipped_filter += 1
                continue

            question = raw.get("question", "")
            match = UPDOWN_RE.match(question)
            if not match:
                skipped_filter += 1
                continue

            symbol = SYMBOL_MAP.get(match.group("symbol").upper(), match.group("symbol").upper())
            if symbol not in ALLOWED_SYMBOLS:
                skipped_filter += 1
                continue

            interval = _extract_interval(question)
            if interval not in ALLOWED_INTERVALS:
                skipped_filter += 1
                continue

            winning = _winning_side(outcomes, prices)
            if winning is None:
                skipped_filter += 1
                continue

            end_date = _parse_end_date(raw.get("endDate"))
            if end_date is None:
                skipped_filter += 1
                continue

            market_start = end_date - timedelta(minutes=interval)

            conn.execute(
                """INSERT OR IGNORE INTO markets
                   (market_id, condition_id, symbol, interval_minutes,
                    market_start, market_end, winning_side)
                   VALUES (?,?,?,?,?,?,?)""",
                (mid, raw.get("conditionId", ""), symbol, interval,
                 market_start.strftime("%Y-%m-%d %H:%M:%S"),
                 end_date.strftime("%Y-%m-%d %H:%M:%S"),
                 winning),
            )
            conn.commit()
            saved += 1
            existing.add(mid)

            if saved % 100 == 0:
                print(f"  [{saved}/{args.limit}] saved | raw_processed={raw_processed} "
                      f"| dupes={skipped_dupe} | filtered={skipped_filter}")

            if saved >= args.limit:
                print(f"  Достигнут лимит {args.limit}, остановка.")
                break

    finally:
        http.close()
        conn.close()

    print()
    print(f"Готово!")
    print(f"  Новых:          {saved}")
    print(f"  Уже было:       {skipped_dupe}")
    print(f"  Нерелевантных:  {skipped_filter}")
    print(f"  Всего raw API:  {raw_processed}")


if __name__ == "__main__":
    main()
