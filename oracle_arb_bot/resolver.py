from __future__ import annotations

import json
import re
from typing import Optional

import httpx


_HEADERS = {"User-Agent": "Mozilla/5.0"}
_TIMEOUT = 8.0


def check_polymarket_result(market_id: str, retries: int = 5) -> Optional[str]:
    """
    Проверяет закрыт ли рынок и вернул ли "up" (→ "yes") или "down" (→ "no").
    Возвращает None если рынок ещё не закрыт или результат неоднозначен.
    """
    import time as _time
    for attempt in range(retries):
        try:
            resp = httpx.get(
                f"https://gamma-api.polymarket.com/markets/{market_id}",
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("closed"):
                return None

            outcomes = data.get("outcomes") or []
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            prices = data.get("outcomePrices") or []
            if isinstance(prices, str):
                prices = json.loads(prices)

            outcome_map: dict[str, float] = {}
            for name, price in zip(outcomes, prices):
                try:
                    outcome_map[name.lower()] = float(price)
                except (TypeError, ValueError):
                    pass

            up_p = outcome_map.get("up", 0.0)
            down_p = outcome_map.get("down", 0.0)

            if up_p >= 0.95 and down_p <= 0.05:
                return "yes"
            if down_p >= 0.95 and up_p <= 0.05:
                return "no"
            return None
        except Exception as exc:
            if attempt < retries - 1:
                _time.sleep(5)
            else:
                print(f"[resolver] PM result check {market_id}: {exc}")
    return None


def fetch_pm_open_price(pm_event_slug: str) -> Optional[float]:
    """
    Парсит openPrice (Chainlink цена начала окна) с HTML Polymarket.
    Возвращает None если рынок ещё не начался или страница недоступна.
    Кэширование — на стороне вызывающего кода.
    """
    url = f"https://polymarket.com/event/{pm_event_slug}"
    try:
        resp = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True, headers=_HEADERS)
        m = re.search(
            r'"openPrice"\s*:\s*([0-9.]+)\s*,\s*"closePrice"\s*:\s*null',
            resp.text,
        )
        if m:
            return float(m.group(1))
    except Exception as exc:
        print(f"[resolver] openPrice fetch {pm_event_slug}: {exc}")
    return None


def check_kalshi_result(ticker: str) -> Optional[str]:
    """
    Проверяет settled ли Kalshi рынок и возвращает winning side.
    Возвращает "yes" | "no" | None.
    """
    try:
        resp = httpx.get(
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        market = resp.json().get("market", {})

        if market.get("status") not in ("settled", "finalized"):
            return None

        result = market.get("result", "")
        if result in ("yes", "no"):
            return result
        return None
    except Exception as exc:
        print(f"[resolver] Kalshi result check {ticker}: {exc}")
        return None


def fetch_pm_close_price(pm_event_slug: str, retries: int = 5) -> Optional[float]:
    """
    Парсит closePrice (финальная Chainlink цена) с HTML Polymarket после закрытия рынка.
    """
    import time as _time
    url = f"https://polymarket.com/event/{pm_event_slug}"
    for attempt in range(retries):
        try:
            resp = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True, headers=_HEADERS)
            closes = re.findall(r'"closePrice"\s*:\s*([\d.]+)', resp.text)
            if closes:
                return float(closes[-1])
            return None
        except Exception as exc:
            if attempt < retries - 1:
                _time.sleep(5)
            else:
                print(f"[resolver] closePrice fetch {pm_event_slug}: {exc}")
    return None
