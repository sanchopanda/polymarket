#!/usr/bin/env python3
"""
Анализ совпадения резолюций: Kalshi KXBTCD (BTC above $X) vs Polymarket hourly BTC Up/Down.

Polymarket: "Up or Down" = BTC/USDT 1h candle close >= open (Binance).
            market_end = candle close time = Kalshi strike_date.
Kalshi:     floor_strike nearest to candle open = эффективно тот же вопрос.

Логика:
  1. Kalshi: /events?series_ticker=KXBTCD&status=settled  →  strike_date per event
  2. Binance: 1h OHLCV → candle open for [strike_date - 1h, strike_date]
  3. Kalshi sub-market с floor_strike ближайшим к candle open
  4. Polymarket: /events?slug=bitcoin-up-or-down-... → winning_side (up/down)
  5. Сравнить: Kalshi YES ↔ Polymarket UP, Kalshi NO ↔ Polymarket DOWN

Запуск: python3 data/kalshi_pm_resolution_match.py [--days 30]
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import httpx

KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA_BASE   = "https://gamma-api.polymarket.com"
BINANCE_BASE = "https://api.binance.com/api/v3"

ET_OFFSET    = timedelta(hours=-4)   # EDT (апрель = UTC-4)


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_list(v):
    if isinstance(v, list):
        return v
    try:
        return json.loads(v)
    except Exception:
        return []


# ── Kalshi ────────────────────────────────────────────────────────────────────

def fetch_kalshi_events(http: httpx.Client, days_back: int) -> list[dict]:
    """Fetch settled KXBTCD events, newest first, within days_back days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    events = []
    cursor = None

    while True:
        params: dict = {"series_ticker": "KXBTCD", "status": "settled", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = http.get(f"{KALSHI_BASE}/events", params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("events", [])
        if not batch:
            break

        stop = False
        for ev in batch:
            strike_dt = _parse_dt(ev.get("strike_date"))
            if strike_dt and strike_dt < cutoff:
                stop = True
                break
            events.append(ev)

        cursor = data.get("cursor")
        if stop or not cursor:
            break

    return events


def fetch_kalshi_sub_market_nearest(http: httpx.Client, event_ticker: str, candle_open: float) -> dict | None:
    """
    Fetch the single Kalshi sub-market nearest to candle_open.
    BTC sub-markets use $100 increments; floor_strike = (rounded_price - 0.01).
    Tries the nearest two candidates ($100 below and $100 above candle_open).
    """
    # Round to nearest $100
    lower = (int(candle_open) // 100) * 100          # e.g. 69300
    upper = lower + 100                               # e.g. 69400
    candidates = sorted([lower, upper], key=lambda x: abs(x - candle_open))

    for rounded in candidates:
        if rounded <= 0:
            continue
        floor_strike_str = f"{rounded - 0.01:.2f}"   # "69399.99"
        ticker = f"{event_ticker}-T{floor_strike_str}"
        try:
            resp = http.get(f"{KALSHI_BASE}/markets/{ticker}")
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            m = resp.json().get("market") or resp.json()
            if m.get("result") in ("yes", "no"):
                return m
        except Exception:
            continue
    return None


# ── Binance ───────────────────────────────────────────────────────────────────

def fetch_binance_1h(http: httpx.Client, days_back: int) -> dict[int, float]:
    """
    Fetch BTC/USDT 1h candles.
    Returns {open_time_ms: open_price} for easy lookup by candle-open timestamp.
    """
    now_ms  = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - days_back * 24 * 3600 * 1000

    candles: dict[int, float] = {}
    limit = 1000
    current_start = start_ms

    while current_start < now_ms:
        resp = http.get(
            f"{BINANCE_BASE}/klines",
            params={"symbol": "BTCUSDT", "interval": "1h",
                    "startTime": current_start, "limit": limit},
        )
        resp.raise_for_status()
        rows = resp.json()   # [[open_time, open, high, low, close, ...], ...]
        if not rows:
            break
        for row in rows:
            open_time_ms = int(row[0])
            open_price   = float(row[1])
            candles[open_time_ms] = open_price
        # Last candle's open_time + 1h
        current_start = int(rows[-1][0]) + 3_600_000
        if len(rows) < limit:
            break
        time.sleep(0.05)

    return candles


# ── Polymarket ────────────────────────────────────────────────────────────────

def _slug_for_strike(strike_utc: datetime) -> str:
    """
    Build Polymarket event slug from Kalshi strike_date (UTC).
    Kalshi strike_date = candle close time = market_end in Polymarket.
    Polymarket slug describes the candle START hour (= strike - 1h) in ET.
    """
    candle_start_utc = strike_utc - timedelta(hours=1)
    et_time = candle_start_utc + ET_OFFSET   # UTC → EDT

    month    = et_time.strftime("%B").lower()
    day      = et_time.day
    year     = et_time.year
    hour     = et_time.hour

    if hour == 0:
        time_str = "12am"
    elif hour < 12:
        time_str = f"{hour}am"
    elif hour == 12:
        time_str = "12pm"
    else:
        time_str = f"{hour - 12}pm"

    return f"bitcoin-up-or-down-{month}-{day}-{year}-{time_str}-et"


def fetch_pm_market(http: httpx.Client, slug: str) -> dict | None:
    """Fetch resolved Polymarket BTC hourly market by event slug."""
    try:
        resp = http.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=20)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"  [PM] ошибка slug {slug}: {e}")
        return None

    if not events:
        return None

    for ev in events:
        for m in ev.get("markets", []):
            outcomes = _parse_list(m.get("outcomes"))
            prices   = _parse_list(m.get("outcomePrices"))
            if not outcomes or not prices:
                continue
            winning_side = None
            for name, price in zip(outcomes, prices):
                try:
                    p = float(price)
                except (ValueError, TypeError):
                    continue
                if name.lower() == "up" and p >= 0.95:
                    winning_side = "yes"
                elif name.lower() == "down" and p >= 0.95:
                    winning_side = "no"
            if winning_side is not None:
                return {"question": m.get("question", ""), "winning_side": winning_side}

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14, help="Look back N days (default 14)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print each event")
    args = parser.parse_args()

    http = httpx.Client(timeout=30)

    print(f"=== Kalshi KXBTCD vs Polymarket BTC hourly (последние {args.days} дней) ===\n")

    # 1. Binance 1h candles
    print("Fetching Binance BTCUSDT 1h candles...")
    candles = fetch_binance_1h(http, args.days + 1)
    print(f"  → {len(candles)} candles\n")

    # 2. Kalshi settled events
    print("Fetching Kalshi KXBTCD settled events...")
    events = fetch_kalshi_events(http, args.days)
    print(f"  → {len(events)} событий\n")

    if not events:
        print("Нет settled событий Kalshi.")
        return

    # 3. Для каждого события
    rows = []
    pm_cache: dict[str, dict | None] = {}   # slug → pm market

    for ev in events:
        et        = ev["event_ticker"]
        strike_dt = _parse_dt(ev.get("strike_date"))
        if strike_dt is None:
            continue

        # Binance candle open: candle that CLOSES at strike_dt opened at strike_dt - 1h
        candle_open_ms = int((strike_dt - timedelta(hours=1)).timestamp() * 1000)
        candle_open = candles.get(candle_open_ms)
        if candle_open is None:
            if args.verbose:
                print(f"[{et}] нет Binance candle open, пропуск")
            continue

        # Polymarket
        slug = _slug_for_strike(strike_dt)
        if slug not in pm_cache:
            pm_cache[slug] = fetch_pm_market(http, slug)
            time.sleep(0.05)
        pm = pm_cache[slug]
        if pm is None:
            if args.verbose:
                print(f"[{et}] PM не найден / не зарезолвился: {slug}")
            continue

        # Kalshi — fetch single nearest sub-market
        best_ka = fetch_kalshi_sub_market_nearest(http, et, candle_open)
        if best_ka is None:
            if args.verbose:
                print(f"[{et}] нет Kalshi sub-market для floor_strike≈{candle_open:.0f}")
            continue

        fs      = float(best_ka["floor_strike"])
        ka_res  = best_ka["result"]   # "yes" / "no"
        pm_res  = pm["winning_side"]  # "yes" / "no"
        agree   = ka_res == pm_res
        diff    = abs(fs - candle_open)

        rows.append({
            "event_ticker":  et,
            "strike_utc":    strike_dt.strftime("%Y-%m-%d %H:%M"),
            "candle_open":   candle_open,
            "kalshi_floor":  fs,
            "price_diff":    diff,
            "pm_result":     pm_res.upper(),
            "kalshi_result": ka_res.upper(),
            "agree":         agree,
        })

        if args.verbose:
            ok = "✓" if agree else "✗"
            print(f"{ok} [{et}] BTC_open={candle_open:,.0f}  "
                  f"Kalshi_floor={fs:,.0f} (Δ{diff:.0f})  "
                  f"PM={pm_res.upper()}  Kalshi={ka_res.upper()}")

    # ── Summary ───────────────────────────────────────────────────────────────
    if not rows:
        print("Нет данных для сравнения.")
        return

    total      = len(rows)
    agreed     = sum(1 for r in rows if r["agree"])
    avg_diff   = sum(r["price_diff"] for r in rows) / total

    print(f"\n{'='*60}")
    print(f"Матчей:      {total}")
    print(f"Совпадений:  {agreed} ({100*agreed/total:.1f}%)")
    print(f"Расхождений: {total - agreed} ({100*(total-agreed)/total:.1f}%)")
    print(f"Средн. Δ floor_strike vs BTC candle open: ${avg_diff:.0f}")
    print(f"{'='*60}\n")

    if total - agreed > 0:
        print("Расхождения:")
        for r in rows:
            if not r["agree"]:
                print(f"  {r['strike_utc']} UTC | BTC_open={r['candle_open']:,.0f}"
                      f" | Kalshi_floor={r['kalshi_floor']:,.0f} | PM={r['pm_result']}"
                      f" | Kalshi={r['kalshi_result']} | Δ${r['price_diff']:.0f}")
        print()

    print("Таблица:")
    print(f"{'Время UTC':<18} {'BTC open':>10} {'Kalshi $':>10} {'Δ$':>6} {'PM':>4} {'Kalshi':>7} {'':>3}")
    print("-" * 60)
    for r in sorted(rows, key=lambda x: x["strike_utc"]):
        ok = "✓" if r["agree"] else "✗"
        print(f"{r['strike_utc']:<18} {r['candle_open']:>10,.0f} {r['kalshi_floor']:>10,.0f}"
              f" {r['price_diff']:>6.0f} {r['pm_result']:>4} {r['kalshi_result']:>7} {ok:>3}")

    # Bucket + threshold analysis
    print(f"\n{'='*60}")
    print("Дельта (|Kalshi floor_strike - BTC candle open|) vs мисматч:")
    print(f"{'='*60}")
    buckets = [0, 10, 20, 30, 50, 100, float("inf")]
    labels  = ["0-10", "10-20", "20-30", "30-50", "50-100", "100+"]
    print(f"{'Δ range ($)':<12} {'матчей':>8} {'мисматч':>9} {'rate':>7}")
    print("-" * 40)
    for i, label in enumerate(labels):
        lo, hi = buckets[i], buckets[i + 1]
        bucket = [r for r in rows if lo <= r["price_diff"] < hi]
        if not bucket:
            continue
        miss = sum(1 for r in bucket if not r["agree"])
        print(f"{label:<12} {len(bucket):>8} {miss:>9} {100*miss/len(bucket):>6.1f}%")

    print(f"\n{'='*60}")
    print("Фильтр: торгуем только рынки с Δ < $30")
    print(f"{'='*60}")
    below = [r for r in rows if r["price_diff"] < 30]
    above = [r for r in rows if r["price_diff"] >= 30]
    b_miss = sum(1 for r in below if not r["agree"])
    a_miss = sum(1 for r in above if not r["agree"])
    print(f"Δ < $30:  {len(below):>4} рынков, мисматчей: {b_miss} ({100*b_miss/len(below):.1f}%)")
    if above:
        a_rate = 100 * a_miss / len(above)
        print(f"Δ ≥ $30:  {len(above):>4} рынков, мисматчей: {a_miss} ({a_rate:.1f}%)")
        print(f"Отсеивается: {len(above)} рынков ({100*len(above)/total:.1f}% от всех), "
              f"из них мисматчей: {a_miss} ({a_rate:.1f}%)")

    http.close()


if __name__ == "__main__":
    main()
