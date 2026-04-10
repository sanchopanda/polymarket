#!/usr/bin/env python3
"""
Скачивает 1m-свечи Binance для всех resolved paper-позиций из БД
и сохраняет в data/binance_klines_cache.json.

Запуск: python3 scripts/fetch_binance_klines_cache.py
"""
import sqlite3, json, time, os, sys
import warnings
warnings.filterwarnings("ignore")

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

SESSION = requests.Session()
SESSION.verify = False

CACHE_FILE = "data/binance_klines_cache.json"
DB_PATH = "data/fast_arb_bot.db"

SYMBOL_MAP = {
    'BTC': 'BTCUSDT',
    'ETH': 'ETHUSDT',
    'SOL': 'SOLUSDT',
    'XRP': 'XRPUSDT',
    'DOGE': 'DOGEUSDT',
    'BNB': 'BNBUSDT',
}


def to_ms(ts: str) -> int:
    from datetime import datetime
    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    return int(dt.timestamp() * 1000)


def fetch_range(ticker: str, start_ms: int, end_ms: int) -> dict:
    result = {}
    cur = start_ms
    while cur < end_ms:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": ticker,
            "interval": "1m",
            "startTime": cur,
            "endTime": end_ms,
            "limit": 1000,
        }
        for attempt in range(5):
            try:
                resp = SESSION.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if not data:
                    return result
                for row in data:
                    result[str(row[0])] = float(row[4])  # close price
                cur = data[-1][0] + 60_000
                time.sleep(0.12)
                break
            except Exception as e:
                print(f"    retry {attempt + 1}/5: {e}", flush=True)
                time.sleep(3 * (attempt + 1))
        else:
            print(f"    FAILED at ts={cur}, частичный результат сохранён", flush=True)
            break
    return result


def main():
    if not os.path.exists(DB_PATH):
        print(f"БД не найдена: {DB_PATH}")
        sys.exit(1)

    db = sqlite3.connect(DB_PATH)
    rows = db.execute('''
        SELECT symbol, opened_at FROM positions
        WHERE status="resolved" AND is_paper=1
          AND kalshi_reference_price IS NOT NULL
          AND kalshi_reference_price > 0
    ''').fetchall()
    db.close()

    print(f"Позиций в БД: {len(rows)}")

    # Загружаем существующий кэш (частичный прогресс)
    cache: dict = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        for sym, candles in cache.items():
            print(f"  {sym}: уже в кэше ({len(candles)} свечей)")

    # Группируем по символу
    by_sym: dict = {}
    for sym, ts in rows:
        by_sym.setdefault(sym, []).append(to_ms(ts))

    for sym, times in by_sym.items():
        if sym in cache:
            continue
        ticker = SYMBOL_MAP.get(sym)
        if not ticker:
            print(f"  {sym}: нет маппинга, пропускаю")
            continue
        start_ms = min(times) - 120_000
        end_ms = max(times) + 120_000
        minutes = (end_ms - start_ms) // 60_000
        print(f"  {ticker}: {len(times)} позиций, ~{minutes} мин...", end=" ", flush=True)
        candles = fetch_range(ticker, start_ms, end_ms)
        cache[sym] = candles
        print(f"{len(candles)} свечей")
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f)
        print(f"    → сохранено в {CACHE_FILE}")

    print(f"\nГотово. Символы: {list(cache.keys())}")


if __name__ == "__main__":
    main()
