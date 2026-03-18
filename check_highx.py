"""
Скрипт проверки: есть ли на Polymarket ставки с мультипликатором ≥ 100x
(цена ≤ 0.01) и экспирацией ≤ 14 дней.
"""

import httpx
import json
import time
from datetime import datetime, timezone, timedelta

GAMMA_URL = "https://gamma-api.polymarket.com"
MAX_PRICE = 0.01      # мультипликатор 100x = цена ≤ 1 цент
MAX_DAYS = 14          # экспирация ≤ 2 недель
PAGE_SIZE = 100

now = datetime.now(timezone.utc)
deadline = now + timedelta(days=MAX_DAYS)


def parse_date(s: str) -> datetime | None:
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_json_field(raw) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return []
    return []


def fetch_all_active():
    """Получаем все активные рынки с пагинацией."""
    markets = []
    offset = 0
    while True:
        resp = httpx.get(
            f"{GAMMA_URL}/markets",
            params={"active": "true", "closed": "false",
                    "limit": PAGE_SIZE, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        markets.extend(batch)
        offset += PAGE_SIZE
        print(f"  загружено {len(markets)} рынков...", end="\r")
        time.sleep(0.3)
    print(f"  загружено {len(markets)} рынков всего")
    return markets


def main():
    print(f"Ищем ставки с ценой ≤ {MAX_PRICE} (множитель ≥ {1/MAX_PRICE:.0f}x)")
    print(f"Экспирация: до {deadline.strftime('%Y-%m-%d %H:%M UTC')} ({MAX_DAYS} дней)")
    print()

    print("Загрузка рынков...")
    raw_markets = fetch_all_active()

    # Также поищем с более мягким порогом для контекста
    candidates = []       # цена ≤ 0.01
    soft_candidates = []  # цена ≤ 0.05 (для контекста)

    for m in raw_markets:
        end_str = m.get("endDate", "")
        if not end_str:
            continue
        end_date = parse_date(end_str)
        if not end_date or end_date > deadline or end_date < now:
            continue

        outcomes = parse_json_field(m.get("outcomes", []))
        prices_raw = parse_json_field(m.get("outcomePrices", []))
        prices = []
        for p in prices_raw:
            try:
                prices.append(float(p))
            except (ValueError, TypeError):
                prices.append(0.0)

        volume = float(m.get("volumeNum", 0) or 0)
        liquidity = float(m.get("liquidityNum", 0) or 0)
        question = m.get("question", "???")
        market_id = m.get("id", "")

        for i, (outcome, price) in enumerate(zip(outcomes, prices)):
            if price <= 0:
                continue

            days_left = (end_date - now).total_seconds() / 86400
            multiplier = 1.0 / price

            entry = {
                "question": question,
                "outcome": outcome,
                "price": price,
                "multiplier": multiplier,
                "days_left": round(days_left, 1),
                "volume": volume,
                "liquidity": liquidity,
                "end_date": end_date.strftime("%Y-%m-%d %H:%M"),
                "market_id": market_id,
            }

            if price <= MAX_PRICE:
                candidates.append(entry)
            if price <= 0.05:
                soft_candidates.append(entry)

    # Сортируем по цене (самые дешёвые = самый высокий множитель)
    candidates.sort(key=lambda x: x["price"])
    soft_candidates.sort(key=lambda x: x["price"])

    print(f"\n{'='*80}")
    print(f"РЕЗУЛЬТАТЫ: цена ≤ {MAX_PRICE} (множитель ≥ {1/MAX_PRICE:.0f}x), экспирация ≤ {MAX_DAYS}д")
    print(f"{'='*80}")

    if candidates:
        print(f"\nНайдено {len(candidates)} ставок:\n")
        for c in candidates:
            print(f"  💰 {c['multiplier']:.0f}x | ${c['price']:.4f} | "
                  f"{c['days_left']}д | vol=${c['volume']:.0f} | liq=${c['liquidity']:.0f}")
            print(f"     {c['question']}")
            print(f"     Исход: {c['outcome']} | Экспирация: {c['end_date']}")
            print()
    else:
        print("\n  ❌ Ничего не найдено с ценой ≤ 0.01\n")

    # Контекст: что есть до 0.05?
    print(f"\n{'='*80}")
    print(f"КОНТЕКСТ: цена ≤ 0.05 (множитель ≥ 20x), экспирация ≤ {MAX_DAYS}д")
    print(f"{'='*80}")

    if soft_candidates:
        print(f"\nНайдено {len(soft_candidates)} ставок (топ-20):\n")
        for c in soft_candidates[:20]:
            print(f"  {c['multiplier']:.0f}x | ${c['price']:.4f} | "
                  f"{c['days_left']}д | vol=${c['volume']:.0f} | liq=${c['liquidity']:.0f}")
            print(f"     {c['question']}")
            print(f"     Исход: {c['outcome']} | Экспирация: {c['end_date']}")
            print()
    else:
        print("\n  ❌ Ничего не найдено даже с ценой ≤ 0.05\n")

    # Статистика по распределению цен
    print(f"\n{'='*80}")
    print("РАСПРЕДЕЛЕНИЕ ЦЕН (все рынки, экспирация ≤ 14д)")
    print(f"{'='*80}\n")

    all_prices = []
    for m in raw_markets:
        end_str = m.get("endDate", "")
        if not end_str:
            continue
        end_date = parse_date(end_str)
        if not end_date or end_date > deadline or end_date < now:
            continue
        prices_raw = parse_json_field(m.get("outcomePrices", []))
        for p in prices_raw:
            try:
                pf = float(p)
                if 0 < pf < 1:
                    all_prices.append(pf)
            except (ValueError, TypeError):
                pass

    if all_prices:
        buckets = [
            ("≤ 0.01 (100x+)", 0, 0.01),
            ("0.01-0.02 (50-100x)", 0.01, 0.02),
            ("0.02-0.05 (20-50x)", 0.02, 0.05),
            ("0.05-0.10 (10-20x)", 0.05, 0.10),
            ("0.10-0.20 (5-10x)", 0.10, 0.20),
            ("0.20-0.50 (2-5x)", 0.20, 0.50),
            ("0.50-1.00 (1-2x)", 0.50, 1.00),
        ]
        for label, lo, hi in buckets:
            count = sum(1 for p in all_prices if lo < p <= hi or (lo == 0 and p <= hi))
            print(f"  {label:25s}: {count:5d} исходов")
        print(f"  {'ВСЕГО':25s}: {len(all_prices):5d} исходов")


if __name__ == "__main__":
    main()
