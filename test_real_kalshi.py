#!/usr/bin/env python3
"""
Тестовая реальная ставка на коротком крипто рынке Kalshi.

Аналог test_real_15m.py для Polymarket, но для Kalshi:
1. Сканирует 15-минутные крипто рынки (BTC/ETH/SOL)
2. Ищет рынок с вероятностью 60-80%
3. Проверяет orderbook ликвидность
4. Ставит и собирает полную аналитику:
   - цена решения vs fill price
   - комиссии, тайминги, slippage

Использование:
    python3 test_real_kalshi.py              # вотчер: ждёт и ставит
    python3 test_real_kalshi.py --dry        # только мониторинг
    python3 test_real_kalshi.py --amount 5   # другая сумма
    python3 test_real_kalshi.py --symbol BTC # конкретный символ

Нужно в .env:
    KALSHI_API_KEY_ID=...
    KALSHI_PRIVATE_KEY_PATH=data/kalshi.key
"""
from __future__ import annotations

import argparse
import base64
import datetime
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()

import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from cross_arb_bot.kalshi_feed import KalshiFeed

# ── Константы ──────────────────────────────────────────────────────────
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
BET_USD = 2.0
PROB_MIN = 0.60
PROB_MAX = 0.80
MIN_MINUTES_TO_EXPIRY = 3.0
SCAN_PAUSE = 20
RESULTS_FILE = Path("data/test_real_kalshi_results.json")

SERIES_TICKERS = ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXDOGE15M", "KXBNB15M"]


# ── Утилиты ────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def save_results(data: dict):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if RESULTS_FILE.exists():
        try:
            existing = json.loads(RESULTS_FILE.read_text())
        except (json.JSONDecodeError, ValueError):
            pass
    existing.append({"run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), **data})
    RESULTS_FILE.write_text(json.dumps(existing, indent=2, default=str))
    print(f"[save] Результаты → {RESULTS_FILE}")

def kalshi_fee(contracts: int, price_cents: int) -> float:
    """Комиссия Kalshi: ceil(0.07 * contracts * price * (1-price) * 100) / 100."""
    p = price_cents / 100.0
    raw = 0.07 * contracts * p * (1 - p)
    cents = int(raw * 100)
    if raw * 100 > cents:
        cents += 1
    return cents / 100.0


# ── Kalshi API клиент ──────────────────────────────────────────────────

class KalshiClient:
    """Авторизованный клиент для Kalshi Trading API v2."""

    def __init__(self):
        self.api_key_id = os.environ["KALSHI_API_KEY_ID"]
        key_path = os.environ["KALSHI_PRIVATE_KEY_PATH"]
        with open(key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        self.base_url = KALSHI_BASE_URL
        self.http = httpx.Client(timeout=15.0)
        print(f"[auth] Kalshi API key: {self.api_key_id[:12]}...")

    def _sign(self, timestamp: str, method: str, path: str) -> str:
        path_clean = path.split("?")[0]
        message = f"{timestamp}{method}{path_clean}".encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
        signature = self._sign(timestamp, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: dict | None = None) -> dict:
        url = self.base_url + path
        sign_path = urlparse(url).path
        headers = self._headers("GET", sign_path)
        resp = self.http.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, data: dict) -> httpx.Response:
        url = self.base_url + path
        sign_path = urlparse(url).path
        headers = self._headers("POST", sign_path)
        resp = self.http.post(url, headers=headers, json=data)
        return resp

    def get_balance(self) -> dict:
        t0 = time.time()
        data = self.get("/portfolio/balance")
        ms = (time.time() - t0) * 1000
        balance = data.get("balance_dollars", data.get("balance", 0))
        # Kalshi может вернуть баланс в центах
        if isinstance(balance, int) and balance > 1000:
            balance = balance / 100.0
        print(f"[wallet] Kalshi balance: ${balance:.2f} ({ms:.0f}ms)")
        return {"balance_usd": balance, "raw": data, "latency_ms": round(ms, 1)}

    def get_market(self, ticker: str) -> dict | None:
        try:
            data = self.get(f"/markets/{ticker}")
            return data.get("market")
        except Exception as e:
            print(f"[kalshi] Ошибка загрузки рынка {ticker}: {e}")
            return None

    def get_orderbook(self, ticker: str) -> dict:
        t0 = time.time()
        try:
            data = self.get(f"/markets/{ticker}/orderbook")
            ms = (time.time() - t0) * 1000
        except Exception as e:
            return {"error": str(e), "latency_ms": round((time.time() - t0) * 1000, 1)}

        orderbook = data.get("orderbook_fp") or data.get("orderbook") or {}
        yes_raw = orderbook.get("yes_dollars") or orderbook.get("yes") or []
        no_raw = orderbook.get("no_dollars") or orderbook.get("no") or []

        def parse_levels(raw):
            levels = []
            for item in raw:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    levels.append({"price": float(item[0]), "size": float(item[1])})
            return levels

        yes_levels = parse_levels(yes_raw)
        no_levels = parse_levels(no_raw)

        # YES asks = 1 - NO bids
        yes_asks = sorted(
            [{"price": round(1.0 - l["price"], 4), "size": l["size"]} for l in no_levels],
            key=lambda x: x["price"]
        )
        no_asks = sorted(
            [{"price": round(1.0 - l["price"], 4), "size": l["size"]} for l in yes_levels],
            key=lambda x: x["price"]
        )

        best_yes_ask = yes_asks[0]["price"] if yes_asks else None
        best_no_ask = no_asks[0]["price"] if no_asks else None
        yes_depth = sum(l["size"] for l in yes_asks)
        no_depth = sum(l["size"] for l in no_asks)

        print(f"[book] {ms:.0f}ms | yes_ask={best_yes_ask} no_ask={best_no_ask} | depth: yes={yes_depth:.0f} no={no_depth:.0f}")
        return {
            "latency_ms": round(ms, 1),
            "yes_asks": yes_asks[:10],
            "no_asks": no_asks[:10],
            "best_yes_ask": best_yes_ask,
            "best_no_ask": best_no_ask,
            "yes_depth": round(yes_depth, 2),
            "no_depth": round(no_depth, 2),
        }

    def place_order(self, ticker: str, side: str, count: int, price_cents: int) -> dict:
        """Размещает limit order на Kalshi.

        side: "yes" или "no"
        count: количество контрактов
        price_cents: цена в центах (1-99)
        """
        client_order_id = str(uuid.uuid4())
        order_data = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": price_cents if side == "yes" else (100 - price_cents),
            "client_order_id": client_order_id,
        }

        print(f"\n[order] {side.upper()} {count} contracts @ {price_cents}¢ on {ticker}")
        print(f"  order data: {json.dumps(order_data)}")

        t0 = time.time()
        resp = self.post("/portfolio/orders", order_data)
        ms = (time.time() - t0) * 1000

        result = {
            "ticker": ticker,
            "side": side,
            "count": count,
            "price_cents": price_cents,
            "client_order_id": client_order_id,
            "order_latency_ms": round(ms, 1),
            "status_code": resp.status_code,
            "placed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        try:
            body = resp.json()
            result["raw_response"] = body
            if resp.status_code == 201:
                order = body.get("order", {})
                result["order_id"] = order.get("order_id", "")
                result["status"] = order.get("status", "")
                result["fill_count"] = order.get("count_fp", order.get("size_matched", 0))
                print(f"  OK | {ms:.0f}ms | id={result['order_id'][:16]}... | status={result['status']}")
            else:
                result["error"] = body
                print(f"  ERROR {resp.status_code} | {ms:.0f}ms | {body}")
        except Exception as e:
            result["error"] = str(e)
            print(f"  ERROR | {ms:.0f}ms | {e}")

        return result


# ── Скан рынков ────────────────────────────────────────────────────────

def scan_kalshi_markets(symbol_filter: str | None = None, prob_min: float = PROB_MIN, prob_max: float = PROB_MAX) -> list[dict]:
    """Ищет 15-минутные крипто рынки на Kalshi с подходящей вероятностью."""
    feed = KalshiFeed(
        base_url=KALSHI_BASE_URL,
        page_size=200,
        max_pages=5,
        request_timeout_seconds=20,
        market_filter={"symbol": symbol_filter or ""},
        series_tickers=SERIES_TICKERS,
    )

    t0 = time.time()
    markets, error = feed.fetch_markets()
    fetch_ms = (time.time() - t0) * 1000

    if error:
        print(f"[scan] Ошибка: {error}")
        return []

    now = now_utc()
    results = []
    for m in markets:
        minutes_left = (m.expiry - now).total_seconds() / 60
        if minutes_left < MIN_MINUTES_TO_EXPIRY or minutes_left > 120:
            continue

        # Ищем сторону в окне вероятности
        best_side = None
        best_prob = 0.0
        for side, ask in [("yes", m.yes_ask), ("no", m.no_ask)]:
            if prob_min <= ask <= prob_max and ask > best_prob:
                best_side = side
                best_prob = ask

        if best_side is None:
            continue

        results.append({
            "ticker": m.market_id,
            "title": m.title,
            "symbol": m.symbol,
            "expiry": m.expiry.isoformat(),
            "expiry_dt": m.expiry,
            "interval_minutes": m.interval_minutes,
            "minutes_to_expiry": round(minutes_left, 1),
            "side": best_side,
            "probability": best_prob,
            "yes_ask": m.yes_ask,
            "no_ask": m.no_ask,
            "volume": m.volume,
            "reference_price": m.reference_price,
        })

    results.sort(key=lambda r: r["expiry_dt"])
    print(f"[scan] {len(markets)} рынков за {fetch_ms:.0f}ms | подходящих: {len(results)}")
    return results


# ── Резолюция ──────────────────────────────────────────────────────────

def wait_for_resolution(client: KalshiClient, ticker: str, expiry_iso: str) -> dict:
    expiry = datetime.datetime.fromisoformat(expiry_iso)
    t_start = time.time()
    t_expiry_passed = None

    print(f"\n[watch] Ожидаем резолюцию {ticker}")
    while True:
        now = now_utc()
        minutes_left = (expiry - now).total_seconds() / 60
        if minutes_left <= 0 and t_expiry_passed is None:
            t_expiry_passed = time.time()

        t0 = time.time()
        market = client.get_market(ticker)
        api_ms = (time.time() - t0) * 1000

        if market is None:
            time.sleep(10)
            continue

        status = market.get("status", "")
        result = market.get("result", "")
        yes_ask = market.get("yes_ask_dollars", "?")
        no_ask = market.get("no_ask_dollars", "?")

        print(f"  [{ts()}] {status} | result={result or '-'} | yes={yes_ask} no={no_ask} | {minutes_left:+.1f}мин | {api_ms:.0f}ms")

        # Kalshi statuses: active → closed → determined → finalized
        if status in ("determined", "finalized") and result:
            elapsed_total = time.time() - t_start
            elapsed_after_expiry = (time.time() - t_expiry_passed) if t_expiry_passed else 0

            settlement = market.get("settlement_value_dollars")
            res = {
                "ticker": ticker,
                "status": status,
                "result": result,  # "yes" or "no"
                "settlement_value": settlement,
                "resolved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "wait_total_seconds": round(elapsed_total, 1),
                "wait_after_expiry_seconds": round(elapsed_after_expiry, 1),
            }
            print(f"\n[resolved] {result.upper()} | settlement=${settlement} | после экспирации: {elapsed_after_expiry:.0f}с")
            return res

        if minutes_left > 5:
            time.sleep(15)
        elif minutes_left > 0:
            time.sleep(10)
        else:
            time.sleep(5)


# ── Полный цикл ────────────────────────────────────────────────────────

def run_full_cycle(client: KalshiClient, candidate: dict, amount_usd: float, dry: bool = False) -> dict:
    analytics: dict = {
        "market": {k: v for k, v in candidate.items() if k != "expiry_dt"},
        "amount_usd": amount_usd,
    }

    print(f"\n{'─'*60}")
    print(f"  {candidate['title']}")
    print(f"  {candidate['symbol']} | {candidate['interval_minutes']}мин | через {candidate['minutes_to_expiry']}мин")
    print(f"  Ставка: {candidate['side'].upper()} @ {candidate['probability']:.2f} ({candidate['probability']*100:.0f}%)")
    print(f"  yes={candidate['yes_ask']:.2f} no={candidate['no_ask']:.2f}")
    print(f"{'─'*60}")

    if dry:
        print("\n[dry] Режим просмотра — ордер не размещаем")
        save_results(analytics)
        return analytics

    # 1. Баланс до
    print("\n[1/5] Баланс до...")
    balance_before = client.get_balance()
    analytics["balance_before"] = balance_before

    # 2. Orderbook
    print("\n[2/5] Orderbook...")
    book = client.get_orderbook(candidate["ticker"])
    analytics["orderbook"] = book

    # Определяем цену для ордера
    side = candidate["side"]
    if side == "yes":
        book_best = book.get("best_yes_ask")
        book_depth = book.get("yes_depth", 0)
    else:
        book_best = book.get("best_no_ask")
        book_depth = book.get("no_depth", 0)

    if book_best is None or book_best <= 0:
        print(f"  Нет ликвидности в стакане — пропускаем")
        save_results(analytics)
        return analytics

    # Количество контрактов
    price_cents = int(round(book_best * 100))
    contracts = max(1, int(amount_usd / book_best))
    actual_cost = contracts * book_best
    fee = kalshi_fee(contracts, price_cents)

    analytics["decision"] = {
        "side": side,
        "scan_price": candidate["probability"],
        "book_best_ask": book_best,
        "price_cents": price_cents,
        "contracts": contracts,
        "estimated_cost": round(actual_cost, 4),
        "estimated_fee": round(fee, 4),
        "book_depth": book_depth,
        "signal_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    print(f"\n  Решение: {side.upper()} {contracts} контрактов @ {price_cents}¢")
    print(f"  scan_price={candidate['probability']:.2f} → book_best={book_best:.2f}")
    print(f"  cost=${actual_cost:.2f} | fee=${fee:.4f} | depth={book_depth:.0f}")

    # 3. Ордер
    print(f"\n[3/5] Ставка...")
    order = client.place_order(candidate["ticker"], side, contracts, price_cents)
    analytics["order"] = order

    if order.get("status_code") != 201:
        print(f"  Ордер не размещён")
        save_results(analytics)
        return analytics

    # Аналитика цен
    fill_status = order.get("status", "")
    analytics["price_comparison"] = {
        "scan_price": candidate["probability"],
        "book_best_ask": book_best,
        "order_price_cents": price_cents,
        "order_status": fill_status,
    }

    print(f"\n  ЦЕНА РЕШЕНИЯ vs FILL:")
    print(f"    Scan price (API):    {candidate['probability']:.4f}")
    print(f"    Book best ask:       {book_best:.4f}")
    print(f"    Order price:         {price_cents}¢")
    print(f"    Order status:        {fill_status}")

    # 4. Ожидание резолюции
    print(f"\n[4/5] Ожидаем резолюцию...")
    resolution = wait_for_resolution(client, candidate["ticker"], candidate["expiry"])
    analytics["resolution"] = resolution

    # P&L
    winner = resolution.get("result", "")  # "yes" or "no"
    bet_won = (winner == side)
    payout = contracts * 1.0 if bet_won else 0.0
    cost = actual_cost + fee
    pnl = payout - cost

    analytics["result"] = {
        "won": bet_won,
        "winner": winner,
        "bet_side": side,
        "contracts": contracts,
        "price_cents": price_cents,
        "fee": round(fee, 4),
        "cost": round(cost, 4),
        "payout": round(payout, 4),
        "pnl": round(pnl, 4),
        "roi_percent": round(pnl / cost * 100, 2) if cost > 0 else 0,
    }

    print(f"\n[result] {'ВЫИГРЫШ' if bet_won else 'ПРОИГРЫШ'} | P&L: ${pnl:+.4f}")

    # 5. Баланс после (на Kalshi settlement автоматический)
    print(f"\n[5/5] Баланс после...")
    time.sleep(5)
    balance_after = client.get_balance()
    analytics["balance_after"] = balance_after
    analytics["balance_diff_usd"] = round(balance_after["balance_usd"] - balance_before["balance_usd"], 4)

    # Итоговая сводка
    print(f"\n{'='*60}")
    print(f"  ИТОГОВАЯ АНАЛИТИКА (Kalshi)")
    print(f"{'='*60}")
    print(f"  Рынок:       {candidate['title'][:55]}")
    print(f"  Ставка:      {side.upper()} {contracts} контрактов @ {price_cents}¢ | ${actual_cost:.2f}")
    print(f"  Результат:   {'ВЫИГРЫШ' if bet_won else 'ПРОИГРЫШ'} | P&L: ${pnl:+.4f}")
    print(f"  ")
    print(f"  ЦЕНЫ:")
    print(f"    Scan (API):        {candidate['probability']:.4f}")
    print(f"    Book best ask:     {book_best:.4f}")
    print(f"    Order price:       {price_cents}¢")
    print(f"  ")
    print(f"  ТАЙМИНГИ:")
    print(f"    Ордер:             {order.get('order_latency_ms', '?')}ms")
    print(f"    После экспирации:  {resolution.get('wait_after_expiry_seconds', '?')}с")
    print(f"  ")
    print(f"  КОМИССИИ:")
    print(f"    Trading fee:       ${fee:.4f}")
    print(f"  ")
    print(f"  ЛИКВИДНОСТЬ:")
    print(f"    Depth:             {book_depth:.0f} contracts")
    print(f"    Book best ask:     {book_best}")
    print(f"  ")
    print(f"  БАЛАНС:")
    print(f"    До:     ${balance_before['balance_usd']:.2f}")
    print(f"    После:  ${balance_after['balance_usd']:.2f}")
    print(f"    Разница: ${analytics['balance_diff_usd']:+.4f}")
    print(f"{'='*60}")

    save_results(analytics)
    return analytics


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Тестовая ставка на Kalshi 15-минутном крипто рынке")
    parser.add_argument("--dry", action="store_true", help="Только мониторинг без ставки")
    parser.add_argument("--amount", type=float, default=BET_USD, help=f"Размер ставки (default: ${BET_USD})")
    parser.add_argument("--symbol", default=None, help="Фильтр: BTC, ETH, SOL")
    parser.add_argument("--prob-min", type=float, default=PROB_MIN)
    parser.add_argument("--prob-max", type=float, default=PROB_MAX)
    args = parser.parse_args()

    print(f"[start] Kalshi тест")
    print(f"  ставка: ${args.amount} | вероятность: {args.prob_min*100:.0f}–{args.prob_max*100:.0f}%")
    print(f"  символ: {args.symbol or 'любой'}")
    print(f"  режим: {'мониторинг (dry)' if args.dry else 'реальная ставка'}")
    print()

    client = KalshiClient()

    # Проверяем баланс
    client.get_balance()
    print()

    iteration = 0
    while True:
        iteration += 1
        print(f"\n[scan #{iteration}] {ts()}")

        candidates = scan_kalshi_markets(
            symbol_filter=args.symbol,
            prob_min=args.prob_min,
            prob_max=args.prob_max,
        )

        if not candidates:
            print(f"  ждём {SCAN_PAUSE}с...")
            time.sleep(SCAN_PAUSE)
            continue

        # Показываем кандидатов
        for i, c in enumerate(candidates[:5]):
            marker = " <<<" if i == 0 else ""
            print(f"  #{i+1} {c['symbol']} {c['interval_minutes']}мин {c['side']}@{c['probability']:.2f} | {c['minutes_to_expiry']}мин{marker}")

        best = candidates[0]
        result = run_full_cycle(client, best, args.amount, dry=args.dry)

        if args.dry:
            time.sleep(SCAN_PAUSE)
            continue

        # Останавливаемся после выигрыша
        won = result.get("result", {}).get("won", False)
        if won:
            print(f"\n[done] Выигрыш зафиксирован, аналитика собрана.")
            return

        print(f"\n[retry] Проигрыш — ставим снова через {SCAN_PAUSE}с...")
        time.sleep(SCAN_PAUSE)


if __name__ == "__main__":
    main()
