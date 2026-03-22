#!/usr/bin/env python3
"""
Тестовая реальная ставка на коротком крипто рынке Polymarket.

Работает как арбитражный бот:
1. HTTP скан → находит 5/15 мин рынки с хорошим объёмом
2. Подписка на websocket → отслеживает live цены
3. При попадании цены в окно 60-80% → проверяет orderbook ликвидность
4. Ставка FOK → собирает аналитику: цена решения vs цена fill
5. Мониторинг → резолюция → redeem → итоговый отчёт

Использование:
    python3 test_real_15m.py                 # вотчер с WS
    python3 test_real_15m.py --dry           # только мониторинг без ставки
    python3 test_real_15m.py --amount 5      # другая сумма
    python3 test_real_15m.py --symbol BTC    # конкретный символ
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import httpx
from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from web3 import Web3
from eth_account import Account

from arb_bot.ws import MarketWebSocketClient, TopOfBook
from src.api.clob import ClobClient as BookClobClient
from src.api.gamma import GammaClient
from cross_arb_bot.polymarket_feed import PolymarketFeed

# ── Константы ──────────────────────────────────────────────────────────
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
CHAIN_ID = 137
BET_USD = 2.0
PROB_MIN = 0.60
PROB_MAX = 0.80
INTERVALS = (5, 15)
MIN_MINUTES_TO_EXPIRY = 3.0
MIN_VOLUME = 1000
MIN_LIQUIDITY = 500
UNIVERSE_REFRESH_SECONDS = 60
RESULTS_FILE = Path("data/test_real_15m_results.json")

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]
REDEEM_ABI = [{"inputs": [{"name": "collateralToken", "type": "address"}, {"name": "parentCollectionId", "type": "bytes32"}, {"name": "conditionId", "type": "bytes32"}, {"name": "indexSets", "type": "uint256[]"}], "name": "redeemPositions", "outputs": [], "stateMutability": "nonpayable", "type": "function"}]
ERC20_BALANCE_ABI = [{"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}]
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


# ── Утилиты ────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

def save_results(data: dict):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if RESULTS_FILE.exists():
        try:
            existing = json.loads(RESULTS_FILE.read_text())
        except (json.JSONDecodeError, ValueError):
            pass
    existing.append({"run_at": datetime.now(timezone.utc).isoformat(), **data})
    RESULTS_FILE.write_text(json.dumps(existing, indent=2, default=str))
    print(f"[save] Результаты → {RESULTS_FILE}")

def polymarket_fee(shares: float, price: float) -> float:
    return shares * price * 0.25 * ((price * (1 - price)) ** 2)


# ── Кошелёк ────────────────────────────────────────────────────────────

def get_wallet_address() -> str:
    return Account.from_key(os.environ["WALLET_PRIVATE_KEY"]).address

def check_usdc_balance() -> dict:
    address = get_wallet_address()
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            usdc_contract = w3.eth.contract(address=Web3.to_checksum_address(USDC_E), abi=ERC20_BALANCE_ABI)
            usdc = usdc_contract.functions.balanceOf(Web3.to_checksum_address(address)).call() / 1e6
            pol = float(w3.from_wei(w3.eth.get_balance(Web3.to_checksum_address(address)), "ether"))
            print(f"[wallet] {address} | USDC.e: ${usdc:.4f} | POL: {pol:.4f}")
            return {"address": address, "usdc": usdc, "pol": pol}
        except Exception as e:
            print(f"[wallet] RPC {rpc}: {e}")
    raise RuntimeError("Все RPC недоступны")

def get_order_client() -> PyClobClient:
    pk = os.environ["WALLET_PRIVATE_KEY"]
    funder = os.getenv("WALLET_PROXY", "")
    l1 = PyClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
    creds = l1.create_or_derive_api_creds()
    print(f"[auth] CLOB ключи: {creds.api_key[:8]}...")
    return PyClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk, creds=creds, funder=funder or None)


# ── Скан рынков (HTTP) ────────────────────────────────────────────────

def scan_universe(symbol_filter: str | None = None) -> list[dict]:
    """Быстрый HTTP скан: находит 5/15 мин крипто рынки с хорошим объёмом."""
    feed = PolymarketFeed(
        base_url=GAMMA_URL, page_size=500, request_delay_ms=100,
        market_filter={
            "min_days_to_expiry": 0,
            "max_days_to_expiry": 0.083,  # ~2 часа
            "min_volume": MIN_VOLUME,
            "min_liquidity": MIN_LIQUIDITY,
            "fee_type": "crypto_fees",
        },
    )
    t0 = time.time()
    markets = feed.fetch_markets()
    fetch_ms = (time.time() - t0) * 1000

    now = now_utc()
    results = []
    for m in markets:
        if m.interval_minutes not in INTERVALS:
            continue
        if symbol_filter and m.symbol != symbol_filter.upper():
            continue
        minutes_left = (m.expiry - now).total_seconds() / 60
        if minutes_left < MIN_MINUTES_TO_EXPIRY or minutes_left > 120:
            continue

        results.append({
            "market_id": m.market_id,
            "title": m.title,
            "symbol": m.symbol,
            "expiry": m.expiry.isoformat(),
            "expiry_dt": m.expiry,
            "interval_minutes": m.interval_minutes,
            "minutes_to_expiry": round(minutes_left, 1),
            "yes_ask_gamma": m.yes_ask,
            "no_ask_gamma": m.no_ask,
            "volume": m.volume,
            "liquidity": m.liquidity,
            "reference_price": m.reference_price,
            "rule_family": m.rule_family,
            "yes_token_id": m.yes_token_id,
            "no_token_id": m.no_token_id,
        })

    results.sort(key=lambda r: r["expiry_dt"])
    print(f"[scan] {len(markets)} рынков за {fetch_ms:.0f}ms | 5/15мин с объёмом: {len(results)}")
    return results


# ── Orderbook ──────────────────────────────────────────────────────────

def check_orderbook(token_id: str, amount_usd: float, label: str = "") -> dict:
    """Получает orderbook через HTTP и рассчитывает fill для заданной суммы."""
    book_client = BookClobClient(base_url=CLOB_HOST, delay_ms=100)
    t0 = time.time()
    book = book_client.get_orderbook(token_id)
    latency_ms = (time.time() - t0) * 1000

    if not book or not book.asks:
        return {"available": False, "latency_ms": round(latency_ms, 1)}

    asks = book.asks
    best_ask = asks[0].price
    total_depth = sum(a.size for a in asks)

    # Симулируем fill
    remaining_usd = amount_usd
    total_cost = 0.0
    total_shares = 0.0
    for level in asks:
        level_usd = level.price * level.size
        if level_usd <= remaining_usd:
            total_cost += level_usd
            total_shares += level.size
            remaining_usd -= level_usd
        else:
            shares = remaining_usd / level.price
            total_cost += remaining_usd
            total_shares += shares
            remaining_usd = 0.0
            break

    avg_price = total_cost / total_shares if total_shares > 0 else 0.0
    slippage = avg_price - best_ask

    tag = f"[book {label}]" if label else "[book]"
    print(f"{tag} {latency_ms:.0f}ms | best={best_ask:.4f} avg={avg_price:.4f} slip={slippage:+.4f} | depth={total_depth:.0f}sh")

    return {
        "label": label,
        "available": remaining_usd < 0.01,
        "latency_ms": round(latency_ms, 1),
        "best_ask": best_ask,
        "avg_fill_price": round(avg_price, 6),
        "slippage": round(slippage, 6),
        "total_depth_shares": round(total_depth, 2),
        "simulated_shares": round(total_shares, 6),
        "simulated_cost": round(total_cost, 6),
        "ask_levels": [{"price": a.price, "size": a.size} for a in asks[:10]],
    }


# ── Ордер ──────────────────────────────────────────────────────────────

def place_order(order_client: PyClobClient, token_id: str, side: str, amount_usd: float) -> dict:
    print(f"\n[order] {side} ${amount_usd} ...")
    args = MarketOrderArgs(token_id=token_id, amount=amount_usd, side="BUY")

    t0 = time.time()
    order = order_client.create_market_order(args)
    sign_ms = (time.time() - t0) * 1000

    t1 = time.time()
    resp = order_client.post_order(order, orderType=OrderType.FOK)
    post_ms = (time.time() - t1) * 1000

    status = resp.get("status", "") if isinstance(resp, dict) else str(resp)
    order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""

    print(f"  sign={sign_ms:.0f}ms post={post_ms:.0f}ms total={sign_ms+post_ms:.0f}ms | status={status}")

    result = {
        "side": side, "amount_usd": amount_usd, "token_id": token_id,
        "sign_ms": round(sign_ms, 1), "post_ms": round(post_ms, 1),
        "total_order_ms": round(sign_ms + post_ms, 1),
        "status": status, "order_id": order_id,
        "raw_response": resp,
        "placed_at": datetime.now(timezone.utc).isoformat(),
    }

    if order_id:
        time.sleep(1.5)
        try:
            info = order_client.get_order(order_id)
            result["order_info"] = info
            matched = float(info.get("size_matched", 0))
            original = float(info.get("original_size", 0))
            price = float(info.get("price", 0))
            result["fill_price"] = price
            result["shares_matched"] = matched
            result["shares_original"] = original
            result["fill_percent"] = round(matched / original * 100, 1) if original > 0 else 0
            fee = polymarket_fee(matched, price)
            result["estimated_fee"] = round(fee, 6)
            result["fee_percent"] = round(fee / amount_usd * 100, 2) if amount_usd > 0 else 0
            print(f"  fill: {matched}/{original} @ {price} ({result['fill_percent']}%) fee=${fee:.4f}")
        except Exception as e:
            print(f"  order info error: {e}")

    return result


# ── Резолюция ──────────────────────────────────────────────────────────

def wait_for_resolution(market_id: str, expiry_iso: str) -> dict:
    gamma = GammaClient(GAMMA_URL, page_size=100, delay_ms=300)
    expiry = datetime.fromisoformat(expiry_iso)
    t_start = time.time()
    t_expiry_passed = None

    print(f"\n[watch-resolve] Ожидаем резолюцию {market_id}")
    while True:
        now = now_utc()
        minutes_left = (expiry - now).total_seconds() / 60
        if minutes_left <= 0 and t_expiry_passed is None:
            t_expiry_passed = time.time()

        market = gamma.fetch_market(market_id)
        if market is None:
            time.sleep(10)
            continue

        prices_str = " ".join(f"{o}={p:.4f}" for o, p in zip(market.outcomes, market.outcome_prices))
        tag = "CLOSED" if market.closed else "active"
        print(f"  [{ts()}] {tag} | {prices_str} | {minutes_left:+.1f}мин")

        if market.closed:
            elapsed_total = time.time() - t_start
            elapsed_after_expiry = (time.time() - t_expiry_passed) if t_expiry_passed else 0
            result = {
                "market_id": market_id,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "wait_total_seconds": round(elapsed_total, 1),
                "wait_after_expiry_seconds": round(elapsed_after_expiry, 1),
                "outcomes": market.outcomes,
                "outcome_prices": market.outcome_prices,
            }
            for i, (outcome, price) in enumerate(zip(market.outcomes, market.outcome_prices)):
                if price >= 0.9:
                    result["winner"] = outcome
                    result["winner_idx"] = i
            print(f"  результат: {result.get('winner', '?')} | после экспирации: {elapsed_after_expiry:.0f}с")
            return result

        time.sleep(5 if minutes_left <= 0 else (10 if minutes_left <= 5 else 20))


# ── Redeem ─────────────────────────────────────────────────────────────

def do_redeem(market_id: str) -> dict:
    pk = os.environ["WALLET_PRIVATE_KEY"]
    address = get_wallet_address()
    try:
        data = httpx.get(f"{GAMMA_URL}/markets/{market_id}", timeout=10).json()
        condition_id = data.get("conditionId", "")
        neg_risk = data.get("negRisk", False)
    except Exception as e:
        return {"success": False, "error": str(e)}

    if not condition_id:
        return {"success": False, "error": "conditionId не найден"}
    if neg_risk:
        return {"success": False, "error": "neg-risk market", "neg_risk": True}

    condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
            ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=REDEEM_ABI)
            addr_cs = Web3.to_checksum_address(address)
            gas_price = max(w3.eth.gas_price, w3.to_wei(50, "gwei"))
            nonce = w3.eth.get_transaction_count(addr_cs, "latest")

            t0 = time.time()
            tx = ctf.functions.redeemPositions(
                Web3.to_checksum_address(USDC_E), bytes(32), condition_bytes, [1, 2],
            ).build_transaction({
                "from": addr_cs, "nonce": nonce, "gasPrice": gas_price,
                "gas": 200000, "chainId": CHAIN_ID,
            })
            signed = w3.eth.account.sign_transaction(tx, pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hex = "0x" + tx_hash.hex()
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            total_ms = (time.time() - t0) * 1000
            gas_cost = receipt.gasUsed * gas_price / 1e18

            status_str = "OK" if receipt.status == 1 else "FAIL"
            print(f"[redeem] {status_str} | {total_ms:.0f}ms | gas={receipt.gasUsed} ({gas_cost:.6f} POL)")
            return {
                "success": receipt.status == 1, "tx_hash": tx_hex,
                "total_ms": round(total_ms, 1), "gas_used": receipt.gasUsed,
                "gas_price_gwei": round(gas_price / 1e9, 1), "gas_cost_pol": round(gas_cost, 6),
                "redeemed_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            print(f"[redeem] RPC {rpc}: {e}")
    return {"success": False, "error": "все RPC недоступны"}


# ── Главный вотчер (websocket) ─────────────────────────────────────────

class LiveWatcher:
    """Мониторит рынки через websocket, ставит при попадании в окно."""

    def __init__(self, amount_usd: float, prob_min: float, prob_max: float,
                 symbol_filter: str | None, dry: bool):
        self.amount_usd = amount_usd
        self.prob_min = prob_min
        self.prob_max = prob_max
        self.symbol_filter = symbol_filter
        self.dry = dry

        # Текущий набор рынков: market_id -> market_info dict
        self.universe: dict[str, dict] = {}
        # token_id -> list of (market_id, side)
        self.token_to_market: dict[str, list[tuple[str, str]]] = {}
        # Live top-of-book от WS
        self.live_books: dict[str, TopOfBook] = {}

        self.ws: MarketWebSocketClient | None = None
        self.order_client: PyClobClient | None = None
        self._done = threading.Event()
        self._lock = threading.Lock()
        self.result: dict | None = None

    LIVE_PRINT_INTERVAL = 5  # секунд между выводами live цен

    def run(self) -> dict | None:
        last_refresh = 0.0
        last_cleanup = 0.0
        last_live_print = 0.0
        try:
            while not self._done.is_set():
                now = time.time()
                if now - last_refresh >= UNIVERSE_REFRESH_SECONDS:
                    self._refresh_universe()
                    last_refresh = time.time()  # после скана, не до
                if now - last_cleanup >= 10:
                    self._cleanup_expired()
                    last_cleanup = now
                if now - last_live_print >= self.LIVE_PRINT_INTERVAL and self.universe:
                    self._print_live_prices()
                    last_live_print = now
                self._done.wait(timeout=1)
        except KeyboardInterrupt:
            print("\n[stop] Остановлено.")
        finally:
            if self.ws:
                self.ws.stop()
        return self.result

    def _print_live_prices(self):
        """Выводит текущие live цены всех отслеживаемых рынков."""
        now = now_utc()
        lines = []
        for mid, m in sorted(self.universe.items(), key=lambda x: x[1]["expiry_dt"]):
            yes_book = self.live_books.get(m["yes_token_id"] or "")
            no_book = self.live_books.get(m["no_token_id"] or "")

            minutes_left = (m["expiry_dt"] - now).total_seconds() / 60
            if minutes_left < 0:
                continue

            if yes_book and no_book and yes_book.best_ask > 0 and no_book.best_ask > 0:
                ya = yes_book.best_ask
                na = no_book.best_ask
                # Определяем лучшую сторону для ставки
                best_side = ""
                best_prob = 0.0
                for side, ask in [("Up", ya), ("Dn", na)]:
                    if self.prob_min <= ask <= self.prob_max and ask > best_prob:
                        best_side = side
                        best_prob = ask
                marker = f" <<< {best_side}@{best_prob:.2f}" if best_side else ""
                lines.append(
                    f"  {m['symbol']:>4} {m['interval_minutes']:2}м | "
                    f"Up={ya:.4f} Dn={na:.4f} sum={ya+na:.4f} | "
                    f"{minutes_left:5.1f}мин{marker}"
                )
            else:
                lines.append(
                    f"  {m['symbol']:>4} {m['interval_minutes']:2}м | "
                    f"(ожидаем WS данные) | {minutes_left:5.1f}мин"
                )

        if lines:
            print(f"\n[live {ts()}] {len(lines)} рынков:")
            for line in lines:
                print(line)

    def _cleanup_expired(self):
        """Убирает истёкшие рынки из universe и token_to_market."""
        now = now_utc()
        expired_ids = [
            mid for mid, m in self.universe.items()
            if m["expiry_dt"] <= now
        ]
        if not expired_ids:
            return

        for mid in expired_ids:
            m = self.universe.pop(mid, None)
            if m:
                # Убираем маппинг токенов
                for token_id in [m.get("yes_token_id"), m.get("no_token_id")]:
                    if token_id and token_id in self.token_to_market:
                        self.token_to_market[token_id] = [
                            (mkt_id, side) for mkt_id, side in self.token_to_market[token_id]
                            if mkt_id != mid
                        ]
                        if not self.token_to_market[token_id]:
                            del self.token_to_market[token_id]
                            self.live_books.pop(token_id, None)

        print(f"[cleanup] Убрано {len(expired_ids)} истёкших рынков | осталось: {len(self.universe)}")

    def _refresh_universe(self):
        """Пересканирует рынки и подписывается на WS."""
        if self.ws:
            self.ws.stop()

        markets = scan_universe(symbol_filter=self.symbol_filter)
        if not markets:
            self.universe = {}
            self.token_to_market = {}
            print(f"[watch] Нет подходящих рынков, ждём {UNIVERSE_REFRESH_SECONDS}с...")
            return

        # Убираем рынки с истёкшим expiry
        now = now_utc()
        markets = [m for m in markets if m["expiry_dt"] > now + timedelta(minutes=MIN_MINUTES_TO_EXPIRY)]

        self.universe = {m["market_id"]: m for m in markets}
        self.token_to_market = {}
        asset_ids = []

        for m in markets:
            for token_id, side in [(m["yes_token_id"], "Up"), (m["no_token_id"], "Down")]:
                if token_id:
                    self.token_to_market.setdefault(token_id, []).append((m["market_id"], side))
                    asset_ids.append(token_id)

        asset_ids = sorted(set(asset_ids))
        self.live_books = {}

        print(f"[watch] Подписка на {len(markets)} рынков ({len(asset_ids)} токенов) через WS")
        for m in markets[:5]:
            print(f"  {m['symbol']} {m['interval_minutes']}мин | до экспирации: {m['minutes_to_expiry']}мин | vol=${m['volume']:,.0f} | gamma: Up={m['yes_ask_gamma']:.2f} Down={m['no_ask_gamma']:.2f}")

        self.ws = MarketWebSocketClient(url=WS_URL, asset_ids=asset_ids, on_message=self._on_ws_message)
        self.ws.start()

    def _on_ws_message(self, payload: dict):
        """Обрабатывает WS сообщение — обновляет цены, проверяет условия."""
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._on_ws_message(item)
            return

        event_type = payload.get("event_type")
        if event_type not in ("book", "best_bid_ask"):
            return

        asset_id = str(payload.get("asset_id", ""))
        if not asset_id:
            return

        if event_type == "book":
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.0
            best_bid = float(bids[0]["price"]) if bids else 0.0
        else:
            best_ask = float(payload.get("best_ask", 0) or 0)
            best_bid = float(payload.get("best_bid", 0) or 0)

        timestamp = int(payload.get("timestamp", 0) or 0)
        self.live_books[asset_id] = TopOfBook(best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp)

        # Проверяем все рынки привязанные к этому токену
        for market_id, side in self.token_to_market.get(asset_id, []):
            self._check_opportunity(market_id, side, best_ask)

    def _check_opportunity(self, market_id: str, side_updated: str, new_ask: float):
        """Проверяет попадание live цены в окно вероятности."""
        if self._done.is_set():
            return

        market = self.universe.get(market_id)
        if not market:
            return

        # Проверяем expiry
        now = now_utc()
        minutes_left = (market["expiry_dt"] - now).total_seconds() / 60
        if minutes_left < MIN_MINUTES_TO_EXPIRY:
            return

        # Получаем live цены обеих сторон
        yes_book = self.live_books.get(market["yes_token_id"] or "")
        no_book = self.live_books.get(market["no_token_id"] or "")
        if not yes_book or not no_book:
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            return

        # Ищем сторону с вероятностью в нашем окне
        best_side = None
        best_prob = 0.0
        for side, ask in [("Up", yes_book.best_ask), ("Down", no_book.best_ask)]:
            if self.prob_min <= ask <= self.prob_max and ask > best_prob:
                best_side = side
                best_prob = ask

        if best_side is None:
            return

        # Нашли! Логируем и пробуем ставку
        ws_yes = yes_book.best_ask
        ws_no = no_book.best_ask

        print(f"\n[signal] {ts()} | {market['symbol']} {market['interval_minutes']}мин | {best_side}@{best_prob:.4f}")
        print(f"  WS live:  Up={ws_yes:.4f} Down={ws_no:.4f} | sum={ws_yes+ws_no:.4f}")
        print(f"  Gamma:    Up={market['yes_ask_gamma']:.2f} Down={market['no_ask_gamma']:.2f}")
        print(f"  до экспирации: {minutes_left:.1f}мин | vol=${market['volume']:,.0f}")

        with self._lock:
            if self._done.is_set():
                return
            self._execute_bet(market, best_side, best_prob, ws_yes, ws_no)

    def _execute_bet(self, market: dict, side: str, ws_price: float,
                     ws_yes: float, ws_no: float):
        """Проверяет ликвидность, ставит, собирает аналитику."""
        token_id = market["yes_token_id"] if side == "Up" else market["no_token_id"]

        # 1. Проверяем orderbook
        print(f"\n[1/6] Orderbook {side}...")
        ob = check_orderbook(token_id, self.amount_usd, label=side)
        if not ob.get("available"):
            print(f"  Недостаточно ликвидности — пропускаем")
            return

        # Также другую сторону
        other_token = market["no_token_id"] if side == "Up" else market["yes_token_id"]
        ob_other = check_orderbook(other_token, self.amount_usd, label=("Down" if side == "Up" else "Up"))

        analytics: dict = {
            "market": {k: v for k, v in market.items() if k != "expiry_dt"},
            "amount_usd": self.amount_usd,
            "decision": {
                "side": side,
                "ws_price_at_signal": ws_price,
                "ws_yes": ws_yes,
                "ws_no": ws_no,
                "ws_sum": round(ws_yes + ws_no, 4),
                "gamma_yes": market["yes_ask_gamma"],
                "gamma_no": market["no_ask_gamma"],
                "book_best_ask": ob["best_ask"],
                "book_avg_fill": ob["avg_fill_price"],
                "book_slippage": ob["slippage"],
                "book_depth_shares": ob["total_depth_shares"],
                "signal_at": datetime.now(timezone.utc).isoformat(),
            },
            "orderbook_target": ob,
            "orderbook_other": ob_other,
        }

        if self.dry:
            print(f"\n[dry] Сигнал зафиксирован, ордер не размещаем")
            save_results(analytics)
            return

        # 2. Баланс до
        print(f"\n[2/6] Баланс до...")
        balance_before = check_usdc_balance()
        analytics["balance_before"] = balance_before

        # 3. Ставка
        print(f"\n[3/6] Ставка {side} ${self.amount_usd}...")
        if self.order_client is None:
            self.order_client = get_order_client()
        order = place_order(self.order_client, token_id, side, self.amount_usd)
        analytics["order"] = order

        if order.get("status") not in ("matched", "MATCHED"):
            print(f"  Ордер не исполнен: {order.get('status')}")
            save_results(analytics)
            return

        # Аналитика: цена решения vs fill
        fill_price = order.get("fill_price", 0)
        print(f"\n  ЦЕНА РЕШЕНИЯ vs FILL:")
        print(f"    Gamma API snapshot:    {market['yes_ask_gamma'] if side == 'Up' else market['no_ask_gamma']:.4f}")
        print(f"    WS live (сигнал):      {ws_price:.4f}")
        print(f"    Orderbook best ask:    {ob['best_ask']:.4f}")
        print(f"    Orderbook avg fill:    {ob['avg_fill_price']:.4f}")
        print(f"    Реальный fill price:   {fill_price:.4f}")
        print(f"    Slippage (ws→fill):    {fill_price - ws_price:+.4f}")
        print(f"    Slippage (book→fill):  {fill_price - ob['best_ask']:+.4f}")

        analytics["price_comparison"] = {
            "gamma_snapshot": market['yes_ask_gamma'] if side == 'Up' else market['no_ask_gamma'],
            "ws_live_signal": ws_price,
            "book_best_ask": ob["best_ask"],
            "book_avg_fill_simulated": ob["avg_fill_price"],
            "actual_fill_price": fill_price,
            "slippage_ws_to_fill": round(fill_price - ws_price, 6),
            "slippage_book_to_fill": round(fill_price - ob["best_ask"], 6),
        }

        # 4. Ожидание резолюции
        print(f"\n[4/6] Ожидаем резолюцию...")
        resolution = wait_for_resolution(market["market_id"], market["expiry"])
        analytics["resolution"] = resolution

        # P&L
        winner = resolution.get("winner", "")
        bet_won = (winner == side)
        shares = order.get("shares_matched", 0)
        fee = order.get("estimated_fee", 0)
        cost = shares * fill_price + fee
        payout = shares if bet_won else 0.0
        pnl = payout - cost

        analytics["result"] = {
            "won": bet_won, "winner": winner, "bet_side": side,
            "shares": shares, "fill_price": fill_price, "fee": fee,
            "cost": round(cost, 4), "payout": round(payout, 4),
            "pnl": round(pnl, 4),
            "roi_percent": round(pnl / cost * 100, 2) if cost > 0 else 0,
        }

        print(f"\n[result] {'ВЫИГРЫШ' if bet_won else 'ПРОИГРЫШ'} | P&L: ${pnl:+.4f}")

        # 5. Redeem
        if bet_won:
            print(f"\n[5/6] Redeem...")
            time.sleep(3)
            analytics["redeem"] = do_redeem(market["market_id"])
        else:
            analytics["redeem"] = {"skipped": True, "reason": "lost"}

        # 6. Баланс после
        print(f"\n[6/6] Баланс после...")
        time.sleep(5)
        balance_after = check_usdc_balance()
        analytics["balance_after"] = balance_after
        analytics["balance_diff_usdc"] = round(balance_after["usdc"] - balance_before["usdc"], 4)

        # Итоговая сводка
        self._print_summary(analytics, market, side, ob)
        save_results(analytics)
        self.result = analytics

        if bet_won:
            self._done.set()
        else:
            print(f"\n[retry] Проигрыш — продолжаем мониторинг...")

    def _print_summary(self, a: dict, market: dict, side: str, ob: dict):
        order = a.get("order", {})
        resolution = a.get("resolution", {})
        result = a.get("result", {})
        pc = a.get("price_comparison", {})
        redeem = a.get("redeem", {})
        bb = a.get("balance_before", {})
        ba = a.get("balance_after", {})

        print(f"\n{'='*60}")
        print(f"  ИТОГОВАЯ АНАЛИТИКА")
        print(f"{'='*60}")
        print(f"  Рынок:       {market['title'][:55]}")
        print(f"  Ставка:      {side} ${a['amount_usd']} | {market['interval_minutes']}мин")
        print(f"  Результат:   {'ВЫИГРЫШ' if result.get('won') else 'ПРОИГРЫШ'} | P&L: ${result.get('pnl', 0):+.4f}")
        print(f"  ")
        print(f"  ЦЕНЫ (решение vs реальность):")
        print(f"    Gamma snapshot:    {pc.get('gamma_snapshot', '?')}")
        print(f"    WS live сигнал:    {pc.get('ws_live_signal', '?')}")
        print(f"    Book best ask:     {pc.get('book_best_ask', '?')}")
        print(f"    Book avg sim:      {pc.get('book_avg_fill_simulated', '?')}")
        print(f"    Actual fill:       {pc.get('actual_fill_price', '?')}")
        print(f"    Slip (ws→fill):    {pc.get('slippage_ws_to_fill', '?')}")
        print(f"    Slip (book→fill):  {pc.get('slippage_book_to_fill', '?')}")
        print(f"  ")
        print(f"  ТАЙМИНГИ:")
        print(f"    Ордер:             {order.get('total_order_ms', '?')}ms")
        print(f"    После экспирации:  {resolution.get('wait_after_expiry_seconds', '?')}с")
        if redeem.get("success"):
            print(f"    Redeem:            {redeem.get('total_ms', '?')}ms | gas={redeem.get('gas_used', '?')} ({redeem.get('gas_cost_pol', '?')} POL)")
        print(f"  ")
        print(f"  КОМИССИИ:")
        print(f"    Trading fee:       ${order.get('estimated_fee', 0):.4f} ({order.get('fee_percent', '?')}%)")
        if redeem.get("gas_cost_pol"):
            print(f"    Gas (redeem):      {redeem['gas_cost_pol']:.6f} POL")
        print(f"  ")
        print(f"  ЛИКВИДНОСТЬ:")
        print(f"    Depth:             {ob.get('total_depth_shares', '?')} shares")
        print(f"    Slippage sim:      {ob.get('slippage', '?')}")
        print(f"  ")
        print(f"  БАЛАНС:")
        print(f"    До:     ${bb.get('usdc', 0):.4f}")
        print(f"    После:  ${ba.get('usdc', 0):.4f}")
        print(f"    Разница: ${a.get('balance_diff_usdc', 0):+.4f}")
        print(f"{'='*60}")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WS-вотчер: отслеживает цены, ставит при попадании в окно")
    parser.add_argument("--dry", action="store_true", help="Только мониторинг без ставки")
    parser.add_argument("--amount", type=float, default=BET_USD, help=f"Размер ставки (default: ${BET_USD})")
    parser.add_argument("--symbol", default=None, help="Фильтр: BTC, ETH, SOL")
    parser.add_argument("--prob-min", type=float, default=PROB_MIN, help=f"Мин. вероятность (default: {PROB_MIN})")
    parser.add_argument("--prob-max", type=float, default=PROB_MAX, help=f"Макс. вероятность (default: {PROB_MAX})")
    args = parser.parse_args()

    print(f"[start] WS-вотчер")
    print(f"  ставка: ${args.amount} | вероятность: {args.prob_min*100:.0f}–{args.prob_max*100:.0f}%")
    print(f"  интервалы: {INTERVALS} мин | объём >= ${MIN_VOLUME} | символ: {args.symbol or 'любой'}")
    print(f"  режим: {'мониторинг (dry)' if args.dry else 'реальная ставка'}")
    print()

    watcher = LiveWatcher(
        amount_usd=args.amount,
        prob_min=args.prob_min,
        prob_max=args.prob_max,
        symbol_filter=args.symbol,
        dry=args.dry,
    )
    watcher.run()


if __name__ == "__main__":
    main()
