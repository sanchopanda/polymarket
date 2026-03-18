#!/usr/bin/env python3
"""Находим реальный адрес Polymarket-аккаунта через CLOB API."""
from dotenv import load_dotenv
load_dotenv()

import os, httpx
from py_clob_client.client import ClobClient

CLOB_HOST = "https://clob.polymarket.com"
pk = os.getenv("WALLET_PRIVATE_KEY")
proxy = os.getenv("WALLET_PROXY")
chain_id = 137

# Деривация ключей
l1 = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk)
creds = l1.create_or_derive_api_creds()
c = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk, creds=creds)

print(f"EOA (ключ): {c.get_address()}")
print(f"Proxy (.env): {proxy}")
print()

# Пробуем получить информацию об аккаунте через разные эндпоинты
headers = {
    "POLY_ADDRESS": c.get_address(),
    "POLY_SIGNATURE": "",
    "POLY_TIMESTAMP": "",
    "POLY_NONCE": "",
}

# Попробуем эндпоинт профиля
for path in ["/profile", "/user", "/account"]:
    try:
        r = httpx.get(f"{CLOB_HOST}{path}", timeout=5)
        if r.status_code != 404:
            print(f"GET {path}: {r.status_code} → {r.text[:200]}")
    except:
        pass

# L2 авторизованные запросы
print("\n=== L2 auth запросы ===")
import time, hmac, hashlib, base64

ts = str(int(time.time()))
nonce = "0"

def sign_l2(method, path, body=""):
    msg = ts + method + path + body
    sig = hmac.new(
        base64.b64decode(creds.api_secret + "=="),
        msg.encode(), hashlib.sha256
    ).digest()
    return base64.b64encode(sig).decode()

# Попробуем через py_clob_client методы
try:
    # get_orders возвращает открытые ордера
    orders = c.get_orders()
    print(f"Открытых ордеров: {len(orders) if orders else 0}")
    if orders:
        print(f"  Пример: {orders[0]}")
except Exception as e:
    print(f"get_orders: {e}")

try:
    trades = c.get_trades()
    print(f"Трейдов: {len(trades) if trades else 0}")
except Exception as e:
    print(f"get_trades: {e}")

# Прямой запрос баланса с L2 auth headers
print("\n=== Прямой запрос balance-allowance с L2 headers ===")
try:
    from py_clob_client.headers.headers import create_l2_headers
    l2h = create_l2_headers(c.signer, {"method": "GET", "requestPath": "/balance-allowance", "body": ""})
    r = httpx.get(f"{CLOB_HOST}/balance-allowance",
                  params={"asset_type": "COLLATERAL"},
                  headers={**l2h, "Content-Type": "application/json"}, timeout=10)
    print(f"Status: {r.status_code}")
    print(f"Body: {r.text[:500]}")
except Exception as e:
    print(f"Ошибка: {e}")
