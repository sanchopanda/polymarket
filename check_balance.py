#!/usr/bin/env python3
"""Диагностика баланса: EOA vs proxy, с funder при деривации ключей."""
from dotenv import load_dotenv
load_dotenv()

import os
import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

CLOB_HOST = "https://clob.polymarket.com"
pk = os.getenv("WALLET_PRIVATE_KEY")
proxy = os.getenv("WALLET_PROXY")
chain_id = 137

print(f"EOA:   {pk[:6]}...  → {ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk).get_address()}")
print(f"Proxy: {proxy}")
print()

def check(label, client):
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    try:
        client.update_balance_allowance(params=params)
        r = client.get_balance_allowance(params=params)
        bal = float(r.get("balance", 0)) / 1e6
        # allowances — dict или одно значение
        raw_all = r.get("allowances") or r.get("allowance") or {}
        if isinstance(raw_all, dict):
            total_all = sum(float(v) for v in raw_all.values()) / 1e6
        else:
            total_all = float(raw_all) / 1e6
        print(f"{label}: balance=${bal:.4f}  allowances_total=${total_all:.4f}")
        print(f"  raw={r}")
    except Exception as e:
        print(f"{label}: ОШИБКА {e}")

# Вариант 1: creds без funder → проверка EOA
print("=== Вариант 1: creds без funder ===")
l1a = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk)
creds_a = l1a.create_or_derive_api_creds()
ca = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk, creds=creds_a)
check("  EOA", ca)
ca_f = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk, creds=creds_a, funder=proxy)
check("  EOA+funder", ca_f)
print()

# Вариант 2: creds с funder → деривация от имени proxy
print("=== Вариант 2: creds С funder при деривации ===")
l1b = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk, funder=proxy)
try:
    creds_b = l1b.create_or_derive_api_creds()
    print(f"  creds_b.api_key={creds_b.api_key[:12]}...")
    cb = ClobClient(host=CLOB_HOST, chain_id=chain_id, key=pk, creds=creds_b, funder=proxy)
    check("  proxy-creds+funder", cb)
except Exception as e:
    print(f"  ОШИБКА при деривации: {e}")
print()

# Вариант 3: прямой запрос к CLOB для proxy-адреса
print("=== Вариант 3: прямой GET /balance-allowance для proxy ===")
try:
    url = f"{CLOB_HOST}/balance-allowance?asset_type=COLLATERAL&owner={proxy}"
    r = httpx.get(url, timeout=10)
    print(f"  status={r.status_code}  body={r.text[:300]}")
except Exception as e:
    print(f"  ОШИБКА: {e}")
