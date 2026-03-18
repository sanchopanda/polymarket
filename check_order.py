#!/usr/bin/env python3
"""Проверяет статус последнего ордера."""
from dotenv import load_dotenv
load_dotenv()
import os
from py_clob_client.client import ClobClient

CLOB_HOST = "https://clob.polymarket.com"
pk = os.getenv("WALLET_PRIVATE_KEY")

l1 = ClobClient(host=CLOB_HOST, chain_id=137, key=pk)
creds = l1.create_or_derive_api_creds()
c = ClobClient(host=CLOB_HOST, chain_id=137, key=pk, creds=creds)

order_id = "0xaba5c7d4f5c3ea753537894ba70bda492e2b3546df59f4416fb46ad53476bca4"

try:
    order = c.get_order(order_id)
    print(f"Status:      {order.get('status')}")
    print(f"Size:        {order.get('size_matched')} / {order.get('original_size')}")
    print(f"Price:       {order.get('price')}")
    print(f"Side:        {order.get('side')}")
except Exception as e:
    print(f"Ошибка: {e}")

print()
try:
    trades = c.get_trades(id=order_id)
    print(f"Трейды: {trades}")
except Exception as e:
    print(f"Трейды: {e}")
