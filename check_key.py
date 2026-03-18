#!/usr/bin/env python3
"""Проверяет: какому адресу соответствует приватный ключ."""
from dotenv import load_dotenv
load_dotenv()
import os
from eth_account import Account

pk = os.getenv("WALLET_PRIVATE_KEY")
account = Account.from_key(pk)
print(f"Приватный ключ: {pk[:10]}...{pk[-6:]}")
print(f"Адрес (этот ключ): {account.address}")
print()
print(f"Адрес Polymarket: 0xaee3768194d61c09712babcb8a97283e86b9d68b")
print()
match = account.address.lower() == "0xaee3768194d61c09712babcb8a97283e86b9d68b"
print(f"Совпадают: {'ДА ✓' if match else 'НЕТ ✗ — ключ от другого кошелька'}")
