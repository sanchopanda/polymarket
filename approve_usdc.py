#!/usr/bin/env python3
"""Одобряет CTF Exchange контракты тратить USDC (одноразовая операция)."""
from dotenv import load_dotenv
load_dotenv()

import os
from web3 import Web3
from eth_account import Account

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://rpc.ankr.com/polygon",
]
w3 = None
for RPC in RPCS:
    try:
        _w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 10}))
        _w3.eth.block_number  # test
        w3 = _w3
        print(f"RPC: {RPC}")
        break
    except Exception as e:
        print(f"✗ {RPC}: {e}")
if not w3:
    raise SystemExit("Нет доступного RPC")

pk = os.getenv("WALLET_PRIVATE_KEY")
acct = Account.from_key(pk)
addr = acct.address
print(f"Кошелёк: {addr}")
print(f"POL:     {w3.from_wei(w3.eth.get_balance(addr), 'ether'):.4f}")

# USDC.e (bridged) — именно его использует Polymarket
USDC = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

# CTF Exchange контракты Polymarket
SPENDERS = {
    "CTF Exchange":         "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "NegRisk CTF Exchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "NegRisk Adapter":      "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
}

APPROVE_ABI = [{
    "name": "approve", "type": "function", "stateMutability": "nonpayable",
    "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
    "outputs": [{"name": "", "type": "bool"}]
}, {
    "name": "allowance", "type": "function", "stateMutability": "view",
    "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
    "outputs": [{"name": "", "type": "uint256"}]
}]

usdc = w3.eth.contract(address=USDC, abi=APPROVE_ABI)
MAX = 2**256 - 1

print()
for name, spender in SPENDERS.items():
    spender_cs = Web3.to_checksum_address(spender)
    current = usdc.functions.allowance(addr, spender_cs).call()
    if current > 10**18:
        print(f"✓ {name}: уже одобрен (${current/1e6:.0f})")
        continue

    print(f"→ Approve {name}...", end=" ", flush=True)
    nonce = w3.eth.get_transaction_count(addr)
    tx = usdc.functions.approve(spender_cs, MAX).build_transaction({
        "from": addr,
        "nonce": nonce,
        "gas": 100000,
        "gasPrice": w3.eth.gas_price,
        "chainId": 137,
    })
    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    status = "✓ OK" if receipt.status == 1 else "✗ FAIL"
    print(f"{status} | tx: {tx_hash.hex()[:16]}...")

print("\nГотово. Запускай: python -m src.main real balance")
