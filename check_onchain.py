#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()

import os, httpx
from eth_account import Account

pk = os.getenv("WALLET_PRIVATE_KEY")
ADDR = Account.from_key(pk).address
RPC  = "https://polygon-rpc.com"

USDC_CONTRACTS = {
    "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    "USDC":   "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
}
SPENDERS = {
    "NegRisk CTF Exchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "CTF Exchange":         "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
}

def rpc(method, params):
    r = httpx.post(RPC, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=15)
    return r.json().get("result")

def to_int(raw): return int(raw, 16) if raw and raw != "0x" else 0
def call(to, data): return rpc("eth_call", [{"to": to, "data": data}, "latest"]) or "0x"
def balance_of(token, addr): return to_int(call(token, "0x70a08231" + addr[2:].lower().zfill(64))) / 1e6
def allowance(token, owner, spender):
    return to_int(call(token, "0xdd62ed3e" + owner[2:].lower().zfill(64) + spender[2:].lower().zfill(64))) / 1e6
def get_pol(addr): return to_int(rpc("eth_getBalance", [addr, "latest"]) or "0x0") / 1e18

print(f"Кошелёк: {ADDR}")
print(f"POL:     {get_pol(ADDR):.4f}")
print()
print("USDC баланс:")
for name, token in USDC_CONTRACTS.items():
    print(f"  {name}: ${balance_of(token, ADDR):.4f}")
print()
print("Allowance для CTF Exchange:")
for tname, token in USDC_CONTRACTS.items():
    for sname, spender in SPENDERS.items():
        a = allowance(token, ADDR, spender)
        mark = "✓" if a > 0 else "✗"
        print(f"  {mark} {tname} → {sname}: ${a:.2f}")
