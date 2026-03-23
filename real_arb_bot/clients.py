from __future__ import annotations

import base64
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from eth_account import Account
from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from web3 import Web3

# ── Константы Polymarket ────────────────────────────────────────────────

CLOB_HOST = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
CHAIN_ID = 137
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]
REDEEM_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]
ERC20_BALANCE_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# ── Dataclass-ы результатов ─────────────────────────────────────────────


@dataclass
class OrderResult:
    order_id: str
    status: str
    fill_price: float
    shares_matched: float
    shares_requested: float
    fee: float
    latency_ms: float
    raw_response: dict


@dataclass
class RedeemResult:
    success: bool
    tx_hash: str = ""
    total_ms: float = 0.0
    gas_used: int = 0
    gas_price_gwei: float = 0.0
    gas_cost_pol: float = 0.0
    payout_usdc: float = 0.0
    error: str = ""


# ── PolymarketTrader ────────────────────────────────────────────────────


def _polymarket_fee(shares: float, price: float) -> float:
    return shares * price * 0.25 * ((price * (1 - price)) ** 2)


class PolymarketTrader:
    def __init__(self) -> None:
        pk = os.environ["WALLET_PRIVATE_KEY"]
        funder = os.getenv("WALLET_PROXY", "")
        l1 = PyClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
        creds = l1.create_or_derive_api_creds()
        self._client = PyClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=pk,
            creds=creds,
            funder=funder or None,
        )
        self._pk = pk
        self._address = Account.from_key(pk).address
        print(f"[auth] Polymarket CLOB: {creds.api_key[:8]}... | wallet: {self._address[:10]}...")

    def get_balance(self) -> float:
        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                usdc = w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_BALANCE_ABI
                ).functions.balanceOf(Web3.to_checksum_address(self._address)).call() / 1e6
                return usdc
            except Exception as e:
                print(f"[pm-balance] RPC {rpc}: {e}")
        raise RuntimeError("Все Polygon RPC недоступны")

    def place_fok_order(self, token_id: str, amount_usd: float) -> OrderResult:
        args = MarketOrderArgs(token_id=token_id, amount=amount_usd, side="BUY")
        t0 = time.time()
        order = self._client.create_market_order(args)
        sign_ms = (time.time() - t0) * 1000

        t1 = time.time()
        resp = self._client.post_order(order, orderType=OrderType.FOK)
        post_ms = (time.time() - t1) * 1000
        total_ms = sign_ms + post_ms

        status = resp.get("status", "") if isinstance(resp, dict) else str(resp)
        order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
        fill_price = 0.0
        shares_matched = 0.0
        shares_requested = 0.0
        fee = 0.0

        if order_id:
            time.sleep(1.5)
            try:
                info = self._client.get_order(order_id)
                shares_matched = float(info.get("size_matched", 0))
                shares_requested = float(info.get("original_size", 0))
                fill_price = float(info.get("price", 0))
                fee = _polymarket_fee(shares_matched, fill_price)
            except Exception as e:
                print(f"[pm-order] get_order error: {e}")

        print(
            f"[pm-order] sign={sign_ms:.0f}ms post={post_ms:.0f}ms | "
            f"status={status} | fill={shares_matched:.4f}@{fill_price} | fee=${fee:.4f}"
        )
        return OrderResult(
            order_id=order_id,
            status=status,
            fill_price=fill_price,
            shares_matched=shares_matched,
            shares_requested=shares_requested,
            fee=fee,
            latency_ms=round(total_ms, 1),
            raw_response=resp if isinstance(resp, dict) else {},
        )

    def redeem(self, market_id: str) -> RedeemResult:
        try:
            data = httpx.get(f"{GAMMA_URL}/markets/{market_id}", timeout=10).json()
            condition_id = data.get("conditionId", "")
            neg_risk = data.get("negRisk", False)
        except Exception as e:
            return RedeemResult(success=False, error=str(e))

        if not condition_id:
            return RedeemResult(success=False, error="conditionId не найден")
        if neg_risk:
            return RedeemResult(success=False, error="neg-risk market")

        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        addr_cs = Web3.to_checksum_address(self._address)

        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                usdc_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_BALANCE_ABI
                )
                balance_before = usdc_contract.functions.balanceOf(addr_cs).call() / 1e6

                ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=REDEEM_ABI)
                gas_price = max(w3.eth.gas_price, w3.to_wei(50, "gwei"))
                nonce = w3.eth.get_transaction_count(addr_cs, "latest")
                t0 = time.time()
                tx = ctf.functions.redeemPositions(
                    Web3.to_checksum_address(USDC_E), bytes(32), condition_bytes, [1, 2]
                ).build_transaction({
                    "from": addr_cs, "nonce": nonce,
                    "gasPrice": gas_price, "gas": 200000, "chainId": CHAIN_ID,
                })
                signed = w3.eth.account.sign_transaction(tx, self._pk)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                total_ms = (time.time() - t0) * 1000
                gas_cost = receipt.gasUsed * gas_price / 1e18
                success = receipt.status == 1
                tx_hex = "0x" + tx_hash.hex()

                balance_after = usdc_contract.functions.balanceOf(addr_cs).call() / 1e6
                payout = round(balance_after - balance_before, 6)

                print(f"[pm-redeem] {'OK' if success else 'FAIL'} | {total_ms:.0f}ms | gas={receipt.gasUsed} ({gas_cost:.6f} POL) | payout=${payout:.2f}")
                return RedeemResult(
                    success=success,
                    tx_hash=tx_hex,
                    total_ms=round(total_ms, 1),
                    gas_used=receipt.gasUsed,
                    gas_price_gwei=round(gas_price / 1e9, 1),
                    gas_cost_pol=round(gas_cost, 6),
                    payout_usdc=payout,
                )
            except Exception as e:
                print(f"[pm-redeem] RPC {rpc}: {e}")
        return RedeemResult(success=False, error="все RPC недоступны")


# ── KalshiTrader ────────────────────────────────────────────────────────


def _kalshi_fee(contracts: float, price: float) -> float:
    raw = 0.07 * contracts * price * (1 - price)
    cents = int(raw * 100)
    if raw * 100 > cents:
        cents += 1
    return cents / 100.0


class KalshiTrader:
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self) -> None:
        self._api_key_id = os.environ["KALSHI_API_KEY_ID"]
        key_path = os.environ["KALSHI_PRIVATE_KEY_PATH"]
        with open(key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        self._http = httpx.Client(timeout=15.0)
        print(f"[auth] Kalshi API key: {self._api_key_id[:12]}...")

    def _sign(self, timestamp: str, method: str, path: str) -> str:
        path_clean = path.split("?")[0]
        message = f"{timestamp}{method}{path_clean}".encode("utf-8")
        sig = self._private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(datetime.now().timestamp() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = self.BASE_URL + path
        sign_path = urlparse(url).path
        resp = self._http.get(url, headers=self._headers("GET", sign_path), params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, data: dict) -> httpx.Response:
        url = self.BASE_URL + path
        sign_path = urlparse(url).path
        return self._http.post(url, headers=self._headers("POST", sign_path), json=data)

    def _delete(self, path: str) -> httpx.Response:
        url = self.BASE_URL + path
        sign_path = urlparse(url).path
        return self._http.delete(url, headers=self._headers("DELETE", sign_path))

    def cancel_order(self, order_id: str) -> bool:
        try:
            resp = self._delete(f"/portfolio/orders/{order_id}")
            resp.raise_for_status()
            print(f"[kalshi-cancel] OK | order={order_id[:16]}...")
            return True
        except Exception as e:
            print(f"[kalshi-cancel] FAILED | order={order_id[:16]}... | {e}")
            return False

    def get_balance(self) -> float:
        data = self._get("/portfolio/balance")
        balance = data.get("balance_dollars", data.get("balance", 0))
        if isinstance(balance, int):
            balance = balance / 100.0
        return float(balance)

    def get_market(self, ticker: str) -> dict | None:
        try:
            return self._get(f"/markets/{ticker}").get("market")
        except Exception as e:
            print(f"[kalshi] get_market {ticker}: {e}")
            return None

    def place_limit_order(self, ticker: str, side: str, count: int, price_cents: int, action: str = "buy") -> OrderResult:
        client_order_id = str(uuid.uuid4())
        order_data = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": price_cents if side == "yes" else (100 - price_cents),
            "client_order_id": client_order_id,
        }
        t0 = time.time()
        resp = self._post("/portfolio/orders", order_data)
        latency_ms = (time.time() - t0) * 1000

        order_id = ""
        status = ""
        fill_count = 0.0

        try:
            body = resp.json()
            if resp.status_code == 201:
                o = body.get("order", {})
                order_id = o.get("order_id", "")
                status = o.get("status", "")
                fill_count = float(o.get("fill_count_fp", o.get("count_fp", o.get("size_matched", count))))
                print(f"[kalshi-order] OK | {latency_ms:.0f}ms | id={order_id[:16]}... | status={status} | fill={fill_count}")
            else:
                status = f"error_{resp.status_code}"
                print(f"[kalshi-order] ERROR {resp.status_code} | {latency_ms:.0f}ms | {body}")
                body = body
        except Exception as e:
            status = "parse_error"
            body = {}
            print(f"[kalshi-order] parse error: {e}")

        price = price_cents / 100.0
        fee = _kalshi_fee(fill_count, price) if fill_count > 0 else 0.0

        return OrderResult(
            order_id=order_id,
            status=status,
            fill_price=price,
            shares_matched=fill_count,
            shares_requested=float(count),
            fee=fee,
            latency_ms=round(latency_ms, 1),
            raw_response=body if isinstance(body, dict) else {},
        )
