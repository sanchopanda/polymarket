from __future__ import annotations

import base64
import math
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
try:
    from py_clob_client.clob_types import OrderArgs
except ImportError:
    OrderArgs = None
from web3 import Web3

# ── Константы Polymarket ────────────────────────────────────────────────

CLOB_HOST = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
CHAIN_ID = 137
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
NEG_RISK_WCOL = "0x3A3BD7bb9528E159577F7C2e685CC81A765002E2"
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
NEG_RISK_REDEEM_ABI = [
    {
        "inputs": [
            {"name": "_conditionId", "type": "bytes32"},
            {"name": "_amounts", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]
CTF_ERC1155_ABI = [
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
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
    pending: bool = False  # TX ушла но ещё не замайнена


# ── PolymarketTrader ────────────────────────────────────────────────────


def _ctf_position_id(collateral: str, condition_id: bytes, index_set: int) -> int:
    """Compute CTF ERC1155 token ID: keccak(address ++ keccak(conditionId ++ indexSet))."""
    collection_id = Web3.keccak(condition_id + index_set.to_bytes(32, "big"))
    return int.from_bytes(
        Web3.keccak(bytes.fromhex(collateral.replace("0x", "")) + collection_id), "big"
    )


def _polymarket_fee(shares: float, price: float) -> float:
    return shares * price * 0.25 * ((price * (1 - price)) ** 2)


def _kalshi_price_to_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if price > 1.0:
        price /= 100.0
    return max(0.0, min(1.0, price))


class PolymarketTrader:
    def __init__(self) -> None:
        import time as _time
        pk = os.environ["WALLET_PRIVATE_KEY"]
        funder = os.getenv("WALLET_PROXY", "")
        l1 = PyClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
        creds = None
        for attempt in range(1, 6):
            try:
                creds = l1.create_or_derive_api_creds()
                break
            except Exception as e:
                if attempt == 5:
                    raise
                print(f"[auth] Polymarket CLOB: попытка {attempt}/5 не удалась ({e}), повтор через 5с...")
                _time.sleep(5)
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

        status, order_id, fill_price, shares_matched, shares_requested, fee = self._extract_order_fields(
            resp=resp,
            wait_seconds=1.5,
        )

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

    def place_limit_buy_order(
        self,
        token_id: str,
        price: float,
        size: float,
        wait_seconds: float = 1.5,
    ) -> OrderResult:
        return self._place_limit_order(
            token_id=token_id,
            price=price,
            size=size,
            side="BUY",
            wait_seconds=wait_seconds,
        )

    def place_limit_sell_order(
        self,
        token_id: str,
        price: float,
        size: float,
        wait_seconds: float = 1.5,
    ) -> OrderResult:
        return self._place_limit_order(
            token_id=token_id,
            price=price,
            size=size,
            side="SELL",
            wait_seconds=wait_seconds,
        )

    def _place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        wait_seconds: float = 1.5,
    ) -> OrderResult:
        if OrderArgs is None:
            raise RuntimeError("py_clob_client.OrderArgs недоступен")

        rounded_price = math.floor(price * 100) / 100.0
        if rounded_price <= 0:
            raise ValueError(f"Некорректная PM limit price: {price}")
        if size <= 0:
            raise ValueError(f"Некорректный PM size: {size}")

        args = OrderArgs(token_id=token_id, price=rounded_price, size=size, side=side)
        t0 = time.time()
        order = self._client.create_order(args)
        sign_ms = (time.time() - t0) * 1000

        t1 = time.time()
        resp = self._client.post_order(order, orderType=OrderType.GTC)
        post_ms = (time.time() - t1) * 1000
        total_ms = sign_ms + post_ms

        status, order_id, fill_price, shares_matched, shares_requested, fee = self._extract_order_fields(
            resp=resp,
            wait_seconds=wait_seconds,
        )

        print(
            f"[pm-limit] sign={sign_ms:.0f}ms post={post_ms:.0f}ms | "
            f"side={side} | status={status} | req={shares_requested:.4f} | fill={shares_matched:.4f}@{fill_price:.4f} | "
            f"limit={rounded_price:.4f} | fee=${fee:.4f}"
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

    def cancel_order(self, order_id: str) -> bool:
        if not order_id:
            return False
        try:
            response = self._client.cancel(order_id)
            canceled = response.get("canceled", []) if isinstance(response, dict) else []
            ok = order_id in canceled if canceled else True
            print(f"[pm-cancel] {'OK' if ok else 'FAILED'} | order={order_id[:16]}...")
            return ok
        except Exception as e:
            print(f"[pm-cancel] FAILED | order={order_id[:16]}... | {e}")
            return False

    def _extract_order_fields(
        self,
        resp: dict | object,
        wait_seconds: float,
    ) -> tuple[str, str, float, float, float, float]:
        status = resp.get("status", "") if isinstance(resp, dict) else str(resp)
        order_id = resp.get("orderID", "") if isinstance(resp, dict) else ""
        fill_price = 0.0
        shares_matched = 0.0
        shares_requested = 0.0
        fee = 0.0

        if not order_id:
            return status, order_id, fill_price, shares_matched, shares_requested, fee

        time.sleep(wait_seconds)
        try:
            info = self._client.get_order(order_id)
            status = info.get("status", status)
            shares_matched = float(info.get("size_matched", 0))
            shares_requested = float(info.get("original_size", 0))
            fill_price = float(info.get("price", 0))
            fee = _polymarket_fee(shares_matched, fill_price)
        except Exception as e:
            print(f"[pm-order] get_order error: {e}")

        return status, order_id, fill_price, shares_matched, shares_requested, fee

    def redeem(self, market_id: str, pending_tx_hash: str = "",
               condition_id: str | None = None, neg_risk: bool | None = None) -> RedeemResult:
        if condition_id and neg_risk is not None:
            pass  # caller already provided market data, skip Gamma
        else:
            try:
                data = httpx.get(f"{GAMMA_URL}/markets/{market_id}", timeout=10).json()
                condition_id = data.get("conditionId", "")
                neg_risk = data.get("negRisk", False)
            except Exception as e:
                return RedeemResult(success=False, error=str(e))

        if not condition_id:
            return RedeemResult(success=False, error="conditionId не найден")

        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        addr_cs = Web3.to_checksum_address(self._address)

        if neg_risk:
            return self._redeem_neg_risk(condition_bytes, addr_cs)

        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                usdc_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_BALANCE_ABI
                )

                balance_before = usdc_contract.functions.balanceOf(addr_cs).call() / 1e6

                # Если предыдущая TX ещё в pending — ждём её, не шлём новую
                if pending_tx_hash:
                    print(f"[pm-redeem] ждём pending TX {pending_tx_hash[:16]}...")
                    try:
                        receipt = w3.eth.wait_for_transaction_receipt(
                            bytes.fromhex(pending_tx_hash.replace("0x", "")), timeout=120
                        )
                    except Exception:
                        print(f"[pm-redeem] pending TX не подтвердилась через RPC {rpc}")
                        continue
                    gas_price = w3.to_wei(50, "gwei")
                    tx_hex = pending_tx_hash
                    t0 = time.time()
                else:
                    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=REDEEM_ABI)
                    # Используем pending nonce чтобы не конфликтовать с застрявшими TX
                    nonce = w3.eth.get_transaction_count(addr_cs, "pending")
                    network_gas = w3.eth.gas_price
                    # Для замены застрявших TX используем 2x от текущей цены сети
                    gas_price = max(network_gas * 2, w3.to_wei(50, "gwei"))
                    t0 = time.time()
                    tx = ctf.functions.redeemPositions(
                        Web3.to_checksum_address(USDC_E), bytes(32), condition_bytes, [1, 2]
                    ).build_transaction({
                        "from": addr_cs, "nonce": nonce,
                        "gasPrice": gas_price, "gas": 200000, "chainId": CHAIN_ID,
                    })
                    signed = w3.eth.account.sign_transaction(tx, self._pk)
                    try:
                        tx_hash_bytes = w3.eth.send_raw_transaction(signed.raw_transaction)
                    except Exception as send_err:
                        err_msg = str(send_err)
                        if "underpriced" in err_msg or "already known" in err_msg:
                            # Застрявшая TX — пробуем с ещё более высоким gas
                            nonce = w3.eth.get_transaction_count(addr_cs, "latest")
                            gas_price = max(network_gas * 3, w3.to_wei(100, "gwei"))
                            print(f"[pm-redeem] retry nonce={nonce} gas={gas_price/1e9:.0f} gwei")
                            tx = ctf.functions.redeemPositions(
                                Web3.to_checksum_address(USDC_E), bytes(32), condition_bytes, [1, 2]
                            ).build_transaction({
                                "from": addr_cs, "nonce": nonce,
                                "gasPrice": gas_price, "gas": 200000, "chainId": CHAIN_ID,
                            })
                            signed = w3.eth.account.sign_transaction(tx, self._pk)
                            tx_hash_bytes = w3.eth.send_raw_transaction(signed.raw_transaction)
                        else:
                            raise
                    tx_hex = "0x" + tx_hash_bytes.hex()
                    try:
                        receipt = w3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=90)
                    except Exception:
                        elapsed = (time.time() - t0) * 1000
                        print(f"[pm-redeem] TX в pending ({elapsed:.0f}ms), tx={tx_hex[:16]}...")
                        return RedeemResult(success=False, pending=True, tx_hash=tx_hex)

                total_ms = (time.time() - t0) * 1000
                gas_cost = receipt.gasUsed * gas_price / 1e18
                success = receipt.status == 1
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

    def _redeem_neg_risk(self, condition_bytes: bytes, addr_cs: str) -> RedeemResult:
        """Redeem positions on a neg-risk market via NegRiskAdapter."""
        adapter_cs = Web3.to_checksum_address(NEG_RISK_ADAPTER)
        ctf_cs = Web3.to_checksum_address(CTF_ADDRESS)

        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                usdc_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E), abi=ERC20_BALANCE_ABI
                )
                ctf = w3.eth.contract(address=ctf_cs, abi=CTF_ERC1155_ABI)

                # Query YES/NO token balances
                yes_id = _ctf_position_id(NEG_RISK_WCOL, condition_bytes, 1)
                no_id = _ctf_position_id(NEG_RISK_WCOL, condition_bytes, 2)
                yes_bal = ctf.functions.balanceOf(addr_cs, yes_id).call()
                no_bal = ctf.functions.balanceOf(addr_cs, no_id).call()

                if yes_bal == 0 and no_bal == 0:
                    return RedeemResult(success=False, error="no tokens to redeem (yes=0, no=0)")

                print(f"[pm-redeem-neg] YES={yes_bal / 1e6:.2f} NO={no_bal / 1e6:.2f}")

                balance_before = usdc_contract.functions.balanceOf(addr_cs).call() / 1e6

                # Ensure ERC1155 approval for NegRiskAdapter
                approved = ctf.functions.isApprovedForAll(addr_cs, adapter_cs).call()
                if not approved:
                    print("[pm-redeem-neg] setting ERC1155 approval for NegRiskAdapter...")
                    nonce = w3.eth.get_transaction_count(addr_cs, "pending")
                    gas_price = max(w3.eth.gas_price * 2, w3.to_wei(50, "gwei"))
                    approve_tx = ctf.functions.setApprovalForAll(
                        adapter_cs, True
                    ).build_transaction({
                        "from": addr_cs, "nonce": nonce,
                        "gasPrice": gas_price, "gas": 60000, "chainId": CHAIN_ID,
                    })
                    signed = w3.eth.account.sign_transaction(approve_tx, self._pk)
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                    if receipt.status != 1:
                        return RedeemResult(success=False, error="approval tx failed")
                    print("[pm-redeem-neg] approval OK")

                # Build and send redeem TX
                adapter = w3.eth.contract(address=adapter_cs, abi=NEG_RISK_REDEEM_ABI)
                nonce = w3.eth.get_transaction_count(addr_cs, "pending")
                network_gas = w3.eth.gas_price
                gas_price = max(network_gas * 2, w3.to_wei(50, "gwei"))
                t0 = time.time()

                tx = adapter.functions.redeemPositions(
                    condition_bytes, [yes_bal, no_bal]
                ).build_transaction({
                    "from": addr_cs, "nonce": nonce,
                    "gasPrice": gas_price, "gas": 300000, "chainId": CHAIN_ID,
                })
                signed = w3.eth.account.sign_transaction(tx, self._pk)
                try:
                    tx_hash_bytes = w3.eth.send_raw_transaction(signed.raw_transaction)
                except Exception as send_err:
                    err_msg = str(send_err)
                    if "underpriced" in err_msg or "already known" in err_msg:
                        nonce = w3.eth.get_transaction_count(addr_cs, "latest")
                        gas_price = max(network_gas * 3, w3.to_wei(100, "gwei"))
                        print(f"[pm-redeem-neg] retry nonce={nonce} gas={gas_price / 1e9:.0f} gwei")
                        tx = adapter.functions.redeemPositions(
                            condition_bytes, [yes_bal, no_bal]
                        ).build_transaction({
                            "from": addr_cs, "nonce": nonce,
                            "gasPrice": gas_price, "gas": 300000, "chainId": CHAIN_ID,
                        })
                        signed = w3.eth.account.sign_transaction(tx, self._pk)
                        tx_hash_bytes = w3.eth.send_raw_transaction(signed.raw_transaction)
                    else:
                        raise

                tx_hex = "0x" + tx_hash_bytes.hex()
                try:
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=90)
                except Exception:
                    elapsed = (time.time() - t0) * 1000
                    print(f"[pm-redeem-neg] TX в pending ({elapsed:.0f}ms), tx={tx_hex[:16]}...")
                    return RedeemResult(success=False, pending=True, tx_hash=tx_hex)

                total_ms = (time.time() - t0) * 1000
                gas_cost = receipt.gasUsed * gas_price / 1e18
                success = receipt.status == 1
                balance_after = usdc_contract.functions.balanceOf(addr_cs).call() / 1e6
                payout = round(balance_after - balance_before, 6)

                print(f"[pm-redeem-neg] {'OK' if success else 'FAIL'} | {total_ms:.0f}ms | gas={receipt.gasUsed} ({gas_cost:.6f} POL) | payout=${payout:.2f}")
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
                print(f"[pm-redeem-neg] RPC {rpc}: {e}")
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

    def get_order(self, order_id: str) -> OrderResult | None:
        if not order_id:
            return None
        try:
            payload = self._get(f"/portfolio/orders/{order_id}")
        except Exception as e:
            print(f"[kalshi] get_order {order_id[:16]}...: {e}")
            return None

        order = payload.get("order", payload) if isinstance(payload, dict) else {}
        if not isinstance(order, dict):
            return None

        status = str(order.get("status", ""))
        fill_count = float(order.get("fill_count_fp", order.get("count_fp", order.get("size_matched", 0))) or 0)
        requested = float(order.get("count", order.get("initial_count", fill_count)) or fill_count or 0)
        side = str(order.get("side", "")).lower()
        yes_price = _kalshi_price_to_float(order.get("yes_price"))
        no_price = _kalshi_price_to_float(order.get("no_price"))
        direct_price = _kalshi_price_to_float(order.get("price"))

        if side == "yes":
            price = yes_price
            if price is None and no_price is not None:
                price = max(0.0, 1.0 - no_price)
        else:
            price = no_price
            if price is None and yes_price is not None:
                price = max(0.0, 1.0 - yes_price)

        if price is None:
            price = direct_price if direct_price is not None else 0.0
        fee = _kalshi_fee(fill_count, price) if fill_count > 0 else 0.0

        return OrderResult(
            order_id=str(order.get("order_id", order_id)),
            status=status,
            fill_price=price,
            shares_matched=fill_count,
            shares_requested=requested,
            fee=fee,
            latency_ms=0.0,
            raw_response=order,
        )

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
