from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional
from urllib.parse import urlencode

import httpx


MAINNET_URL = "https://api.bybit.com"
TESTNET_URL = "https://api-testnet.bybit.com"
DEMO_URL = "https://api-demo.bybit.com"


class BybitClient:
    def __init__(self, api_key: str, api_secret: str, mode: str = "demo") -> None:
        """mode: 'demo' | 'testnet' | 'mainnet'"""
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = {"demo": DEMO_URL, "testnet": TESTNET_URL, "mainnet": MAINNET_URL}[mode]
        self.client = httpx.Client(timeout=10)

    def _sign(self, params: dict, timestamp: str) -> str:
        param_str = timestamp + self.api_key + "5000" + urlencode(params)
        return hmac.new(
            self.api_secret.encode(), param_str.encode(), hashlib.sha256
        ).hexdigest()

    def _headers(self, timestamp: str, sign: str) -> dict:
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": sign,
            "X-BAPI-RECV-WINDOW": "5000",
        }

    def _get(self, path: str, params: dict) -> dict:
        timestamp = str(int(time.time() * 1000))
        sign = self._sign(params, timestamp)
        resp = self.client.get(
            self.base_url + path,
            params=params,
            headers=self._headers(timestamp, sign),
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        import json
        timestamp = str(int(time.time() * 1000))
        body_str = json.dumps(body)
        param_str = timestamp + self.api_key + "5000" + body_str
        sign = hmac.new(
            self.api_secret.encode(), param_str.encode(), hashlib.sha256
        ).hexdigest()
        headers = self._headers(timestamp, sign)
        headers["Content-Type"] = "application/json"
        resp = self.client.post(
            self.base_url + path,
            content=body_str,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()

    def get_wallet_balance(self) -> dict:
        """Баланс аккаунта (USDT)."""
        data = self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        return data

    def get_ticker(self, symbol: str) -> float:
        """Текущая цена."""
        data = self._get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        return float(data["result"]["list"][0]["lastPrice"])

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Установить плечо."""
        return self._post("/v5/position/set-leverage", {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        })

    def place_order(self, symbol: str, side: str, qty: str) -> dict:
        """Открыть позицию маркет-ордером (без TP/SL — задаётся отдельно после исполнения).

        side: "Buy" или "Sell"
        qty: размер в базовой валюте
        """
        return self._post("/v5/order/create", {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": qty,
            "timeInForce": "IOC",
            "positionIdx": 0,
        })

    def set_trading_stop(self, symbol: str, take_profit: float, stop_loss: float) -> dict:
        """Установить TP и SL на открытую позицию (на всю позицию)."""
        return self._post("/v5/position/trading-stop", {
            "category": "linear",
            "symbol": symbol,
            "takeProfit": str(take_profit),
            "stopLoss": str(stop_loss),
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "LastPrice",
            "positionIdx": 0,
        })

    def get_positions(self, symbol: str = "") -> list:
        """Открытые позиции. Без symbol — все позиции."""
        params: dict = {"category": "linear"}
        if symbol:
            params["symbol"] = symbol
        data = self._get("/v5/position/list", params)
        return data["result"]["list"]

    def close_position(self, symbol: str, side: str, qty: str) -> dict:
        """Закрыть позицию маркет-ордером.

        side: сторона ЗАКРЫТИЯ (Buy для закрытия Short, Sell для закрытия Long).
        """
        return self._post("/v5/order/create", {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": qty,
            "timeInForce": "IOC",
            "positionIdx": 0,
            "reduceOnly": True,
        })

    def get_order(self, symbol: str, order_id: str) -> Optional[dict]:
        """Статус ордера (сначала active, потом history)."""
        for endpoint in ["/v5/order/realtime", "/v5/order/history"]:
            data = self._get(endpoint, {
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id,
            })
            orders = data.get("result", {}).get("list", [])
            if orders:
                return orders[0]
        return None

    def get_instrument_info(self, symbol: str) -> dict:
        """Параметры инструмента: qtyStep, minOrderQty."""
        data = self._get("/v5/market/instruments-info", {
            "category": "linear",
            "symbol": symbol,
        })
        items = data.get("result", {}).get("list", [])
        if not items:
            return {}
        lot = items[0].get("lotSizeFilter", {})
        return {
            "qty_step": float(lot.get("qtyStep", 0.001)),
            "min_qty": float(lot.get("minOrderQty", 0.001)),
        }

    def get_closed_pnl(self, symbol: str, limit: int = 20) -> list:
        """Закрытые позиции с P&L."""
        data = self._get("/v5/position/closed-pnl", {
            "category": "linear",
            "symbol": symbol,
            "limit": str(limit),
        })
        return data["result"]["list"]
