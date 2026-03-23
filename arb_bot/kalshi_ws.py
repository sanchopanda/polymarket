from __future__ import annotations

import base64
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from websockets.sync.client import connect

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


@dataclass
class KalshiTopOfBook:
    best_yes_ask: float = 0.0
    best_no_ask: float = 0.0
    updated_at: float = 0.0


class KalshiWebSocketClient:
    """WebSocket клиент для Kalshi orderbook_delta.

    Читает KALSHI_API_KEY_ID и KALSHI_PRIVATE_KEY_PATH из env.
    on_update(ticker, KalshiTopOfBook) вызывается при каждом обновлении стакана.
    """

    def __init__(
        self,
        tickers: list[str],
        on_update: Callable[[str, KalshiTopOfBook], None],
    ) -> None:
        self.tickers = tickers
        self.on_update = on_update
        self._stop = False
        self._thread: threading.Thread | None = None
        self._books: dict[str, dict[str, dict[float, float]]] = {}
        self._seq: int = 0

        api_key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
        if not api_key_id or not key_path:
            raise RuntimeError("KALSHI_API_KEY_ID и KALSHI_PRIVATE_KEY_PATH должны быть в .env")
        self._api_key_id = api_key_id
        with open(key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

    def _auth_headers(self) -> dict:
        ts = str(int(datetime.now().timestamp() * 1000))
        message = f"{ts}GET{WS_PATH}".encode("utf-8")
        sig = self._private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop:
            try:
                with connect(
                    WS_URL,
                    additional_headers=self._auth_headers(),
                    open_timeout=10,
                    close_timeout=3,
                    ping_interval=None,
                ) as ws:
                    for i, ticker in enumerate(self.tickers, start=1):
                        ws.send(json.dumps({
                            "id": i,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_ticker": ticker,
                            },
                        }))

                    last_ping = time.time()
                    while not self._stop:
                        if time.time() - last_ping >= 20:
                            ws.send(json.dumps({"id": 0, "cmd": "ping"}))
                            last_ping = time.time()

                        try:
                            raw = ws.recv(timeout=5)
                        except TimeoutError:
                            continue
                        if not raw:
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        self._handle(msg)

            except Exception as exc:
                if self._stop:
                    return
                print(f"[Kalshi WS] reconnect after error: {exc}")
                self._books.clear()
                self._seq = 0
                time.sleep(3)

    def _handle(self, msg: dict) -> None:
        msg_type = msg.get("type")

        if msg_type == "orderbook_snapshot":
            payload = msg.get("msg", {})
            ticker = payload.get("market_ticker", "")
            if not ticker:
                return
            self._books[ticker] = {
                "yes": {float(p): float(s) for p, s in payload.get("yes_dollars_fp", [])},
                "no":  {float(p): float(s) for p, s in payload.get("no_dollars_fp", [])},
            }
            seq = int(msg.get("seq", 0))
            if seq > self._seq:
                self._seq = seq
            self._emit(ticker)

        elif msg_type == "orderbook_delta":
            payload = msg.get("msg", {})
            ticker = payload.get("market_ticker", "")
            if not ticker or ticker not in self._books:
                return

            seq = int(msg.get("seq", 0))
            if seq <= self._seq:
                return  # дубль или старое сообщение
            if seq != self._seq + 1:
                print(f"[Kalshi WS] seq gap {self._seq + 1}->{seq}, reconnecting")
                raise Exception("seq gap")

            self._seq = seq
            side = payload.get("side", "")
            price = float(payload.get("price_dollars", 0))
            delta = float(payload.get("delta_fp", 0))
            if side not in ("yes", "no") or price <= 0:
                return

            book = self._books[ticker][side]
            new_size = book.get(price, 0.0) + delta
            if new_size <= 1e-6:
                book.pop(price, None)
            else:
                book[price] = new_size

            self._emit(ticker)

    def _emit(self, ticker: str) -> None:
        book = self._books.get(ticker, {})
        # yes/no книги содержат BIDS (покупатели).
        # ask для YES = 1 - лучший NO bid (максимальный)
        # ask для NO  = 1 - лучший YES bid (максимальный)
        yes_bids = [p for p, s in book.get("yes", {}).items() if s > 0]
        no_bids  = [p for p, s in book.get("no",  {}).items() if s > 0]
        best_yes_ask = round(1.0 - max(no_bids),  4) if no_bids  else 0.0
        best_no_ask  = round(1.0 - max(yes_bids), 4) if yes_bids else 0.0
        self.on_update(ticker, KalshiTopOfBook(
            best_yes_ask=best_yes_ask,
            best_no_ask=best_no_ask,
            updated_at=time.time(),
        ))
