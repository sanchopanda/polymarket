from __future__ import annotations

import json
import threading
import time
from typing import Callable, Optional

from websockets.sync.client import connect


_SYMBOL_MAP: dict[str, str] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
}


class BinanceFeed:
    """
    Подписывается на Binance aggTrade stream для нескольких символов.
    Вызывает on_price(symbol, price, ts_ms) из WS-потока при каждом тике.
    get_price(symbol) — thread-safe доступ к последней известной цене.
    """

    WS_BASE = "wss://stream.binance.com:9443/stream"

    def __init__(
        self,
        symbols: list[str],             # ["BTCUSDT", "ETHUSDT", ...]
        on_price: Callable[[str, float, int], None],
    ) -> None:
        self._binance_symbols = symbols
        self._on_price = on_price
        self._prices: dict[str, float] = {}
        self._lock = threading.Lock()
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True, name="binance-ws")
        self._thread.start()
        print(f"[binance] WS starting for {self._binance_symbols}")

    def stop(self) -> None:
        self._stop = True

    def get_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            return self._prices.get(symbol)

    def _run(self) -> None:
        streams = "/".join(s.lower() + "@aggTrade" for s in self._binance_symbols)
        url = f"{self.WS_BASE}?streams={streams}"

        while not self._stop:
            try:
                with connect(url, open_timeout=10, close_timeout=3, ping_interval=20) as ws:
                    print("[binance] WS connected")
                    while not self._stop:
                        try:
                            raw = ws.recv(timeout=5)
                        except TimeoutError:
                            continue
                        if raw is None:
                            continue
                        self._handle(raw)
            except Exception as exc:
                if self._stop:
                    return
                print(f"[binance] WS error, reconnecting: {exc}")
                time.sleep(2)

    def _handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return

        stream = msg.get("stream") or ""
        data = msg.get("data") or {}

        # "btcusdt@aggTrade" → "BTCUSDT"
        binance_sym = stream.split("@")[0].upper() if stream else ""
        symbol = _SYMBOL_MAP.get(binance_sym)
        if not symbol:
            return

        try:
            price = float(data.get("p", 0))
            ts_ms = int(data.get("T", 0))
        except (TypeError, ValueError):
            return

        if price <= 0:
            return

        with self._lock:
            self._prices[symbol] = price

        self._on_price(symbol, price, ts_ms)
