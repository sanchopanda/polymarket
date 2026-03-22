from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Callable

from websockets.sync.client import connect


@dataclass
class TopOfBook:
    best_bid: float = 0.0
    best_ask: float = 0.0
    updated_at_ms: int = 0


class MarketWebSocketClient:
    def __init__(
        self,
        url: str,
        asset_ids: list[str],
        on_message: Callable[[dict], None],
    ) -> None:
        self.url = url
        self.asset_ids = asset_ids
        self.on_message = on_message
        self._stop = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop:
            try:
                with connect(self.url, open_timeout=10, close_timeout=3, ping_interval=None) as ws:
                    ws.send(
                        json.dumps(
                            {
                                "assets_ids": self.asset_ids,
                                "type": "market",
                                "custom_feature_enabled": True,
                            }
                        )
                    )
                    last_ping = time.time()
                    while not self._stop:
                        if time.time() - last_ping >= 10:
                            ws.send("PING")
                            last_ping = time.time()

                        try:
                            raw = ws.recv(timeout=5)
                        except TimeoutError:
                            continue
                        if raw in ("PONG", "PING", None):
                            continue
                        payload = json.loads(raw)
                        self.on_message(payload)
            except Exception as exc:
                if self._stop:
                    return
                print(f"[WS] reconnect after error: {exc}")
                time.sleep(2)
