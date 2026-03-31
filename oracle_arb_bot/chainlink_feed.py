from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from web3 import Web3


_SYMBOL_MAP: dict[str, str] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
}

# Chainlink EACAggregatorProxy addresses on Polygon mainnet
_FEED_ADDRESSES: dict[str, str] = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
}

_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

_STALENESS_WARN_SECONDS = 120


class ChainlinkFeed:
    """
    Читает Chainlink price feed контракты на Polygon через polling.
    Интерфейс идентичен BinanceFeed: on_price(symbol, price, ts_ms) + get_price(symbol).
    Вызывает on_price только при новом roundId (не дублирует одну и ту же цену).
    """

    def __init__(
        self,
        symbols: list[str],                          # ["BTCUSDT", "ETHUSDT", ...]
        on_price: Callable[[str, float, int], None],
        rpc_urls: list[str],
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self._on_price = on_price
        self._poll_interval = poll_interval_seconds
        self._prices: dict[str, float] = {}
        self._lock = threading.Lock()
        self._stop = False
        self._thread: Optional[threading.Thread] = None

        # Map from canonical symbol → contract + decimals
        self._feeds: dict[str, dict] = {}
        for binance_sym in symbols:
            sym = _SYMBOL_MAP.get(binance_sym)
            if sym and sym in _FEED_ADDRESSES:
                self._feeds[sym] = {
                    "address": _FEED_ADDRESSES[sym],
                    "decimals": None,       # fetched on first call
                    "last_round_id": None,  # for dedup
                }
            else:
                print(f"[chainlink] no feed for {binance_sym}, skipping")

        # Build Web3 instances with failover
        self._w3_list: list[Web3] = []
        for url in rpc_urls:
            self._w3_list.append(Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5})))
        if not self._w3_list:
            raise ValueError("chainlink: no rpc_urls provided")

    def start(self) -> None:
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True, name="chainlink-poll")
        self._thread.start()
        print(f"[chainlink] polling started for {list(self._feeds.keys())} (interval={self._poll_interval}s)")

    def stop(self) -> None:
        self._stop = True

    def get_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            return self._prices.get(symbol)

    def _get_w3(self) -> Optional[Web3]:
        return self._w3_list[0] if self._w3_list else None

    def _get_w3_fallback(self, failed_url: str) -> Optional[Web3]:
        """Returns the next available w3 after a failure."""
        for w3 in self._w3_list:
            if w3.provider.endpoint_uri != failed_url:
                return w3
        return None

    def _fetch_decimals(self, w3: Web3, address: str) -> int:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(address), abi=_ABI
        )
        return contract.functions.decimals().call()

    def _fetch_symbol(self, sym: str, feed: dict, rpc_index: int, now_ts: int) -> None:
        """Fetches one symbol. Runs concurrently. Tries RPCs in order on failure."""
        for i in range(len(self._w3_list)):
            idx = (rpc_index + i) % len(self._w3_list)
            w3 = self._w3_list[idx]
            try:
                address = Web3.to_checksum_address(feed["address"])
                contract = w3.eth.contract(address=address, abi=_ABI)

                # Lazy-fetch decimals once (thread-safe: worst case fetched twice)
                if feed["decimals"] is None:
                    feed["decimals"] = contract.functions.decimals().call()

                round_id, answer, _started, updated_at, _answered = (
                    contract.functions.latestRoundData().call()
                )

                # Skip if same round (no new data)
                if feed["last_round_id"] == round_id:
                    return
                feed["last_round_id"] = round_id

                price = answer / (10 ** feed["decimals"])

                # Staleness check
                age = now_ts - updated_at
                if age > _STALENESS_WARN_SECONDS:
                    print(f"[chainlink] WARN {sym} stale {age}s (updatedAt={updated_at})")

                with self._lock:
                    self._prices[sym] = price

                ts_ms = updated_at * 1000
                self._on_price(sym, price, ts_ms)
                return

            except Exception as exc:
                if i < len(self._w3_list) - 1:
                    print(f"[chainlink] {sym} RPC[{idx}] error, trying next: {exc}")
                else:
                    print(f"[chainlink] fetch error {sym}: {exc}")

    def _poll_once(self) -> None:
        if not self._w3_list:
            print("[chainlink] no RPC available")
            return

        now_ts = int(time.time())

        # Fetch all symbols concurrently, each with its own RPC failover
        threads = [
            threading.Thread(
                target=self._fetch_symbol,
                args=(sym, feed, 0, now_ts),
                daemon=True,
            )
            for sym, feed in self._feeds.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=8)

    def _run(self) -> None:
        # Initial fetch immediately
        try:
            self._poll_once()
        except Exception as exc:
            print(f"[chainlink] initial poll error: {exc}")

        while not self._stop:
            time.sleep(self._poll_interval)
            if self._stop:
                break
            try:
                self._poll_once()
            except Exception as exc:
                print(f"[chainlink] poll error: {exc}")
