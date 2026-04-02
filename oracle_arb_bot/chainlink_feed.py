from __future__ import annotations

import asyncio
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
    {
        "inputs": [],
        "name": "aggregator",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
_ANSWER_UPDATED_TOPIC = "0x" + Web3.keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()

_STALENESS_WARN_SECONDS = 120

# proxy roundId = (phaseId << 64) | aggRoundId; aggregator event has only aggRoundId
_AGG_ROUND_MASK = 0xFFFFFFFFFFFFFFFF


class ChainlinkFeed:
    """
    Читает Chainlink price feed контракты на Polygon.
    Основной путь: WebSocket подписка на AnswerUpdated events (~0.5s lag от on-chain update).
    Fallback: HTTP polling каждые poll_interval_seconds (на случай обрыва WS).
    """

    def __init__(
        self,
        symbols: list[str],                          # ["BTCUSDT", "ETHUSDT", ...]
        on_price: Callable[[str, float, int], None],
        rpc_urls: list[str],
        wss_urls: list[str] | None = None,
        poll_interval_seconds: float = 30.0,
    ) -> None:
        self._on_price = on_price
        self._poll_interval = poll_interval_seconds
        self._prices: dict[str, float] = {}
        self._prev_prices: dict[str, float] = {}
        self._lock = threading.Lock()
        self._stop = False
        self._thread: Optional[threading.Thread] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._wss_urls: list[str] = wss_urls or []

        self._feeds: dict[str, dict] = {}
        for binance_sym in symbols:
            sym = _SYMBOL_MAP.get(binance_sym)
            if sym and sym in _FEED_ADDRESSES:
                self._feeds[sym] = {
                    "address": _FEED_ADDRESSES[sym],
                    "decimals": None,
                    "last_round_id": None,
                }
            else:
                print(f"[chainlink] no feed for {binance_sym}, skipping")

        self._w3_list: list[Web3] = []
        for url in rpc_urls:
            self._w3_list.append(Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5})))
        if not self._w3_list:
            raise ValueError("chainlink: no rpc_urls provided")

        # aggregator_addr_lower → sym (filled at start() if wss_urls provided)
        self._agg_to_sym: dict[str, str] = {}

    def start(self) -> None:
        self._stop = False

        if self._wss_urls:
            self._agg_to_sym = self._fetch_aggregator_map()
            if self._agg_to_sym:
                self._ws_thread = threading.Thread(
                    target=self._start_ws_loop, daemon=True, name="chainlink-ws"
                )
                self._ws_thread.start()
            else:
                print("[chainlink] WARN: no aggregator addresses — WS disabled, poll only")

        self._thread = threading.Thread(target=self._run, daemon=True, name="chainlink-poll")
        self._thread.start()
        mode = "WS+poll-fallback" if self._agg_to_sym else "poll-only"
        print(f"[chainlink] started ({mode}, poll={self._poll_interval}s) symbols={list(self._feeds.keys())}")

    def stop(self) -> None:
        self._stop = True

    def get_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            return self._prices.get(symbol)

    def get_prev_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            return self._prev_prices.get(symbol)

    # ── Aggregator address discovery ────────────────────────────────────────────

    def _fetch_aggregator_map(self) -> dict[str, str]:
        """Returns {aggregator_addr_lower: sym}. Called once at start."""
        result: dict[str, str] = {}
        for sym, feed in self._feeds.items():
            address = Web3.to_checksum_address(feed["address"])
            for w3 in self._w3_list:
                try:
                    contract = w3.eth.contract(address=address, abi=_ABI)
                    agg = contract.functions.aggregator().call()
                    result[agg.lower()] = sym
                    print(f"[chainlink] {sym} aggregator: {agg}")
                    break
                except Exception as exc:
                    print(f"[chainlink] {sym} aggregator fetch error: {exc}")
        return result

    # ── WebSocket AnswerUpdated listener ────────────────────────────────────────

    def _start_ws_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_run())

    async def _ws_run(self) -> None:
        idx = 0
        while not self._stop:
            url = self._wss_urls[idx % len(self._wss_urls)]
            try:
                await self._ws_listen(url)
            except Exception as exc:
                print(f"[chainlink-ws] {url} error: {exc} — reconnecting in 3s", flush=True)
            idx += 1
            await asyncio.sleep(3)

    async def _ws_listen(self, wss_url: str) -> None:
        import json
        import websockets

        agg_addresses = [Web3.to_checksum_address(a) for a in self._agg_to_sym]

        async with websockets.connect(wss_url, ping_interval=20, ping_timeout=30) as ws:
            print(f"[chainlink-ws] connected: {wss_url}", flush=True)
            await ws.send(json.dumps({
                "id": 1, "jsonrpc": "2.0",
                "method": "eth_subscribe",
                "params": ["logs", {
                    "address": agg_addresses,
                    "topics": [_ANSWER_UPDATED_TOPIC],
                }],
            }))

            async for raw in ws:
                if self._stop:
                    return
                msg = json.loads(raw)

                if msg.get("id") == 1:
                    if "error" in msg:
                        print(f"[chainlink-ws] subscription error: {msg['error']}", flush=True)
                    continue

                if msg.get("method") != "eth_subscription":
                    continue

                log = msg["params"]["result"]
                addr = log["address"].lower()
                sym = self._agg_to_sym.get(addr)
                if not sym:
                    continue

                ts = time.time()

                # topics[1] = price (int256 indexed), topics[2] = aggRoundId, data = updatedAt
                price_raw = int(log["topics"][1], 16)
                if price_raw >= 2 ** 255:
                    price_raw -= 2 ** 256

                feed = self._feeds[sym]
                decimals = feed["decimals"] if feed["decimals"] is not None else 8
                price = price_raw / (10 ** decimals)
                agg_round_id = int(log["topics"][2], 16)

                raw_data = log.get("data") or ""
                updated_at = int(raw_data, 16) if raw_data and raw_data != "0x" else int(ts)

                with self._lock:
                    if feed["last_round_id"] == agg_round_id:
                        continue
                    feed["last_round_id"] = agg_round_id
                    self._prev_prices[sym] = self._prices.get(sym, price)
                    self._prices[sym] = price

                ts_ms = updated_at * 1000
                self._on_price(sym, price, ts_ms)

    # ── HTTP polling (fallback) ──────────────────────────────────────────────────

    def _fetch_symbol(self, sym: str, feed: dict, rpc_index: int, now_ts: int) -> None:
        """Fetches one symbol. Skips if WS already reported this round."""
        for i in range(len(self._w3_list)):
            idx = (rpc_index + i) % len(self._w3_list)
            w3 = self._w3_list[idx]
            try:
                address = Web3.to_checksum_address(feed["address"])
                contract = w3.eth.contract(address=address, abi=_ABI)

                if feed["decimals"] is None:
                    feed["decimals"] = contract.functions.decimals().call()

                round_id, answer, _started, updated_at, _answered = (
                    contract.functions.latestRoundData().call()
                )

                # Normalize: proxy roundId = (phaseId << 64) | aggRoundId
                agg_round_id = round_id & _AGG_ROUND_MASK
                price = answer / (10 ** feed["decimals"])

                with self._lock:
                    if feed["last_round_id"] == agg_round_id:
                        return  # WS already fired for this round
                    feed["last_round_id"] = agg_round_id
                    self._prev_prices[sym] = self._prices.get(sym, price)
                    self._prices[sym] = price

                age = now_ts - updated_at
                if age > _STALENESS_WARN_SECONDS:
                    print(f"[chainlink] WARN {sym} stale {age}s (updatedAt={updated_at})")

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
