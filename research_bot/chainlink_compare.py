"""
research_bot/chainlink_compare.py

Параллельный запуск HTTP polling и WebSocket event subscription для Chainlink.
Измеряет кто быстрее детектирует новый раунд и совпадают ли цены.

Запуск: python3 -m research_bot.compare
"""
from __future__ import annotations

import asyncio
import csv
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from web3 import Web3

# ── Контракты ─────────────────────────────────────────────────────────────────

FEEDS: dict[str, str] = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
}
DECIMALS = 8

# address → symbol (для быстрого lookup в WS handler)
ADDR_TO_SYM = {v.lower(): k for k, v in FEEDS.items()}

HTTPS_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
]

WSS_RPCS = [
    "wss://polygon-bor-rpc.publicnode.com",
    "wss://polygon.drpc.org",
    "wss://1rpc.io/matic",
]

POLL_INTERVAL = 2.0

# AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
ANSWER_UPDATED_TOPIC = "0x" + Web3.keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()

AGGREGATOR_ABI = [
    {"inputs":[],"name":"aggregator","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},
]

LATEST_ROUND_ABI = [{
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
        {"name": "roundId",        "type": "uint80"},
        {"name": "answer",         "type": "int256"},
        {"name": "startedAt",      "type": "uint256"},
        {"name": "updatedAt",      "type": "uint256"},
        {"name": "answeredInRound","type": "uint80"},
    ],
    "stateMutability": "view",
    "type": "function",
}]

# ── Shared state ──────────────────────────────────────────────────────────────

class RoundRecord:
    __slots__ = ("symbol", "round_id", "poll_price", "ws_price", "chain_updated_at",
                 "poll_ts", "ws_ts", "logged")

    def __init__(self, symbol: str, round_id: int) -> None:
        self.symbol = symbol
        self.round_id = round_id
        self.poll_price: Optional[float] = None
        self.ws_price: Optional[float] = None
        self.chain_updated_at: Optional[int] = None
        self.poll_ts: Optional[float] = None
        self.ws_ts: Optional[float] = None
        self.logged = False


# round_key = (symbol, round_id)
_records: dict[tuple, RoundRecord] = {}
_records_lock = threading.Lock()

# ── CSV ───────────────────────────────────────────────────────────────────────

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)
CSV_PATH = OUT_DIR / f"compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

_csv_lock = threading.Lock()
_csv_file = open(CSV_PATH, "w", newline="")
_csv_writer = csv.writer(_csv_file)
_csv_writer.writerow([
    "ts_utc", "symbol", "round_id",
    "poll_price", "ws_price", "price_delta",
    "chain_updated_at", "poll_wall_ts", "ws_wall_ts",
    "poll_lag_s", "ws_lag_s", "delta_ms", "faster",
])


def _flush_record(rec: RoundRecord) -> None:
    """Если оба метода получили данные — логируем."""
    if rec.logged:
        return
    if rec.poll_ts is None or rec.ws_ts is None:
        return
    rec.logged = True

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    chain_ts = rec.chain_updated_at or 0

    poll_lag = rec.poll_ts - chain_ts if chain_ts else None
    ws_lag   = rec.ws_ts   - chain_ts if chain_ts else None
    delta_ms = round((rec.ws_ts - rec.poll_ts) * 1000, 1)   # >0 = poll faster, <0 = ws faster
    faster = "poll" if delta_ms > 0 else "ws"

    poll_lag_s = round(poll_lag, 3) if poll_lag is not None else ""
    ws_lag_s   = round(ws_lag,   3) if ws_lag   is not None else ""

    price_delta = round(rec.ws_price - rec.poll_price, 6) if (rec.poll_price and rec.ws_price) else None
    price_delta_str = f"{price_delta:+.6f}" if price_delta is not None else "—"
    display_price = rec.poll_price or rec.ws_price or 0.0

    # stdout
    abs_delta = abs(delta_ms)
    print(
        f"[{ts_utc}] {rec.symbol:3s}  round={rec.round_id}  poll_price={rec.poll_price or '—'}  ws_price={rec.ws_price or '—'}  price_Δ={price_delta_str}\n"
        f"  poll lag: {poll_lag_s:>7}s  |  ws lag: {ws_lag_s:>7}s  "
        f"|  {faster} faster by {abs_delta:.0f}ms",
        flush=True,
    )

    # csv
    with _csv_lock:
        _csv_writer.writerow([
            ts_utc, rec.symbol, rec.round_id,
            rec.poll_price, rec.ws_price, price_delta,
            chain_ts, rec.poll_ts, rec.ws_ts,
            poll_lag_s, ws_lag_s, delta_ms, faster,
        ])
        _csv_file.flush()


def _get_or_create(symbol: str, round_id: int) -> RoundRecord:
    # proxy возвращает (phaseId << 64) | aggRoundId; aggregator эмитит только aggRoundId.
    # Нормализуем к aggRoundId чтобы оба метода использовали одинаковый ключ.
    agg_round_id = round_id & 0xFFFFFFFFFFFFFFFF
    key = (symbol, agg_round_id)
    if key not in _records:
        _records[key] = RoundRecord(symbol, agg_round_id)
    return _records[key]


# ── HTTP polling ──────────────────────────────────────────────────────────────

def _poll_symbol(sym: str, w3_list: list[Web3]) -> None:
    address = Web3.to_checksum_address(FEEDS[sym])
    last_round_id: Optional[int] = None

    while True:
        for w3 in w3_list:
            try:
                contract = w3.eth.contract(address=address, abi=LATEST_ROUND_ABI)
                round_id, answer, _, updated_at, _ = contract.functions.latestRoundData().call()
                break
            except Exception:
                continue
        else:
            time.sleep(POLL_INTERVAL)
            continue

        now = time.time()

        if round_id != last_round_id:
            last_round_id = round_id
            price = answer / (10 ** DECIMALS)

            with _records_lock:
                rec = _get_or_create(sym, round_id)
                rec.poll_price = price
                rec.chain_updated_at = updated_at
                rec.poll_ts = now
                _flush_record(rec)

        time.sleep(POLL_INTERVAL)


# ── WebSocket listener (AnswerUpdated logs) ───────────────────────────────────
# Подписываемся на logs aggregator-контрактов напрямую.
# Цена и roundId приходят в topics события — HTTP запрос не нужен.

def _get_aggregator_map(w3_list: list[Web3]) -> dict[str, str]:
    """Возвращает {aggregator_addr_lower: symbol}. Вызывается один раз при старте."""
    result: dict[str, str] = {}
    for sym, proxy_addr in FEEDS.items():
        address = Web3.to_checksum_address(proxy_addr)
        for w3 in w3_list:
            try:
                contract = w3.eth.contract(address=address, abi=AGGREGATOR_ABI)
                agg = contract.functions.aggregator().call()
                result[agg.lower()] = sym
                print(f"[WS] {sym} aggregator: {agg}", flush=True)
                break
            except Exception:
                continue
    return result


async def _ws_listener(wss_url: str, agg_to_sym: dict[str, str]) -> None:
    import json
    import websockets

    agg_addresses = [Web3.to_checksum_address(a) for a in agg_to_sym]

    while True:
        try:
            async with websockets.connect(wss_url, ping_interval=20, ping_timeout=30) as ws:
                print(f"[WS] подключён: {wss_url}", flush=True)

                await ws.send(json.dumps({
                    "id": 1, "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": agg_addresses,
                        "topics": [ANSWER_UPDATED_TOPIC],
                    }],
                }))

                async for raw in ws:
                    msg = json.loads(raw)

                    if msg.get("id") == 1:
                        if "result" in msg:
                            print(f"[WS] подписка на logs: {msg['result']}", flush=True)
                        else:
                            print(f"[WS] ошибка подписки: {msg.get('error')}", flush=True)
                        continue

                    if msg.get("method") != "eth_subscription":
                        continue

                    log = msg["params"]["result"]
                    addr = log["address"].lower()
                    sym = agg_to_sym.get(addr)
                    if not sym:
                        continue

                    ts = time.time()

                    # topics[1] = price (int256), topics[2] = roundId, data = updatedAt
                    price_raw = int(log["topics"][1], 16)
                    if price_raw >= 2 ** 255:
                        price_raw -= 2 ** 256
                    price = price_raw / (10 ** DECIMALS)
                    round_id = int(log["topics"][2], 16)
                    updated_at = int(log.get("data", "0x0"), 16)

                    with _records_lock:
                        rec = _get_or_create(sym, round_id)
                        rec.ws_price = price
                        if rec.chain_updated_at is None:
                            rec.chain_updated_at = updated_at
                        if rec.ws_ts is None:
                            rec.ws_ts = ts
                        _flush_record(rec)

        except Exception as exc:
            print(f"[WS] ошибка: {exc} — переподключение через 3s", flush=True)
            await asyncio.sleep(3)


async def _run_ws(agg_to_sym: dict[str, str]) -> None:
    idx = 0
    while True:
        url = WSS_RPCS[idx % len(WSS_RPCS)]
        try:
            await _ws_listener(url, agg_to_sym)
        except Exception as exc:
            print(f"[WS] {url} упал: {exc}", flush=True)
        idx += 1
        await asyncio.sleep(2)


def _start_ws_thread(agg_to_sym: dict[str, str]) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run_ws(agg_to_sym))


# ── Stats ─────────────────────────────────────────────────────────────────────

def _print_stats() -> None:
    with _records_lock:
        complete = [r for r in _records.values() if r.logged]

    if not complete:
        print("[stats] нет завершённых раундов пока", flush=True)
        return

    ws_faster   = [r for r in complete if r.ws_ts < r.poll_ts]
    poll_faster  = [r for r in complete if r.poll_ts <= r.ws_ts]

    def avg_delta(recs):
        if not recs:
            return 0.0
        return sum(abs(r.ws_ts - r.poll_ts) * 1000 for r in recs) / len(recs)

    print(f"\n{'='*60}")
    print(f"Всего раундов: {len(complete)}")
    print(f"  WS  быстрее: {len(ws_faster):3d}  ({len(ws_faster)/len(complete)*100:.0f}%)  avg delta: {avg_delta(ws_faster):.0f}ms")
    print(f"  Poll быстрее: {len(poll_faster):3d}  ({len(poll_faster)/len(complete)*100:.0f}%)  avg delta: {avg_delta(poll_faster):.0f}ms")

    # по символам
    print()
    for sym in ["BTC", "ETH", "SOL", "XRP"]:
        sym_recs = [r for r in complete if r.symbol == sym]
        if not sym_recs:
            continue
        ws_f = [r for r in sym_recs if r.ws_ts < r.poll_ts]
        deltas = [(r.poll_ts - r.ws_ts) * 1000 for r in sym_recs]
        avg_d = sum(deltas) / len(deltas)
        print(f"  {sym:3s}: {len(sym_recs):3d} раундов  ws faster: {len(ws_f)}/{len(sym_recs)}  avg Δ: {avg_d:+.0f}ms  (+ = ws faster)")
    print(f"{'='*60}\n", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Chainlink compare — polling vs WebSocket")
    print(f"CSV: {CSV_PATH}\n")

    w3_list = [Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5})) for url in HTTPS_RPCS]

    # Получаем aggregator адреса для подписки на logs
    agg_to_sym = _get_aggregator_map(w3_list)
    if len(agg_to_sym) < len(FEEDS):
        print(f"[WARN] получили только {len(agg_to_sym)}/{len(FEEDS)} aggregator адресов", flush=True)
    print()

    # Polling threads (по одному на символ)
    for sym in FEEDS:
        t = threading.Thread(target=_poll_symbol, args=(sym, w3_list), daemon=True, name=f"poll-{sym}")
        t.start()

    # WebSocket thread — подписка на AnswerUpdated events
    ws_thread = threading.Thread(target=_start_ws_thread, args=(agg_to_sym,), daemon=True, name="ws-listener")
    ws_thread.start()

    # Периодическая сводка
    try:
        while True:
            time.sleep(300)
            _print_stats()
    except KeyboardInterrupt:
        print("\nОстановка...")
        _print_stats()
        _csv_file.close()
        print(f"Данные: {CSV_PATH}")


if __name__ == "__main__":
    main()
