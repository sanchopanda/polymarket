"""
research_bot/chainlink_monitor.py

Мониторинг Chainlink price feeds: как часто меняется roundId, размер движений.
Запуск: python3 -m research_bot.chainlink_monitor
"""
from __future__ import annotations

import csv
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from web3 import Web3

# ── Контракты на Polygon mainnet ──────────────────────────────────────────────

FEEDS: dict[str, dict] = {
    "BTC": {"address": "0xc907E116054Ad103354f2D350FD2514433D57F6f", "decimals": 8},
    "ETH": {"address": "0xF9680D99D6C9589e2a93a78A04A279e509205945", "decimals": 8},
    "SOL": {"address": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC", "decimals": 8},
    "XRP": {"address": "0x785ba89291f676b5386652eB12b30cF361020694", "decimals": 8},
}

RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
]

ABI = [
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
    }
]

POLL_INTERVAL = 2.0   # секунд между опросами

# ── CSV output ────────────────────────────────────────────────────────────────

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)
CSV_PATH = OUT_DIR / f"chainlink_rounds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

_csv_lock = threading.Lock()
_csv_file = open(CSV_PATH, "w", newline="")
_csv_writer = csv.writer(_csv_file)
_csv_writer.writerow([
    "ts_utc", "symbol", "round_id", "price",
    "price_change", "price_change_pct",
    "seconds_since_last", "updated_at_utc",
])


def _write_row(row: list) -> None:
    with _csv_lock:
        _csv_writer.writerow(row)
        _csv_file.flush()


# ── Per-symbol state ──────────────────────────────────────────────────────────

class SymbolState:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.last_round_id: Optional[int] = None
        self.last_price: Optional[float] = None
        self.last_updated_at: Optional[int] = None   # unix seconds (on-chain)
        self.last_seen_ts: Optional[float] = None    # wall clock

        # Stats
        self.round_count: int = 0
        self.intervals: list[float] = []   # seconds between rounds
        self.changes: list[float] = []     # |price change| per round


# ── Polling ───────────────────────────────────────────────────────────────────

def _build_w3_list() -> list[Web3]:
    return [Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5})) for url in RPC_URLS]


def _fetch_once(sym: str, state: SymbolState, w3_list: list[Web3]) -> None:
    address = Web3.to_checksum_address(FEEDS[sym]["address"])
    decimals = FEEDS[sym]["decimals"]

    for w3 in w3_list:
        try:
            contract = w3.eth.contract(address=address, abi=ABI)
            round_id, answer, _started, updated_at, _ = contract.functions.latestRoundData().call()
            break
        except Exception as exc:
            continue
    else:
        print(f"[{sym}] все RPC недоступны", flush=True)
        return

    if state.last_round_id == round_id:
        return   # раунд не изменился

    now_wall = time.time()
    price = answer / (10 ** decimals)
    price_change = (price - state.last_price) if state.last_price is not None else 0.0
    price_change_pct = (price_change / state.last_price * 100) if state.last_price else 0.0

    # Интервал между раундами (по on-chain updatedAt)
    seconds_since = None
    if state.last_updated_at is not None:
        seconds_since = updated_at - state.last_updated_at
        state.intervals.append(seconds_since)

    if state.last_price is not None:
        state.changes.append(abs(price_change))

    state.round_count += 1
    state.last_round_id = round_id
    state.last_price = price
    state.last_updated_at = updated_at
    state.last_seen_ts = now_wall

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    updated_str = datetime.fromtimestamp(updated_at, tz=timezone.utc).strftime("%H:%M:%S")

    sec_str = f"{seconds_since:.0f}s" if seconds_since is not None else "—"

    print(
        f"[{ts_utc}] {sym:3s}  round={round_id}  "
        f"price={price:>10.4f}  Δ={price_change:+.4f} ({price_change_pct:+.4f}%)  "
        f"interval={sec_str}  chain_ts={updated_str}",
        flush=True,
    )

    _write_row([
        ts_utc, sym, round_id, price,
        round(price_change, 6), round(price_change_pct, 6),
        seconds_since, updated_str,
    ])


def _poll_symbol(sym: str, state: SymbolState, w3_list: list[Web3]) -> None:
    while True:
        try:
            _fetch_once(sym, state, w3_list)
        except Exception as exc:
            print(f"[{sym}] ошибка: {exc}", flush=True)
        time.sleep(POLL_INTERVAL)


def _print_stats(states: dict[str, SymbolState]) -> None:
    print("\n" + "=" * 70)
    print(f"{'SYMBOL':<6} {'ROUNDS':>7} {'AVG_INTERVAL':>13} {'MIN_INT':>8} {'MAX_INT':>8} {'AVG_|Δ|':>10} {'MAX_|Δ|':>10}")
    print("-" * 70)
    for sym, st in states.items():
        if not st.intervals:
            print(f"{sym:<6} {'—':>7}")
            continue
        avg_iv = sum(st.intervals) / len(st.intervals)
        min_iv = min(st.intervals)
        max_iv = max(st.intervals)
        avg_chg = sum(st.changes) / len(st.changes) if st.changes else 0
        max_chg = max(st.changes) if st.changes else 0
        print(
            f"{sym:<6} {st.round_count:>7}  "
            f"{avg_iv:>11.1f}s  {min_iv:>7.0f}s  {max_iv:>7.0f}s  "
            f"{avg_chg:>10.4f}  {max_chg:>10.4f}"
        )
    print("=" * 70 + "\n", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Chainlink monitor — запись в {CSV_PATH}")
    print(f"Символы: {list(FEEDS.keys())}  |  Polling: {POLL_INTERVAL}s\n")

    w3_list = _build_w3_list()
    states = {sym: SymbolState(sym) for sym in FEEDS}

    # Запуск потоков
    for sym, state in states.items():
        t = threading.Thread(target=_poll_symbol, args=(sym, state, w3_list), daemon=True, name=f"monitor-{sym}")
        t.start()

    # Периодическая сводка каждые 5 минут
    try:
        while True:
            time.sleep(300)
            _print_stats(states)
    except KeyboardInterrupt:
        print("\nОстановка...")
        _print_stats(states)
        _csv_file.close()
        print(f"Данные сохранены: {CSV_PATH}")


if __name__ == "__main__":
    main()
