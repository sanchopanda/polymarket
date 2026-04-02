"""
research_bot/fetch_cl_history.py

Загружает исторические раунды Chainlink (eth_getLogs) для BTC/ETH/SOL/XRP
и сохраняет в research_bot/data/cl_history_{symbol}.csv.

Запуск:
  python3 -m research_bot.fetch_cl_history --days 30
  python3 -m research_bot.fetch_cl_history --days 30 --force   # перезаписать
"""
from __future__ import annotations

import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from web3 import Web3

DATA_DIR = Path("research_bot/data")

# Proxy-адреса на Polygon mainnet (из chainlink_monitor.py)
FEEDS: dict[str, dict] = {
    "BTC": {"address": "0xc907E116054Ad103354f2D350FD2514433D57F6f", "decimals": 8},
    "ETH": {"address": "0xF9680D99D6C9589e2a93a78A04A279e509205945", "decimals": 8},
    "SOL": {"address": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC", "decimals": 8},
    "XRP": {"address": "0x785ba89291f676b5386652eB12b30cF361020694", "decimals": 8},
}

RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]

# event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
ANSWER_UPDATED_TOPIC = "0x" + Web3.keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()

AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "aggregator",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

POLYGON_BLOCKS_PER_DAY = 43_200  # ~2s per block
BATCH_SIZE = 500                 # blocks per eth_getLogs call (1rpc.io limit)
REQUEST_DELAY = 0.2              # seconds between batches


def _build_w3_list() -> list[Web3]:
    from web3.middleware import ExtraDataToPOAMiddleware
    result = []
    for url in RPC_URLS:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5}))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        result.append(w3)
    return result


def _call(w3_list: list[Web3], fn, *args):
    """Try each RPC until one succeeds."""
    last_exc = None
    for w3 in w3_list:
        try:
            return fn(w3, *args)
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"All RPCs failed: {last_exc}")


def _get_latest_block(w3: Web3) -> dict:
    block = w3.eth.get_block("latest")
    return {"number": block["number"], "timestamp": block["timestamp"]}


def _get_aggregator(w3: Web3, proxy_addr: str) -> str:
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(proxy_addr),
        abi=AGGREGATOR_ABI,
    )
    return contract.functions.aggregator().call()


def _get_logs(w3: Web3, address: str, from_block: int, to_block: int) -> list:
    return w3.eth.get_logs({
        "address": Web3.to_checksum_address(address),
        "topics": [ANSWER_UPDATED_TOPIC],
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
    })


def _decode_answer_updated(log: dict, decimals: int) -> Optional[tuple]:
    """Returns (ts_unix, round_id, price) or None."""
    try:
        topics = log["topics"]
        # topics[1] = current (int256, signed)
        raw_price = int(topics[1].hex(), 16)
        if raw_price >= 2**255:
            raw_price -= 2**256
        price = raw_price / (10 ** decimals)

        # topics[2] = roundId (uint256)
        round_id = int(topics[2].hex(), 16)

        # data = updatedAt (uint256, 32 bytes)
        data = log["data"]
        if isinstance(data, bytes):
            updated_at = int(data.hex(), 16)
        else:
            updated_at = int(data, 16)

        return updated_at, round_id, price
    except Exception:
        return None


def fetch_symbol(symbol: str, from_block: int, to_block: int,
                 w3_list: list[Web3], decimals: int, out_path: Path) -> int:
    proxy_addr = FEEDS[symbol]["address"]

    print(f"  [{symbol}] Получаем aggregator address...", flush=True)
    aggregator_addr = _call(w3_list, _get_aggregator, proxy_addr)
    print(f"  [{symbol}] Aggregator: {aggregator_addr}", flush=True)

    total_blocks = to_block - from_block
    total_batches = (total_blocks + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  [{symbol}] Блоки {from_block}–{to_block} ({total_blocks:,} блоков, {total_batches} батчей)", flush=True)

    rows: list[tuple] = []
    batch_num = 0

    cur = from_block
    while cur <= to_block:
        end = min(cur + BATCH_SIZE - 1, to_block)

        # retry up to 3 times on failure
        for attempt in range(3):
            try:
                logs = _call(w3_list, _get_logs, aggregator_addr, cur, end)
                for log in logs:
                    decoded = _decode_answer_updated(log, decimals)
                    if decoded:
                        rows.append(decoded)
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(1.0)
                else:
                    print(f"  [{symbol}] ПРОПУЩЕН батч {cur}-{end}: {exc}", flush=True)

        batch_num += 1
        pct = (cur - from_block) / max(total_blocks, 1) * 100
        print(f"  [{symbol}] {pct:.0f}% | батч {batch_num}/{total_batches} | раундов: {len(rows)}", flush=True)

        cur = end + 1
        time.sleep(REQUEST_DELAY)

    if not rows:
        print(f"  [{symbol}] Нет данных!")
        return 0

    # Sort by timestamp
    rows.sort(key=lambda r: r[0])

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts_utc", "symbol", "round_id", "price",
                         "price_change", "price_change_pct"])
        prev_price: Optional[float] = None
        for ts_unix, round_id, price in rows:
            ts_str = datetime.fromtimestamp(ts_unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            price_change = round(price - prev_price, 8) if prev_price is not None else 0.0
            price_change_pct = round(price_change / prev_price * 100, 6) if prev_price else 0.0
            writer.writerow([ts_str, symbol, round_id, price, price_change, price_change_pct])
            prev_price = price

    print(f"  [{symbol}] Сохранено {len(rows)} раундов → {out_path}")
    return len(rows)


MARKETS_CACHE = DATA_DIR / "markets_cache.csv"
BUFFER_MINUTES = 30  # extra buffer before earliest market_start for prev_price context


def _range_from_markets(path: Path) -> tuple[datetime, datetime]:
    """Read markets_cache.csv, return (earliest_start, latest_end)."""
    starts = []
    ends = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                starts.append(datetime.strptime(row["market_start"], "%Y-%m-%d %H:%M:%S")
                               .replace(tzinfo=timezone.utc))
                ends.append(datetime.strptime(row["market_end"], "%Y-%m-%d %H:%M:%S")
                             .replace(tzinfo=timezone.utc))
            except Exception:
                continue
    if not starts:
        raise ValueError(f"Нет данных в {path}")
    from datetime import timedelta
    return min(starts) - timedelta(minutes=BUFFER_MINUTES), max(ends)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None,
                        help="явно задать кол-во дней вместо авто-определения из markets_cache.csv")
    parser.add_argument("--force", action="store_true", help="перезаписать существующие файлы")
    parser.add_argument("--symbols", nargs="+", default=list(FEEDS.keys()),
                        choices=list(FEEDS.keys()), help="символы для загрузки")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    w3_list = _build_w3_list()

    print("Получаем текущий блок...")
    latest = _call(w3_list, _get_latest_block)
    latest_block = latest["number"]
    latest_ts = latest["timestamp"]
    now_dt = datetime.fromtimestamp(latest_ts, tz=timezone.utc)
    print(f"Текущий блок: {latest_block} ({now_dt.strftime('%Y-%m-%d %H:%M')})")

    if args.days is not None:
        from_ts = latest_ts - args.days * 86400
        from_block = latest_block - args.days * POLYGON_BLOCKS_PER_DAY
        print(f"Загрузка CL истории ({args.days} дней) для: {args.symbols}")
    elif MARKETS_CACHE.exists():
        range_start, range_end = _range_from_markets(MARKETS_CACHE)
        days_needed = (now_dt - range_start).total_seconds() / 86400
        from_ts = int(range_start.timestamp())
        from_block = latest_block - int(days_needed * POLYGON_BLOCKS_PER_DAY) - 500  # +buffer
        print(f"Авто-диапазон из {MARKETS_CACHE.name}:")
        print(f"  от {range_start.strftime('%Y-%m-%d %H:%M')} до {range_end.strftime('%Y-%m-%d %H:%M')}")
        print(f"  ~{days_needed:.1f} дней | символы: {args.symbols}")
    else:
        print(f"markets_cache.csv не найден, используем --days 7 по умолчанию")
        from_block = latest_block - 7 * POLYGON_BLOCKS_PER_DAY

    from_block = max(0, from_block)

    for symbol in args.symbols:
        out_path = DATA_DIR / f"cl_history_{symbol}.csv"
        if out_path.exists() and not args.force:
            size = out_path.stat().st_size
            print(f"[{symbol}] Файл уже существует ({size:,} байт), пропускаем (--force для перезаписи)")
            continue

        print(f"\n[{symbol}] Загрузка...")
        fetch_symbol(
            symbol=symbol,
            from_block=from_block,
            to_block=latest_block,
            w3_list=w3_list,
            decimals=FEEDS[symbol]["decimals"],
            out_path=out_path,
        )

    print("\nГотово!")


if __name__ == "__main__":
    main()
