"""
research_bot/market_prices.py

Для каждого завершённого рынка из БД:
- Находит Chainlink тики вокруг момента открытия и закрытия рынка
- Сравнивает pm_open_price / pm_close_price с Chainlink ценой в тот момент
- Показывает: предсказывал ли Chainlink результат рынка?

Запуск: python3 -m research_bot prices
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

from web3 import Web3

HTTPS_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]

FEEDS: dict[str, str] = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
}
DECIMALS = 8

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
        "inputs": [{"name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
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
]


def _make_w3_list() -> list[Web3]:
    return [Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 8})) for url in HTTPS_RPCS]


def _get_round(contract, phase_id: int, agg_round_id: int) -> Optional[tuple[int, float, int]]:
    """Возвращает (agg_round_id, price, updatedAt) или None."""
    full_id = (phase_id << 64) | agg_round_id
    try:
        _, answer, _, updated_at, _ = contract.functions.getRoundData(full_id).call()
        if updated_at == 0:
            return None
        price = answer / (10 ** DECIMALS)
        return (agg_round_id, price, updated_at)
    except Exception:
        return None


def find_rounds_near_ts(w3_list: list[Web3], sym: str, target_ts: int) -> list[tuple[int, float, int]]:
    """
    Находит Chainlink тики вокруг target_ts (unix).
    Возвращает список (agg_round_id, price, updatedAt) — тик до, во время и после.
    """
    proxy_addr = Web3.to_checksum_address(FEEDS[sym])
    contract = None
    for w3 in w3_list:
        try:
            contract = w3.eth.contract(address=proxy_addr, abi=_ABI)
            cur_round_id, _, _, cur_updated_at, _ = contract.functions.latestRoundData().call()
            break
        except Exception:
            contract = None
    if contract is None:
        return []

    cur_ts = int(time.time())
    phase_id = cur_round_id >> 64
    cur_agg_round = cur_round_id & 0xFFFFFFFFFFFFFFFF

    # Оцениваем стартовую точку (avg ~30s per round)
    seconds_back = max(0, cur_ts - target_ts)
    est_back = int(seconds_back / 30)
    lo = max(1, cur_agg_round - est_back - 100)
    hi = cur_agg_round

    # Бинарный поиск: ищем последний round где updatedAt <= target_ts
    while lo < hi:
        mid = (lo + hi + 1) // 2
        r = _get_round(contract, phase_id, mid)
        if r is None:
            lo = mid + 1
            continue
        if r[2] <= target_ts:
            lo = mid
        else:
            hi = mid - 1

    # Возвращаем тик-1, тик, тик+1
    result = []
    for offset in (-1, 0, 1):
        r = _get_round(contract, phase_id, lo + offset)
        if r:
            result.append(r)
    return result


def _parse_ts(s: str) -> int:
    """ISO datetime → unix timestamp (UTC)."""
    s = s.replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1]
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def main() -> None:
    db_path = "data/oracle_arb_bot.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Уникальные рынки с открытием, закрытием, pm_open_price
    markets = conn.execute("""
        SELECT DISTINCT
            market_id, symbol, interval_minutes,
            market_start, market_end,
            pm_open_price, pm_close_price,
            winning_side
        FROM bets
        WHERE status = 'resolved'
          AND pm_open_price IS NOT NULL
        ORDER BY market_start
    """).fetchall()

    print(f"Анализ {len(markets)} рынков\n")
    print(f"{'sym':>4} {'int':>4} {'market_open':>11}  {'pm_open':>8}  "
          f"{'cl_before':>10} {'cl_at_open':>10} {'cl_after':>10}  "
          f"{'pm_close':>8}  {'cl_before':>10} {'cl_at_close':>11} {'cl_after':>10}  "
          f"{'cl_Δ%':>7}  {'winner':>6}  {'cl_pred?':>8}")
    print("─" * 165)

    w3_list = _make_w3_list()
    correct = 0
    total = 0

    for m in markets:
        sym = m["symbol"]
        interval = m["interval_minutes"]
        open_ts  = _parse_ts(m["market_start"])
        close_ts = _parse_ts(m["market_end"])
        pm_open  = m["pm_open_price"]
        pm_close = m["pm_close_price"]
        winner   = m["winning_side"] or "?"

        # Chainlink вокруг открытия
        open_rounds  = find_rounds_near_ts(w3_list, sym, open_ts)
        close_rounds = find_rounds_near_ts(w3_list, sym, close_ts)

        def fmt_round(rounds, idx):
            if idx < len(rounds):
                r = rounds[idx]
                age = r[2] - (open_ts if idx <= 1 else close_ts)
                return f"{r[1]:>10.4f}"
            return f"{'—':>10}"

        cl_open_price   = open_rounds[1][1]   if len(open_rounds)  > 1 else None
        cl_close_price  = close_rounds[1][1]  if len(close_rounds) > 1 else None

        cl_before_open  = open_rounds[0][1]   if len(open_rounds)  > 0 else None
        cl_after_open   = open_rounds[2][1]   if len(open_rounds)  > 2 else None
        cl_before_close = close_rounds[0][1]  if len(close_rounds) > 0 else None
        cl_after_close  = close_rounds[2][1]  if len(close_rounds) > 2 else None

        # Направление движения Chainlink за время рынка
        cl_move = None
        cl_pred = "—"
        if cl_open_price and cl_close_price:
            cl_move = (cl_close_price - cl_open_price) / cl_open_price * 100
            cl_direction = "yes" if cl_move > 0 else "no"
            cl_pred = "✓" if cl_direction == winner else "✗"
            if winner not in ("yes", "no"):
                cl_pred = "?"
            else:
                total += 1
                if cl_direction == winner:
                    correct += 1

        open_str  = m["market_start"][5:16].replace("T", " ")
        cl_move_s = f"{cl_move:+.4f}%" if cl_move is not None else "      —"
        cl_before_s  = f"{cl_before_open:>10.4f}" if cl_before_open else f"{'—':>10}"
        cl_open_s    = f"{cl_open_price:>10.4f}"  if cl_open_price  else f"{'—':>10}"
        cl_after_s   = f"{cl_after_open:>10.4f}"  if cl_after_open  else f"{'—':>10}"
        cl_close_s        = f"{cl_close_price:>11.4f}"  if cl_close_price  else f"{'—':>11}"
        cl_before_close_s = f"{cl_before_close:>10.4f}" if cl_before_close else f"{'—':>10}"
        cl_after_close_s  = f"{cl_after_close:>10.4f}"  if cl_after_close  else f"{'—':>10}"
        pm_open_s         = f"{pm_open:>8.3f}"          if pm_open         else f"{'—':>8}"
        pm_close_s        = f"{pm_close:>8.3f}"         if pm_close        else f"{'—':>8}"

        print(f"{sym:>4} {interval:>4}m {open_str:>11}  {pm_open_s}  "
              f"{cl_before_s} {cl_open_s} {cl_after_s}  "
              f"{pm_close_s}  {cl_before_close_s} {cl_close_s} {cl_after_close_s}  "
              f"{cl_move_s:>7}  {winner:>6}  {cl_pred:>8}")

    print("─" * 140)
    if total > 0:
        print(f"\nChainlink верно предсказал направление: {correct}/{total} = {correct/total*100:.0f}%")
