"""
research_bot/pm_correlate.py

Реал-тайм: логирует Chainlink тики и PM book updates параллельно.
Сэмплирует PM цену каждые 3s в течение 30s после CL тика (или до следующего тика).

Запуск: python3 -m research_bot correlate
"""
from __future__ import annotations

import asyncio
import csv
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from web3 import Web3

from cross_arb_bot.polymarket_feed import PolymarketFeed

# ── Конфиг ────────────────────────────────────────────────────────────────────

HTTPS_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]
WSS_RPCS = [
    "wss://polygon-bor-rpc.publicnode.com",
    "wss://polygon.drpc.org",
]

PM_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_GAMMA_URL = "https://gamma-api.polymarket.com"
PM_BOUNDARY_INTERVAL = 300   # рынки открываются каждые 5 минут
PM_BOUNDARY_OFFSET_S = 5     # просыпаемся через 5s после границы

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
BINANCE_SYMBOLS: dict[str, str] = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}

FEEDS: dict[str, str] = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP": "0x785ba89291f676b5386652eB12b30cF361020694",
}
DECIMALS = 8
ANSWER_UPDATED_TOPIC = "0x" + Web3.keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()
AGG_ABI = [{"inputs":[],"name":"aggregator","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]

# Временная сетка сэмплов в секундах после CL тика
SAMPLE_TIMES_S = [1, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
MAX_SAMPLES = len(SAMPLE_TIMES_S)

_APPEND_PATH = Path("research_bot/data/correlate_all.csv")
CSV_PATH = _APPEND_PATH  # единый файл, append-режим

# ── Состояние ──────────────────────────────────────────────────────────────────

_lock = threading.Lock()

# Последний Chainlink тик по символу: {sym: (price, ts)}
_cl_latest: dict[str, tuple[float, float]] = {}

# Последняя Binance цена по символу: {sym: (price, ts)}
_binance_latest: dict[str, tuple[float, float]] = {}

# Цена YES per-market: {asset_id: (best_ask, best_bid, ts)}
_pm_prices: dict[str, tuple[float, float, float]] = {}

# Мета-данные рынков: {asset_id: (sym, market_start_ts, market_end_ts)}
_market_info: dict[str, tuple[str, float, float]] = {}


@dataclass
class CorrelEvent:
    sym: str
    asset_id: str                              # конкретный YES-токен рынка
    cl_price: float
    cl_ts: float
    cl_prev_price: Optional[float] = None       # предыдущий CL тик (для направления)
    binance_price_at_cl: Optional[float] = None  # Binance цена в момент CL тика
    binance_ts_at_cl: Optional[float] = None
    minutes_remaining: Optional[float] = None  # минут до закрытия рынка в момент тика
    pm_price_before: Optional[float] = None    # PM ask в момент CL тика (цена входа)
    pm_bid_before: Optional[float] = None      # PM bid в момент CL тика (цена выхода)
    pm_first_after: Optional[float] = None     # первое значимое изменение ask (≥0.001)
    pm_first_ts: Optional[float] = None
    # Сэмплы PM цены: список (ts, pm_price) через 3, 6, 9, ... секунд после тика
    samples: list = field(default_factory=list)
    # Когда пришёл следующий CL тик (обрезает окно раньше 30s)
    next_cl_ts: Optional[float] = None
    logged: bool = False


_pending: list[CorrelEvent] = []  # все незавершённые события
_all_events: list[CorrelEvent] = []

_CSV_HEADER = [
    "ts_utc", "symbol",
    "cl_price", "cl_prev_price", "cl_direction", "cl_delta_pct",
    "window_s",         # реальная длина окна (≤30s)
    "minutes_remaining", # минут до закрытия рынка
    "pm_ask_before",    # ask при входе (цена покупки)
    "pm_bid_before",    # bid при входе (цена продажи если уже держим)
    "spread_before",    # ask - bid в центах
    "first_lag_ms",     # лаг до первого PM изменения
    "pm_first_after",   # цена первой реакции по WS (≥0.001 изменение)
    "pm_entry_t1",      # цена через 1s — реальная цена входа с учётом задержки исполнения
    *[f"pm_t{t}" for t in SAMPLE_TIMES_S],
    "pm_end",           # PM цена в конце окна
    "pm_delta_cents",   # изменение pm_end vs pm_before
    "pm_max_fav_cents", # макс. движение в сторону CL за окно (потенциальный profit)
    "pm_direction",     # up/down/= относительно pm_before
    "direction_match",  # Y/N/—
    "binance_price",    # Binance цена в момент CL тика
    "binance_cl_delta_pct",  # (binance - cl) / cl * 100
]

_need_header = not _APPEND_PATH.exists() or _APPEND_PATH.stat().st_size == 0
_csv_file = open(CSV_PATH, "a", newline="")
_csv_writer = csv.writer(_csv_file)
if _need_header:
    _csv_writer.writerow(_CSV_HEADER)

# asset_ids для WS подписки (обновляется периодически)
_asset_id_to_sym: dict[str, str] = {}
_pm_asset_ids: set[str] = set()
_pm_ws_restart = threading.Event()


# ── PM рынки: получить активные ───────────────────────────────────────────────

_pm_feed = PolymarketFeed(
    base_url=PM_GAMMA_URL,
    page_size=500,
    request_delay_ms=100,
    market_filter={
        "min_days_to_expiry": 0,
        "max_days_to_expiry": 0.5,
        "min_volume": 0,
        "min_liquidity": 0,
        "fee_type": "crypto_fees",
        "symbol": "",
    },
)


def _fetch_pm_markets() -> dict[str, list[str]]:
    """Возвращает {sym: [yes_token_id, ...]} для всех активных 5m/15m рынков."""
    sym_to_assets: dict[str, list[str]] = {s: [] for s in FEEDS}
    try:
        markets = _pm_feed.fetch_markets()
    except Exception as exc:
        print(f"[pm] fetch markets error: {exc}")
        return sym_to_assets

    from datetime import timedelta
    matched = 0
    for m in markets:
        if m.symbol not in FEEDS:
            continue
        if m.interval_minutes not in (5, 15):
            continue
        if not m.yes_token_id or not m.expiry:
            continue
        sym = m.symbol
        market_end = m.expiry.replace(tzinfo=timezone.utc).timestamp()
        market_start = (m.expiry - timedelta(minutes=m.interval_minutes)).replace(tzinfo=timezone.utc).timestamp()
        if m.yes_token_id not in sym_to_assets[sym]:
            sym_to_assets[sym].append(m.yes_token_id)
            _asset_id_to_sym[m.yes_token_id] = sym
            _market_info[m.yes_token_id] = (sym, market_start, market_end)
            matched += 1

    print(f"[pm] fetched {sum(len(v) for v in sym_to_assets.values())} assets for {matched} markets")
    return sym_to_assets


def _next_boundary_ts() -> float:
    """Время следующей 5-минутной границы + PM_BOUNDARY_OFFSET_S."""
    now = time.time()
    return (now // PM_BOUNDARY_INTERVAL + 1) * PM_BOUNDARY_INTERVAL + PM_BOUNDARY_OFFSET_S


def _market_refresh_loop() -> None:
    """Обновляет список PM рынков на каждой 5-минутной границе (+5s буфер).
    WS перезапускается только если появились новые asset_id."""
    global _pm_asset_ids
    while True:
        wait = _next_boundary_ts() - time.time()
        if wait > 0:
            time.sleep(wait)

        next_dt = datetime.fromtimestamp(_next_boundary_ts() - PM_BOUNDARY_OFFSET_S, tz=timezone.utc)
        print(f"[pm] обновление рынков (граница {next_dt.strftime('%H:%M')})", flush=True)

        sym_to_assets = _fetch_pm_markets()
        new_ids: set[str] = set()
        for ids in sym_to_assets.values():
            new_ids.update(ids)
        with _lock:
            added = new_ids - _pm_asset_ids
            removed = _pm_asset_ids - new_ids
            _pm_asset_ids = new_ids
            if added:
                print(f"[pm] +{len(added)} новых asset_ids → перезапуск WS", flush=True)
                _pm_ws_restart.set()
            elif removed:
                print(f"[pm] -{len(removed)} закрытых рынков (WS не перезапускаем)", flush=True)


# ── PM WebSocket ───────────────────────────────────────────────────────────────

def _on_pm_message(payload: dict) -> None:
    if isinstance(payload, list):
        for item in payload:
            _on_pm_message(item)
        return
    changes = payload.get("changes")
    if changes:
        for c in changes:
            _process_pm_change(c)
    else:
        _process_pm_change(payload)


def _get_active_assets(sym: str, now_ts: float) -> list[str]:
    """Возвращает все asset_id рынков которые сейчас в своём торговом окне."""
    result = []
    for asset_id, (s, start, end) in _market_info.items():
        if s == sym and start <= now_ts <= end:
            result.append(asset_id)
    return result


def _process_pm_change(item: dict) -> None:
    asset_id = item.get("asset_id") or item.get("token_id") or ""
    if not _asset_id_to_sym.get(asset_id):
        return

    best_ask = None
    best_bid = None
    event_type = item.get("event_type") or ""
    if event_type == "best_bid_ask":
        try:
            best_ask = float(item.get("best_ask") or 0)
            best_bid = float(item.get("best_bid") or 0)
        except (TypeError, ValueError):
            pass
    elif event_type == "book":
        asks = item.get("asks") or []
        bids = item.get("bids") or []
        if asks:
            try:
                best_ask = float(asks[0]["price"])
            except Exception:
                pass
        if bids:
            try:
                best_bid = float(bids[-1]["price"])  # bids ascending → last = best bid
            except Exception:
                pass

    if not best_ask:
        return

    ts = time.time()
    with _lock:
        prev = _pm_prices.get(asset_id)
        _pm_prices[asset_id] = (best_ask, best_bid or 0.0, ts)

        # Обновляем pm_first_after только для самого свежего незакрытого события
        if prev and abs(best_ask - prev[0]) >= 0.001:  # prev[0] = предыдущий ask
            newest_ev: Optional[CorrelEvent] = None
            for ev in _pending:
                if ev.asset_id == asset_id and not ev.logged and ts > ev.cl_ts:
                    if newest_ev is None or ev.cl_ts > newest_ev.cl_ts:
                        newest_ev = ev
            if newest_ev and newest_ev.pm_first_after is None:
                newest_ev.pm_first_after = best_ask
                newest_ev.pm_first_ts = ts


# ── Сэмплирование PM цены ─────────────────────────────────────────────────────

def _sample_events() -> None:
    """
    Каждые 0.5s проверяем: нужно ли записать очередной сэмпл (каждые 3s).
    Финализируем события когда окно закрылось (next_cl_ts или cl_ts+30s).
    """
    while True:
        time.sleep(0.5)
        now = time.time()
        to_finalize = []

        with _lock:
            for ev in _pending:
                if ev.logged:
                    continue

                # Конец окна: следующий тик или последний сэмпл
                window_end = ev.next_cl_ts if ev.next_cl_ts else (ev.cl_ts + SAMPLE_TIMES_S[-1])
                elapsed = min(now, window_end) - ev.cl_ts

                # Берём все сэмплы, время которых уже наступило
                while len(ev.samples) < MAX_SAMPLES:
                    next_t = SAMPLE_TIMES_S[len(ev.samples)]
                    if elapsed < next_t:
                        break
                    pm_entry = _pm_prices.get(ev.asset_id)
                    ev.samples.append(pm_entry[0] if pm_entry else None)

                # Финализируем если окно закончилось
                if now >= window_end:
                    ev.logged = True
                    to_finalize.append(ev)

        for ev in to_finalize:
            _log_event(ev)


def _log_event(ev: CorrelEvent) -> None:
    window_s = (ev.next_cl_ts - ev.cl_ts) if ev.next_cl_ts else SAMPLE_TIMES_S[-1]

    first_lag_ms = (ev.pm_first_ts - ev.cl_ts) * 1000 if ev.pm_first_ts else None

    # CL направление
    cl_direction = "—"
    cl_delta_pct = None
    if ev.cl_prev_price:
        delta = ev.cl_price - ev.cl_prev_price
        cl_delta_pct = delta / ev.cl_prev_price * 100
        cl_direction = "up" if delta > 0 else ("down" if delta < 0 else "=")

    # Фильтруем thin-book спайки: изолированные 0.990/0.010 между нормальными ценами
    def _is_spike(s: float) -> bool:
        return s >= 0.98 or s <= 0.02

    # PM в конце окна (последний не-спайк сэмпл)
    pm_end = next((s for s in reversed(ev.samples) if s is not None and not _is_spike(s)), None)
    pm_delta_cents = (pm_end - ev.pm_price_before) * 100 if (pm_end and ev.pm_price_before) else None

    pm_direction = "—"
    if pm_delta_cents is not None:
        if pm_delta_cents > 0.1:
            pm_direction = "up"
        elif pm_delta_cents < -0.1:
            pm_direction = "down"
        else:
            pm_direction = "="

    direction_match = "—"
    if cl_direction in ("up", "down") and pm_direction in ("up", "down"):
        direction_match = "Y" if cl_direction == pm_direction else "N"

    # Реальная цена входа: t+1s
    pm_entry_t1 = ev.samples[0] if ev.samples else None

    # Максимальное движение в сторону CL начиная с t+1s — без спайков
    samples_after_entry = [s for s in ev.samples[1:] if s is not None and not _is_spike(s)]
    pm_max_fav_cents = None
    entry_price = pm_entry_t1 or ev.pm_price_before
    if cl_direction in ("up", "down") and entry_price and samples_after_entry:
        if cl_direction == "up":
            pm_max_fav_cents = (max(samples_after_entry) - entry_price) * 100
        else:
            pm_max_fav_cents = (entry_price - min(samples_after_entry)) * 100

    # Спред bid-ask в момент тика
    spread_before = None
    if ev.pm_price_before and ev.pm_bid_before and ev.pm_bid_before > 0:
        spread_before = (ev.pm_price_before - ev.pm_bid_before) * 100

    # Binance vs CL дельта
    binance_cl_delta_pct = None
    if ev.binance_price_at_cl and ev.cl_price:
        binance_cl_delta_pct = (ev.binance_price_at_cl - ev.cl_price) / ev.cl_price * 100
    bnb_age_ms = (ev.cl_ts - ev.binance_ts_at_cl) * 1000 if ev.binance_ts_at_cl else None

    _ts_dt  = datetime.fromtimestamp(ev.cl_ts, tz=timezone.utc)
    ts_str  = _ts_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    pm_b    = f"{ev.pm_price_before:.3f}" if ev.pm_price_before else "—"
    pm_bid  = f"{ev.pm_bid_before:.3f}"   if ev.pm_bid_before   else "—"
    pm_fa   = f"{ev.pm_first_after:.3f}"  if ev.pm_first_after  else "—"
    pm_e1   = f"{pm_entry_t1:.3f}"        if pm_entry_t1        else "—"
    pm_e    = f"{pm_end:.3f}"             if pm_end             else "—"
    dc      = f"{pm_delta_cents:+.1f}c"   if pm_delta_cents is not None else "—"
    mf      = f"{pm_max_fav_cents:+.1f}c" if pm_max_fav_cents is not None else "—"
    spr     = f"{spread_before:.1f}c"     if spread_before is not None else "—"
    lag_f   = f"{first_lag_ms:.0f}ms"     if first_lag_ms else "—"
    cl_d    = cl_direction + (f"({cl_delta_pct:+.3f}%)" if cl_delta_pct is not None else "")
    bnb_s   = (f"bnb={ev.binance_price_at_cl:.4f} Δ={binance_cl_delta_pct:+.3f}% "
               f"age={bnb_age_ms:.0f}ms") if ev.binance_price_at_cl else "bnb=—"

    tradeable = "✓TRADE" if (first_lag_ms and first_lag_ms < 3000
                             and pm_max_fav_cents is not None and pm_max_fav_cents >= 5
                             and direction_match == "Y") else ""

    samples_str = " ".join(
        f"t{SAMPLE_TIMES_S[i]}={s:.3f}" if s is not None else f"t{SAMPLE_TIMES_S[i]}=—"
        for i, s in enumerate(ev.samples)
    )
    mins_s = f"{ev.minutes_remaining:.1f}m" if ev.minutes_remaining is not None else "—"
    print(
        f"[{ts_str}] {ev.sym}  cl={cl_d}  win={window_s:.0f}s  market_left={mins_s}\n"
        f"  t=0: ask={pm_b} bid={pm_bid} spread={spr}  {bnb_s}\n"
        f"  t=0+ws: first_reaction={pm_fa} (lag={lag_f})\n"
        f"  t=+1s: entry_realistic={pm_e1}  →  max_fav_after={mf}  pm_end={pm_e} ({dc})\n"
        f"  match={direction_match}  {tradeable}\n"
        f"  samples: {samples_str}",
        flush=True
    )

    _all_events.append(ev)

    sample_vals = [f"{s:.4f}" if s is not None else "" for s in ev.samples]
    sample_vals += [""] * (MAX_SAMPLES - len(sample_vals))

    _csv_writer.writerow([
        ts_str, ev.sym,
        f"{ev.cl_price:.6f}",
        f"{ev.cl_prev_price:.6f}" if ev.cl_prev_price else "",
        cl_direction,
        f"{cl_delta_pct:.4f}" if cl_delta_pct is not None else "",
        f"{window_s:.1f}",
        f"{ev.minutes_remaining:.2f}" if ev.minutes_remaining is not None else "",
        f"{ev.pm_price_before:.4f}" if ev.pm_price_before else "",
        f"{ev.pm_bid_before:.4f}"   if ev.pm_bid_before   else "",
        f"{spread_before:.2f}"      if spread_before is not None else "",
        f"{first_lag_ms:.0f}"       if first_lag_ms else "",
        f"{ev.pm_first_after:.4f}"  if ev.pm_first_after  else "",
        f"{pm_entry_t1:.4f}"        if pm_entry_t1 else "",
        *sample_vals,
        f"{pm_end:.4f}"             if pm_end else "",
        f"{pm_delta_cents:.2f}"     if pm_delta_cents is not None else "",
        f"{pm_max_fav_cents:.2f}"   if pm_max_fav_cents is not None else "",
        pm_direction,
        direction_match,
        f"{ev.binance_price_at_cl:.6f}" if ev.binance_price_at_cl else "",
        f"{binance_cl_delta_pct:.4f}"   if binance_cl_delta_pct is not None else "",
    ])
    _csv_file.flush()


def _start_pm_ws() -> None:
    from websockets.sync.client import connect as ws_connect

    while True:
        with _lock:
            asset_ids = list(_pm_asset_ids)

        if not asset_ids:
            print("[pm-ws] нет активных рынков, ждём...", flush=True)
            _pm_ws_restart.wait(timeout=30)
            _pm_ws_restart.clear()
            continue

        _pm_ws_restart.clear()
        try:
            with ws_connect(PM_WS_URL, open_timeout=10, ping_interval=None) as ws:
                ws.send(json.dumps({
                    "assets_ids": asset_ids,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                print(f"[pm-ws] подключён, {len(asset_ids)} assets", flush=True)
                last_ping = time.time()
                while True:
                    if _pm_ws_restart.is_set():
                        break
                    if time.time() - last_ping >= 10:
                        ws.send("PING")
                        last_ping = time.time()
                    try:
                        raw = ws.recv(timeout=5)
                    except TimeoutError:
                        continue
                    if raw in ("PONG", "PING", None):
                        continue
                    _on_pm_message(json.loads(raw))
        except Exception as exc:
            print(f"[pm-ws] error: {exc} — reconnect in 3s", flush=True)
            time.sleep(3)


# ── Binance WebSocket ─────────────────────────────────────────────────────────

def _start_binance_ws() -> None:
    """Подключается к Binance aggTrade stream и обновляет _binance_latest."""
    from websockets.sync.client import connect as ws_connect

    streams = "/".join(f"{s}@aggTrade" for s in BINANCE_SYMBOLS.values())
    url = f"{BINANCE_WS_URL}?streams={streams}"
    sym_upper = {v: k for k, v in BINANCE_SYMBOLS.items()}  # btcusdt → BTC

    while True:
        try:
            with ws_connect(url, open_timeout=10, ping_interval=20) as ws:
                print(f"[bnb-ws] подключён: {', '.join(BINANCE_SYMBOLS.values())}", flush=True)
                while True:
                    try:
                        raw = ws.recv(timeout=10)
                    except TimeoutError:
                        continue
                    msg = json.loads(raw)
                    data = msg.get("data", msg)
                    if data.get("e") != "aggTrade":
                        continue
                    sym = sym_upper.get(data["s"].lower())
                    if not sym:
                        continue
                    price = float(data["p"])
                    ts = data["T"] / 1000.0  # trade time в секундах
                    with _lock:
                        _binance_latest[sym] = (price, ts)
        except Exception as exc:
            print(f"[bnb-ws] error: {exc} — reconnect in 3s", flush=True)
            time.sleep(3)


# ── Chainlink WS ───────────────────────────────────────────────────────────────

def _get_agg_map(w3_list: list[Web3]) -> dict[str, str]:
    result = {}
    for sym, proxy in FEEDS.items():
        addr = Web3.to_checksum_address(proxy)
        for w3 in w3_list:
            try:
                c = w3.eth.contract(address=addr, abi=AGG_ABI)
                agg = c.functions.aggregator().call()
                result[agg.lower()] = sym
                break
            except Exception:
                continue
    return result


async def _cl_ws_run(agg_to_sym: dict[str, str]) -> None:
    import websockets as _ws
    agg_addrs = [Web3.to_checksum_address(a) for a in agg_to_sym]
    idx = 0
    while True:
        url = WSS_RPCS[idx % len(WSS_RPCS)]
        try:
            async with _ws.connect(url, ping_interval=20, ping_timeout=30) as ws:
                print(f"[cl-ws] подключён: {url}", flush=True)
                await ws.send(json.dumps({
                    "id": 1, "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": agg_addrs,
                        "topics": [ANSWER_UPDATED_TOPIC],
                    }],
                }))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("id") == 1:
                        continue
                    if msg.get("method") != "eth_subscription":
                        continue

                    log = msg["params"]["result"]
                    addr = log["address"].lower()
                    sym = agg_to_sym.get(addr)
                    if not sym:
                        continue

                    ts = time.time()
                    price_raw = int(log["topics"][1], 16)
                    if price_raw >= 2 ** 255:
                        price_raw -= 2 ** 256
                    price = price_raw / (10 ** DECIMALS)

                    with _lock:
                        prev_cl = _cl_latest.get(sym)

                        # Закрываем предыдущие открытые события для этого символа
                        for ev in _pending:
                            if ev.sym == sym and not ev.logged and ev.next_cl_ts is None:
                                ev.next_cl_ts = ts

                        active_assets = _get_active_assets(sym, ts)
                        bnb_now = _binance_latest.get(sym)

                        for active_asset in active_assets:
                            pm_now = _pm_prices.get(active_asset)
                            market_end_ts = _market_info[active_asset][2] if active_asset in _market_info else None
                            mins_left = (market_end_ts - ts) / 60 if market_end_ts else None
                            ev = CorrelEvent(
                                sym=sym,
                                asset_id=active_asset,
                                cl_price=price,
                                cl_prev_price=prev_cl[0] if prev_cl else None,
                                cl_ts=ts,
                                binance_price_at_cl=bnb_now[0] if bnb_now else None,
                                binance_ts_at_cl=bnb_now[1] if bnb_now else None,
                                minutes_remaining=mins_left,
                                pm_price_before=pm_now[0] if pm_now else None,
                                pm_bid_before=pm_now[1] if pm_now else None,
                            )
                            _pending.append(ev)

                        _cl_latest[sym] = (price, ts)

                    ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
                    if prev_cl:
                        delta_pct = (price - prev_cl[0]) / prev_cl[0] * 100
                        arrow = "↑" if delta_pct > 0 else "↓"
                        cl_s = f"price={price:.4f} {arrow}{abs(delta_pct):.3f}%"
                    else:
                        cl_s = f"price={price:.4f} (первый тик)"
                    if active_assets:
                        ends = [datetime.fromtimestamp(_market_info[a][2], tz=timezone.utc).strftime("%H:%M")
                                for a in active_assets if a in _market_info]
                        pm_s = f"{len(active_assets)} рынков (→{', '.join(ends)})"
                    else:
                        pm_s = "нет активного рынка"
                    print(f"[CL] [{ts_str}] {sym}  {cl_s}  {pm_s}", flush=True)

        except Exception as exc:
            print(f"[cl-ws] error: {exc}", flush=True)
        idx += 1
        await asyncio.sleep(3)


def _start_cl_thread(agg_to_sym: dict[str, str]) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_cl_ws_run(agg_to_sym))


# ── Stats ──────────────────────────────────────────────────────────────────────

def _print_stats() -> None:
    events = list(_all_events)

    if not events:
        print("[stats] нет завершённых корреляций пока", flush=True)
        return

    with_pm = [e for e in events if e.pm_price_before is not None and e.samples]

    print(f"\n{'='*60}")
    print(f"CL тиков: {len(events)}  |  с активным PM рынком: {len(with_pm)}")

    # Длина окна
    windows = [(e.next_cl_ts - e.cl_ts) if e.next_cl_ts else SAMPLE_TIMES_S[-1]
               for e in with_pm]
    if windows:
        print(f"Длина окна: avg={sum(windows)/len(windows):.1f}s  "
              f"min={min(windows):.1f}s  max={max(windows):.1f}s")

    # Лаг до первого PM изменения
    lags = [(e.pm_first_ts - e.cl_ts) * 1000 for e in with_pm if e.pm_first_ts]
    if lags:
        print(f"Лаг PM (первое изменение): avg={sum(lags)/len(lags):.0f}ms  "
              f"min={min(lags):.0f}ms  max={max(lags):.0f}ms")

    # PM изменение за окно
    def pm_end(e: CorrelEvent) -> Optional[float]:
        return next((s for s in reversed(e.samples) if s is not None), None)

    deltas = [
        (pm_end(e) - e.pm_price_before) * 100
        for e in with_pm
        if pm_end(e) is not None and e.pm_price_before
    ]
    if deltas:
        abs_deltas = [abs(d) for d in deltas]
        avg_abs = sum(abs_deltas) / len(abs_deltas)
        print(f"PM движение за окно: avg_abs={avg_abs:.1f}c  "
              f"min={min(abs_deltas):.1f}c  max={max(abs_deltas):.1f}c")
        print(f"  |Δ| ≥ 1c: {sum(1 for d in abs_deltas if d>=1):3d}/{len(deltas)}")
        print(f"  |Δ| ≥ 3c: {sum(1 for d in abs_deltas if d>=3):3d}/{len(deltas)}")
        print(f"  |Δ| ≥ 5c: {sum(1 for d in abs_deltas if d>=5):3d}/{len(deltas)}  ← минимум для стратегии")

    # Совпадение направлений CL → PM
    directional = [
        e for e in with_pm
        if e.cl_prev_price and pm_end(e) is not None and e.pm_price_before
    ]
    if directional:
        def _cl_dir(e: CorrelEvent) -> str:
            return "up" if e.cl_price > e.cl_prev_price else "down"

        def _pm_dir(e: CorrelEvent) -> str:
            d = (pm_end(e) - e.pm_price_before) * 100
            return "up" if d > 0.1 else ("down" if d < -0.1 else "=")

        matched = [e for e in directional if _cl_dir(e) == _pm_dir(e) and _pm_dir(e) != "="]
        comparable = [e for e in directional if _pm_dir(e) != "="]
        if comparable:
            pct = len(matched) / len(comparable) * 100
            print(f"\nСовпадение направления CL→PM: {len(matched)}/{len(comparable)} = {pct:.1f}%")

            for threshold in (0.01, 0.05, 0.10, 0.20):
                bucket = [e for e in comparable
                          if abs(e.cl_price - e.cl_prev_price) / e.cl_prev_price * 100 >= threshold]
                if not bucket:
                    continue
                bm = [e for e in bucket if _cl_dir(e) == _pm_dir(e)]
                print(f"  |cl_Δ%| ≥ {threshold:.2f}%: {len(bm)}/{len(bucket)} = {len(bm)/len(bucket)*100:.1f}%")

    print()
    for sym in sorted(set(e.sym for e in events)):
        se = [e for e in with_pm if e.sym == sym]
        if not se:
            continue
        sd = [abs((pm_end(e) - e.pm_price_before) * 100) for e in se
              if pm_end(e) is not None and e.pm_price_before]
        sl = [(e.pm_first_ts - e.cl_ts)*1000 for e in se if e.pm_first_ts]
        d_str = f"  pm_move avg={sum(sd)/len(sd):.1f}c" if sd else ""
        l_str = f"  lag avg={sum(sl)/len(sl):.0f}ms" if sl else ""
        print(f"  {sym}: {len(se)} событий{l_str}{d_str}")

    print(f"{'='*60}\n", flush=True)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    global _pm_asset_ids

    print("PM ↔ Chainlink correlate — реал-тайм мониторинг")
    print(f"CSV: {CSV_PATH}\n")

    w3_list = [Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 8})) for url in HTTPS_RPCS]

    print("Получаем Chainlink aggregator адреса...", flush=True)
    agg_to_sym = _get_agg_map(w3_list)
    if not agg_to_sym:
        print("Ошибка: не удалось получить aggregator адреса")
        return
    for agg, sym in agg_to_sym.items():
        print(f"  {sym}: {agg}")
    print()

    print("Получаем активные PM рынки...", flush=True)
    sym_to_assets = _fetch_pm_markets()
    with _lock:
        for ids in sym_to_assets.values():
            _pm_asset_ids.update(ids)
    total = len(_pm_asset_ids)
    if total:
        for sym, ids in sym_to_assets.items():
            if ids:
                print(f"  {sym}: {len(ids)} рынков")
    else:
        print("  Нет активных рынков прямо сейчас — PM WS будет ждать")
    print()

    threading.Thread(target=_start_binance_ws, daemon=True, name="bnb-ws").start()
    threading.Thread(target=_start_cl_thread, args=(agg_to_sym,), daemon=True, name="cl-ws").start()
    threading.Thread(target=_start_pm_ws, daemon=True, name="pm-ws").start()
    threading.Thread(target=_sample_events, daemon=True, name="sampler").start()
    threading.Thread(target=_market_refresh_loop, daemon=True, name="pm-refresh").start()
    print(f"[pm] обновление рынков на каждой 5-минутной границе (+{PM_BOUNDARY_OFFSET_S}s буфер)\n", flush=True)

    try:
        while True:
            time.sleep(300)
            _print_stats()
    except KeyboardInterrupt:
        print("\nОстановка...")
        _print_stats()
        _csv_file.close()
        print(f"Данные: {CSV_PATH}")
