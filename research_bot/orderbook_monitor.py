"""
research_bot/orderbook_monitor.py

Реал-тайм сборщик best_bid/best_ask для YES и NO токенов
BTC/ETH/SOL/XRP 5min/15min рынков на Polymarket.

Для каждого рынка:
  - open_yes_ask: цена входа YES в начале рынка
  - max_yes_bid:  макс. bid на YES (лучшая цена продажи)
  - min_no_ask:   мин. ask на NO (лучшая цена покупки NO)

Zapуск:
    python3 -m research_bot orderbook-monitor
    python3 -m research_bot orderbook-monitor --snapshot-interval 30
"""
from __future__ import annotations

import argparse
import csv
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from arb_bot.ws import MarketWebSocketClient
from cross_arb_bot.polymarket_feed import PolymarketFeed

PM_GAMMA_URL = "https://gamma-api.polymarket.com"
PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SCAN_INTERVAL = 300  # пересканируем рынки каждые 5 минут (+ 5s буфер)
SCAN_OFFSET = 5      # секунд после 5-минутной границы


# ── Структуры данных ────────────────────────────────────────────────────────────

@dataclass
class MarketInfo:
    market_id: str
    symbol: str
    interval_min: int
    start_ts: float
    end_ts: float
    yes_token_id: str
    no_token_id: str
    # Накопленные данные
    open_yes_ask: float = 0.0
    open_no_ask: float = 0.0
    max_yes_bid: float = 0.0
    min_yes_ask: float = 1.0
    min_no_ask: float = 1.0
    max_no_bid: float = 0.0
    final_yes_mid: float = 0.0
    snapshot_count: int = 0
    open_captured: bool = False


# ── Глобальное состояние ────────────────────────────────────────────────────────

_lock = threading.Lock()
_tracked: dict[str, MarketInfo] = {}           # market_id → MarketInfo
_prices: dict[str, tuple[float, float]] = {}   # token_id → (best_bid, best_ask)
_ws_token_ids: set[str] = set()
_ws_restart = threading.Event()
_stop = threading.Event()


# ── WS обработчик ───────────────────────────────────────────────────────────────

def _on_message(payload) -> None:
    if isinstance(payload, list):
        for item in payload:
            _on_message(item)
        return
    changes = payload.get("changes")
    if changes:
        for c in changes:
            _handle_change(c)
    else:
        _handle_change(payload)


def _handle_change(item: dict) -> None:
    asset_id = item.get("asset_id") or item.get("token_id") or ""
    if not asset_id:
        return
    if item.get("event_type") != "best_bid_ask":
        return
    try:
        bid = float(item.get("best_bid") or 0)
        ask = float(item.get("best_ask") or 0)
    except (TypeError, ValueError):
        return
    with _lock:
        _prices[asset_id] = (bid, ask)


# ── WS поток ───────────────────────────────────────────────────────────────────

def _ws_loop() -> None:
    client: Optional[MarketWebSocketClient] = None
    current_ids: frozenset[str] = frozenset()

    while not _stop.is_set():
        _ws_restart.wait(timeout=10)
        _ws_restart.clear()

        with _lock:
            wanted = frozenset(_ws_token_ids)

        if wanted == current_ids and client is not None:
            continue

        if client is not None:
            client.stop()
            client = None

        if not wanted:
            current_ids = frozenset()
            continue

        current_ids = wanted
        client = MarketWebSocketClient(
            url=PM_WS_URL,
            asset_ids=list(current_ids),
            on_message=_on_message,
        )
        client.start()
        print(f"[WS] подписка на {len(current_ids)} токенов", flush=True)

    if client:
        client.stop()


# ── Сканер рынков ──────────────────────────────────────────────────────────────

def _market_scan_loop(feed: PolymarketFeed) -> None:
    while not _stop.is_set():
        try:
            markets = feed.fetch_markets()
        except Exception as exc:
            print(f"[scan] ошибка: {exc}", flush=True)
            _stop.wait(timeout=60)
            continue

        new_count = 0
        with _lock:
            known = set(_tracked.keys())

        for m in markets:
            if m.interval_minutes not in (5, 15):
                continue
            if not m.yes_token_id or not m.no_token_id:
                continue
            if not m.expiry:
                continue
            if m.market_id in known:
                continue

            end_ts = m.expiry.replace(tzinfo=timezone.utc).timestamp()
            start_ts = end_ts - m.interval_minutes * 60

            info = MarketInfo(
                market_id=m.market_id,
                symbol=m.symbol,
                interval_min=m.interval_minutes,
                start_ts=start_ts,
                end_ts=end_ts,
                yes_token_id=m.yes_token_id,
                no_token_id=m.no_token_id,
            )
            with _lock:
                _tracked[m.market_id] = info
                _ws_token_ids.add(m.yes_token_id)
                _ws_token_ids.add(m.no_token_id)
            new_count += 1

        if new_count:
            end_str = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
            print(f"[scan] {end_str} +{new_count} новых рынков, перезапуск WS...", flush=True)
            _ws_restart.set()

        # Ждём до следующей 5-минутной границы + SCAN_OFFSET
        now = time.time()
        next_boundary = (now // SCAN_INTERVAL + 1) * SCAN_INTERVAL + SCAN_OFFSET
        wait = next_boundary - time.time()
        if wait > 0:
            _stop.wait(timeout=wait)


# ── Снимки + summary ───────────────────────────────────────────────────────────

def _snapshot_loop(
    snap_interval: int,
    snap_writer: csv.writer,
    summary_writer: csv.writer,
    snap_f,
    summary_f,
) -> None:
    while not _stop.is_set():
        now = time.time()
        now_dt = datetime.fromtimestamp(now, tz=timezone.utc)

        with _lock:
            markets = list(_tracked.values())

        for info in markets:
            with _lock:
                yes_bid, yes_ask = _prices.get(info.yes_token_id, (0.0, 0.0))
                no_bid, no_ask = _prices.get(info.no_token_id, (0.0, 0.0))

            if yes_ask == 0.0 and yes_bid == 0.0:
                continue

            yes_mid = (yes_bid + yes_ask) / 2 if yes_bid and yes_ask else (yes_ask or yes_bid)
            no_mid = (no_bid + no_ask) / 2 if no_bid and no_ask else (no_ask or no_bid)
            minutes_elapsed = (now - info.start_ts) / 60
            minutes_remaining = (info.end_ts - now) / 60

            with _lock:
                if yes_bid > 0:
                    info.max_yes_bid = max(info.max_yes_bid, yes_bid)
                if 0 < yes_ask < 1:
                    info.min_yes_ask = min(info.min_yes_ask, yes_ask)
                if 0 < no_ask < 1:
                    info.min_no_ask = min(info.min_no_ask, no_ask)
                if no_bid > 0:
                    info.max_no_bid = max(info.max_no_bid, no_bid)

                # Открытие: первый снимок в пределах первых 2 минут
                if not info.open_captured and 0 <= minutes_elapsed < 2:
                    if yes_ask > 0:
                        info.open_yes_ask = yes_ask
                    if no_ask > 0:
                        info.open_no_ask = no_ask
                    info.open_captured = True

                info.final_yes_mid = yes_mid
                info.snapshot_count += 1

            snap_writer.writerow([
                now_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                info.market_id,
                info.symbol,
                info.interval_min,
                round(minutes_elapsed, 2),
                round(minutes_remaining, 2),
                round(yes_bid, 4),
                round(yes_ask, 4),
                round(yes_mid, 4),
                round(no_bid, 4),
                round(no_ask, 4),
                round(no_mid, 4),
            ])

        # Закрытые рынки: пишем summary и удаляем из трекинга
        expired: list[MarketInfo] = []
        with _lock:
            for mid in list(_tracked.keys()):
                if now > _tracked[mid].end_ts + 30:
                    expired.append(_tracked.pop(mid))

        for info in expired:
            end_str = datetime.fromtimestamp(info.end_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            min_no = info.min_no_ask if info.min_no_ask < 1.0 else 0.0
            min_yes = info.min_yes_ask if info.min_yes_ask < 1.0 else 0.0
            summary_writer.writerow([
                info.market_id,
                info.symbol,
                info.interval_min,
                end_str,
                round(info.open_yes_ask, 4),
                round(info.open_no_ask, 4),
                round(info.max_yes_bid, 4),
                round(min_yes, 4),
                round(min_no, 4),
                round(info.max_no_bid, 4),
                round(info.final_yes_mid, 4),
                info.snapshot_count,
            ])
            print(
                f"[closed] {info.symbol} {info.interval_min}m {end_str} | "
                f"open_yes_ask={info.open_yes_ask:.3f}  "
                f"max_yes_bid={info.max_yes_bid:.3f}  "
                f"min_no_ask={min_no:.3f}  "
                f"snapshots={info.snapshot_count}",
                flush=True,
            )

        snap_f.flush()
        summary_f.flush()
        _stop.wait(timeout=snap_interval)


# ── Точка входа ────────────────────────────────────────────────────────────────

def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Реал-тайм мониторинг стакана BTC/ETH/SOL/XRP рынков")
    parser.add_argument("--snapshot-interval", type=int, default=30,
                        help="Интервал снимков стакана в секундах (default: 30)")
    opts = parser.parse_args(args or [])

    feed = PolymarketFeed(
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

    data_dir = Path("research_bot/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = data_dir / f"ob_snapshots_{ts_str}.csv"
    summary_path = data_dir / f"ob_summary_{ts_str}.csv"

    snap_f = open(snap_path, "w", newline="")
    summary_f = open(summary_path, "w", newline="")
    snap_writer = csv.writer(snap_f)
    summary_writer = csv.writer(summary_f)

    snap_writer.writerow([
        "ts_utc", "market_id", "symbol", "interval_min",
        "minutes_elapsed", "minutes_remaining",
        "yes_best_bid", "yes_best_ask", "yes_mid",
        "no_best_bid", "no_best_ask", "no_mid",
    ])
    summary_writer.writerow([
        "market_id", "symbol", "interval_min", "end_date",
        "open_yes_ask", "open_no_ask",
        "max_yes_bid", "min_yes_ask",
        "min_no_ask", "max_no_bid",
        "final_yes_mid", "snapshot_count",
    ])

    def _shutdown(signum=None, frame=None):
        print("\n[monitor] Завершение...", flush=True)
        _stop.set()
        snap_f.flush()
        summary_f.flush()
        snap_f.close()
        summary_f.close()
        print(f"[monitor] Snapshots: {snap_path}")
        print(f"[monitor] Summary:   {summary_path}")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"[monitor] Snapshots → {snap_path}", flush=True)
    print(f"[monitor] Summary   → {summary_path}", flush=True)
    print(f"[monitor] Интервал снимков: {opts.snapshot_interval}s. Ctrl+C для остановки.", flush=True)

    threading.Thread(target=_ws_loop, daemon=True, name="ws-loop").start()
    threading.Thread(target=_market_scan_loop, args=(feed,), daemon=True, name="scan-loop").start()

    _snapshot_loop(opts.snapshot_interval, snap_writer, summary_writer, snap_f, summary_f)
