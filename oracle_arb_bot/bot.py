from __future__ import annotations

import calendar
import json
import statistics
import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_UP
from typing import Optional

from src.api.clob import ClobClient
from volatility_bot.strategy import compute_position_pct, compute_market_minute

from oracle_arb_bot.binance_feed import BinanceFeed
from oracle_arb_bot.chainlink_feed import ChainlinkFeed
from oracle_arb_bot.db import OracleDB
from oracle_arb_bot.models import OracleMarket, OracleBet, OracleSignal
from oracle_arb_bot.resolver import (
    check_polymarket_result,
    check_kalshi_result,
    fetch_pm_open_price,
    fetch_pm_close_price,
)
from oracle_arb_bot.real_trader import OracleRealTrader
from oracle_arb_bot.scanner import OracleScanner
from oracle_arb_bot.strategy import SignalResult, evaluate_oracle_signal, evaluate_cl_contradiction_signal
from oracle_arb_bot.telegram_notify import OracleTelegramNotifier


def _sleep_until_next_boundary(interval_seconds: int) -> None:
    """
    Спит до следующей границы кратной interval_seconds от начала часа.
    Например, при interval_seconds=300: :00, :05, :10, :15, ..., :55
    """
    now = datetime.utcnow()
    seconds_in_hour = now.minute * 60 + now.second + now.microsecond / 1e6
    next_boundary = (seconds_in_hour // interval_seconds + 1) * interval_seconds
    sleep_for = next_boundary - seconds_in_hour
    next_min = int(next_boundary % 3600 // 60)
    next_sec = int(next_boundary % 60)
    print(f"[oracle] next scan at :{next_min:02d}:{next_sec:02d} UTC (sleep {sleep_for:.1f}s)")
    time.sleep(sleep_for)


class OracleArbBot:
    def __init__(self, config: dict, db: OracleDB) -> None:
        self._cfg = config
        self._db = db

        self._stake_usd: float = config["trading"]["stake_usd"]
        self._paper_max_price: float = config["trading"].get("max_price", 0.49)
        scfg = config["strategy"]
        self._strategy_mode: str = scfg.get("mode", "crossing")
        self._delta_threshold_pct: float = scfg.get("delta_threshold_pct", 0.05)
        self._max_entry_price: float = scfg.get("max_entry_price", 0.48)
        self._min_orderbook_usd: float = scfg["min_orderbook_usd"]

        # Multi-config paper betting
        # Each config: {"name": "A", "delta_pct": 0.05, "max_ask": 0.50}
        raw_cfgs = config.get("paper_configs", [])
        if raw_cfgs:
            self._paper_configs: list[dict] = []
            for pc in raw_cfgs:
                self._paper_configs.append({
                    "name": pc["name"],
                    "delta_pct": pc["delta_pct"],
                    "max_ask": pc["max_ask"],
                    "venue": pc.get("venue"),  # None = all venues
                    "min_secs_from_start": pc.get("min_secs_from_start"),  # {interval_str: secs}
                    "max_secs_from_start": pc.get("max_secs_from_start"),  # secs (int)
                    "max_delta_std": pc.get("max_delta_std"),  # float, e.g. 0.015
                    "require_binance_move": pc.get("require_binance_move", False),
                })
        else:
            # Single config fallback from strategy section
            self._paper_configs = [{
                "name": "default",
                "delta_pct": self._delta_threshold_pct,
                "max_ask": self._max_entry_price,
            }]
        self._depth_slippage_cents: float = scfg.get("depth_slippage_cents", 4.0)
        self._active_zone_min: float = scfg.get("active_zone_min", 0.10)
        self._active_zone_max: float = scfg.get("active_zone_max", 0.90)
        self._min_cl_delta_pct: float = scfg.get("min_cl_delta_pct", 0.0)
        # binance_momentum
        self._momentum_delta_pct: float = scfg.get("momentum_delta_pct", 0.05)
        self._momentum_min_minute: int = scfg.get("momentum_min_minute", 0)
        self._momentum_adaptive: bool = scfg.get("momentum_adaptive", False)
        self._momentum_adaptive_window: int = scfg.get("momentum_adaptive_window", 600)  # 10 min
        self._momentum_adaptive_rules: list[tuple[int, float]] = scfg.get(
            "momentum_adaptive_rules", [[2, 0.05], [5, 0.08], [999, 0.12]]
        )
        self._momentum_cheap_delta: float = scfg.get("momentum_cheap_delta_pct", 0.10)
        self._momentum_continuous: bool = scfg.get("momentum_continuous", False)
        self._momentum_buckets: dict[str, tuple[int, float, float | None]] = {}  # symbol → (bucket_ts, last_price, prev_close)
        self._momentum_markets_bet: set[str] = set()  # market_ids где уже поставили
        self._momentum_signal_history: dict[str, list[int]] = {}  # symbol → [bucket_ts, ...]
        self._scan_interval: int = config["runtime"]["scan_interval_seconds"]

        self._1s_last: dict[str, int] = {}  # symbol → sec_ts последнего закрытого тика
        self._1s_close: dict[str, float] = {}  # symbol → последняя цена в текущей секунде
        self._1s_history: dict[str, dict[int, float]] = {}  # symbol → {sec_ts: close} последние 6с

        # PM ask 1s history for backtests (sample-and-hold from book poller)
        # market_id → {sec_ts: (yes_ask, no_ask)}
        self._pm_1s_history: dict[str, dict[int, tuple[float | None, float | None]]] = {}

        # Per-venue paper/real flags
        self._pm_paper: bool = config["polymarket"].get("paper", True)
        self._pm_real: bool = config["polymarket"].get("real", False)
        self._kalshi_paper: bool = config.get("kalshi", {}).get("paper", False)

        # Crossing tracker: per-config last_bet_side
        # key = (config_name, market_id) → side
        self._last_bet_side: dict[tuple[str, str], str | None] = {}
        self._depth_cooldown: dict[tuple[str, str], float] = {}
        self._last_bet_time: dict[tuple[str, str], float] = {}  # (config, market_id) → ts
        self._bet_cooldown_s: float = 60.0
        self._placed_lock = threading.Lock()
        self._load_crossings_from_db()

        # Previous tick tracker for require_binance_move filter
        # market_id → (binance_price, yes_ask, no_ask)
        self._prev_market_tick: dict[str, tuple[float, float | None, float | None]] = {}

        # 0.5s sliding price window for avg-based filter (require_binance_move configs)
        # market_id → list of (ts, binance_price, yes_ask, no_ask), last 10s
        self._price_window: dict[str, list[tuple[float, float, float | None, float | None]]] = {}

        # Diagnostic: track min/max delta per market to understand signal distribution
        self._delta_tracker: dict[str, dict] = {}  # market_id → {min, max, neg_count, total}
        self._delta_tracker_last_print: float = 0.0

        # Signal tick tracker: record when delta CROSSES threshold (not while above)
        # market_id → (was_above: bool, last_side: str|None, seq: int)
        self._signal_tick_state: dict[str, tuple[bool, str | None, int]] = {}

        # CLOB для проверки PM ликвидности
        self._clob = ClobClient(base_url=config["polymarket"]["clob_base_url"])

        # Сканер PM + Kalshi рынков
        self._scanner = OracleScanner(config)
        # PM WS не нужен в momentum режиме — цена берётся из REST (_check_depth)
        if scfg.get("mode") != "binance_momentum":
            self._scanner.set_pm_price_callback(self._on_pm_price)

        # Price feed: Chainlink (default) or Binance
        self._price_source: str = config.get("price_source", "binance")
        if self._price_source == "chainlink":
            cl_cfg = config.get("chainlink", {})
            self._price_feed = ChainlinkFeed(
                symbols=config["binance"]["symbols"],
                on_price=self._on_source_price,
                rpc_urls=cl_cfg.get("rpc_urls", ["https://polygon-bor-rpc.publicnode.com"]),
                wss_urls=cl_cfg.get("wss_urls"),
                poll_interval_seconds=cl_cfg.get("poll_interval_seconds", 30.0),
            )
        else:
            self._price_feed = BinanceFeed(
                symbols=config["binance"]["symbols"],
                on_price=self._on_source_price,
            )
        print(f"[oracle] price_source={self._price_source}")

        # В режиме cl_contradiction нужен Binance параллельно с Chainlink
        self._binance_feed: Optional[BinanceFeed] = None
        if self._strategy_mode == "cl_contradiction" and self._price_source == "chainlink":
            self._binance_feed = BinanceFeed(
                symbols=config["binance"]["symbols"],
                on_price=None,  # только для get_price(), не генерирует сигналы
            )
            print("[oracle] cl_contradiction: запускаем Binance feed как второй источник")

        # Real trading (создаём до Telegram чтобы передать get_balance_info)
        rcfg = config.get("real_trading", {})
        self._real_time_filter = {
            "min_secs_from_start": rcfg.get("min_secs_from_start"),
            "max_secs_from_start": rcfg.get("max_secs_from_start"),
        }
        self._real_require_binance_move: bool = rcfg.get("require_binance_move", False)
        self._real = None
        if rcfg.get("enabled"):
            real_max = (
                self._active_zone_max
                if self._strategy_mode == "binance_momentum"
                else rcfg.get("max_price", 0.48)
            )
            self._real: Optional[OracleRealTrader] = OracleRealTrader(
                db=db,
                stake_usd=rcfg.get("stake_usd", 1.0),
                initial_deposit=rcfg.get("initial_deposit", 6.0),
                floor_pct=rcfg.get("floor_pct", 0.20),
                tg=None,  # будет установлен ниже
                price_10s_fn=self._record_price_after_delay,
                max_price=real_max,
            )
        else:
            self._real = None

        # Telegram
        _pc_info = [(pc["name"], pc["delta_pct"], pc["max_ask"]) for pc in self._paper_configs]

        def _status_fn() -> str:
            text = db.get_status_text(paper_configs=_pc_info)
            if self._real:
                try:
                    real_bal = self._real.sync_balance()
                    info = self._real.deposit_info()
                    text += f"\n💳 реальный: {info}"
                    if real_bal is not None:
                        text += f"  (PM CLOB ${real_bal:.2f})"
                except Exception:
                    text += f"\n💳 реальный: {self._real.deposit_info()}"
            return text

        tg_cfg = config.get("telegram", {})
        self._tg = (
            OracleTelegramNotifier(
                get_status_fn=_status_fn,
                token_env=tg_cfg.get("token_env", "SIMPLE_BOT_TOKEN"),
                chat_id_file=tg_cfg.get("chat_id_file", "data/.telegram_chat_id"),
            )
            if tg_cfg.get("enabled")
            else None
        )
        if self._real:
            self._real._tg = self._tg

    def _load_crossings_from_db(self) -> None:
        """Восстанавливаем last_bet_side и momentum_markets_bet из открытых ставок."""
        for bet in self._db.get_open_bets():
            cfg_name = getattr(bet, "paper_config", None) or "default"
            self._last_bet_side[(cfg_name, bet.market_id)] = bet.side
            self._momentum_markets_bet.add(bet.market_id)

    # ── Main loop ─────────────────────────────────────────────────────────

    def _start_book_poller(self) -> None:
        """Фоновый поллинг CLOB каждые 3 сек — держит market.yes_ask/no_ask актуальными.
        Также записывает pm_price_after для ставок ожидающих первого poll после входа."""
        self._after_pending: dict[str, tuple[str, str]] = {}  # bet_id → (token_id, side)
        self._after_pending_lock = threading.Lock()
        _last_open_price_fetch = [0.0]  # mutable for closure; monotonic time

        def _loop():
            while True:
                try:
                    now = datetime.utcnow()

                    # Retry openPrice fetch every 5s while any PM market is missing it
                    mono = time.monotonic()
                    if mono - _last_open_price_fetch[0] >= 5:
                        need = any(
                            m.venue == "polymarket" and m.pm_open_price is None
                            and m.pm_event_slug and now >= m.market_start
                            for m in self._scanner.all_markets()
                        )
                        if need:
                            _last_open_price_fetch[0] = mono
                            try:
                                self._fetch_pm_open_prices()
                            except Exception:
                                pass

                    kalshi_feed = self._scanner.kalshi_feed
                    for market in self._scanner.all_markets():
                        if market.venue not in ("polymarket", "kalshi"):
                            continue
                        if now < market.market_start or now >= market.expiry:
                            continue

                        yes_bid = None
                        no_bid = None
                        yes_asks_raw = None
                        yes_bids_raw = None
                        no_asks_raw = None
                        no_bids_raw = None

                        if market.venue == "polymarket":
                            for side, token_id in [("yes", market.yes_token_id), ("no", market.no_token_id)]:
                                if not token_id:
                                    continue
                                try:
                                    book = self._clob.get_orderbook(token_id)
                                    if book and book.asks:
                                        price = book.asks[0].price
                                        if side == "yes":
                                            market.yes_ask = price
                                            yes_asks_raw = [[str(l.price), str(l.size)] for l in book.asks]
                                        else:
                                            market.no_ask = price
                                            no_asks_raw = [[str(l.price), str(l.size)] for l in book.asks]
                                        # записываем after для ожидающих ставок
                                        with self._after_pending_lock:
                                            done = [bid for bid, (tid, s) in self._after_pending.items()
                                                    if tid == token_id and s == side]
                                        for bid in done:
                                            try:
                                                self._db.update_price_after(bid, price)
                                            except Exception:
                                                pass
                                            with self._after_pending_lock:
                                                self._after_pending.pop(bid, None)
                                    if book and book.bids:
                                        if side == "yes":
                                            yes_bid = book.bids[0].price
                                            yes_bids_raw = [[str(l.price), str(l.size)] for l in book.bids]
                                        else:
                                            no_bid = book.bids[0].price
                                            no_bids_raw = [[str(l.price), str(l.size)] for l in book.bids]
                                except Exception:
                                    pass

                        elif market.venue == "kalshi" and kalshi_feed:
                            try:
                                ya, yb, na, nb, err = kalshi_feed.fetch_full_book(market.market_id)
                                if not err:
                                    if ya:
                                        market.yes_ask = ya[0].price
                                        yes_asks_raw = [[str(l.price), str(l.size)] for l in ya]
                                    if yb:
                                        yes_bid = yb[0].price
                                        yes_bids_raw = [[str(l.price), str(l.size)] for l in yb]
                                    if na:
                                        market.no_ask = na[0].price
                                        no_asks_raw = [[str(l.price), str(l.size)] for l in na]
                                    if nb:
                                        no_bid = nb[0].price
                                        no_bids_raw = [[str(l.price), str(l.size)] for l in nb]
                            except Exception:
                                pass

                        # Record price tick for backtesting
                        bp = self._price_feed.get_price(market.symbol)
                        ste = (market.expiry - now).total_seconds()
                        ref = market.pm_open_price
                        delta = ((bp - ref) / ref * 100) if bp and ref else None
                        ts_iso = now.isoformat()
                        try:
                            self._db.insert_price_tick(
                                market_id=market.market_id,
                                symbol=market.symbol,
                                interval_minutes=market.interval_minutes,
                                ts=ts_iso,
                                seconds_to_expiry=round(ste, 2),
                                binance_price=bp,
                                pm_open_price=ref,
                                pm_yes_ask=market.yes_ask,
                                pm_no_ask=market.no_ask,
                                pm_yes_bid=yes_bid,
                                pm_no_bid=no_bid,
                                delta_pct=round(delta, 4) if delta is not None else None,
                            )
                        except Exception:
                            pass

                        # Update prev tick for require_binance_move filter
                        # (matches backtest which used consecutive price_ticks)
                        if bp is not None:
                            self._prev_market_tick[market.market_id] = (
                                bp, market.yes_ask, market.no_ask,
                            )

                        # PM ask 1s history (sample-and-hold for backtests)
                        sec_ts = int(time.time())
                        pm_hist = self._pm_1s_history.setdefault(market.market_id, {})
                        pm_hist[sec_ts] = (market.yes_ask, market.no_ask)
                        for old_ts in [t for t in pm_hist if t < sec_ts - 6]:
                            del pm_hist[old_ts]

                        # Record signal ticks: transition into signal state
                        # Signal = delta >= threshold AND ask < 0.50
                        if delta is not None and ref:
                            sig_side = "yes" if delta > 0 else "no"
                            sig_ask = market.yes_ask if sig_side == "yes" else market.no_ask
                            in_signal = abs(delta) >= self._delta_threshold_pct and sig_ask < 0.50
                            was_in, prev_side, prev_seq = self._signal_tick_state.get(
                                market.market_id, (False, None, 0)
                            )
                            # Record on entering signal (was out → now in) or side change
                            if in_signal and (not was_in or sig_side != prev_side):
                                seq = prev_seq + 1
                                self._signal_tick_state[market.market_id] = (True, sig_side, seq)
                                elapsed = market.interval_minutes * 60 - ste
                                try:
                                    self._db.insert_signal_tick(
                                        market_id=market.market_id,
                                        symbol=market.symbol,
                                        venue=market.venue,
                                        interval_minutes=market.interval_minutes,
                                        ts=ts_iso,
                                        seconds_to_expiry=round(ste, 2),
                                        market_minute=int(elapsed / 60),
                                        side=sig_side,
                                        delta_pct=round(delta, 4),
                                        pm_open_price=ref,
                                        binance_price=bp,
                                        ask_price=sig_ask,
                                        seq=seq,
                                    )
                                except Exception:
                                    pass
                            elif not in_signal and was_in:
                                # Exited signal state — update flag, keep seq
                                self._signal_tick_state[market.market_id] = (False, prev_side, prev_seq)

                        # Record full orderbook snapshot
                        try:
                            self._db.insert_orderbook_snapshot(
                                market_id=market.market_id,
                                symbol=market.symbol,
                                interval_minutes=market.interval_minutes,
                                ts=ts_iso,
                                seconds_to_expiry=round(ste, 2),
                                yes_asks=json.dumps(yes_asks_raw) if yes_asks_raw else None,
                                yes_bids=json.dumps(yes_bids_raw) if yes_bids_raw else None,
                                no_asks=json.dumps(no_asks_raw) if no_asks_raw else None,
                                no_bids=json.dumps(no_bids_raw) if no_bids_raw else None,
                            )
                        except Exception:
                            pass
                    try:
                        self._db.conn.commit()
                    except Exception:
                        pass
                except Exception as exc:
                    print(f"[book_poll] error: {exc}")
                time.sleep(1)
        threading.Thread(target=_loop, daemon=True, name="book-poller").start()
        print("[oracle] book poller started (1s interval)")

    def _start_price_window_sampler(self) -> None:
        """0.5s sampler for require_binance_move avg filter.
        Reads in-memory values (no HTTP). Keeps last 10s per market."""
        def _loop():
            while True:
                now = time.monotonic()
                wall = time.time()
                cutoff = wall - 10.0
                for market in self._scanner.all_markets():
                    if market.venue != "polymarket":
                        continue
                    bp = self._price_feed.get_price(market.symbol)
                    if bp is None:
                        continue
                    mid = market.market_id
                    window = self._price_window.setdefault(mid, [])
                    window.append((wall, bp, market.yes_ask, market.no_ask))
                    # Drop entries older than 10s
                    while window and window[0][0] < cutoff:
                        window.pop(0)
                time.sleep(0.5)
        threading.Thread(target=_loop, daemon=True, name="price-window").start()
        print("[oracle] price window sampler started (0.5s)")

    def run(self) -> None:
        print("[oracle] Starting OracleArbBot")
        print(f"[oracle] strategy_mode={self._strategy_mode}")
        if self._tg:
            self._tg.start()
        self._price_feed.start()
        if self._binance_feed is not None:
            self._binance_feed.start()
        self._start_book_poller()
        self._start_price_window_sampler()
        while True:
            try:
                self._scanner.scan_and_subscribe()
            except Exception as exc:
                print(f"[oracle] scan error: {exc}")
            try:
                self._fetch_pm_open_prices()
            except Exception as exc:
                print(f"[oracle] openPrice fetch error: {exc}")
            try:
                self._resolve_expired()
            except Exception as exc:
                print(f"[oracle] resolve error: {exc}")
            if self._real:
                try:
                    self._real.sync_balance()
                except Exception as exc:
                    print(f"[oracle] balance sync error: {exc}")
            _sleep_until_next_boundary(self._scan_interval)

    def scan_once(self) -> None:
        """Один цикл сканирования для CLI команды scan."""
        self._price_feed.start()
        time.sleep(1)  # дать WS/poll соединиться
        self._scanner.scan_and_subscribe()
        markets = self._scanner.all_markets()

        print(f"\nFound {len(markets)} markets:")
        for m in sorted(markets, key=lambda x: (x.venue, x.symbol, x.interval_minutes)):
            now = datetime.utcnow()
            minute = compute_market_minute(now, m.market_start) if now >= m.market_start else -1
            ref = f" ref={m.pm_open_price}" if m.pm_open_price else ""
            print(
                f"  [{m.venue:4s}] {m.symbol:4s} {m.interval_minutes:2d}m "
                f"start={m.market_start.strftime('%H:%M')} end={m.expiry.strftime('%H:%M')} "
                f"yes={m.yes_ask:.3f} no={m.no_ask:.3f} min={minute}{ref}"
            )

    # ── Price source callback ─────────────────────────────────────────────

    def _on_source_price(self, symbol: str, price: float, ts_ms: int) -> None:
        now = datetime.utcnow()

        # ── запись 1s Binance цен (последний тик каждой секунды) ──
        sec_ts = ts_ms // 1000
        prev = self._1s_last.get(symbol)
        if prev != sec_ts:
            # новая секунда — сохраняем close предыдущей
            if prev is not None and symbol in self._1s_close:
                prev_close = self._1s_close[symbol]
                # обновляем историю последних 6 секунд для continuous режима
                hist = self._1s_history.setdefault(symbol, {})
                hist[prev] = prev_close
                for old_ts in [t for t in hist if t < prev - 6]:
                    del hist[old_ts]
                # continuous: проверяем дельту за 1..5 секунд
                if self._strategy_mode == "binance_momentum" and self._momentum_continuous:
                    self._on_momentum_continuous(symbol, prev, prev_close, now)
            self._1s_last[symbol] = sec_ts
        self._1s_close[symbol] = price

        if self._strategy_mode == "binance_momentum":
            self._on_momentum_price(symbol, price, ts_ms, now)
            return

        for market in self._scanner.all_markets():
            if market.symbol != symbol:
                continue
            if now < market.market_start or now >= market.expiry:
                continue
            if market.binance_price_at_start is None:
                market.binance_price_at_start = price
            self._check_signal(market, price, now)

    def _on_momentum_price(self, symbol: str, price: float, ts_ms: int, now: datetime) -> None:
        """Отслеживает 5-секундные бакеты Binance. При резком движении — ставка."""
        ts_sec = ts_ms // 1000
        bucket = (ts_sec // 5) * 5

        entry = self._momentum_buckets.get(symbol)

        if entry is None:
            self._momentum_buckets[symbol] = (bucket, price, None)
            return

        curr_bucket, curr_price, prev_close = entry

        if bucket == curr_bucket:
            # обновляем last price текущего бакета
            self._momentum_buckets[symbol] = (bucket, price, prev_close)
            return

        # Бакет завершился — используем 1s close (то же что в binance_1s DB)
        # чтобы бот и бэктест работали по идентичным данным
        bucket_close = self._1s_close.get(symbol, curr_price)
        self._momentum_buckets[symbol] = (bucket, price, bucket_close)

        if prev_close is None:
            return  # нужно два завершённых бакета для сигнала

        # Сравниваем close завершённого бакета vs close предыдущего
        delta_pct = (bucket_close - prev_close) / prev_close * 100

        # Adaptive delta: считаем сигналы ≥0.05% за последние 10 мин
        if self._momentum_adaptive and abs(delta_pct) >= 0.05:
            history = self._momentum_signal_history.setdefault(symbol, [])
            cutoff = bucket - self._momentum_adaptive_window
            recent_count = sum(1 for t in history if t > cutoff)
            history.append(bucket)
            self._momentum_signal_history[symbol] = [t for t in history if t > cutoff]

            min_delta = self._momentum_delta_pct
            for max_n, threshold in self._momentum_adaptive_rules:
                if recent_count <= max_n:
                    min_delta = threshold
                    break
        else:
            min_delta = self._momentum_delta_pct
            recent_count = 0

        if abs(delta_pct) < min_delta:
            return

        signal_side = "yes" if delta_pct > 0 else "no"
        adaptive_tag = f" [n={recent_count}→{min_delta}%]" if self._momentum_adaptive else ""
        print(
            f"[momentum] {symbol} Δ{delta_pct:+.4f}% → {signal_side.upper()} "
            f"| {prev_close:.4f} → {curr_price:.4f}{adaptive_tag}"
        )

        for market in self._scanner.all_markets():
            if market.symbol != symbol:
                continue
            if now < market.market_start or now >= market.expiry:
                continue
            market_minute = int((now - market.market_start).total_seconds() // 60)
            if market_minute < self._momentum_min_minute:
                continue
            if market.market_id in self._momentum_markets_bet:
                continue
            if market.binance_price_at_start is None:
                market.binance_price_at_start = price
            self._fire_momentum_bet(market, signal_side, delta_pct, price, now)

    def _on_momentum_continuous(self, symbol: str, sec_ts: int, curr_price: float, now: datetime) -> None:
        """Continuous режим: ищет кратчайшее окно (1..5s) где дельта достигнута."""
        hist = self._1s_history.get(symbol, {})

        # Определяем эффективную дельту (adaptive)
        if self._momentum_adaptive:
            # Используем sec_ts как proxy для bucket для подсчёта истории
            cutoff = sec_ts - self._momentum_adaptive_window
            recent_count = sum(1 for t in self._momentum_signal_history.get(symbol, []) if t > cutoff)
            min_delta = self._momentum_delta_pct
            for max_n, threshold in self._momentum_adaptive_rules:
                if recent_count <= max_n:
                    min_delta = threshold
                    break
        else:
            min_delta = self._momentum_delta_pct
            recent_count = 0

        # Ищем кратчайшее окно lb=1..5 где дельта достигнута
        formation_sec = None
        delta_pct = 0.0
        for lb in range(1, 6):
            ref_ts = sec_ts - lb
            if ref_ts not in hist:
                continue
            d = (curr_price - hist[ref_ts]) / hist[ref_ts] * 100
            if abs(d) >= min_delta:
                formation_sec = lb
                delta_pct = d
                break

        if formation_sec is None:
            return

        signal_side = "yes" if delta_pct > 0 else "no"
        adaptive_tag = f" [n={recent_count}→{min_delta}%]" if self._momentum_adaptive else ""
        print(
            f"[momentum/cont] {symbol} Δ{delta_pct:+.4f}% ({formation_sec}s) → {signal_side.upper()}{adaptive_tag}"
        )

        if self._momentum_adaptive and abs(delta_pct) >= 0.05:
            history = self._momentum_signal_history.setdefault(symbol, [])
            history.append(sec_ts)
            self._momentum_signal_history[symbol] = [t for t in history if t > sec_ts - self._momentum_adaptive_window]

        for market in self._scanner.all_markets():
            if market.symbol != symbol:
                continue
            if now < market.market_start or now >= market.expiry:
                continue
            market_minute = int((now - market.market_start).total_seconds() // 60)
            if market_minute < self._momentum_min_minute:
                continue
            if market.market_id in self._momentum_markets_bet:
                continue
            if market.binance_price_at_start is None:
                market.binance_price_at_start = curr_price
            self._fire_momentum_bet(market, signal_side, delta_pct, curr_price, now,
                                    signal_mode="continuous")

    def _fire_momentum_bet(
        self,
        market: OracleMarket,
        signal_side: str,
        delta_pct: float,
        price: float,
        now: datetime,
        signal_mode: str = "5s_bucket",
    ) -> None:
        from volatility_bot.strategy import compute_position_pct, compute_market_minute

        mid = market.market_id
        signal = SignalResult(
            should_bet=True,
            side=signal_side,
            delta_pct=round(delta_pct, 4),
            position_pct=compute_position_pct(now, market.market_start, market.interval_minutes),
            market_minute=compute_market_minute(now, market.market_start),
            reason="binance_momentum",
        )

        self._log_signal(market, signal, price, now, bet_placed=True)

        # Цена из фонового поллинга (до сигнала) — для аналитики
        signal_ask = market.yes_ask if signal_side == "yes" else market.no_ask

        # REST запрос: актуальная цена и ликвидность
        available_usd = self._check_depth(market, signal_side)
        if available_usd < self._stake_usd:
            print(
                f"[momentum] skip {market.symbol} {signal_side}: "
                f"depth ${available_usd:.2f} < ${self._stake_usd}"
            )
            return

        # Цена из REST стакана (обновлена _check_depth) — по ней ставим
        entry_ask = market.yes_ask if signal_side == "yes" else market.no_ask

        if entry_ask <= 0 or entry_ask >= 0.95:
            print(
                f"[momentum] skip {market.symbol} {signal_side}: "
                f"invalid REST price {entry_ask:.3f}"
            )
            return

        # Дешёвые ставки (< 0.50) требуют большей дельты
        if entry_ask < 0.50 and abs(delta_pct) < self._momentum_cheap_delta:
            print(
                f"[momentum] skip cheap {market.symbol} {signal_side}: "
                f"price {entry_ask:.2f} < 0.50, delta {abs(delta_pct):.4f}% < {self._momentum_cheap_delta}%"
            )
            return

        crossing_seq = self._db.count_bets_for_market(mid) + 1
        bet_id = self._place_paper_bet(market, signal, price, now, crossing_seq,
                                       depth_usd=available_usd, signal_ask=signal_ask,
                                       signal_mode=signal_mode)

        # Регистрируем в поллере для записи pm_price_after
        if bet_id and hasattr(self, "_after_pending"):
            token_id = market.yes_token_id if signal_side == "yes" else market.no_token_id
            if token_id:
                with self._after_pending_lock:
                    self._after_pending[bet_id] = (token_id, signal_side)

        with self._placed_lock:
            self._momentum_markets_bet.add(mid)
            self._last_bet_time[mid] = time.time()

        if market.venue == "polymarket" and self._real and self._pm_real:
            threading.Thread(
                target=self._real.try_place,
                args=(market, signal, price, now),
                kwargs=dict(delta_pct=delta_pct, cheap_delta=self._momentum_cheap_delta),
                daemon=True,
                name=f"real-{market.symbol}-{signal.side}",
            ).start()

    # ── PM WS callback ────────────────────────────────────────────────────

    def _on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        if self._strategy_mode == "binance_momentum":
            return  # momentum fires only from _on_momentum_price
        now = datetime.utcnow()
        if now < market.market_start or now >= market.expiry:
            return
        current_price = self._price_feed.get_price(market.symbol)
        if current_price is None:
            return
        self._check_signal(market, current_price, now)

    # ── Signal evaluation ─────────────────────────────────────────────────

    def _check_signal(self, market: OracleMarket, current_price: float, now: datetime) -> None:
        # Per-venue paper gate
        if market.venue == "polymarket" and not self._pm_paper:
            return
        if market.venue == "kalshi" and not self._kalshi_paper:
            return

        mid = market.market_id

        # Previous tick from book poller (updated every ~4-6s, matches backtest)
        prev_tick = self._prev_market_tick.get(mid)

        _asks_cache: dict[tuple[str, str], list[tuple[float, float]]] = {}

        # Evaluate each paper config independently
        for pcfg in self._paper_configs:
            # Venue filter: skip if config is restricted to a different venue
            cfg_venue = pcfg.get("venue")
            if cfg_venue and cfg_venue != market.venue:
                continue

            cfg_name = pcfg["name"]
            cfg_key = (cfg_name, mid)

            with self._placed_lock:
                last_side = self._last_bet_side.get(cfg_key)

            if self._strategy_mode == "cl_contradiction":
                cl_prev = (
                    self._price_feed.get_prev_price(market.symbol)
                    if isinstance(self._price_feed, ChainlinkFeed)
                    else None
                )
                binance_price = (
                    self._binance_feed.get_price(market.symbol)
                    if self._binance_feed is not None
                    else self._price_feed.get_price(market.symbol)
                )
                signal = evaluate_cl_contradiction_signal(
                    market=market,
                    cl_price=current_price,
                    cl_prev_price=cl_prev,
                    binance_price=binance_price,
                    now=now,
                    last_bet_side=last_side,
                    min_cl_delta_pct=self._min_cl_delta_pct,
                )
            else:
                signal = evaluate_oracle_signal(
                    market=market,
                    current_price=current_price,
                    now=now,
                    delta_threshold_pct=pcfg["delta_pct"],
                    max_entry_price=pcfg["max_ask"],
                    last_bet_side=last_side,
                )

            # Diagnostic: track delta distribution (once, for first config only)
            if pcfg is self._paper_configs[0]:
                self._track_delta(market, signal.delta_pct, market.no_ask)

            if not signal.should_bet:
                continue

            # Time-from-start filter (V1/V2-style configs)
            max_secs = pcfg.get("max_secs_from_start")
            min_secs_map = pcfg.get("min_secs_from_start")
            if max_secs is not None or min_secs_map is not None:
                elapsed = (now - market.market_start).total_seconds()
                im_key = str(market.interval_minutes)
                min_secs = 0
                if min_secs_map and im_key in min_secs_map:
                    min_secs = min_secs_map[im_key]
                if max_secs is not None and elapsed > max_secs:
                    continue
                if elapsed < min_secs:
                    continue

            # Delta volatility filter (V2-style configs)
            max_delta_std = pcfg.get("max_delta_std")
            if max_delta_std is not None:
                try:
                    deltas = self._db.get_market_deltas(mid)
                    if len(deltas) >= 3:
                        std = statistics.pstdev(deltas)
                        if std > max_delta_std:
                            continue
                except Exception:
                    pass  # no data yet → allow bet

            # Binance-move filter: avg over last 5s must confirm direction
            if pcfg.get("require_binance_move"):
                window = self._price_window.get(mid, [])
                now_wall = time.time()
                recent = [(bp, ya, na) for ts, bp, ya, na in window if ts >= now_wall - 5.0]
                if len(recent) >= 3:
                    avg_bp = sum(bp for bp, _, _ in recent) / len(recent)
                    if signal.side == "yes":
                        ya_vals = [ya for _, ya, _ in recent if ya is not None]
                        avg_ya = sum(ya_vals) / len(ya_vals) if ya_vals else None
                        # Binance avg was lower → rose to signal; PM ask not below avg
                        if avg_bp >= current_price:
                            continue
                        if avg_ya is not None and market.yes_ask is not None and market.yes_ask < avg_ya:
                            continue
                    else:
                        na_vals = [na for _, _, na in recent if na is not None]
                        avg_na = sum(na_vals) / len(na_vals) if na_vals else None
                        # Binance avg was higher → fell to signal; PM ask not below avg
                        if avg_bp <= current_price:
                            continue
                        if avg_na is not None and market.no_ask is not None and market.no_ask < avg_na:
                            continue

            # Одна ставка на рынок на конфиг — если уже ставили, пропускаем
            with self._placed_lock:
                if self._last_bet_side.get(cfg_key) is not None:
                    continue
                self._last_bet_side[cfg_key] = signal.side

            # Логируем (один раз — для первого конфига)
            if pcfg is self._paper_configs[0]:
                self._log_signal(market, signal, current_price, now, bet_placed=True)

            # Real trading FIRST (PM only, trigger on first config only) — скорость важна
            if pcfg is self._paper_configs[0]:
                if market.venue == "polymarket" and self._real and self._pm_real:
                    real_ok = True
                    # Time filter for real trading
                    r_max = self._real_time_filter.get("max_secs_from_start")
                    r_min_map = self._real_time_filter.get("min_secs_from_start")
                    if r_max is not None or r_min_map is not None:
                        elapsed = (now - market.market_start).total_seconds()
                        im_key = str(market.interval_minutes)
                        r_min = 0
                        if r_min_map and im_key in r_min_map:
                            r_min = r_min_map[im_key]
                        if r_max is not None and elapsed > r_max:
                            real_ok = False
                        if elapsed < r_min:
                            real_ok = False
                    # Binance-move filter for real trading (avg-based)
                    if real_ok and self._real_require_binance_move:
                        window = self._price_window.get(mid, [])
                        now_wall = time.time()
                        recent = [(bp, ya, na) for ts, bp, ya, na in window if ts >= now_wall - 5.0]
                        if len(recent) >= 3:
                            avg_bp = sum(bp for bp, _, _ in recent) / len(recent)
                            if signal.side == "yes":
                                ya_vals = [ya for _, ya, _ in recent if ya is not None]
                                avg_ya = sum(ya_vals) / len(ya_vals) if ya_vals else None
                                if avg_bp >= current_price:
                                    real_ok = False
                                elif avg_ya is not None and market.yes_ask is not None and market.yes_ask < avg_ya:
                                    real_ok = False
                            else:
                                na_vals = [na for _, _, na in recent if na is not None]
                                avg_na = sum(na_vals) / len(na_vals) if na_vals else None
                                if avg_bp <= current_price:
                                    real_ok = False
                                elif avg_na is not None and market.no_ask is not None and market.no_ask < avg_na:
                                    real_ok = False
                    if real_ok:
                        threading.Thread(
                            target=self._real.try_place,
                            args=(market, signal, current_price, now),
                            kwargs=dict(delta_pct=signal.delta_pct, cheap_delta=self._momentum_cheap_delta),
                            daemon=True,
                            name=f"real-{market.symbol}-{signal.side}",
                        ).start()

            # Достаём стакан (один раз на рынок, кэшируем)
            cache_key = (mid, signal.side)
            if cache_key not in _asks_cache:
                _asks_cache[cache_key] = self._fetch_asks(market, signal.side)
            asks = _asks_cache[cache_key]

            # Симулируем fill по стакану с лимитом конфига
            fill_price, depth_usd = self._simulate_fill(asks, self._stake_usd, pcfg["max_ask"])

            if fill_price is None:
                if pcfg is self._paper_configs[0]:
                    print(
                        f"[oracle] skip {market.symbol} [{market.venue}] {signal.side}: "
                        f"depth ${depth_usd:.2f} < ${self._stake_usd} (max_ask={pcfg['max_ask']})"
                    )
                with self._placed_lock:
                    self._last_bet_side[cfg_key] = None  # разрешаем повторную попытку
                continue

            # Snapshot 10s price window for future backtests
            window = self._price_window.get(mid, [])
            pst = json.dumps([
                {"ts": round(ts, 2), "bp": bp, "ya": ya, "na": na}
                for ts, bp, ya, na in window
            ]) if window else None

            self._place_paper_bet(
                market, signal, current_price, now,
                crossing_seq=1, depth_usd=depth_usd,
                paper_config=cfg_name, max_ask=pcfg["max_ask"],
                fill_price=fill_price,
                pre_signal_ticks=pst,
            )

    def _depth_price_limit(self, best_ask: float) -> float:
        """Верхняя граница цены при проверке стакана."""
        if self._strategy_mode in ("cl_contradiction", "binance_momentum"):
            return best_ask + self._depth_slippage_cents / 100
        return self._paper_max_price

    def _fetch_asks(self, market: OracleMarket, side: str) -> list[tuple[float, float]]:
        """Достаёт ask-уровни стакана [(price, size), ...].
        Побочный эффект: обновляет market.yes_ask/no_ask из реального стакана."""
        asks: list[tuple[float, float]] = []

        if market.venue == "kalshi":
            kalshi_feed = self._scanner.kalshi_feed
            if kalshi_feed:
                try:
                    raw_asks, err = kalshi_feed.fetch_side_asks(market.market_id, side)
                    if raw_asks:
                        asks = [(a.price, a.size) for a in raw_asks]
                except Exception as exc:
                    print(f"[oracle] Kalshi depth check failed {market.symbol}: {exc}")
        else:
            token_id = market.yes_token_id if side == "yes" else market.no_token_id
            if token_id:
                try:
                    book = self._clob.get_orderbook(token_id)
                    if book and book.asks:
                        asks = [(a.price, a.size) for a in book.asks]
                except Exception as exc:
                    print(f"[oracle] CLOB depth check failed {market.symbol}: {exc}")

        if asks:
            real_best_ask = asks[0][0]
            if side == "yes":
                market.yes_ask = real_best_ask
            else:
                market.no_ask = real_best_ask

        return asks

    @staticmethod
    def _simulate_fill(
        asks: list[tuple[float, float]], stake_usd: float, max_price: float,
    ) -> tuple[float | None, float]:
        """Симулирует FOK fill: идёт по ask-уровням, тратит до stake_usd.
        Возвращает (avg_fill_price, depth_usd). avg_fill_price=None если не хватило ликвидности."""
        remaining = stake_usd
        total_shares = 0.0
        total_cost = 0.0
        depth_usd = 0.0

        for price, size in asks:
            if price > max_price:
                break
            level_usd = price * size
            depth_usd += level_usd
            can_spend = min(remaining, level_usd)
            shares = can_spend / price
            total_cost += can_spend
            total_shares += shares
            remaining -= can_spend
            if remaining < 0.001:
                break

        if remaining > 0.01:
            return None, depth_usd  # не хватило ликвидности

        avg_price = total_cost / total_shares if total_shares > 0 else None
        return avg_price, depth_usd

    def _check_depth(self, market: OracleMarket, side: str) -> float:
        """Обратная совместимость для momentum mode."""
        asks = self._fetch_asks(market, side)
        price_limit = self._depth_price_limit(
            asks[0][0] if asks else 0.0
        )
        _, depth_usd = self._simulate_fill(asks, self._stake_usd, price_limit)
        return depth_usd

    def _place_paper_bet(
        self,
        market: OracleMarket,
        signal: SignalResult,
        current_price: float,
        now: datetime,
        crossing_seq: int = 1,
        depth_usd: float = 0.0,
        signal_ask: float = 0.0,
        signal_mode: str = "5s_bucket",
        paper_config: str | None = None,
        max_ask: float | None = None,
        fill_price: float | None = None,
        pre_signal_ticks: str | None = None,
    ) -> None:
        raw_ask = market.yes_ask if signal.side == "yes" else market.no_ask
        if raw_ask <= 0:
            return

        # Use per-config max_ask if provided, else fallback to paper_max_price
        effective_max = max_ask if max_ask is not None else self._paper_max_price

        if fill_price is not None:
            # Средневзвешенная цена из симуляции стакана
            entry_price = round(fill_price, 4)
        else:
            # Fallback (momentum и пр.): округляем ask вверх до 2 знаков
            entry_price = float(
                Decimal(str(raw_ask)).quantize(Decimal("0.01"), rounding=ROUND_UP)
            )

        if self._strategy_mode == "cl_contradiction":
            if not (self._active_zone_min <= entry_price <= self._active_zone_max):
                return
        elif self._strategy_mode == "binance_momentum":
            if not (self._active_zone_min <= entry_price <= self._active_zone_max):
                return
        else:
            # crossing — cap at effective max
            if entry_price <= 0 or entry_price > effective_max:
                return

        if entry_price <= 0 or entry_price < 0.15:
            return

        shares = self._stake_usd / entry_price

        # Analytics fields
        seconds_to_close = int((market.expiry - now).total_seconds()) if market.expiry else None
        opposite_ask = market.no_ask if signal.side == "yes" else market.yes_ask

        bet = OracleBet(
            id=str(uuid.uuid4()),
            market_id=market.market_id,
            symbol=market.symbol,
            interval_minutes=market.interval_minutes,
            market_start=market.market_start,
            market_end=market.expiry,
            opened_at=now,
            market_minute=signal.market_minute,
            position_pct=round(signal.position_pct, 4),
            side=signal.side,
            entry_price=entry_price,
            shares=shares,
            total_cost=self._stake_usd,
            binance_price_at_start=market.binance_price_at_start,
            binance_price_at_bet=current_price,
            delta_pct=round(signal.delta_pct, 4),
            pm_open_price=market.pm_open_price or 0.0,
            venue=market.venue,
            seconds_to_close=seconds_to_close,
            opposite_ask=round(opposite_ask, 4) if opposite_ask else None,
            depth_usd=round(depth_usd, 2),
            volume=round(market.volume, 2) if market.volume else None,
            strategy=self._strategy_mode,
            paper_config=paper_config,
        )

        self._db.record_bet(bet, crossing_seq=crossing_seq, signal_ask=signal_ask,
                            signal_mode=signal_mode, paper_config=paper_config,
                            pre_signal_ticks=pre_signal_ticks)
        self._db.mark_signal_bet_placed(market.market_id, signal.side)

        # Фиксируем цену через 10 секунд (only first config to avoid parallel requests)
        if market.venue == "polymarket" and (paper_config is None or paper_config == self._paper_configs[0]["name"]):
            token_id = market.yes_token_id if signal.side == "yes" else market.no_token_id
            if token_id:
                threading.Thread(
                    target=self._record_price_after_delay,
                    args=(bet.id, token_id, 10, "bets"),
                    daemon=True,
                ).start()

        seq_tag = " [#1-NEW]" if crossing_seq == 1 else f" [#{crossing_seq}-REPEAT]"
        venue_tag = f" [{market.venue}]" if market.venue != "polymarket" else ""
        cfg_tag = f" [{paper_config}]" if paper_config else ""
        self._db.audit("bet_placed", bet.id, {
            "venue": market.venue,
            "symbol": market.symbol,
            "side": signal.side,
            "entry_price": entry_price,
            "delta_pct": round(signal.delta_pct, 4),
            "pm_open_price": market.pm_open_price,
            "price_source": self._price_source,
            "source_price_at_bet": current_price,
            "source_price_at_start": market.binance_price_at_start,
            "market_minute": signal.market_minute,
            "interval_minutes": market.interval_minutes,
        })

        print(
            f"[oracle] PAPER BET{cfg_tag}{venue_tag}{seq_tag} {market.symbol} {market.interval_minutes}m "
            f"{signal.side.upper()} @ {entry_price:.3f} "
            f"| Δ{signal.delta_pct:+.3f}% | ${self._stake_usd:.0f} | min={signal.market_minute}"
        )

        # Telegram only for first config to avoid spam
        if self._tg and (paper_config is None or paper_config == self._paper_configs[0]["name"]):
            self._tg.send_bet(
                market.symbol, signal.side, entry_price, signal.delta_pct, self._stake_usd,
                label="paper", market_slug=market.pm_event_slug,
                venue=market.venue, market_id=market.market_id,
                pre_price=signal_ask,
            )

        return bet.id

    def _record_price_after_delay(self, bet_id: str, token_id: str, delay: int, table: str) -> None:
        time.sleep(delay)
        try:
            price = self._clob.get_midpoint(token_id)
            if price is not None:
                self._db.update_price_10s(bet_id, price, table)
        except Exception as exc:
            print(f"[oracle] price_10s fetch failed: {exc}")

    # ── NO rejection diagnostics ────────────────────────────────────────

    def _track_delta(self, market: OracleMarket, delta_pct: float, no_ask: float) -> None:
        """Track min/max delta per market. Print summary every 60s."""
        mid = market.market_id
        key = f"{market.symbol}_{market.interval_minutes}m"
        if mid not in self._delta_tracker:
            self._delta_tracker[mid] = {
                "key": key, "min": delta_pct, "max": delta_pct,
                "neg_count": 0, "neg_cheap_count": 0, "total": 0,
            }
        t = self._delta_tracker[mid]
        t["min"] = min(t["min"], delta_pct)
        t["max"] = max(t["max"], delta_pct)
        t["total"] += 1
        if delta_pct < 0:
            t["neg_count"] += 1
            if no_ask < self._max_entry_price:
                t["neg_cheap_count"] += 1

        now_ts = time.time()
        if now_ts - self._delta_tracker_last_print > 60:
            self._delta_tracker_last_print = now_ts
            active_ids = {m.market_id for m in self._scanner.all_markets()}
            items = sorted(
                (v for k, v in self._delta_tracker.items() if k in active_ids),
                key=lambda x: x["key"],
            )
            print("[DELTA-DIAG] per-market delta range (last period):")
            for t in items:
                print(
                    f"  {t['key']:12s} min={t['min']:+.4f}% max={t['max']:+.4f}% "
                    f"neg={t['neg_count']}/{t['total']} neg_cheap={t['neg_cheap_count']}"
                )

    def _log_no_rejection(self, market: OracleMarket, signal: SignalResult, current_price: float) -> None:
        """Log when delta < -threshold but NO signal didn't fire. Rate-limited: once per market per 30s."""
        key = market.market_id
        now_ts = time.time()
        if not hasattr(self, "_no_diag_last"):
            self._no_diag_last: dict[str, float] = {}
        if key in self._no_diag_last and now_ts - self._no_diag_last[key] < 30:
            return
        self._no_diag_last[key] = now_ts
        print(
            f"[DEBUG-NO] {market.symbol} {market.interval_minutes}m [{market.venue}] "
            f"Δ={signal.delta_pct:+.4f}% no_ask={market.no_ask:.4f} yes_ask={market.yes_ask:.4f} "
            f"reason={signal.reason} | price={current_price:.2f} open={market.pm_open_price}"
        )

    # ── Signal logging ───────────────────────────────────────────────────

    def _log_signal(
        self,
        market: OracleMarket,
        signal: SignalResult,
        current_price: float,
        now: datetime,
        bet_placed: bool,
    ) -> None:
        side = "yes" if signal.delta_pct > 0 else "no"

        s = OracleSignal(
            id=str(uuid.uuid4()),
            market_id=market.market_id,
            symbol=market.symbol,
            interval_minutes=market.interval_minutes,
            market_minute=signal.market_minute,
            position_pct=round(signal.position_pct, 4),
            fired_at=now,
            side=side,
            delta_pct=round(signal.delta_pct, 4),
            pm_open_price=market.pm_open_price or 0.0,
            binance_price=current_price,
            pm_yes_ask=market.yes_ask,
            pm_no_ask=market.no_ask,
            bet_placed=bet_placed,
        )
        self._db.record_signal(s)

        already_priced = (
            (side == "yes" and market.yes_ask >= self._max_entry_price) or
            (side == "no" and market.no_ask >= self._max_entry_price)
        )
        pm_price = market.yes_ask if side == "yes" else market.no_ask
        tag = "PRICED_IN" if already_priced else "SIGNAL"
        print(
            f"[oracle] {tag} {market.symbol} {market.interval_minutes}m {side.upper()} "
            f"| Δ{signal.delta_pct:+.3f}% | pm_{side}={pm_price:.3f} | min={signal.market_minute}"
        )


    # ── Open price fetching ───────────────────────────────────────────────

    def _fetch_pm_open_prices(self) -> None:
        """Загружает pm_open_price для PM рынков (Kalshi получает reference_price сразу из REST)."""
        now = datetime.utcnow()
        for market in self._scanner.all_markets():
            if market.pm_open_price is not None:
                continue
            if market.venue != "polymarket":
                continue  # Kalshi уже имеет reference_price
            if market.pm_event_slug is None:
                continue
            if now < market.market_start:
                continue
            price = fetch_pm_open_price(market.pm_event_slug)
            if price is not None:
                market.pm_open_price = price
                print(
                    f"[oracle] openPrice {market.symbol} {market.interval_minutes}m "
                    f"= {price} (slug={market.pm_event_slug})"
                )

    # ── Resolution ────────────────────────────────────────────────────────

    def _resolve_expired(self) -> None:
        now = datetime.utcnow()
        for bet in self._db.get_open_bets():
            if bet.market_end and bet.market_end <= now:
                self._resolve_one(bet)
        if self._real:
            for bet in self._db.get_open_real_bets():
                if bet.market_end and bet.market_end <= now:
                    self._resolve_real_one(bet)

    def _resolve_one(self, bet: OracleBet) -> None:
        if bet.venue == "kalshi":
            winning_side = check_kalshi_result(bet.market_id)
        else:
            winning_side = check_polymarket_result(bet.market_id)
        if winning_side is None:
            return

        close_price = None
        if bet.venue == "polymarket":
            slug = self._make_slug(bet)
            close_price = fetch_pm_close_price(slug) if slug else None

        # Capture source price at resolution time
        binance_at_close = self._price_feed.get_price(bet.symbol)

        won = winning_side == bet.side
        pnl = (bet.shares - bet.total_cost) if won else -bet.total_cost

        self._db.resolve_bet(bet.id, winning_side, close_price, round(pnl, 6), binance_at_close)
        self._db.audit("bet_resolved", bet.id, {
            "venue": bet.venue,
            "symbol": bet.symbol,
            "side": bet.side,
            "winning_side": winning_side,
            "won": won,
            "pnl": round(pnl, 4),
            "pm_close_price": close_price,
        })

        venue_tag = f" [{bet.venue}]" if bet.venue != "polymarket" else ""
        tag = "WIN" if won else "LOSS"
        print(
            f"[oracle][resolve]{venue_tag} {bet.symbol} {bet.side} → {winning_side} "
            f"| {tag} | pnl=${pnl:+.2f}"
        )

        if self._tg:
            self._tg.send_resolve(bet.symbol, bet.side, won, pnl, label="paper", venue=bet.venue)

    def _resolve_real_one(self, bet) -> None:
        from oracle_arb_bot.models import RealBet
        winning_side = check_polymarket_result(bet.market_id)
        if winning_side is None:
            return
        slug = f"{bet.symbol.lower()}-updown-{bet.interval_minutes}m-{calendar.timegm(bet.market_start.timetuple())}"
        close_price = fetch_pm_close_price(slug)
        pnl = self._real.resolve(bet, winning_side, close_price)
        if pnl is not None and self._tg:
            won = winning_side == bet.side
            self._tg.send_resolve(bet.symbol, bet.side, won, pnl, label="real", venue="polymarket")

    def _make_slug(self, bet: OracleBet) -> Optional[str]:
        if bet.market_start is None:
            return None
        ts = calendar.timegm(bet.market_start.timetuple())
        return f"{bet.symbol.lower()}-updown-{bet.interval_minutes}m-{ts}"
