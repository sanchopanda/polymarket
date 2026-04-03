from __future__ import annotations

import calendar
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
        self._momentum_buckets: dict[str, tuple[int, float, float | None]] = {}  # symbol → (bucket_ts, last_price, prev_close)
        self._momentum_markets_bet: set[str] = set()  # market_ids где уже поставили
        self._momentum_signal_history: dict[str, list[int]] = {}  # symbol → [bucket_ts, ...]
        self._scan_interval: int = config["runtime"]["scan_interval_seconds"]

        # ── Data collection для бэктестов (SQLite) ────────────────────
        from research_bot.backtest_db import BacktestDB
        self._backtest_db = BacktestDB()
        self._1s_last: dict[str, int] = {}  # symbol → last written sec_ts
        self._1s_close: dict[str, float] = {}  # symbol → last price in current second

        # Per-venue paper/real flags
        self._pm_paper: bool = config["polymarket"].get("paper", True)
        self._pm_real: bool = config["polymarket"].get("real", False)
        self._kalshi_paper: bool = config.get("kalshi", {}).get("paper", False)

        # Crossing tracker: последняя сторона ставки per market
        self._last_bet_side: dict[str, str | None] = {}
        self._depth_cooldown: dict[tuple[str, str], float] = {}
        self._last_bet_time: dict[str, float] = {}   # market_id → timestamp последней ставки
        self._bet_cooldown_s: float = 60.0           # кулдаун между ставками на один рынок
        self._placed_lock = threading.Lock()
        self._load_crossings_from_db()

        # Diagnostic: track min/max delta per market to understand signal distribution
        self._delta_tracker: dict[str, dict] = {}  # market_id → {min, max, neg_count, total}
        self._delta_tracker_last_print: float = 0.0

        # CLOB для проверки PM ликвидности
        self._clob = ClobClient(base_url=config["polymarket"]["clob_base_url"])

        # Сканер PM + Kalshi рынков
        self._scanner = OracleScanner(config)
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
        def _status_fn() -> str:
            text = db.get_status_text()
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

        self._tg = (
            OracleTelegramNotifier(get_status_fn=_status_fn)
            if config.get("telegram", {}).get("enabled")
            else None
        )
        if self._real:
            self._real._tg = self._tg

    def _load_crossings_from_db(self) -> None:
        """Восстанавливаем last_bet_side и momentum_markets_bet из открытых ставок."""
        for bet in self._db.get_open_bets():
            self._last_bet_side[bet.market_id] = bet.side
            self._momentum_markets_bet.add(bet.market_id)

    # ── Main loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        print("[oracle] Starting OracleArbBot")
        print(f"[oracle] strategy_mode={self._strategy_mode}")
        if self._tg:
            self._tg.start()
        self._price_feed.start()
        if self._binance_feed is not None:
            self._binance_feed.start()
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

        # Пишем все рынки в backtest DB (winning_side обновится при резолве)
        for m in markets:
            ms = m.market_start.strftime("%Y-%m-%d %H:%M:%S")
            me = m.expiry.strftime("%Y-%m-%d %H:%M:%S")
            self._backtest_db.write_market(m.market_id, "", m.symbol, m.interval_minutes, ms, me)

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

    # ── Data collection ─────────────────────────────────────────────────

    _logged_markets: set[str] = set()
    def _log_pm_price(self, market_id: str, side: str, price: float) -> None:
        """Записывает PM цену в backtest DB."""
        if price <= 0 or price >= 0.95:
            return  # стейл/невалидная цена из WS
        ts = int(time.time())
        outcome = "Up" if side == "yes" else "Down"
        self._backtest_db.write_trade(market_id, ts, outcome, price)

    def _log_resolved_market(self, bet: OracleBet, winning_side: str) -> None:
        """Записывает резолвнутый рынок в backtest DB (один раз на market_id)."""
        if bet.market_id in self._logged_markets:
            return
        self._logged_markets.add(bet.market_id)
        ms = bet.market_start.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bet.market_start, "strftime") else bet.market_start
        me = bet.market_end.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bet.market_end, "strftime") else bet.market_end
        self._backtest_db.write_market(
            bet.market_id, "", bet.symbol, bet.interval_minutes, ms, me, winning_side,
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
                self._backtest_db.write_1s(symbol, prev, self._1s_close[symbol])
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
        self._backtest_db.write_5s(symbol, curr_bucket, bucket_close)

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

    def _fire_momentum_bet(
        self,
        market: OracleMarket,
        signal_side: str,
        delta_pct: float,
        price: float,
        now: datetime,
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

        # PM цена в момент сигнала (WS)
        signal_ask = market.yes_ask if signal_side == "yes" else market.no_ask

        # Стейловая WS цена — не знаем реальную цену, скипаем
        if signal_ask <= 0 or signal_ask >= 0.95:
            print(
                f"[momentum] skip {market.symbol} {signal_side}: "
                f"stale WS price {signal_ask:.3f}"
            )
            return

        available_usd = self._check_depth(market, signal_side)
        if available_usd < self._stake_usd:
            print(
                f"[momentum] skip {market.symbol} {signal_side}: "
                f"depth ${available_usd:.2f} < ${self._stake_usd}"
            )
            return

        # Реальная цена стакана после depth check
        entry_ask = market.yes_ask if signal_side == "yes" else market.no_ask

        # Цена убежала от signal_ask — не ставить (как в реальном боте: макс +3c)
        if entry_ask > signal_ask + 0.03:
            print(
                f"[momentum] skip {market.symbol} {signal_side}: "
                f"price moved {signal_ask:.3f} → {entry_ask:.3f} (+{(entry_ask-signal_ask)*100:.1f}c)"
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
        self._place_paper_bet(market, signal, price, now, crossing_seq,
                              depth_usd=available_usd, signal_ask=signal_ask)

        with self._placed_lock:
            self._momentum_markets_bet.add(mid)
            self._last_bet_time[mid] = time.time()

        if market.venue == "polymarket" and self._real and self._pm_real:
            self._real.try_place(market, signal, price, now,
                                delta_pct=delta_pct, cheap_delta=self._momentum_cheap_delta)

    # ── PM WS callback ────────────────────────────────────────────────────

    def _on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        # ── логируем PM цену для бэктестов ──
        self._log_pm_price(market.market_id, side, best_ask)

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

        # Получаем last_bet_side атомарно
        with self._placed_lock:
            last_side = self._last_bet_side.get(mid)

        if self._strategy_mode == "cl_contradiction":
            # current_price = CL цена (тик только что пришёл)
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
                delta_threshold_pct=self._delta_threshold_pct,
                max_entry_price=self._max_entry_price,
                last_bet_side=last_side,
            )

        # Diagnostic: track delta distribution
        self._track_delta(market, signal.delta_pct, market.no_ask)

        if not signal.should_bet:
            # Diagnostic: track negative delta rejections
            if signal.delta_pct < -self._delta_threshold_pct:
                self._log_no_rejection(market, signal, current_price)
            return

        # Кулдаун 60s между ставками на один рынок
        with self._placed_lock:
            last_bet_ts = self._last_bet_time.get(mid, 0.0)
        if time.time() - last_bet_ts < self._bet_cooldown_s:
            return

        # Атомарно проверяем и обновляем last_bet_side (защита от дублей между тиками)
        with self._placed_lock:
            if self._last_bet_side.get(mid) == signal.side:
                return
            cooldown_key = (mid, signal.side)
            now_ts = time.time()
            if cooldown_key in self._depth_cooldown:
                if now_ts - self._depth_cooldown[cooldown_key] < 5:
                    return
            self._last_bet_side[mid] = signal.side

        # Логируем пересечение (один раз, после прохождения дедупа/кулдауна)
        self._log_signal(market, signal, current_price, now, bet_placed=True)

        # Проверка ликвидности (разная для PM и Kalshi)
        available_usd = self._check_depth(market, signal.side)

        if available_usd < self._stake_usd:
            print(
                f"[oracle] skip {market.symbol} [{market.venue}] {signal.side}: "
                f"depth ${available_usd:.2f} < ${self._stake_usd} (max_price={self._paper_max_price})"
            )
            with self._placed_lock:
                self._last_bet_side[mid] = last_side
                self._depth_cooldown[cooldown_key] = now_ts
            return

        crossing_seq = self._db.count_bets_for_market(mid) + 1
        self._place_paper_bet(market, signal, current_price, now, crossing_seq, depth_usd=available_usd)
        with self._placed_lock:
            self._last_bet_time[mid] = time.time()

        # Real trading (PM only для теперь)
        if market.venue == "polymarket" and self._real and self._pm_real:
            self._real.try_place(market, signal, current_price, now,
                                delta_pct=signal.delta_pct, cheap_delta=self._momentum_cheap_delta)

        is_arb = self._db.has_both_sides(mid)
        if is_arb:
            print(f"[oracle] ARB {market.symbol} {market.interval_minutes}m [{market.venue}] — ставки на обе стороны")

    def _depth_price_limit(self, best_ask: float) -> float:
        """Верхняя граница цены при проверке стакана."""
        if self._strategy_mode in ("cl_contradiction", "binance_momentum"):
            return best_ask + self._depth_slippage_cents / 100
        return self._paper_max_price

    def _check_depth(self, market: OracleMarket, side: str) -> float:
        """Проверяет ликвидность стакана. Возвращает доступный USD до max_price.
        Побочный эффект: обновляет market.yes_ask/no_ask из реального стакана.

        В режиме cl_contradiction: лимит = best_ask + depth_slippage_cents (нет потолка).
        В режиме crossing: лимит = paper_max_price (обычно 0.48).
        """
        available_usd = 0.0
        real_best_ask = None

        if market.venue == "kalshi":
            kalshi_feed = self._scanner.kalshi_feed
            if kalshi_feed:
                try:
                    asks, err = kalshi_feed.fetch_side_asks(market.market_id, side)
                    if asks:
                        real_best_ask = asks[0].price
                        price_limit = self._depth_price_limit(real_best_ask)
                        for level in asks:
                            if level.price > price_limit:
                                break
                            available_usd += level.price * level.size
                except Exception as exc:
                    print(f"[oracle] Kalshi depth check failed {market.symbol}: {exc}")
        else:
            token_id = market.yes_token_id if side == "yes" else market.no_token_id
            if token_id:
                try:
                    book = self._clob.get_orderbook(token_id)
                    if book and book.asks:
                        real_best_ask = book.asks[0].price
                        price_limit = self._depth_price_limit(real_best_ask)
                        for level in book.asks:
                            if level.price > price_limit:
                                break
                            available_usd += level.price * level.size
                except Exception as exc:
                    print(f"[oracle] CLOB depth check failed {market.symbol}: {exc}")

        # Обновляем цену из реального стакана — не даём стейлым ценам триггерить сигналы
        if real_best_ask is not None:
            if side == "yes":
                market.yes_ask = real_best_ask
            else:
                market.no_ask = real_best_ask

        return available_usd

    def _place_paper_bet(
        self,
        market: OracleMarket,
        signal: SignalResult,
        current_price: float,
        now: datetime,
        crossing_seq: int = 1,
        depth_usd: float = 0.0,
        signal_ask: float = 0.0,
    ) -> None:
        raw_ask = market.yes_ask if signal.side == "yes" else market.no_ask
        if raw_ask <= 0:
            return

        # Округляем ask вверх до 2 знаков
        entry_price = float(
            Decimal(str(raw_ask)).quantize(Decimal("0.01"), rounding=ROUND_UP)
        )

        if self._strategy_mode == "cl_contradiction":
            if not (self._active_zone_min <= entry_price <= self._active_zone_max):
                print(
                    f"[oracle] skip paper {market.symbol} {signal.side}: "
                    f"цена {entry_price:.2f} вне зоны "
                    f"[{self._active_zone_min:.2f}, {self._active_zone_max:.2f}]"
                )
                return
        elif self._strategy_mode == "binance_momentum":
            if not (self._active_zone_min <= entry_price <= self._active_zone_max):
                print(
                    f"[momentum] skip paper {market.symbol} {signal.side}: "
                    f"цена {entry_price:.2f} вне зоны "
                    f"[{self._active_zone_min:.2f}, {self._active_zone_max:.2f}]"
                )
                return
        else:
            # crossing — капаем по paper_max_price
            entry_price = min(entry_price, self._paper_max_price)
            if entry_price <= 0 or entry_price > self._paper_max_price:
                print(
                    f"[oracle] skip paper {market.symbol} {signal.side}: "
                    f"цена {entry_price:.2f} > max {self._paper_max_price}"
                )
                return

        if entry_price <= 0:
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
        )

        self._db.record_bet(bet, crossing_seq=crossing_seq, signal_ask=signal_ask)
        self._db.mark_signal_bet_placed(market.market_id, signal.side)

        # Фиксируем цену через 10 секунд после ставки (только PM — у Kalshi нет midpoint API)
        if market.venue == "polymarket":
            token_id = market.yes_token_id if signal.side == "yes" else market.no_token_id
            if token_id:
                threading.Thread(
                    target=self._record_price_after_delay,
                    args=(bet.id, token_id, 10, "bets"),
                    daemon=True,
                ).start()

        seq_tag = " [#1-NEW]" if crossing_seq == 1 else f" [#{crossing_seq}-REPEAT]"
        venue_tag = f" [{market.venue}]" if market.venue != "polymarket" else ""
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
            f"[oracle] PAPER BET{venue_tag}{seq_tag} {market.symbol} {market.interval_minutes}m "
            f"{signal.side.upper()} @ {entry_price:.3f} "
            f"| Δ{signal.delta_pct:+.3f}% | ${self._stake_usd:.0f} | min={signal.market_minute}"
        )

        if self._tg:
            self._tg.send_bet(
                market.symbol, signal.side, entry_price, signal.delta_pct, self._stake_usd,
                label="paper", market_slug=market.pm_event_slug,
                venue=market.venue, market_id=market.market_id,
            )

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

        # Независимо: резолвим все рынки backtest DB без winning_side
        now_ts = int(now.timestamp())
        for m in self._backtest_db.get_unresolved_markets(now_ts):
            try:
                winning_side = check_polymarket_result(m["market_id"])
                if winning_side:
                    self._backtest_db.write_market(
                        m["market_id"], "", m["symbol"], m["interval_minutes"],
                        m["market_start"], m["market_end"], winning_side,
                    )
            except Exception as exc:
                print(f"[backtest] resolve {m['market_id']}: {exc}")

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
        self._log_resolved_market(bet, winning_side)
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
        self._real.resolve(bet, winning_side, close_price)
        if self._tg:
            won = winning_side == bet.side
            pnl = bet.shares_filled - bet.stake_usd if won else -bet.stake_usd
            self._tg.send_resolve(bet.symbol, bet.side, won, pnl, label="real", venue="polymarket")

    def _make_slug(self, bet: OracleBet) -> Optional[str]:
        if bet.market_start is None:
            return None
        ts = calendar.timegm(bet.market_start.timetuple())
        return f"{bet.symbol.lower()}-updown-{bet.interval_minutes}m-{ts}"
