from __future__ import annotations

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook

from cross_arb_bot.matcher import build_opportunities, kalshi_taker_fee, polymarket_crypto_taker_fee
from cross_arb_bot.models import CrossVenueOpportunity, ExecutionLegInfo, MatchedMarketPair
from src.api.clob import OrderLevel

from real_arb_bot.clients import OrderResult
from real_arb_bot.engine import RealArbEngine

from fast_arb_bot.executor import FastArbExecutor, FastExecutionResult, _empty_order

try:
    from oracle_arb_bot.chainlink_feed import ChainlinkFeed as _ChainlinkFeed
except ImportError:
    _ChainlinkFeed = None  # type: ignore[assignment,misc]


@dataclass
class WatchedPair:
    opportunity: CrossVenueOpportunity
    matched: MatchedMarketPair


@dataclass
class PMCompletionCandidate:
    best_ask: float
    avg_price: float
    worst_price: float


@dataclass
class _DangerExit:
    """State for a position in the pre-expiry danger zone sell monitor."""
    pm_yes_token: str | None
    pm_no_token: str | None
    kalshi_ticker: str | None
    venue_yes: str
    venue_no: str
    yes_entry: float
    no_entry: float
    shares: float
    symbol: str
    ref_price: float
    yes_sell_price: float | None = None
    no_sell_price: float | None = None
    timer: threading.Timer | None = None


@dataclass
class _EdgeMonitorState:
    """Per-position edge tracking state."""
    max_edge: float = 0.0
    signal_fired: bool = False
    signal_at: str | None = None
    edge_at_signal: float = 0.0
    # Milestone: when edge first crossed each threshold (for trajectory analysis)
    milestone_10: bool = False
    milestone_15: bool = False
    milestone_20: bool = False
    # Cached market IDs so we can look up live books even after pair leaves watch list
    pm_yes_token_id: str | None = None
    pm_no_token_id: str | None = None
    kalshi_market_id: str | None = None
    interval_minutes: int | None = None


class FastArbWatchRunner:
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    STATUS_INTERVAL_SECONDS = 15
    HIGH_EDGE_THRESHOLD = 0.15

    def __init__(self, engine: RealArbEngine, dry_run: bool = False) -> None:
        self.engine = engine
        self.dry_run = dry_run or bool(engine.safety.dry_run)

        fast_cfg = engine.config.get("fast_arb", {})
        self.budget_usd: float = float(fast_cfg.get("budget_usd", 40.0))
        self.max_realized_loss_usd: float = float(fast_cfg.get("max_realized_loss_usd", 20.0))
        self.liquidity_ratio: float = float(fast_cfg.get("order_book_min_liquidity_ratio", 1.5))
        self.scan_interval: float = float(fast_cfg.get("scan_interval_seconds", 60.0))

        safety_cfg = engine.config.get("safety", {})
        self.kalshi_slippage_cents: int = int(safety_cfg.get("kalshi_slippage_cents", 1))
        self.pm_price_buffer: float = safety_cfg.get("polymarket_price_buffer_cents", 2) / 100.0

        self.executor = FastArbExecutor(
            pm_trader=engine.pm_trader,
            kalshi_trader=engine.kalshi_trader,
            db=engine.db,
            kalshi_slippage_cents=self.kalshi_slippage_cents,
            pm_price_buffer=self.pm_price_buffer,
        )

        self.live_books: dict[str, TopOfBook] = {}
        self.live_books_kalshi: dict[str, KalshiTopOfBook] = {}
        self.watch_by_pair_key: dict[str, WatchedPair] = {}
        self.pairs_by_asset_id: dict[str, set[str]] = {}
        self.pairs_by_kalshi_ticker: dict[str, set[str]] = {}
        self._kalshi_ws: KalshiWebSocketClient | None = None
        self._signal_lock = threading.Lock()
        self._last_skip_log: dict[str, float] = {}
        self._SKIP_LOG_INTERVAL = 30.0
        self._last_completion_attempt: dict[str, float] = {}
        self._COMPLETION_COOLDOWN = 5.0  # секунд между попытками докупки
        self._last_kalshi_resting_sync_ts: float = 0.0
        self._KALSHI_RESTING_SYNC_INTERVAL = 2.0
        self._cached_balances: dict = {"polymarket": None, "kalshi": None}
        self._last_scan_ts: float = 0.0
        self._pm_price_cache: dict[str, float] = {}  # slug → openPrice
        # one_legged_polymarket cancel/reenter state machine
        # pos_id → pm_live_ask at cancellation time
        self._one_leg_pm_cancelled: dict[str, float] = {}
        # pos_id → True means re-entered once, don't cancel again
        self._one_leg_pm_final: set[str] = set()
        # pair_key → pending Timer (paper mode: 0.5s delay before orderbook check)
        self._pending_timers: dict[str, threading.Timer] = {}

        # Edge divergence monitor (replaces Chainlink danger zone)
        edge_cfg = fast_cfg.get("edge_monitor", {})
        self._edge_signal_threshold: float = float(edge_cfg.get("signal_threshold", 0.20))
        self._edge_monitor_seconds: float = float(edge_cfg.get("monitor_seconds", 300))
        self._edge_monitor_seconds_1h: float = float(edge_cfg.get("monitor_seconds_1h", 600))
        self._edge_sell_delay: float = float(edge_cfg.get("sell_delay_seconds", 1.0))
        self._edge_states: dict[str, _EdgeMonitorState] = {}  # pos_id → state
        # Binance price cache for distance calculation: symbol → (price, last_fetch_ts)
        self._binance_cache: dict[str, tuple[float, float]] = {}
        self._binance_fetch_interval: float = 5.0  # seconds between fetches

        # Chainlink pre-expiry danger zone monitor (disabled — replaced by edge monitor)
        cl_cfg = engine.config.get("chainlink", {})
        self._danger_zone_pct: float = float(cl_cfg.get("danger_zone_pct", 0.05))
        self._pre_expiry_seconds: float = float(cl_cfg.get("pre_expiry_check_seconds", 120))
        self._chainlink: _ChainlinkFeed | None = None  # type: ignore[valid-type]
        self._danger_exits: dict[str, _DangerExit] = {}  # pos_id → sell state
        if _ChainlinkFeed and cl_cfg.get("rpc_urls"):
            _cl_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
            self._chainlink = _ChainlinkFeed(
                symbols=_cl_symbols,
                on_price=lambda sym, price, ts: None,
                rpc_urls=cl_cfg["rpc_urls"],
                wss_urls=cl_cfg.get("wss_urls"),
                poll_interval_seconds=float(cl_cfg.get("poll_interval_seconds", 30)),
            )

        # Подменяем функцию статуса в Telegram-нотификаторе
        if engine.notifier:
            engine.notifier._get_status = self._get_status_text

    # ── Основной цикл ──────────────────────────────────────────────────

    _DANGER_CHECK_INTERVAL = 10.0  # Chainlink updates ~every 27s; 10s is enough

    def run(self) -> None:
        ws: MarketWebSocketClient | None = None
        last_status = 0.0
        last_danger_check = 0.0

        if self._chainlink:
            self._chainlink.start()
            # Chainlink danger-zone disabled — edge monitor replaces it
            # print(f"[fast-arb] Chainlink danger-zone monitor: ±{self._danger_zone_pct}% within {self._pre_expiry_seconds:.0f}s of expiry")

        print(
            f"[fast-arb] Edge divergence monitor: signal >= {self._edge_signal_threshold:.0%} "
            f"within {self._edge_monitor_seconds:.0f}s of expiry"
        )

        try:
            while True:
                now = time.time()
                if now - self._last_scan_ts >= self.scan_interval:
                    ws = self._refresh_watchlist(prev_ws=ws)
                    self._last_scan_ts = time.time()

                if (now - last_status) >= self.STATUS_INTERVAL_SECONDS:
                    self._print_status()
                    last_status = now

                self._sync_one_legged_polymarket_orders()
                self._monitor_one_leg_pm_cancel_reenter()
                self._monitor_paper_one_legged()
                self._monitor_edge_divergence()
                # Chainlink danger zone disabled — replaced by edge divergence monitor
                # if now - last_danger_check >= self._DANGER_CHECK_INTERVAL:
                #     self._check_pre_expiry_danger()
                #     last_danger_check = now
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n[fast-arb] Stopped.")
        finally:
            if ws is not None:
                ws.stop()
            if self._kalshi_ws is not None:
                self._kalshi_ws.stop()
            if self._chainlink:
                self._chainlink.stop()
            for state in self._danger_exits.values():
                if state.timer:
                    state.timer.cancel()

    # ── Refresh watchlist ──────────────────────────────────────────────

    def _refresh_watchlist(self, prev_ws: MarketWebSocketClient | None = None) -> MarketWebSocketClient | None:
        print("[fast-arb] Scanning markets...")
        self.engine.pm_clob._dead_tokens.clear()
        self.engine.scan(execute=False)
        pm_markets, kalshi_markets, matches, opportunities = self.engine.last_snapshot

        # Обновляем балансы при каждом скане (кешируем для hot path)
        self._cached_balances = self.engine.get_real_balances()

        max_edge = self.engine.trading["max_lock_edge"]
        tracking_candidates = self._build_tracking_candidates(matches)
        match_index = {
            f"{m.polymarket.market_id}:{m.kalshi.market_id}": m
            for m in matches
            if m.kalshi.venue == "kalshi"
        }

        watched: dict[str, WatchedPair] = {}
        pairs_by_asset: dict[str, set[str]] = {}
        pairs_by_kalshi: dict[str, set[str]] = {}
        asset_ids: list[str] = []

        for opp in tracking_candidates:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if opp.buy_yes_venue not in {"polymarket", "kalshi"} or opp.buy_no_venue not in {"polymarket", "kalshi"}:
                continue

            watch_key = self._watch_key(opp)
            watched[watch_key] = WatchedPair(opportunity=opp, matched=matched)
            for token_id in [matched.polymarket.yes_token_id, matched.polymarket.no_token_id]:
                if not token_id:
                    continue
                asset_ids.append(token_id)
                pairs_by_asset.setdefault(token_id, set()).add(watch_key)

            kalshi_ticker = matched.kalshi.market_id
            if kalshi_ticker:
                pairs_by_kalshi.setdefault(kalshi_ticker, set()).add(watch_key)

        self.watch_by_pair_key = watched
        self.pairs_by_asset_id = pairs_by_asset
        self.pairs_by_kalshi_ticker = pairs_by_kalshi

        # Отменяем таймеры для пар которые больше не отслеживаются
        for key in list(self._pending_timers.keys()):
            if key not in self.watch_by_pair_key:
                self._pending_timers.pop(key).cancel()

        # Пробуем сразу исполнить найденные возможности
        self._try_immediate_open(opportunities, match_index)

        new_asset_ids = sorted(set(asset_ids))
        new_kalshi_tickers = sorted(set(pairs_by_kalshi.keys()))
        self._refresh_kalshi_ws(new_kalshi_tickers)

        if not new_asset_ids:
            self.live_books = {}
            print("[fast-arb] No candidates for live monitoring.")
            return None

        kept = set(new_asset_ids)
        self.live_books = {k: v for k, v in self.live_books.items() if k in kept}

        prev_asset_ids = getattr(self, "_current_asset_ids", [])
        if new_asset_ids == prev_asset_ids and prev_ws is not None:
            print(f"[fast-arb] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens (WS reused).")
            self._current_asset_ids = new_asset_ids
            return prev_ws

        if prev_ws is not None:
            prev_ws.stop()
        print(f"[fast-arb] Tracking {len(watched)} pairs via {len(new_asset_ids)} PM tokens.")
        self._current_asset_ids = new_asset_ids
        ws = MarketWebSocketClient(
            url=self.MARKET_WS_URL,
            asset_ids=new_asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

    def _try_immediate_open(self, opportunities: list, match_index: dict) -> None:
        if not opportunities:
            return
        balances = self._cached_balances
        if balances["polymarket"] is None or balances["kalshi"] is None:
            print("[fast-arb] Пропуск немедленного открытия: нет балансов")
            return
        for opp in opportunities:
            matched = match_index.get(opp.pair_key)
            if matched is None:
                continue
            if self._has_open_one_legged_pair(opp.pair_key):
                self._maybe_complete_one_legged(opp, matched)
                continue
            if self._is_too_close_to_expiry(opp.expiry):
                continue
            if not self._prices_within_bounds(opp.yes_ask, opp.no_ask):
                continue
            if self._edge_above_max_allowed(opp.yes_ask, opp.no_ask):
                continue
            with self._signal_lock:
                open_for_pair = self._count_open_positions_for_pair(opp.pair_key)
                if open_for_pair >= int(self.engine.trading.get("max_entries_per_pair", 1)):
                    continue
                if not self.dry_run:
                    halt, reason = self._check_loss_limits(new_position_cost=opp.total_cost)
                    if halt:
                        print(f"[fast-arb] HALT {reason} (немедленное открытие)")
                        if "realized_losses" in reason:
                            self.engine.safety.dry_run = True
                        return
                executed, yes_leg, no_leg = self._apply_execution_pricing_parallel(opp, matched)
                if executed is None:
                    continue
                if not self._prices_within_bounds(executed.yes_ask, executed.no_ask):
                    continue
                if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                    continue
                if self._edge_above_max_allowed(executed.yes_ask, executed.no_ask):
                    continue
                if executed.expected_profit <= 0:
                    continue
                self._execute_and_record(executed, matched, yes_leg, no_leg)
                self._cached_balances = self.engine.get_real_balances()

    # ── Kalshi WS ──────────────────────────────────────────────────────

    def _refresh_kalshi_ws(self, new_tickers: list[str]) -> None:
        prev_tickers = getattr(self, "_current_kalshi_tickers", [])
        if new_tickers == prev_tickers and self._kalshi_ws is not None:
            return
        if self._kalshi_ws is not None:
            self._kalshi_ws.stop()
            self._kalshi_ws = None
        self._current_kalshi_tickers = new_tickers
        kept = set(new_tickers)
        self.live_books_kalshi = {k: v for k, v in self.live_books_kalshi.items() if k in kept}
        if not new_tickers:
            return
        try:
            self._kalshi_ws = KalshiWebSocketClient(
                tickers=new_tickers,
                on_update=self.on_kalshi_update,
            )
            self._kalshi_ws.start()
            print(f"[fast-arb] Kalshi WS started for {len(new_tickers)} tickers.")
        except Exception as exc:
            print(f"[fast-arb] Kalshi WS unavailable: {exc}")
            self._kalshi_ws = None

    def on_kalshi_update(self, ticker: str, top: KalshiTopOfBook) -> None:
        self.live_books_kalshi[ticker] = top
        for pair_key in self.pairs_by_kalshi_ticker.get(ticker, set()):
            self._maybe_open_pair(pair_key)

    def on_ws_message(self, payload: dict) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self.on_ws_message(item)
            return

        event_type = payload.get("event_type")

        if event_type == "price_change":
            timestamp = int(payload.get("timestamp", 0) or 0)
            for change in payload.get("price_changes", []):
                asset_id = str(change.get("asset_id", ""))
                if not asset_id:
                    continue
                try:
                    best_ask = float(change.get("best_ask", 0) or 0)
                    best_bid = float(change.get("best_bid", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if best_ask <= 0:
                    continue
                self.live_books[asset_id] = TopOfBook(
                    best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp,
                )
                for pair_key in self.pairs_by_asset_id.get(asset_id, set()):
                    self._maybe_open_pair(pair_key)
            return

        if event_type not in {"book", "best_bid_ask"}:
            return

        asset_id = str(payload.get("asset_id", ""))
        if not asset_id:
            return

        if event_type == "book":
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            best_ask = float(asks[0]["price"]) if asks else 0.0
            best_bid = float(bids[0]["price"]) if bids else 0.0
            timestamp = int(payload.get("timestamp", 0) or 0)
        else:
            best_ask = float(payload.get("best_ask", 0) or 0)
            best_bid = float(payload.get("best_bid", 0) or 0)
            timestamp = int(payload.get("timestamp", 0) or 0)

        self.live_books[asset_id] = TopOfBook(best_bid=best_bid, best_ask=best_ask, updated_at_ms=timestamp)
        for pair_key in self.pairs_by_asset_id.get(asset_id, set()):
            self._maybe_open_pair(pair_key)

    # ── Hot path ───────────────────────────────────────────────────────

    def _maybe_open_pair(self, pair_key: str) -> None:
        watched = self.watch_by_pair_key.get(pair_key)
        if watched is None:
            return
        opp = watched.opportunity
        matched = watched.matched

        # 1. Проверяем существующую позицию (любое направление)
        if self._has_open_one_legged_pair(opp.pair_key):
            self._maybe_complete_one_legged(opp, matched)
            return
        if self._is_too_close_to_expiry(opp.expiry):
            return
        open_for_pair = self._count_open_positions_for_pair(opp.pair_key)
        max_entries_per_pair = int(self.engine.trading.get("max_entries_per_pair", 1))
        if open_for_pair >= max_entries_per_pair:
            self._maybe_complete_one_legged(opp, matched)
            return

        # 2. Лимит одновременных позиций
        open_count = len(self.engine.db.get_open_positions())
        if open_count >= self.engine.trading["max_open_pairs"]:
            self._skip_log(
                pair_key,
                f"[fast-arb][SKIP] {opp.symbol} | reason=max_open_pairs ({open_count})",
            )
            return

        # 3. Быстрая оценка edge из WS-данных
        yes_book = self.live_books.get(matched.polymarket.yes_token_id or "")
        no_book = self.live_books.get(matched.polymarket.no_token_id or "")
        if yes_book is None or no_book is None:
            return
        if yes_book.best_ask <= 0 or no_book.best_ask <= 0:
            return

        kalshi_live = self.live_books_kalshi.get(matched.kalshi.market_id or "")
        rough_yes = (
            yes_book.best_ask if opp.buy_yes_venue == "polymarket"
            else (kalshi_live.best_yes_ask if kalshi_live else matched.kalshi.yes_ask)
        )
        rough_no = (
            no_book.best_ask if opp.buy_no_venue == "polymarket"
            else (kalshi_live.best_no_ask if kalshi_live else matched.kalshi.no_ask)
        )
        rough_edge = 1.0 - (rough_yes + rough_no)
        if rough_edge < self.engine.trading["min_lock_edge"]:
            return
        if rough_edge > self.engine.trading["max_lock_edge"]:
            rough_polarized = (rough_yes > 0.5) != (rough_no > 0.5)
            if not rough_polarized:
                return

        # 4. Проверка min/max цены ноги (в памяти)
        if not self._prices_within_bounds(rough_yes, rough_no):
            self._skip_log(pair_key, f"[fast-arb][SKIP] {opp.symbol} | leg_price_out_of_bounds ({rough_yes:.4f}, {rough_no:.4f})")
            return

        # 4b. Fast oracle gap guard (из кэша, без сетевых запросов)
        _k_ref = opp.kalshi_reference_price
        _slug = getattr(opp, "pm_event_slug", None)
        if _k_ref and _slug:
            _cached_pm = self._pm_price_cache.get(_slug)
            if _cached_pm is not None:
                _safety = self.engine.config.get("safety", {})
                _max_gap = (
                    _safety.get("max_oracle_gap_pct_1h", 0.10)
                    if getattr(opp, "interval_minutes", None) == 60
                    else _safety.get("max_oracle_gap_pct", 0.02)
                )
                _gap = abs(_cached_pm - _k_ref) / _k_ref * 100
                if _gap > _max_gap:
                    self._skip_log(
                        pair_key,
                        f"[fast-arb][SKIP] {opp.symbol} | oracle_gap_guard K={_k_ref:.2f} PM={_cached_pm:.2f} gap={_gap:.4f}%",
                    )
                    return

        # 5. (XRP и SOL торгуются как обычные пары)

        # 6. Balance check (пропускаем в paper-режиме — реальные деньги не тратятся)
        if not self.dry_run:
            _bal = self._cached_balances
            _min_pm = float(self.engine.safety.min_balance_polymarket)
            _min_k = float(self.engine.safety.min_balance_kalshi)
            _pm_bal = _bal.get("polymarket") or 0.0
            _k_bal = _bal.get("kalshi") or 0.0
            if _pm_bal > 0 and _pm_bal < _min_pm:
                self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | pm_balance_low (${_pm_bal:.2f} < ${_min_pm:.2f})")
                return
            if _k_bal > 0 and _k_bal < _min_k:
                self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | kalshi_balance_low (${_k_bal:.2f} < ${_min_k:.2f})")
                return

        # 7. Loss limit check (пропускаем в paper-режиме)
        if not self.dry_run:
            halt, reason = self._check_loss_limits()
            if halt:
                self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | {reason}")
                if "realized_losses" in reason:
                    self.engine.safety.dry_run = True
                return

        # Paper mode: симулируем задержку исполнения реальной ставки (0.5s)
        if self.dry_run:
            watch_key = self._watch_key(opp)
            if watch_key not in self._pending_timers:
                t = threading.Timer(1.5, self._delayed_paper_check, args=[watch_key])
                self._pending_timers[watch_key] = t
                t.start()
            return

        with self._signal_lock:
            open_for_pair = self._count_open_positions_for_pair(opp.pair_key)
            if open_for_pair >= max_entries_per_pair:
                return

            # 5. Параллельная проверка стаканов + liquidity margin
            executed, yes_leg, no_leg = self._apply_execution_pricing_parallel(opp, matched)
            if executed is None:
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
                    f"              reason=insufficient_liquidity ({self.liquidity_ratio:.1f}x margin)\n"
                    f"              YES: {self.engine._format_leg_summary(yes_leg)}\n"
                    f"              NO:  {self.engine._format_leg_summary(no_leg)}",
                )
                return
            if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | edge_disappeared ({executed.edge_per_share:.4f})",
                )
                return
            if not self._prices_within_bounds(executed.yes_ask, executed.no_ask):
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | leg_price_out_of_bounds ({executed.yes_ask:.4f}, {executed.no_ask:.4f})",
                )
                return
            if self._edge_above_max_allowed(executed.yes_ask, executed.no_ask):
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | edge_too_high ({executed.edge_per_share:.4f})",
                )
                return
            if executed.expected_profit <= 0:
                self._skip_log(
                    pair_key,
                    f"[fast-arb][SKIP] {opp.symbol} | negative_expected_profit ({executed.expected_profit:.2f})",
                )
                return

            # Финальная проверка лимита с точной стоимостью позиции (пропускаем в paper-режиме)
            if not self.dry_run:
                halt2, reason2 = self._check_loss_limits(new_position_cost=executed.total_cost)
                if halt2:
                    self._skip_log(pair_key, f"[fast-arb][HALT] {opp.symbol} | {reason2}")
                    if "realized_losses" in reason2:
                        self.engine.safety.dry_run = True
                    return

            # 6. Выставляем ордера параллельно
            self._execute_and_record(executed, matched, yes_leg, no_leg)

    # ── Исполнение ─────────────────────────────────────────────────────

    def _execute_and_record(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> None:
        _k_tgt = opp.kalshi_reference_price
        _p_tgt = self._fetch_pm_open_price(opp.pm_event_slug) if opp.pm_event_slug else None
        if _k_tgt and _p_tgt:
            _gap_pct = abs(_p_tgt - _k_tgt) / _k_tgt * 100
            _tgt_str = f" | K.tgt={_k_tgt:.2f} PM.tgt={_p_tgt:.2f} gap={_gap_pct:.4f}%"
        elif _k_tgt:
            _tgt_str = f" | K.tgt={_k_tgt:.2f} PM.tgt=N/A"
        else:
            _tgt_str = ""
        print(
            f"\n[fast-arb][OPEN] {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO\n"
            f"                 ask_sum={opp.ask_sum:.4f} edge={opp.edge_per_share:.4f} "
            f"cost=${opp.total_cost:.2f} exp_profit=${opp.expected_profit:.2f}{_tgt_str}"
        )

        if self.dry_run:
            self.engine.db.open_paper_position(opp, pm_price_to_beat=_p_tgt)
            print(
                f"[fast-arb][PAPER] {opp.symbol} | {opp.buy_yes_venue}:YES@{opp.yes_ask:.4f} "
                f"+ {opp.buy_no_venue}:NO@{opp.no_ask:.4f} | edge={opp.edge_per_share:.4f}{_tgt_str}"
            )
            if self.engine.notifier:
                self.engine.notifier.notify_open(
                    symbol=opp.symbol,
                    yes_venue=opp.buy_yes_venue,
                    no_venue=opp.buy_no_venue,
                    yes_ask=opp.yes_ask,
                    no_ask=opp.no_ask,
                    ask_sum=opp.ask_sum,
                    edge=opp.edge_per_share,
                    cost=opp.total_cost,
                    expected_profit=opp.expected_profit,
                    execution_status="paper",
                    is_paper=True,
                    kalshi_target=_k_tgt,
                    pm_target=_p_tgt,
                    interval_minutes=opp.interval_minutes,
                )
            return

        # Прописываем правильные token_id для PM ног
        mapped_yes_leg = yes_leg
        mapped_no_leg = no_leg
        if opp.buy_yes_venue == "polymarket" and yes_leg and matched.polymarket.yes_token_id:
            mapped_yes_leg = replace(yes_leg, market_id=matched.polymarket.yes_token_id)
        if opp.buy_no_venue == "polymarket" and no_leg and matched.polymarket.no_token_id:
            mapped_no_leg = replace(no_leg, market_id=matched.polymarket.no_token_id)

        result: FastExecutionResult = self.executor.execute_pair_parallel(
            opp, mapped_yes_leg, mapped_no_leg
        )

        if result.execution_status == "both_filled":
            print(
                f"[fast-arb][OPEN] SUCCESS | "
                f"kalshi={result.kalshi_order.shares_matched:.2f}@{result.kalshi_order.fill_price:.4f} | "
                f"pm={result.polymarket_order.shares_matched:.2f}@{result.polymarket_order.fill_price:.4f}"
            )
        else:
            print(f"[fast-arb][OPEN] {result.execution_status.upper()} | {result.reason}")
            if result.execution_status == "failed":
                return

        kalshi_res = result.kalshi_order or _empty_order("no_order")
        pm_res = result.polymarket_order or _empty_order("no_order")

        opened = self.engine.db.open_position(
            opportunity=opp,
            kalshi_result=kalshi_res,
            polymarket_result=pm_res,
            execution_status=result.execution_status,
            route="parallel",
            polymarket_snapshot_open=None,
            kalshi_snapshot_open=None,
            yes_leg=yes_leg,
            no_leg=no_leg,
            pm_price_to_beat=_p_tgt,
        )
        if result.execution_status == "both_filled":
            self._cancel_redundant_one_legged_polymarket_orders(
                pair_key=opp.pair_key,
                exclude_position_id=opened.id,
            )

        if self.engine.notifier:
            kalshi_fill = result.kalshi_order.fill_price if result.kalshi_order else 0.0
            pm_fill = result.polymarket_order.fill_price if result.polymarket_order else 0.0
            self.engine.notifier.notify_open(
                symbol=opp.symbol,
                yes_venue=opp.buy_yes_venue,
                no_venue=opp.buy_no_venue,
                yes_ask=opp.yes_ask,
                no_ask=opp.no_ask,
                ask_sum=opp.ask_sum,
                edge=opp.edge_per_share,
                cost=opp.total_cost,
                expected_profit=opp.expected_profit,
                execution_status=result.execution_status,
                kalshi_fill=kalshi_fill,
                pm_fill=pm_fill,
                kalshi_target=_k_tgt,
                pm_target=_p_tgt,
                interval_minutes=opp.interval_minutes,
            )

    # ── Paper simulation: задержка + одноногие позиции ────────────────

    def _delayed_paper_check(self, watch_key: str) -> None:
        """Вызывается через 0.5с после WS-сигнала. Делает REST-проверку стаканов
        и открывает paper позицию (двуногую или одноногую)."""
        self._pending_timers.pop(watch_key, None)

        watched = self.watch_by_pair_key.get(watch_key)
        if watched is None:
            return
        opp = watched.opportunity
        matched = watched.matched

        if self._is_too_close_to_expiry(opp.expiry):
            return

        max_entries = int(self.engine.trading.get("max_entries_per_pair", 1))
        with self._signal_lock:
            if self._count_open_positions_for_pair(opp.pair_key) >= max_entries:
                return

            executed, yes_leg, no_leg = self._apply_execution_pricing_parallel(opp, matched)

            if executed is not None:
                if executed.edge_per_share < self.engine.trading["min_lock_edge"]:
                    return
                if not self._prices_within_bounds(executed.yes_ask, executed.no_ask):
                    return
                if self._edge_above_max_allowed(executed.yes_ask, executed.no_ask):
                    return
                if executed.expected_profit <= 0:
                    return
                self._execute_and_record(executed, matched, yes_leg, no_leg)
            else:
                self._maybe_open_paper_one_legged(opp, yes_leg, no_leg)

    def _maybe_open_paper_one_legged(
        self,
        opp: CrossVenueOpportunity,
        yes_leg: ExecutionLegInfo | None,
        no_leg: ExecutionLegInfo | None,
    ) -> None:
        """Если только одна нога ликвидна — открываем одноногую paper позицию."""
        shares = math.floor(opp.shares)
        if shares <= 0:
            return
        yes_ok = yes_leg is not None and yes_leg.filled_shares + 1e-6 >= shares
        no_ok = no_leg is not None and no_leg.filled_shares + 1e-6 >= shares
        if yes_ok and not no_ok:
            self.engine.db.open_paper_one_legged_position(opp, "yes", yes_leg)
            print(
                f"[fast-arb][PAPER][ONE-LEG] {opp.symbol} | "
                f"{opp.buy_yes_venue}:YES@{yes_leg.avg_price:.4f} | no-leg unavailable"
            )
        elif no_ok and not yes_ok:
            self.engine.db.open_paper_one_legged_position(opp, "no", no_leg)
            print(
                f"[fast-arb][PAPER][ONE-LEG] {opp.symbol} | "
                f"{opp.buy_no_venue}:NO@{no_leg.avg_price:.4f} | yes-leg unavailable"
            )

    def _monitor_paper_one_legged(self) -> None:
        """Проверяет открытые одноногие paper позиции и симулирует докупку
        когда цена второй ноги возвращается к приемлемому уровню."""
        rows = self.engine.db.conn.execute(
            "SELECT id, pair_key, venue_yes, market_yes, venue_no, market_no, "
            "yes_avg_price, no_avg_price, yes_filled_shares, no_filled_shares, "
            "execution_status "
            "FROM positions "
            "WHERE status='open' AND is_paper=1 "
            "AND execution_status IN ('paper_one_legged_yes', 'paper_one_legged_no')"
        ).fetchall()
        if not rows:
            return

        min_edge = float(self.engine.trading["min_lock_edge"])
        for row in rows:
            pos_id = row["id"]
            exec_status = row["execution_status"]
            if exec_status == "paper_one_legged_yes":
                filled_price = float(row["yes_avg_price"] or 0)
                shares = float(row["yes_filled_shares"] or 0)
                missing_venue = row["venue_no"]
                missing_market = row["market_no"]
                missing_side = "no"
            else:
                filled_price = float(row["no_avg_price"] or 0)
                shares = float(row["no_filled_shares"] or 0)
                missing_venue = row["venue_yes"]
                missing_market = row["market_yes"]
                missing_side = "yes"

            if shares <= 0 or filled_price <= 0:
                continue
            max_completion_price = 1.0 - filled_price - min_edge
            if max_completion_price <= 0:
                continue

            current_price = self._get_live_completion_price(
                missing_venue, missing_market, missing_side,
                row["venue_yes"], row["venue_no"], row["pair_key"],
            )
            if current_price is None or current_price <= 0:
                continue

            if current_price <= max_completion_price:
                self.engine.db.complete_paper_one_legged(pos_id, missing_side, current_price, shares)
                print(
                    f"[fast-arb][PAPER][COMPLETE] pos={pos_id[:8]} | "
                    f"{missing_venue}:{missing_side}@{current_price:.4f} | "
                    f"edge={(1.0 - filled_price - current_price):.4f}"
                )

    def _get_live_completion_price(
        self,
        venue: str,
        market_id: str,
        side: str,
        venue_yes: str,
        venue_no: str,
        pair_key: str,
    ) -> float | None:
        if venue == "kalshi":
            book = self.live_books_kalshi.get(market_id)
            if book is None:
                return None
            return book.best_yes_ask if side == "yes" else book.best_no_ask
        # polymarket — ищем token_id через watched pair
        watch_key = f"{pair_key}|{venue_yes}|{venue_no}"
        watched = self.watch_by_pair_key.get(watch_key)
        if watched is None:
            return None
        token_id = (
            watched.matched.polymarket.yes_token_id if side == "yes"
            else watched.matched.polymarket.no_token_id
        )
        if not token_id:
            return None
        book = self.live_books.get(token_id)
        return book.best_ask if book else None

    # ── Edge divergence monitor ─────────────────────────────────────────

    def _monitor_edge_divergence(self) -> None:
        """Track live edge for open paper positions.
        Within the monitoring window: record edge_ticks, track milestones, poll Binance.
        When edge >= threshold, fire sell-price snapshot."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        now_iso = datetime.utcnow().isoformat()

        # Clean up states for resolved positions
        for pid in list(self._edge_states):
            pos = self.engine.db.get_position(pid)
            if pos is None or pos.status != "open":
                self._edge_states.pop(pid, None)

        positions = self.engine.db.get_open_positions()
        for pos in positions:
            if not pos.is_paper:
                continue
            row = self.engine.db.conn.execute(
                "SELECT execution_status, kalshi_reference_price FROM positions WHERE id=?",
                (pos.id,),
            ).fetchone()
            exec_status = row["execution_status"] if row else None
            if exec_status not in ("paper", "paper_both_filled"):
                continue

            seconds_to_expiry = (pos.expiry - now).total_seconds()
            if seconds_to_expiry < 0:
                continue

            # Initialize state early so we can cache token IDs even before monitoring window
            state = self._edge_states.get(pos.id)
            if state is None:
                state = _EdgeMonitorState()
                self._edge_states[pos.id] = state

            # Look up pair data; cache token IDs on first sighting so we can still
            # get books if the pair later drops out of the watch list (e.g. edge collapsed).
            watch_key = f"{pos.pair_key}|{pos.venue_yes}|{pos.venue_no}"
            watched = self.watch_by_pair_key.get(watch_key)

            if watched is not None and state.pm_yes_token_id is None:
                state.pm_yes_token_id = watched.matched.polymarket.yes_token_id
                state.pm_no_token_id = watched.matched.polymarket.no_token_id
                state.kalshi_market_id = watched.matched.kalshi.market_id
                state.interval_minutes = watched.matched.polymarket.interval_minutes

            # Resolve live books: prefer watched path, fall back to cached IDs
            if watched is not None:
                yes_book = self.live_books.get(watched.matched.polymarket.yes_token_id or "")
                no_book = self.live_books.get(watched.matched.polymarket.no_token_id or "")
                kalshi_live = self.live_books_kalshi.get(watched.matched.kalshi.market_id or "")
            elif state.pm_yes_token_id is not None:
                yes_book = self.live_books.get(state.pm_yes_token_id)
                no_book = self.live_books.get(state.pm_no_token_id or "")
                kalshi_live = self.live_books_kalshi.get(state.kalshi_market_id or "")
            else:
                continue  # Never saw this pair while watched — can't look up books

            # All 4 asks from both platforms
            pm_yes_ask = (yes_book.best_ask if yes_book and yes_book.best_ask > 0 else None)
            pm_no_ask = (no_book.best_ask if no_book and no_book.best_ask > 0 else None)
            kalshi_yes_ask = (kalshi_live.best_yes_ask if kalshi_live else None)
            kalshi_no_ask = (kalshi_live.best_no_ask if kalshi_live else None)

            # All 4 bids: PM bids from WS TopOfBook; Kalshi bids via binary identity (bid = 1 - other_ask)
            pm_yes_bid = (yes_book.best_bid if yes_book and yes_book.best_bid and yes_book.best_bid > 0 else None)
            pm_no_bid = (no_book.best_bid if no_book and no_book.best_bid and no_book.best_bid > 0 else None)
            kalshi_yes_bid = (round(1.0 - kalshi_no_ask, 6) if kalshi_no_ask else None)
            kalshi_no_bid = (round(1.0 - kalshi_yes_ask, 6) if kalshi_yes_ask else None)

            # YES/NO asks on the venues where we actually bought
            if pos.venue_yes == "polymarket":
                current_yes_ask = pm_yes_ask
            else:
                current_yes_ask = kalshi_yes_ask

            if pos.venue_no == "polymarket":
                current_no_ask = pm_no_ask
            else:
                current_no_ask = kalshi_no_ask

            if current_yes_ask is None or current_no_ask is None:
                continue
            if current_yes_ask <= 0 or current_no_ask <= 0:
                continue

            current_edge = 1.0 - (current_yes_ask + current_no_ask)

            # Track max edge over position lifetime
            if current_edge > state.max_edge:
                state.max_edge = current_edge
                self.engine.db.conn.execute(
                    "UPDATE positions SET max_edge=? WHERE id=?",
                    (round(state.max_edge, 6), pos.id),
                )
                self.engine.db.conn.commit()

            # Only record ticks and check signals in the monitoring window
            monitor_secs = (
                self._edge_monitor_seconds_1h
                if state.interval_minutes == 60
                else self._edge_monitor_seconds
            )
            if seconds_to_expiry > monitor_secs:
                continue

            # --- Binance price (cached, fetched every 5s in background) ---
            binance_price, binance_distance_pct = self._get_binance_data(
                pos.symbol,
                row["kalshi_reference_price"],
            )

            # --- Record edge tick ---
            self.engine.db.insert_edge_tick(
                position_id=pos.id,
                ts=now_iso,
                seconds_to_expiry=seconds_to_expiry,
                pm_yes_ask=pm_yes_ask,
                pm_no_ask=pm_no_ask,
                kalshi_yes_ask=kalshi_yes_ask,
                kalshi_no_ask=kalshi_no_ask,
                yes_ask=current_yes_ask,
                no_ask=current_no_ask,
                edge=current_edge,
                binance_price=binance_price,
                binance_distance_pct=binance_distance_pct,
                pm_yes_bid=pm_yes_bid,
                pm_no_bid=pm_no_bid,
                kalshi_yes_bid=kalshi_yes_bid,
                kalshi_no_bid=kalshi_no_bid,
            )

            # --- Milestone tracking ---
            milestone_cols: list[str] = []
            if not state.milestone_10 and current_edge >= 0.10:
                state.milestone_10 = True
                milestone_cols.append("edge_first_10pct")
            if not state.milestone_15 and current_edge >= 0.15:
                state.milestone_15 = True
                milestone_cols.append("edge_first_15pct")
            if not state.milestone_20 and current_edge >= 0.20:
                state.milestone_20 = True
                milestone_cols.append("edge_first_20pct")
            if milestone_cols:
                set_clause = ", ".join(f"{c}=?" for c in milestone_cols)
                params = [now_iso] * len(milestone_cols) + [pos.id]
                self.engine.db.conn.execute(
                    f"UPDATE positions SET {set_clause} WHERE id=?", params
                )
                self.engine.db.conn.commit()

            # --- Signal disabled: data collection only, no exit action ---
            # (edge exit signal was harmful overall — fires on normal positions too)
            # Will re-enable with a better discriminator after analysing edge_ticks data.

    _BINANCE_SYMBOL_MAP: dict[str, str] = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
        "XRP": "XRPUSDT",
        "DOGE": "DOGEUSDT",
        "BNB": "BNBUSDT",
    }

    def _get_binance_data(
        self,
        symbol: str,
        reference_price: float | None,
    ) -> tuple[float | None, float | None]:
        """Return (binance_price, distance_pct) using a 5s cache.
        Fetches in a daemon thread so the main loop is never blocked."""
        now_ts = time.time()
        cached = self._binance_cache.get(symbol)
        if cached is not None:
            price, fetch_ts = cached
            if now_ts - fetch_ts < self._binance_fetch_interval:
                # Return cached value
                if reference_price and reference_price > 0:
                    dist = abs(price - reference_price) / reference_price
                    return price, dist
                return price, None

        # Time to refresh — launch daemon thread so we don't block
        binance_sym = self._BINANCE_SYMBOL_MAP.get(symbol.upper())
        if binance_sym is None:
            return None, None

        def _fetch(sym: str, cache_key: str) -> None:
            try:
                import httpx
                resp = httpx.get(
                    f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                    timeout=3.0,
                )
                data = resp.json()
                price_val = float(data["price"])
                self._binance_cache[cache_key] = (price_val, time.time())
            except Exception:
                pass

        t = threading.Thread(target=_fetch, args=(binance_sym, symbol), daemon=True)
        t.start()

        # Return last known value while thread fetches new one
        if cached is not None:
            price, _ = cached
            if reference_price and reference_price > 0:
                dist = abs(price - reference_price) / reference_price
                return price, dist
            return price, None
        return None, None

    def _edge_record_sell_prices(
        self,
        pos_id: str,
        pm_yes_token: str | None,
        pm_no_token: str | None,
        kalshi_ticker: str | None,
        venue_yes: str,
        venue_no: str,
        yes_entry: float,
        no_entry: float,
        shares: float,
        symbol: str,
    ) -> None:
        """Timer callback: fetch orderbook bids and record theoretical sell prices."""
        pos = self.engine.db.get_position(pos_id)
        if pos is None:
            return

        # Build a temporary _DangerExit to reuse _fetch_sell_bids
        tmp = _DangerExit(
            pm_yes_token=pm_yes_token,
            pm_no_token=pm_no_token,
            kalshi_ticker=kalshi_ticker,
            venue_yes=venue_yes,
            venue_no=venue_no,
            yes_entry=yes_entry,
            no_entry=no_entry,
            shares=shares,
            symbol=symbol,
            ref_price=0,
        )

        # Fetch bids and compute fill price (sell at ANY price, not just >= entry)
        yes_bids = self._fetch_sell_bids(venue_yes, "yes", tmp)
        yes_sell = self._compute_sell_fill(yes_bids, shares)

        no_bids = self._fetch_sell_bids(venue_no, "no", tmp)
        no_sell = self._compute_sell_fill(no_bids, shares)

        # Compute PnL from selling both legs
        if yes_sell is not None and no_sell is not None:
            exit_pnl = round((yes_sell - yes_entry + no_sell - no_entry) * shares, 4)
        else:
            exit_pnl = None

        self.engine.db.conn.execute(
            "UPDATE positions SET edge_yes_sell=?, edge_no_sell=?, edge_exit_pnl=? WHERE id=?",
            (yes_sell, no_sell, exit_pnl, pos_id),
        )
        self.engine.db.conn.commit()

        yes_str = f"{yes_sell:.4f}" if yes_sell is not None else "NO_BIDS"
        no_str = f"{no_sell:.4f}" if no_sell is not None else "NO_BIDS"
        pnl_str = f"${exit_pnl:+.2f}" if exit_pnl is not None else "N/A"

        state = self._edge_states.get(pos_id)
        edge_str = f"{state.edge_at_signal:.1%}" if state else "?"

        print(
            f"[fast-arb][EDGE-SELL] {symbol} pos={pos_id[:8]} | "
            f"edge@signal={edge_str} | "
            f"YES: entry={yes_entry:.4f} sell={yes_str} | "
            f"NO: entry={no_entry:.4f} sell={no_str} | "
            f"exit_pnl={pnl_str}"
        )

    @staticmethod
    def _compute_sell_fill(
        bids: list[OrderLevel] | None,
        shares: float,
    ) -> float | None:
        """Compute avg fill price when selling at any available price."""
        if not bids:
            return None
        remaining = shares
        total_revenue = 0.0
        for level in bids:
            take = min(level.size, remaining)
            total_revenue += take * level.price
            remaining -= take
            if remaining <= 1e-9:
                return round(total_revenue / shares, 6)
        # Not enough liquidity — use partial fill avg price
        if total_revenue > 0:
            filled = shares - remaining
            return round(total_revenue / filled, 6)
        return None

    # ── Pre-expiry danger zone (Chainlink + orderbook sell) [DISABLED] ───

    def _check_pre_expiry_danger(self) -> None:
        """Detect positions approaching expiry with Chainlink price in the
        danger zone.  Start sell-monitoring: try to sell both legs on the
        orderbook at >= entry price (with 1.5s simulated delay per attempt)."""
        if self._chainlink is None:
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Clean up states for positions that got resolved normally
        for pid in list(self._danger_exits):
            pos = self.engine.db.get_position(pid)
            if pos is None or pos.status != "open":
                state = self._danger_exits.pop(pid)
                if state.timer:
                    state.timer.cancel()

        positions = self.engine.db.get_open_positions()
        for pos in positions:
            if not pos.is_paper or pos.id in self._danger_exits:
                continue

            # Only two-legged paper positions
            row = self.engine.db.conn.execute(
                "SELECT execution_status FROM positions WHERE id=?", (pos.id,)
            ).fetchone()
            exec_status = row["execution_status"] if row else None
            if exec_status not in ("paper", "paper_both_filled"):
                continue

            seconds_to_expiry = (pos.expiry - now).total_seconds()
            if seconds_to_expiry > self._pre_expiry_seconds or seconds_to_expiry < 0:
                continue

            cl_price = self._chainlink.get_price(pos.symbol)
            if cl_price is None:
                print(f"[fast-arb][DANGER] {pos.symbol} pos={pos.id[:8]} | no CL price! {seconds_to_expiry:.0f}s to expiry")
                continue
            ref_price = pos.kalshi_reference_price
            if ref_price is None or ref_price <= 0:
                continue

            distance_pct = abs(cl_price - ref_price) / ref_price * 100
            if distance_pct >= self._danger_zone_pct:
                continue

            # Resolve PM token IDs from watched pairs
            watch_key = f"{pos.pair_key}|{pos.venue_yes}|{pos.venue_no}"
            watched = self.watch_by_pair_key.get(watch_key)
            pm_yes_token = watched.matched.polymarket.yes_token_id if watched else None
            pm_no_token = watched.matched.polymarket.no_token_id if watched else None
            kalshi_ticker = (
                pos.market_yes if pos.venue_yes == "kalshi" else pos.market_no
            )

            state = _DangerExit(
                pm_yes_token=pm_yes_token,
                pm_no_token=pm_no_token,
                kalshi_ticker=kalshi_ticker,
                venue_yes=pos.venue_yes,
                venue_no=pos.venue_no,
                yes_entry=pos.yes_ask,
                no_entry=pos.no_ask,
                shares=pos.shares,
                symbol=pos.symbol,
                ref_price=ref_price,
            )
            self._danger_exits[pos.id] = state

            direction = "UP" if cl_price >= ref_price else "DOWN"
            print(
                f"\n[fast-arb][DANGER] {pos.symbol} | CL={cl_price:.4f} ref={ref_price:.4f} "
                f"dist={distance_pct:.4f}% ({direction}) | {seconds_to_expiry:.0f}s to expiry "
                f"| начинаем попытки продажи"
            )

            # Schedule first sell attempt after 1.5s delay
            state.timer = threading.Timer(1.5, self._danger_exit_try_sell, args=[pos.id])
            state.timer.start()

    def _danger_exit_try_sell(self, pos_id: str) -> None:
        """Timer callback: check orderbook bids and sell legs at >= entry price."""
        state = self._danger_exits.get(pos_id)
        if state is None:
            return
        state.timer = None

        # Check position is still open
        pos = self.engine.db.get_position(pos_id)
        if pos is None or pos.status != "open":
            self._danger_exits.pop(pos_id, None)
            return

        # Try to sell unsold YES leg
        if state.yes_sell_price is None:
            yes_bids = self._fetch_sell_bids(state.venue_yes, "yes", state)
            fill = self._check_sell_depth(yes_bids, state.shares, state.yes_entry)
            if fill is not None:
                state.yes_sell_price = fill
                print(
                    f"[fast-arb][DANGER-SELL] {state.symbol} pos={pos_id[:8]} | "
                    f"YES leg sold @{fill:.4f} (entry={state.yes_entry:.4f})"
                )

        # Try to sell unsold NO leg
        if state.no_sell_price is None:
            no_bids = self._fetch_sell_bids(state.venue_no, "no", state)
            fill = self._check_sell_depth(no_bids, state.shares, state.no_entry)
            if fill is not None:
                state.no_sell_price = fill
                print(
                    f"[fast-arb][DANGER-SELL] {state.symbol} pos={pos_id[:8]} | "
                    f"NO leg sold @{fill:.4f} (entry={state.no_entry:.4f})"
                )

        # Both legs sold → resolve position
        if state.yes_sell_price is not None and state.no_sell_price is not None:
            yes_pnl = (state.yes_sell_price - state.yes_entry) * state.shares
            no_pnl = (state.no_sell_price - state.no_entry) * state.shares
            pnl = round(yes_pnl + no_pnl, 2)

            cl_price = self._chainlink.get_price(state.symbol) if self._chainlink else None

            self.engine.db.resolve_position(
                position_id=pos_id,
                winning_side="early_exit",
                pnl=pnl,
                actual_pnl=pnl,
                polymarket_result="early_exit",
                kalshi_result="early_exit",
                lock_valid=True,
                kalshi_close_price=cl_price,
            )
            self._danger_exits.pop(pos_id, None)

            print(
                f"\n[fast-arb][EARLY_EXIT] {state.symbol} pos={pos_id[:8]} | "
                f"YES sell@{state.yes_sell_price:.4f} (was {state.yes_entry:.4f}) | "
                f"NO sell@{state.no_sell_price:.4f} (was {state.no_entry:.4f}) | "
                f"pnl=${pnl:+.2f}"
            )
            if self.engine.notifier:
                self.engine.notifier.notify_resolve(
                    symbol=state.symbol,
                    pm_result="early_exit",
                    kalshi_result="early_exit",
                    pnl=pnl,
                    lock_valid=True,
                    is_paper=True,
                    kalshi_close_price=cl_price,
                )
            return

        # Not both sold yet → schedule retry if time allows
        seconds_left = (pos.expiry - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
        if seconds_left > 5:
            state.timer = threading.Timer(1.5, self._danger_exit_try_sell, args=[pos_id])
            state.timer.start()
        else:
            # Too close to expiry, give up — normal resolution will handle it
            sold = []
            if state.yes_sell_price is not None:
                sold.append(f"YES@{state.yes_sell_price:.4f}")
            if state.no_sell_price is not None:
                sold.append(f"NO@{state.no_sell_price:.4f}")
            print(
                f"[fast-arb][DANGER-TIMEOUT] {state.symbol} pos={pos_id[:8]} | "
                f"{seconds_left:.0f}s left, giving up | sold: {', '.join(sold) or 'none'}"
            )
            self._danger_exits.pop(pos_id, None)

    def _fetch_sell_bids(
        self,
        venue: str,
        side: str,
        state: _DangerExit,
    ) -> list[OrderLevel] | None:
        """Fetch bid-side orderbook levels for selling a position leg."""
        if venue == "polymarket":
            token_id = state.pm_yes_token if side == "yes" else state.pm_no_token
            if not token_id:
                return None
            try:
                book = self.engine.pm_clob.get_orderbook(token_id)
                return book.bids if book and book.bids else None
            except Exception:
                return None
        else:
            if not state.kalshi_ticker:
                return None
            try:
                bids, _ = self.engine.kalshi_feed.fetch_side_bids(state.kalshi_ticker, side)
                return bids
            except Exception:
                return None

    @staticmethod
    def _check_sell_depth(
        bids: list[OrderLevel] | None,
        shares: float,
        min_price: float,
    ) -> float | None:
        """Check if enough bids exist at >= min_price to sell all shares.
        Returns avg fill price if yes, None otherwise."""
        if not bids:
            return None
        remaining = shares
        total_revenue = 0.0
        for level in bids:  # sorted by price desc
            if level.price < min_price:
                break
            take = min(level.size, remaining)
            total_revenue += take * level.price
            remaining -= take
            if remaining <= 1e-9:
                return round(total_revenue / shares, 6)
        return None  # not enough depth at >= min_price

    # ── Execution pricing (параллельные REST + liquidity margin) ──────

    def _apply_execution_pricing_parallel(
        self,
        opp: CrossVenueOpportunity,
        matched: MatchedMarketPair,
    ) -> tuple[CrossVenueOpportunity | None, ExecutionLegInfo | None, ExecutionLegInfo | None]:
        from cross_arb_bot.models import NormalizedMarket

        yes_market = matched.polymarket if opp.buy_yes_venue == "polymarket" else matched.kalshi
        no_market = matched.polymarket if opp.buy_no_venue == "polymarket" else matched.kalshi

        tiered_stake = self.engine._stake_for_edge(opp.edge_per_share)
        raw_shares = min(tiered_stake / opp.ask_sum, opp.shares)
        shares = math.floor(raw_shares)
        if shares <= 0:
            return None, None, None

        min_edge = float(self.engine.config.get("trading", {}).get("min_lock_edge", 0.04))
        max_yes_price = 1.0 - opp.no_ask - min_edge
        max_no_price = 1.0 - opp.yes_ask - min_edge
        yes_asks, no_asks = self._fetch_execution_asks_parallel(
            opp=opp,
            yes_market=yes_market,
            no_market=no_market,
        )
        yes_leg = self._build_execution_leg_from_asks(
            venue=opp.buy_yes_venue,
            market_id=yes_market.market_id,
            side="yes",
            asks=yes_asks,
            shares=shares,
            max_price=max_yes_price,
        )
        no_leg = self._build_execution_leg_from_asks(
            venue=opp.buy_no_venue,
            market_id=no_market.market_id,
            side="no",
            asks=no_asks,
            shares=shares,
            max_price=max_no_price,
        )

        if yes_leg is None or no_leg is None:
            return None, yes_leg, no_leg
        if yes_leg.filled_shares + 1e-6 < shares or no_leg.filled_shares + 1e-6 < shares:
            return None, yes_leg, no_leg

        # Проверка liquidity margin
        min_required = self.liquidity_ratio * shares
        if yes_leg.usable_shares < min_required or no_leg.usable_shares < min_required:
            retry_max_yes_price = max_yes_price
            retry_max_no_price = max_no_price
            if yes_leg.usable_shares < min_required:
                retry_max_yes_price = max(max_yes_price, 1.0 - no_leg.avg_price - min_edge)
            if no_leg.usable_shares < min_required:
                retry_max_no_price = max(max_no_price, 1.0 - yes_leg.avg_price - min_edge)
            yes_leg = self._build_execution_leg_from_asks(
                venue=opp.buy_yes_venue,
                market_id=yes_market.market_id,
                side="yes",
                asks=yes_asks,
                shares=shares,
                max_price=retry_max_yes_price,
            )
            no_leg = self._build_execution_leg_from_asks(
                venue=opp.buy_no_venue,
                market_id=no_market.market_id,
                side="no",
                asks=no_asks,
                shares=shares,
                max_price=retry_max_no_price,
            )
            if yes_leg is None or no_leg is None:
                return None, yes_leg, no_leg
            if yes_leg.filled_shares + 1e-6 < shares or no_leg.filled_shares + 1e-6 < shares:
                return None, yes_leg, no_leg
            if yes_leg.usable_shares < min_required or no_leg.usable_shares < min_required:
                return None, yes_leg, no_leg

        yes_ask = yes_leg.avg_price
        no_ask = no_leg.avg_price
        ask_sum = yes_ask + no_ask
        edge_per_share = 1.0 - ask_sum
        capital_used = yes_leg.total_cost + no_leg.total_cost

        pm_fee = 0.0
        kalshi_fee = 0.0
        if opp.buy_yes_venue == "polymarket":
            pm_fee += self.engine._pm_fee(shares, yes_ask)
        else:
            kalshi_fee += self.engine._kalshi_fee(shares, yes_ask)
        if opp.buy_no_venue == "polymarket":
            pm_fee += self.engine._pm_fee(shares, no_ask)
        else:
            kalshi_fee += self.engine._kalshi_fee(shares, no_ask)

        total_fee = pm_fee + kalshi_fee
        total_cost = capital_used + total_fee

        executed = replace(
            opp,
            shares=shares,
            yes_ask=yes_ask, no_ask=no_ask, ask_sum=ask_sum,
            edge_per_share=edge_per_share, capital_used=capital_used,
            polymarket_fee=pm_fee, kalshi_fee=kalshi_fee,
            total_fee=total_fee, total_cost=total_cost,
            expected_profit=shares - total_cost,
        )
        return executed, yes_leg, no_leg

    def _fetch_execution_asks_parallel(
        self,
        opp: CrossVenueOpportunity,
        yes_market: NormalizedMarket,
        no_market: NormalizedMarket,
    ) -> tuple[list[OrderLevel] | None, list[OrderLevel] | None]:
        with ThreadPoolExecutor(max_workers=2) as pool:
            yes_future = pool.submit(
                self._fetch_asks_for_leg,
                opp.buy_yes_venue, yes_market, "yes",
            )
            no_future = pool.submit(
                self._fetch_asks_for_leg,
                opp.buy_no_venue, no_market, "no",
            )
            return yes_future.result(), no_future.result()

    def _fetch_asks_for_leg(
        self,
        venue: str,
        market: NormalizedMarket,
        side: str,
    ) -> list[OrderLevel] | None:
        if venue == "polymarket":
            token_id = market.yes_token_id if side == "yes" else market.no_token_id
            if not token_id:
                return None
            book = self.engine.pm_clob.get_orderbook(token_id)
            if not book or not book.asks:
                return None
            return book.asks
        asks, _ = self.engine.kalshi_feed.fetch_side_asks(market.market_id, side)
        return asks if asks else None

    def _build_execution_leg_from_asks(
        self,
        venue: str,
        market_id: str,
        side: str,
        asks: list[OrderLevel] | None,
        shares: float,
        max_price: float,
    ) -> ExecutionLegInfo | None:
        if not asks:
            return None

        available = sum(level.size for level in asks)
        usable = sum(level.size for level in asks if level.price <= max_price)
        best_ask = asks[0].price if asks else 0.0
        remaining = shares
        total_cost = 0.0
        for level in asks:
            take = min(level.size, remaining)
            if take <= 0:
                continue
            total_cost += take * level.price
            remaining -= take
            if remaining <= 1e-9:
                break

        filled = shares - remaining
        avg_price = (total_cost / filled) if filled > 1e-9 else 0.0
        return ExecutionLegInfo(
            venue=venue,
            market_id=market_id,
            side=side,
            requested_shares=shares,
            filled_shares=filled,
            available_shares=available,
            usable_shares=usable,
            avg_price=avg_price,
            total_cost=total_cost,
            best_ask=best_ask,
            remaining_shares_after_fill=max(0.0, available - filled),
        )

    # ── Loss limit ─────────────────────────────────────────────────────

    def _check_loss_limits(self, new_position_cost: float = 0.0) -> tuple[bool, str]:
        """Проверяет бюджет и лимит потерь.

        Бюджет = budget_usd + max(0, total_realized_pnl) — растёт с выигрышами.
        Стоп если реализованные потери > max_realized_loss_usd.

        Возвращает (halt, reason).
        """
        try:
            # Суммарный реализованный PnL (только реальные позиции)
            cursor = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(actual_pnl), 0) FROM positions WHERE status='resolved' AND is_paper = 0"
            )
            realized_pnl = float(cursor.fetchone()[0])

            # Стоп по потерям (только реальные)
            if realized_pnl < -self.max_realized_loss_usd:
                return True, f"realized_losses_exceeded (pnl=${realized_pnl:+.2f}, limit=-${self.max_realized_loss_usd:.0f})"

            # Эффективный бюджет растёт с выигрышами
            effective_budget = self.budget_usd + max(0.0, realized_pnl)

            # Открытые позиции + новая не должны превышать бюджет
            cursor = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(CASE "
                "WHEN execution_status='one_legged_kalshi' "
                "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
                "WHEN execution_status='one_legged_polymarket' "
                "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
                "ELSE total_cost END), 0) FROM positions WHERE status='open'"
            )
            open_cost = float(cursor.fetchone()[0]) + new_position_cost
            if open_cost > effective_budget:
                return True, (
                    f"budget_exceeded (open=${open_cost:.2f} > budget=${effective_budget:.2f}"
                    f" [base=${self.budget_usd:.0f} + wins=${max(0.0, realized_pnl):.2f}])"
                )

        except Exception:
            pass
        return False, ""

    # ── Вспомогательные ───────────────────────────────────────────────

    def _build_tracking_candidates(self, matches: list[MatchedMarketPair]) -> list[CrossVenueOpportunity]:
        candidates: list[CrossVenueOpportunity] = []
        stake_per_pair_usd = float(self.engine.trading["stake_per_pair_usd"])
        _safety_cfg = self.engine.config.get("safety", {})

        for item in matches:
            pm = item.polymarket
            ka = item.kalshi
            symbol = pm.symbol

            max_gap_pct = (
                _safety_cfg.get("max_oracle_gap_pct_1h", 0.10)
                if getattr(pm, "interval_minutes", None) == 60
                else _safety_cfg.get("max_oracle_gap_pct", 0.02)
            )

            # Oracle gap check — часть проверки матча: оба рынка должны следить за одной ценой.
            k_ref = ka.reference_price
            slug = pm.pm_event_slug
            pm_open: float | None = self._fetch_pm_open_price(slug) if slug else None

            if k_ref and slug:
                if pm_open is None:
                    print(f"[match] {symbol} | pm_open_price unavailable (slug={slug}) — skipping match")
                    continue
                gap_pct = abs(pm_open - k_ref) / k_ref * 100
                if gap_pct > max_gap_pct:
                    print(
                        f"[match-skip] {symbol} | oracle_gap_too_large"
                        f" K={k_ref:.4f} PM={pm_open:.4f} gap={gap_pct:.4f}% > {max_gap_pct}%"
                    )
                    continue
                print(f"[match-ok] {symbol} | K={k_ref:.4f} PM={pm_open:.4f} gap={gap_pct:.4f}%")

            legs = [
                ("polymarket", "kalshi", pm.yes_ask, ka.no_ask),
                ("kalshi", "polymarket", ka.yes_ask, pm.no_ask),
            ]
            for yes_venue, no_venue, yes_ask, no_ask in legs:
                ask_sum = yes_ask + no_ask
                if ask_sum <= 0:
                    continue
                edge_per_share = 1.0 - ask_sum
                shares = stake_per_pair_usd / ask_sum
                polymarket_fee = 0.0
                kalshi_fee = 0.0
                if yes_venue == "polymarket":
                    polymarket_fee += polymarket_crypto_taker_fee(shares, yes_ask)
                else:
                    kalshi_fee += kalshi_taker_fee(shares, yes_ask)
                if no_venue == "polymarket":
                    polymarket_fee += polymarket_crypto_taker_fee(shares, no_ask)
                else:
                    kalshi_fee += kalshi_taker_fee(shares, no_ask)
                total_fee = polymarket_fee + kalshi_fee
                capital_used = ask_sum * shares
                candidates.append(
                    CrossVenueOpportunity(
                        pair_key=f"{pm.market_id}:{ka.market_id}",
                        polymarket_market_id=pm.market_id,
                        kalshi_market_id=ka.market_id,
                        symbol=symbol,
                        title=f"{pm.title} <> {ka.title}",
                        expiry=min(pm.expiry, ka.expiry),
                        polymarket_title=pm.title,
                        kalshi_title=ka.title,
                        match_score=item.score,
                        expiry_delta_seconds=abs((pm.expiry - ka.expiry).total_seconds()),
                        polymarket_reference_price=pm.reference_price,
                        kalshi_reference_price=k_ref,
                        polymarket_rules=pm.rules_text,
                        kalshi_rules=ka.rules_text,
                        pm_event_slug=slug,
                        interval_minutes=pm.interval_minutes,
                        buy_yes_venue=yes_venue,
                        buy_no_venue=no_venue,
                        yes_ask=yes_ask,
                        no_ask=no_ask,
                        ask_sum=ask_sum,
                        edge_per_share=edge_per_share,
                        shares=shares,
                        capital_used=capital_used,
                        polymarket_fee=polymarket_fee,
                        kalshi_fee=kalshi_fee,
                        total_fee=total_fee,
                        total_cost=capital_used + total_fee,
                        expected_payout=shares,
                        expected_profit=shares - (capital_used + total_fee),
                    )
                )
        return candidates

    def _maybe_complete_one_legged(self, opp: CrossVenueOpportunity, matched: MatchedMarketPair) -> None:
        """Если есть одноногая позиция по паре — пробуем докупить вторую ногу."""
        # Cooldown: не пробуем чаще раза в минуту
        import time as _time
        now_ts = _time.time()
        last = self._last_completion_attempt.get(opp.pair_key, 0.0)
        if now_ts - last < self._COMPLETION_COOLDOWN:
            return
        self._last_completion_attempt[opp.pair_key] = now_ts

        row = self.engine.db.conn.execute(
            "SELECT id, execution_status, venue_yes, venue_no, "
            "kalshi_fill_price, kalshi_fill_shares, "
            "polymarket_fill_price, polymarket_fill_shares "
            "FROM positions WHERE pair_key=? AND status='open' "
            "AND execution_status IN ('one_legged_kalshi','one_legged_polymarket')",
            (opp.pair_key,),
        ).fetchone()
        if row is None:
            return

        exec_status = row["execution_status"]
        pos_id = row["id"]
        min_edge = float(self.engine.trading.get("completion_min_edge", self.engine.trading["min_lock_edge"]))
        skip_pm_above = float(self.engine.trading.get("completion_skip_pm_if_price_above", 0.0) or 0.0)

        # Определяем какая нога уже есть и по какой цене
        if exec_status == "one_legged_kalshi":
            # Kalshi есть, нужна PM нога
            # venue_yes из БД — какая площадка покупала YES при открытии
            db_venue_yes = row["venue_yes"]
            existing_fill_price = float(row["kalshi_fill_price"] or 0)
            existing_kalshi_side = "no" if db_venue_yes == "polymarket" else "yes"
            missing_venue = "polymarket"
            missing_side = "yes" if db_venue_yes == "polymarket" else "no"
            missing_token = (
                matched.polymarket.yes_token_id if missing_side == "yes"
                else matched.polymarket.no_token_id
            )
        else:
            # one_legged_polymarket: Kalshi ордер уже рестингует — не трогаем
            return

        with self._signal_lock:
            # Повторная проверка что позиция ещё одноногая
            row2 = self.engine.db.conn.execute(
                "SELECT id FROM positions WHERE id=? AND status='open' "
                "AND execution_status IN ('one_legged_kalshi','one_legged_polymarket')",
                (pos_id,),
            ).fetchone()
            if row2 is None:
                return

            if self.dry_run:
                print("[fast-arb][DRY] Пропуск")
                return

            # Покупаем только недостающую ногу
            shares = float(self.engine.db.conn.execute(
                "SELECT shares FROM positions WHERE id=?", (pos_id,)
            ).fetchone()[0])
            max_price = min(
                self.engine.safety.max_leg_price,
                1.0 - existing_fill_price - min_edge,
            )

            try:
                if missing_venue == "polymarket":
                    if existing_fill_price >= skip_pm_above:
                        kalshi_live = self.live_books_kalshi.get(matched.kalshi.market_id or "")
                        live_existing_price = (
                            kalshi_live.best_yes_ask if (kalshi_live and existing_kalshi_side == "yes")
                            else kalshi_live.best_no_ask if kalshi_live
                            else 0.0
                        )
                        if live_existing_price > existing_fill_price:
                            print(
                                f"[fast-arb][COMPLETE] skip PM completion | "
                                f"existing_kalshi={existing_fill_price:.4f} >= threshold={skip_pm_above:.4f} "
                                f"and live_kalshi_{existing_kalshi_side}={live_existing_price:.4f} is higher"
                            )
                            return
                    asks = self._fetch_asks_for_leg(missing_venue, matched.polymarket, missing_side)
                    candidates = self._build_pm_completion_candidates_from_asks(
                        asks=asks,
                        shares=shares,
                        max_price=max_price,
                    )
                    if not candidates:
                        return
                    order = None
                    for attempt, candidate in enumerate(candidates, start=1):
                        completion_edge = 1.0 - (existing_fill_price + candidate.avg_price)
                        if completion_edge < min_edge:
                            return
                        if (
                            candidate.best_ask < self.engine.safety.min_leg_price
                            or candidate.best_ask > self.engine.safety.max_leg_price
                        ):
                            return
                        limit_price = math.floor((candidate.worst_price + self.pm_price_buffer) * 100) / 100.0
                        print(
                            f"\n[fast-arb][COMPLETE] {opp.symbol} | докупаем {missing_venue}:{missing_side.upper()}\n"
                            f"  existing={existing_fill_price:.4f} + avg={candidate.avg_price:.4f} "
                            f"(best={candidate.best_ask:.4f}, worst={candidate.worst_price:.4f}) "
                            f"= {existing_fill_price + candidate.avg_price:.4f} | edge={completion_edge:.4f}"
                            f" | try={attempt}"
                        )
                        order = self.executor._place_pm_limit_order(missing_token, limit_price, shares)
                        if order is not None and order.shares_matched > 0:
                            break
                else:
                    book = self.live_books.get(missing_token or "")
                    if book is None or book.best_ask <= 0:
                        return
                    current_ask = book.best_ask
                    total_ask = existing_fill_price + current_ask
                    completion_edge = 1.0 - total_ask
                    if completion_edge < min_edge:
                        return
                    if current_ask < self.engine.safety.min_leg_price or current_ask > self.engine.safety.max_leg_price:
                        return
                    print(
                        f"\n[fast-arb][COMPLETE] {opp.symbol} | докупаем {missing_venue}:{missing_side.upper()}\n"
                        f"  existing={existing_fill_price:.4f} + current={current_ask:.4f} "
                        f"= {total_ask:.4f} | edge={completion_edge:.4f}"
                    )
                    price_cents = round(current_ask * 100) + self.kalshi_slippage_cents
                    price_cents = min(price_cents, 99)
                    count = max(1, math.floor(shares))
                    order = self.executor._place_kalshi_order(
                        matched.kalshi.market_id, missing_side, count, price_cents
                    )
            except Exception as e:
                print(f"[fast-arb][COMPLETE] ошибка ордера: {e}")
                return

            if order is None or order.shares_matched <= 0:
                status_str = order.status if order else "None returned"
                print(f"[fast-arb][COMPLETE] не заполнился: {status_str}")
                return

            print(
                f"[fast-arb][COMPLETE] SUCCESS | {missing_venue} fill={order.shares_matched:.2f}"
                f"@{order.fill_price:.4f} | fee=${order.fee:.4f}"
            )

            # Обновляем позицию в DB
            now = __import__("datetime").datetime.utcnow().isoformat()
            final_total_cost = 0.0
            final_expected_profit = 0.0
            final_yes_ask = 0.0
            final_no_ask = 0.0
            final_ask_sum = 0.0

            if missing_venue == "polymarket":
                final_yes_ask = order.fill_price if missing_side == "yes" else existing_fill_price
                final_no_ask = existing_fill_price if missing_side == "yes" else order.fill_price
                pm_cost = order.shares_matched * order.fill_price + order.fee
                kalshi_cost = shares * existing_fill_price
                final_ask_sum = final_yes_ask + final_no_ask
                final_total_cost = kalshi_cost + pm_cost
                final_expected_profit = shares - final_total_cost
                self.engine.db.conn.execute(
                    "UPDATE positions SET execution_status='both_filled', "
                    "yes_ask=?, no_ask=?, ask_sum=?, total_cost=?, expected_profit=?, "
                    "polymarket_fill_price=?, polymarket_fill_shares=?, polymarket_order_fee=?, "
                    "polymarket_order_id=?, polymarket_order_status=?, execution_completed_at=? "
                    "WHERE id=?",
                    (
                        final_yes_ask, final_no_ask, final_ask_sum, final_total_cost, final_expected_profit,
                        order.fill_price, order.shares_matched, order.fee,
                        order.order_id, order.status, now, pos_id,
                    ),
                )
            else:
                final_yes_ask = existing_fill_price if missing_side == "no" else order.fill_price
                final_no_ask = order.fill_price if missing_side == "no" else existing_fill_price
                pm_cost = shares * existing_fill_price
                kalshi_cost = order.shares_matched * order.fill_price + order.fee
                final_ask_sum = final_yes_ask + final_no_ask
                final_total_cost = pm_cost + kalshi_cost
                final_expected_profit = shares - final_total_cost
                self.engine.db.conn.execute(
                    "UPDATE positions SET execution_status='both_filled', "
                    "yes_ask=?, no_ask=?, ask_sum=?, total_cost=?, expected_profit=?, "
                    "kalshi_fill_price=?, kalshi_fill_shares=?, kalshi_order_fee=?, "
                    "kalshi_order_id=?, kalshi_order_status=?, execution_completed_at=? "
                    "WHERE id=?",
                    (
                        final_yes_ask, final_no_ask, final_ask_sum, final_total_cost, final_expected_profit,
                        order.fill_price, order.shares_matched, order.fee,
                        order.order_id, order.status, now, pos_id,
                    ),
                )
            self.engine.db.conn.commit()

            if self.engine.notifier:
                self.engine.notifier._send(
                    f"🔄 <b>ДОКУПЛЕНА нога: {opp.symbol}</b>\n"
                    f"{missing_venue}:{missing_side.upper()} @ {order.fill_price:.4f} "
                    f"(fill={order.shares_matched:.2f})\n"
                    f"edge итого={completion_edge:.4f} | cost=${final_total_cost:.2f} | exp_profit=${final_expected_profit:.2f}\n"
                    f"позиция закрыта в both_filled"
                )

    def _get_status_text(self) -> str:
        db = self.engine.db
        try:
            r = db.conn.execute(
                "SELECT COUNT(*) as total,"
                " SUM(CASE WHEN actual_pnl > 0 THEN 1 ELSE 0 END) as wins,"
                " SUM(CASE WHEN actual_pnl < 0 THEN 1 ELSE 0 END) as losses,"
                " COALESCE(SUM(actual_pnl), 0) as pnl"
                " FROM positions WHERE status='resolved' AND is_paper=1"
            ).fetchone()
            total, wins, losses, pnl = int(r[0]), int(r[1] or 0), int(r[2] or 0), float(r[3])

            today = db.conn.execute(
                "SELECT COUNT(*) as cnt, COALESCE(SUM(actual_pnl), 0) as pnl"
                " FROM positions WHERE status='resolved' AND is_paper=1"
                " AND DATE(opened_at) = DATE('now')"
            ).fetchone()
            today_cnt, today_pnl = int(today[0]), float(today[1])

            mm = db.conn.execute(
                "SELECT COUNT(*) FROM positions"
                " WHERE status='resolved' AND is_paper=1"
                " AND polymarket_result != kalshi_result"
                " AND polymarket_result IS NOT NULL AND kalshi_result IS NOT NULL"
            ).fetchone()
            mm_cnt = int(mm[0])

            open_r = db.conn.execute(
                "SELECT COUNT(*) FROM positions WHERE status='open'"
            ).fetchone()
            open_cnt = int(open_r[0])

            one_leg = db.conn.execute(
                "SELECT COUNT(*) FROM positions"
                " WHERE status='resolved' AND is_paper=1"
                " AND ((yes_filled_shares > 0 AND (no_filled_shares IS NULL OR no_filled_shares = 0))"
                "   OR (no_filled_shares > 0 AND (yes_filled_shares IS NULL OR yes_filled_shares = 0)))"
            ).fetchone()
            one_leg_cnt = int(one_leg[0])
        except Exception:
            return "📝 <b>Fast Arb (paper)</b>\n⚠️ Ошибка чтения БД"

        wr = (wins / total * 100) if total > 0 else 0
        lines = [
            f"📝 <b>Fast Arb (paper)</b>",
            f"",
            f"📊 <b>Всего:</b> {total} сделок | WR: <b>{wr:.0f}%</b> ({wins}W / {losses}L)",
            f"💰 <b>P&L:</b> <b>${pnl:+.2f}</b>",
            f"📅 <b>Сегодня:</b> {today_cnt} сделок, <b>${today_pnl:+.2f}</b>",
        ]
        if mm_cnt:
            lines.append(f"⚠️ Mismatches: {mm_cnt}")
        if one_leg_cnt:
            lines.append(f"🦵 Одноногих: {one_leg_cnt}")
        if open_cnt:
            lines.append(f"📌 Открытых: {open_cnt}")
        lines.append(f"\n🔍 Пар в мониторинге: {len(self.watch_by_pair_key)}")
        if self.dry_run:
            lines.append("⏸ <b>РЕЖИМ: paper</b>")
        return "\n".join(lines)

    def _watch_key(self, opp: CrossVenueOpportunity) -> str:
        return f"{opp.pair_key}|{opp.buy_yes_venue}|{opp.buy_no_venue}"

    def _cancel_redundant_one_legged_polymarket_orders(
        self,
        pair_key: str,
        exclude_position_id: str,
    ) -> None:
        rows = self.engine.db.conn.execute(
            "SELECT id, kalshi_order_id, kalshi_order_status "
            "FROM positions "
            "WHERE pair_key=? AND status='open' AND execution_status='one_legged_polymarket' AND id<>?",
            (pair_key, exclude_position_id),
        ).fetchall()
        for row in rows:
            order_id = str(row["kalshi_order_id"] or "")
            if not order_id:
                continue
            try:
                ok = self.engine.kalshi_trader.cancel_order(order_id)
            except Exception as e:
                self.engine.db.audit("order_error", row["id"], {
                    "venue": "kalshi",
                    "error": f"cancel_redundant_resting_error: {e}",
                    "order_id": order_id,
                })
                continue
            new_status = "canceled_redundant_after_both_filled" if ok else str(row["kalshi_order_status"] or "")
            self.engine.db.conn.execute(
                "UPDATE positions SET kalshi_order_status=? WHERE id=?",
                (new_status, row["id"]),
            )
        self.engine.db.conn.commit()

    def _count_open_positions_for_pair(self, pair_key: str) -> int:
        row = self.engine.db.conn.execute(
            "SELECT COUNT(*) FROM positions WHERE pair_key=? AND status='open'",
            (pair_key,),
        ).fetchone()
        return int(row[0] or 0) if row else 0

    def _has_open_one_legged_pair(self, pair_key: str) -> bool:
        row = self.engine.db.conn.execute(
            "SELECT 1 FROM positions "
            "WHERE pair_key=? AND status='open' "
            "AND execution_status IN ('one_legged_kalshi','one_legged_polymarket') "
            "LIMIT 1",
            (pair_key,),
        ).fetchone()
        return row is not None

    def _is_too_close_to_expiry(self, expiry: datetime) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return (expiry - now).total_seconds() < 45

    def _prices_within_bounds(self, yes_price: float, no_price: float) -> bool:
        min_p = self.engine.safety.min_leg_price
        max_p = self.engine.safety.max_leg_price
        return min_p <= yes_price <= max_p and min_p <= no_price <= max_p

    def _prices_in_blocked_zone(self, yes_price: float, no_price: float) -> bool:
        blocked_min = float(getattr(self.engine.safety, "blocked_leg_price_min", 0.0) or 0.0)
        blocked_max = float(getattr(self.engine.safety, "blocked_leg_price_max", 0.0) or 0.0)
        if blocked_max <= blocked_min:
            return False
        return (
            blocked_min <= yes_price < blocked_max
            or blocked_min <= no_price < blocked_max
        )

    def _prices_cross_midpoint(self, yes_price: float, no_price: float) -> bool:
        return (yes_price > 0.5) != (no_price > 0.5)

    def _edge_above_max_allowed(self, yes_price: float, no_price: float) -> bool:
        edge = 1.0 - (yes_price + no_price)
        if edge <= self.engine.trading["max_lock_edge"]:
            return False
        polarized = (yes_price > 0.5) != (no_price > 0.5)
        return not polarized

    def _build_pm_completion_candidates_from_asks(
        self,
        asks: list[OrderLevel] | None,
        shares: float,
        max_price: float,
    ) -> list[PMCompletionCandidate]:
        if not asks or shares <= 0 or max_price <= 0:
            return []

        cumulative = 0.0
        total_cost = 0.0
        fill_cost_at_target = 0.0
        crossed_target = False
        best_ask = asks[0].price
        candidates: list[PMCompletionCandidate] = []
        last_price: float | None = None

        for level in asks:
            if level.price > max_price + 1e-9:
                break
            total_cost += level.size * level.price
            prev_cumulative = cumulative
            cumulative += level.size
            if not crossed_target and cumulative + 1e-9 >= shares:
                take_needed = shares - prev_cumulative
                fill_cost_at_target = (total_cost - level.size * level.price) + max(0.0, take_needed) * level.price
                crossed_target = True
            if crossed_target and last_price != level.price:
                candidates.append(
                    PMCompletionCandidate(
                    best_ask=best_ask,
                    avg_price=fill_cost_at_target / shares,
                    worst_price=level.price,
                )
                )
                last_price = level.price
        return candidates

    def _fetch_pm_open_price(self, slug: str) -> float | None:
        """Парсим openPrice текущего окна с HTML-страницы Polymarket (кэшируется по slug)."""
        if slug in self._pm_price_cache:
            return self._pm_price_cache[slug]
        import re as _re
        import httpx as _httpx
        url = f"https://polymarket.com/event/{slug}"
        try:
            resp = _httpx.get(url, timeout=5.0, follow_redirects=True,
                              headers={"User-Agent": "Mozilla/5.0"})
            m = _re.search(
                r'"openPrice"\s*:\s*([0-9.]+)\s*,\s*"closePrice"\s*:\s*null',
                resp.text,
            )
            if m:
                price = float(m.group(1))
                self._pm_price_cache[slug] = price
                return price
        except Exception as e:
            print(f"[oracle] PM page fetch failed for {slug}: {e}")
        return None

    def _skip_log(self, pair_key: str, msg: str) -> None:
        now = time.time()
        if now - self._last_skip_log.get(pair_key, 0.0) < self._SKIP_LOG_INTERVAL:
            return
        self._last_skip_log[pair_key] = now
        print(msg)

    def _sync_one_legged_polymarket_orders(self) -> None:
        now_ts = time.time()
        if now_ts - self._last_kalshi_resting_sync_ts < self._KALSHI_RESTING_SYNC_INTERVAL:
            return
        self._last_kalshi_resting_sync_ts = now_ts

        rows = self.engine.db.conn.execute(
            "SELECT id, symbol, shares, venue_yes, yes_ask, no_ask, kalshi_order_id, kalshi_fill_shares, kalshi_fill_price, "
            "kalshi_order_fee, kalshi_order_status "
            "FROM positions "
            "WHERE status='open' AND execution_status='one_legged_polymarket' AND kalshi_order_id IS NOT NULL"
        ).fetchall()
        for row in rows:
            order_id = str(row["kalshi_order_id"] or "")
            if not order_id:
                continue
            info = self.engine.kalshi_trader.get_order(order_id)
            if info is None:
                continue

            prev_fill = float(row["kalshi_fill_shares"] or 0)
            prev_price = float(row["kalshi_fill_price"] or 0)
            target_shares = float(row["shares"] or 0)
            if (
                abs(info.shares_matched - prev_fill) < 1e-9
                and str(info.status or "") == str(row["kalshi_order_status"] or "")
            ):
                continue

            fallback_price = float(row["no_ask"] or 0.0) if str(row["venue_yes"]) == "polymarket" else float(row["yes_ask"] or 0.0)
            fill_price = float(info.fill_price or 0.0)
            if fill_price <= 0:
                fill_price = prev_price if prev_price > 0 else fallback_price
            fee = float(info.fee or 0.0)
            if fee <= 0 and info.shares_matched > 0 and fill_price > 0:
                fee = self.engine._kalshi_fee(info.shares_matched, fill_price)

            new_execution_status = "both_filled" if info.shares_matched + 1e-6 >= target_shares else "one_legged_polymarket"
            completed_at = __import__("datetime").datetime.utcnow().isoformat() if new_execution_status == "both_filled" else None
            self.engine.db.conn.execute(
                "UPDATE positions SET kalshi_fill_price=?, kalshi_fill_shares=?, kalshi_order_fee=?, "
                "kalshi_order_status=?, execution_status=?, execution_completed_at=COALESCE(?, execution_completed_at) "
                "WHERE id=?",
                (
                    fill_price,
                    info.shares_matched,
                    fee,
                    info.status,
                    new_execution_status,
                    completed_at,
                    row["id"],
                ),
            )
            self.engine.db.conn.commit()

            print(
                f"[fast-arb][SYNC] {row['symbol']} | kalshi status={info.status} "
                f"| fill={info.shares_matched:.2f}/{target_shares:.2f}@{fill_price:.4f}"
            )

            if self.engine.notifier and new_execution_status == "both_filled":
                full_row = self.engine.db.conn.execute(
                    "SELECT venue_yes, shares, polymarket_fill_price, polymarket_fill_shares, polymarket_order_fee, "
                    "kalshi_fill_price, kalshi_fill_shares, kalshi_order_fee "
                    "FROM positions WHERE id=?",
                    (row["id"],),
                ).fetchone()
                if full_row is not None:
                    pm_fill_price = float(full_row["polymarket_fill_price"] or 0.0)
                    pm_fill_shares = float(full_row["polymarket_fill_shares"] or 0.0)
                    pm_fee = float(full_row["polymarket_order_fee"] or 0.0)
                    k_fill_price = float(full_row["kalshi_fill_price"] or 0.0)
                    k_fill_shares = float(full_row["kalshi_fill_shares"] or 0.0)
                    k_fee = float(full_row["kalshi_order_fee"] or 0.0)
                    shares = float(full_row["shares"] or target_shares or 0.0)

                    if str(full_row["venue_yes"]) == "polymarket":
                        final_yes_ask = pm_fill_price
                        final_no_ask = k_fill_price
                    else:
                        final_yes_ask = k_fill_price
                        final_no_ask = pm_fill_price

                    final_ask_sum = final_yes_ask + final_no_ask
                    final_total_cost = (pm_fill_shares * pm_fill_price + pm_fee) + (k_fill_shares * k_fill_price + k_fee)
                    final_expected_profit = shares - final_total_cost

                    self.engine.notifier._send(
                        f"🔄 <b>ДОКУПЛЕНА нога: {row['symbol']}</b>\n"
                        f"kalshi @ {k_fill_price:.4f} (fill={k_fill_shares:.2f})\n"
                        f"edge итого={1.0 - final_ask_sum:.4f} | cost=${final_total_cost:.2f} | exp_profit=${final_expected_profit:.2f}\n"
                        f"позиция закрыта в both_filled"
                    )

    def _monitor_one_leg_pm_cancel_reenter(self) -> None:
        """Cancel/reenter logic for one_legged_polymarket resting Kalshi orders.

        Signal: live PM ask of our held PM leg (not Kalshi price).

        State machine per position:
          watching → (PM live ask rises ≥ pm_fill + cancel_rise) → cancelled
          cancelled → (PM live ask drops ≤ pm_fill + reenter_rise) → final (re-placed once)
          final → no more cancellations
        """
        cancel_rise = float(self.engine.trading.get("completion_cancel_kalshi_rise", 0.10))
        reenter_rise = float(self.engine.trading.get("completion_reenter_kalshi_rise", 0.08))
        min_edge = float(self.engine.trading.get("completion_min_edge", self.engine.trading["min_lock_edge"]))

        rows = self.engine.db.conn.execute(
            "SELECT id, pair_key, symbol, shares, venue_yes, venue_no, market_yes, market_no, "
            "kalshi_order_id, polymarket_fill_price "
            "FROM positions "
            "WHERE status='open' AND execution_status='one_legged_polymarket' AND kalshi_order_id IS NOT NULL"
        ).fetchall()

        for row in rows:
            pos_id = str(row["id"])
            if pos_id in self._one_leg_pm_final:
                continue

            order_id = str(row["kalshi_order_id"] or "")
            pm_fill = float(row["polymarket_fill_price"] or 0.0)
            if pm_fill <= 0 or not order_id:
                continue

            venue_yes = str(row["venue_yes"] or "")
            kalshi_market_id = str(row["market_yes"] if venue_yes == "kalshi" else row["market_no"])
            kalshi_side = "yes" if venue_yes == "kalshi" else "no"

            # PM side is the leg we hold
            pm_side = "no" if venue_yes == "kalshi" else "yes"

            # Look up live PM ask for our held leg via watch_by_pair_key
            pair_key = str(row["pair_key"] or "")
            watch_entry = self.watch_by_pair_key.get(pair_key)
            if watch_entry is None:
                continue
            pm_token_id = (
                watch_entry.matched.polymarket.yes_token_id if pm_side == "yes"
                else watch_entry.matched.polymarket.no_token_id
            )
            if not pm_token_id:
                continue
            pm_live = self.live_books.get(pm_token_id)
            if pm_live is None:
                continue
            pm_live_ask = pm_live.best_ask
            if pm_live_ask <= 0:
                continue

            cancel_threshold = pm_fill + cancel_rise
            reenter_threshold = pm_fill + reenter_rise

            if pos_id not in self._one_leg_pm_cancelled:
                # Watching: cancel if our held PM leg price rose too much
                if pm_live_ask >= cancel_threshold:
                    try:
                        ok = self.engine.kalshi_trader.cancel_order(order_id)
                    except Exception as e:
                        print(f"[fast-arb][ONE-LEG] cancel error for {row['symbol']}: {e}")
                        continue
                    if ok:
                        self._one_leg_pm_cancelled[pos_id] = pm_live_ask
                        self.engine.db.conn.execute(
                            "UPDATE positions SET kalshi_order_status='cancelled_price_rose' WHERE id=?",
                            (pos_id,),
                        )
                        self.engine.db.conn.commit()
                        print(
                            f"[fast-arb][ONE-LEG] {row['symbol']} | cancelled Kalshi resting order "
                            f"pm_fill={pm_fill:.4f} pm_live={pm_live_ask:.4f} >= threshold={cancel_threshold:.4f}"
                        )
                        self.engine.db.audit("one_leg_kalshi_cancelled", pos_id, {
                            "symbol": row["symbol"], "pm_fill": pm_fill,
                            "pm_live_ask": pm_live_ask, "cancel_threshold": cancel_threshold,
                        })
            else:
                # Cancelled: re-enter Kalshi when PM leg price drops back
                if pm_live_ask <= reenter_threshold:
                    shares = float(row["shares"] or 0)
                    # Re-use original resting price: floor((1 - pm_fill - min_edge) * 100)
                    # This preserves the original arb edge, same as executor formula.
                    max_price = min(
                        self.engine.safety.max_leg_price,
                        1.0 - pm_fill - min_edge,
                    )
                    price_cents = max(1, min(99, math.floor(max_price * 100)))
                    count = max(1, math.floor(shares))
                    try:
                        order = self.executor._place_kalshi_order(
                            kalshi_market_id, kalshi_side, count, price_cents
                        )
                    except Exception as e:
                        print(f"[fast-arb][ONE-LEG] reenter error for {row['symbol']}: {e}")
                        continue
                    if order is None:
                        print(f"[fast-arb][ONE-LEG] {row['symbol']} | reenter error: order=None")
                        continue
                    # Mark as final regardless of fill — order is placed at original price, don't cancel again
                    self._one_leg_pm_final.add(pos_id)
                    del self._one_leg_pm_cancelled[pos_id]
                    # Update DB with new Kalshi order
                    fill_price = float(order.fill_price or (price_cents / 100))
                    fee = float(order.fee or 0.0)
                    new_status = "both_filled" if order.shares_matched + 1e-6 >= shares else "one_legged_polymarket"
                    self.engine.db.conn.execute(
                        "UPDATE positions SET kalshi_order_id=?, kalshi_fill_price=?, kalshi_fill_shares=?, "
                        "kalshi_order_fee=?, kalshi_order_status=?, execution_status=? WHERE id=?",
                        (order.order_id, fill_price, order.shares_matched, fee, order.status, new_status, pos_id),
                    )
                    self.engine.db.conn.commit()
                    print(
                        f"[fast-arb][ONE-LEG] {row['symbol']} | re-entered Kalshi {kalshi_side.upper()} "
                        f"fill={order.shares_matched:.2f}@{fill_price:.4f} | status={new_status} | FINAL"
                    )
                    self.engine.db.audit("one_leg_kalshi_reentered", pos_id, {
                        "symbol": row["symbol"], "pm_fill": pm_fill,
                        "pm_live_ask": pm_live_ask, "fill_price": fill_price, "final": True,
                    })

    # ── Резолв одноногих позиций ───────────────────────────────────────

    def _resolve_one_legged_positions(self) -> None:
        """Резолвим одноногие позиции до стандартного resolver.

        Стандартный resolver пытается сделать PM redeem даже если pm_shares=0
        (при one_legged_kalshi), что ломает процесс. Резолвим здесь сами,
        стандартный resolver увидит позицию уже закрытой и пропустит.
        """
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for position in self.engine.db.get_open_positions():
            row = self.engine.db.conn.execute(
                "SELECT execution_status FROM positions WHERE id=?", (position.id,)
            ).fetchone()
            if row is None:
                continue
            exec_status = row["execution_status"]
            if exec_status not in ("one_legged_kalshi", "one_legged_polymarket"):
                continue
            if position.expiry > now:
                continue

            self._resolve_one_legged(position, exec_status)

    def _resolve_one_legged(self, position, exec_status: str) -> None:
        row = self.engine.db.conn.execute(
            "SELECT kalshi_order_id, kalshi_fill_price, kalshi_fill_shares, kalshi_order_fee, "
            "polymarket_fill_price, polymarket_fill_shares, polymarket_order_fee "
            "FROM positions WHERE id=?", (position.id,)
        ).fetchone()

        k_order_id = str(row["kalshi_order_id"] or "")
        k_shares = float(row["kalshi_fill_shares"] or 0)
        k_price  = float(row["kalshi_fill_price"]  or 0)
        k_fee    = float(row["kalshi_order_fee"]   or 0)
        pm_shares = float(row["polymarket_fill_shares"] or 0)
        pm_price  = float(row["polymarket_fill_price"]  or 0)
        pm_fee    = float(row["polymarket_order_fee"]   or 0)

        resolver = self.engine.resolver

        if exec_status == "one_legged_kalshi":
            # Только Kalshi нога — не делаем PM redeem
            kalshi_result, kalshi_snapshot, kalshi_close_price = resolver._check_kalshi(position)
            if kalshi_result is None:
                return  # Рынок ещё не разрешён

            kalshi_side = "yes" if position.venue_yes == "kalshi" else "no"
            kalshi_won = (kalshi_result == kalshi_side)
            kalshi_payout = k_shares * 1.0 if kalshi_won else 0.0
            kalshi_cost = k_shares * k_price + k_fee
            pnl = kalshi_payout - kalshi_cost

            self.engine.db.resolve_position(
                position_id=position.id,
                winning_side=kalshi_result,
                pnl=round(pnl, 6),
                actual_pnl=round(pnl, 6),
                polymarket_result="not_traded",
                kalshi_result="won" if kalshi_won else "lost",
                lock_valid=False,
                kalshi_snapshot_resolved=kalshi_snapshot,
                kalshi_close_price=kalshi_close_price,
            )
            close_str = f" | K.close={kalshi_close_price:.4f}" if kalshi_close_price is not None else ""
            tag = f"WIN +${pnl:.2f}" if pnl > 0 else f"LOSE ${pnl:.2f}"
            print(
                f"[fast-arb][resolve] {position.symbol} | one_legged_kalshi | "
                f"kalshi={'WIN' if kalshi_won else 'LOSE'} {k_shares}x@{k_price} | pnl=${pnl:+.2f} ({tag}){close_str}"
            )

        else:  # one_legged_polymarket
            # Только PM нога
            pm_result, pm_snapshot = resolver._check_polymarket(position)
            if pm_result is None:
                return  # Рынок ещё не разрешён

            if k_order_id:
                try:
                    self.engine.kalshi_trader.cancel_order(k_order_id)
                except Exception as e:
                    self.engine.db.audit("order_error", position.id, {
                        "venue": "kalshi",
                        "error": f"cancel_on_resolve_error: {e}",
                        "order_id": k_order_id,
                    })

            pm_side = "yes" if position.venue_yes == "polymarket" else "no"
            pm_won = (pm_result == pm_side)
            pm_cost = pm_shares * pm_price + pm_fee

            pm_payout_real = 0.0
            if pm_won and pm_shares > 0:
                pm_market_id = (
                    position.market_yes if position.venue_yes == "polymarket"
                    else position.market_no
                )
                print(f"[fast-arb][resolve] Polymarket redeem (one_legged): {pm_market_id}")
                redeem = self.engine.pm_trader.redeem(pm_market_id)
                if not redeem.success and not redeem.pending:
                    print(f"[fast-arb][resolve] redeem failed: {redeem.error}, retry later")
                    self.engine.db.audit("redeem_retry_pending", position.id, {
                        "symbol": position.symbol,
                        "market_id": pm_market_id,
                        "error": redeem.error or "unknown_error",
                    })
                    return
                pm_payout_real = redeem.payout_usdc if redeem.success else 0.0

            pnl = (pm_payout_real if pm_won and pm_shares > 0 else 0.0) - pm_cost
            pm_close_price = resolver._fetch_pm_close_price(position)

            self.engine.db.resolve_position(
                position_id=position.id,
                winning_side=pm_result,
                pnl=round(pnl, 6),
                actual_pnl=round(pnl, 6),
                polymarket_result="won" if pm_won else "lost",
                kalshi_result="not_traded",
                lock_valid=False,
                polymarket_snapshot_resolved=pm_snapshot,
                pm_close_price=pm_close_price,
            )
            close_str = f" | PM.close={pm_close_price:.4f}" if pm_close_price is not None else ""
            tag = f"WIN +${pnl:.2f}" if pnl > 0 else f"LOSE ${pnl:.2f}"
            print(
                f"[fast-arb][resolve] {position.symbol} | one_legged_polymarket | "
                f"pm={'WIN' if pm_won else 'LOSE'} {pm_shares:.2f}x@{pm_price} | pnl=${pnl:+.2f} ({tag}){close_str}"
            )

        if self.engine.notifier:
            if exec_status == "one_legged_kalshi":
                notify_pm = "not_traded"
                notify_kalshi = "won" if kalshi_won else "lost"
                notify_kalshi_close = kalshi_close_price
                notify_pm_close = None
            else:
                notify_pm = "won" if pm_won else "lost"
                notify_kalshi = "not_traded"
                notify_kalshi_close = None
                notify_pm_close = pm_close_price
            self.engine.notifier.notify_resolve(
                symbol=position.symbol,
                pm_result=notify_pm,
                kalshi_result=notify_kalshi,
                pnl=pnl,
                lock_valid=False,
                kalshi_close_price=notify_kalshi_close,
                pm_close_price=notify_pm_close,
            )

    def _print_status(self) -> None:
        self._resolve_one_legged_positions()
        self.engine.resolve()
        self.engine.print_status()
        open_cost = 0.0
        realized_pnl = 0.0
        try:
            c1 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(CASE "
                "WHEN execution_status='one_legged_kalshi' "
                "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
                "WHEN execution_status='one_legged_polymarket' "
                "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
                "ELSE total_cost END), 0) FROM positions WHERE status='open'"
            )
            open_cost = float(c1.fetchone()[0])
            c2 = self.engine.db.conn.execute(
                "SELECT COALESCE(SUM(actual_pnl), 0) FROM positions WHERE status='resolved' AND is_paper = 0"
            )
            realized_pnl = float(c2.fetchone()[0])
        except Exception:
            pass
        effective_budget = self.budget_usd + max(0.0, realized_pnl)
        kalshi_ws_status = (
            f"tickers={len(self._current_kalshi_tickers)}"
            if self._kalshi_ws is not None
            else "off"
        )
        print(
            f"[fast-arb][Status] watched={len(self.watch_by_pair_key)} "
            f"| live_tokens={len(self.live_books)} "
            f"| kalshi_ws={kalshi_ws_status} "
            f"| open=${open_cost:.2f}/budget=${effective_budget:.2f} "
            f"| realized_pnl=${realized_pnl:+.2f} (stop at -${self.max_realized_loss_usd:.0f})"
        )
        # Показываем текущие матчи с ценами и oracle gap
        try:
            _, kalshi_markets, matches, _ = self.engine.last_snapshot
            _safety_disp = self.engine.config.get("safety", {})

            # Какие Kalshi-символы присутствуют в матчах
            matched_symbols = {m.kalshi.symbol for m in matches}

            accepted = 0
            match_lines = []
            for m in matches:
                pm = m.polymarket
                ka = m.kalshi
                max_gap_pct = (
                    _safety_disp.get("max_oracle_gap_pct_1h", 0.10)
                    if getattr(pm, "interval_minutes", None) == 60
                    else _safety_disp.get("max_oracle_gap_pct", 0.02)
                )
                k_ref = ka.reference_price
                slug = pm.pm_event_slug
                pm_open = self._pm_price_cache.get(slug) if slug else None
                if k_ref and pm_open:
                    gap_pct = abs(pm_open - k_ref) / k_ref * 100
                    gap_str = f"K={k_ref:.4f} PM={pm_open:.4f} gap={gap_pct:.4f}%"
                    gap_ok = "✓" if gap_pct <= max_gap_pct else "✗ oracle_gap"
                elif k_ref:
                    gap_str = f"K={k_ref:.4f} PM=N/A"
                    gap_ok = "✗ no_pm_price"
                else:
                    gap_str = "targets=N/A"
                    gap_ok = "?"
                if "✓" in gap_ok:
                    accepted += 1
                best_edge = max(
                    1.0 - (pm.yes_ask + ka.no_ask),
                    1.0 - (ka.yes_ask + pm.no_ask),
                )
                # Live WS prices (если есть)
                pm_yes_live = self.live_books.get(pm.yes_token_id)
                pm_no_live = self.live_books.get(pm.no_token_id)
                k_live = self.live_books_kalshi.get(ka.market_id)
                if pm_yes_live and pm_no_live and k_live:
                    live_pm_yes = pm_yes_live.best_ask
                    live_pm_no = pm_no_live.best_ask
                    live_k_yes = k_live.best_yes_ask
                    live_k_no = k_live.best_no_ask
                    live_edge = max(
                        1.0 - (live_pm_yes + live_k_no),
                        1.0 - (live_k_yes + live_pm_no),
                    )
                    live_str = (
                        f"  {pm.symbol:6}   live: PM yes={live_pm_yes:.3f} no={live_pm_no:.3f}"
                        f" | K yes={live_k_yes:.3f} no={live_k_no:.3f}"
                        f" | edge={live_edge:.3f}"
                    )
                else:
                    live_str = None

                match_lines.append(
                    f"  {pm.symbol:6} | PM yes={pm.yes_ask:.3f} no={pm.no_ask:.3f}"
                    f" | K yes={ka.yes_ask:.3f} no={ka.no_ask:.3f}"
                    f" | best_edge={best_edge:.3f}"
                    f" | {gap_str} {gap_ok}"
                )
                if live_str:
                    match_lines.append(live_str)

            # Kalshi-рынки без PM-матча
            unmatched_kalshi = [k for k in kalshi_markets if k.symbol not in matched_symbols]
            for k in unmatched_kalshi:
                match_lines.append(f"  {k.symbol:6} | no PM match (K yes={k.yes_ask:.3f} no={k.no_ask:.3f})")

            total_shown = len(matches) + len(unmatched_kalshi)
            if total_shown:
                print(f"[fast-arb][Matches] accepted={accepted}/{len(matches)} matched | {len(unmatched_kalshi)} kalshi unmatched")
                for line in match_lines:
                    print(line)
        except Exception:
            pass
