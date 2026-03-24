from __future__ import annotations

import threading
import time

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient, TopOfBook
from cross_arb_bot.models import MatchedMarketPair
from momentum_bot.spike_detector import SpikeDetector

from real_momentum_bot.engine import RealMomentumEngine


class RealMomentumWatchRunner:
    MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, engine: RealMomentumEngine) -> None:
        self.engine = engine
        strat = engine.strategy
        self.spike_detector = SpikeDetector(
            window_seconds=strat["spike_window_seconds"],
            threshold_cents=strat["spike_threshold_cents"],
        )
        self.watched: dict[str, MatchedMarketPair] = {}
        self.pairs_by_asset_id: dict[str, set[str]] = {}
        self.pairs_by_kalshi_ticker: dict[str, set[str]] = {}
        self.token_to_side: dict[str, str] = {}
        self.live_books: dict[str, TopOfBook] = {}
        self.live_books_kalshi: dict[str, KalshiTopOfBook] = {}
        self._kalshi_ws: KalshiWebSocketClient | None = None
        self._signal_lock = threading.Lock()
        self._current_asset_ids: list[str] = []
        self._current_kalshi_tickers: list[str] = []
        self._refresh_lock = threading.Lock()
        self._refresh_in_progress = False
        self._last_skip_log: dict[str, float] = {}  # key -> last log timestamp

    def run(self) -> None:
        runtime = self.engine.config["runtime"]
        universe_refresh = runtime["universe_refresh_seconds"]
        status_interval = runtime["status_interval_seconds"]

        ws: MarketWebSocketClient | None = None
        last_refresh = 0.0
        last_status = 0.0
        ws_active = False  # WS сейчас открыт?
        scanned_this_cycle = False  # сканировали ли рынки в текущем 15м цикле

        try:
            while True:
                if self.engine.is_stopped():
                    print("[RealMomentum][Watch] Stop-loss достигнут, выход.")
                    break

                import datetime as _dt
                now_dt = _dt.datetime.now(_dt.timezone.utc)
                minute_in_cycle = now_dt.minute % 15
                should_be_active = minute_in_cycle >= 8  # за минуту до окна ставок

                # Сканируем рынки один раз на минуте 2 каждого цикла
                if minute_in_cycle == 2 and not scanned_this_cycle and not self._refresh_in_progress:
                    print(f"[RealMomentum][Watch] Сканируем рынки (minute={now_dt.minute})")
                    self._start_refresh_async(prev_ws=None)
                    scanned_this_cycle = True
                elif minute_in_cycle != 2:
                    scanned_this_cycle = False

                # Открываем WS при входе в окно
                if should_be_active and not ws_active:
                    print(f"[RealMomentum][Watch] Окно ставок — открываем WS (minute={now_dt.minute})")
                    # Если ещё не сканировали — делаем scan сейчас
                    if not scanned_this_cycle and not self._refresh_in_progress:
                        self._start_refresh_async(prev_ws=None)
                        scanned_this_cycle = True
                    ws_active = True

                # Закрываем WS при выходе из окна
                elif not should_be_active and ws_active:
                    print(f"[RealMomentum][Watch] Вне окна ставок — закрываем WS (minute={now_dt.minute})")
                    if self._kalshi_ws is not None:
                        self._kalshi_ws.stop()
                        self._kalshi_ws = None
                    self._current_asset_ids = []
                    self._current_kalshi_tickers = []
                    ws_active = False

                now = time.time()
                if (now - last_status) >= status_interval:
                    self.engine.resolve()
                    self.engine.print_status()
                    print(
                        f"[RealMomentum][Watch] pairs={len(self.watched)}"
                        f" | ws={'on' if ws_active else 'off (waiting)'}"
                        f" | minute_in_cycle={minute_in_cycle}"
                    )
                    last_status = now

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[RealMomentum][Watch] Остановлен.")
        finally:
            if ws is not None:
                ws.stop()
            if self._kalshi_ws is not None:
                self._kalshi_ws.stop()

    def _start_refresh_async(self, prev_ws: MarketWebSocketClient | None = None) -> None:
        if self._refresh_in_progress:
            return
        self._refresh_in_progress = True
        t = threading.Thread(target=self._refresh_async_worker, args=(prev_ws,), daemon=True)
        t.start()

    def _refresh_async_worker(self, prev_ws: MarketWebSocketClient | None) -> None:
        try:
            self._refresh_watchlist(prev_ws=prev_ws)
        except Exception as exc:
            print(f"[RealMomentum][Watch] Ошибка сканирования: {exc}")
        finally:
            self._refresh_in_progress = False

    def _refresh_watchlist(self, prev_ws: MarketWebSocketClient | None = None) -> MarketWebSocketClient | None:
        print("[RealMomentum][Watch] Сканирование рынков...")
        matches = self.engine.discover_pairs()

        watched: dict[str, MatchedMarketPair] = {}
        pairs_by_asset: dict[str, set[str]] = {}
        pairs_by_kalshi: dict[str, set[str]] = {}
        token_to_side: dict[str, str] = {}
        asset_ids: list[str] = []

        for match in matches:
            pm = match.polymarket
            ka = match.kalshi
            pair_key = f"{pm.market_id}:{ka.market_id}"
            if not pm.yes_token_id or not pm.no_token_id:
                continue
            watched[pair_key] = match
            asset_ids.extend([pm.yes_token_id, pm.no_token_id])
            pairs_by_asset.setdefault(pm.yes_token_id, set()).add(pair_key)
            pairs_by_asset.setdefault(pm.no_token_id, set()).add(pair_key)
            pairs_by_kalshi.setdefault(ka.market_id, set()).add(pair_key)
            token_to_side[pm.yes_token_id] = "yes"
            token_to_side[pm.no_token_id] = "no"

        # Clear spike data for expired markets
        for pair_key in set(self.watched) - set(watched):
            old = self.watched.get(pair_key)
            if old:
                self.spike_detector.clear_market(old.polymarket.yes_token_id or "")
                self.spike_detector.clear_market(old.polymarket.no_token_id or "")
                self.spike_detector.clear_market(old.kalshi.market_id)

        self.watched = watched
        self.pairs_by_asset_id = pairs_by_asset
        self.pairs_by_kalshi_ticker = pairs_by_kalshi
        self.token_to_side = token_to_side

        new_kalshi_tickers = sorted(set(pairs_by_kalshi.keys()))
        self._refresh_kalshi_ws(new_kalshi_tickers)

        new_asset_ids = sorted(set(asset_ids))
        if not new_asset_ids:
            self.live_books = {}
            print("[RealMomentum][Watch] Нет пар для мониторинга.")
            return None

        kept = set(new_asset_ids)
        self.live_books = {k: v for k, v in self.live_books.items() if k in kept}

        if new_asset_ids == self._current_asset_ids and prev_ws is not None:
            print(f"[RealMomentum][Watch] {len(watched)} пар (WS переиспользован).")
            self._current_asset_ids = new_asset_ids
            return prev_ws

        if prev_ws is not None:
            prev_ws.stop()

        print(f"[RealMomentum][Watch] {len(watched)} пар, {len(new_asset_ids)} PM токенов.")
        self._current_asset_ids = new_asset_ids
        ws = MarketWebSocketClient(
            url=self.MARKET_WS_URL,
            asset_ids=new_asset_ids,
            on_message=self.on_ws_message,
        )
        ws.start()
        return ws

    def _refresh_kalshi_ws(self, new_tickers: list[str]) -> None:
        if new_tickers == self._current_kalshi_tickers and self._kalshi_ws is not None:
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
            self._kalshi_ws = KalshiWebSocketClient(tickers=new_tickers, on_update=self.on_kalshi_update)
            self._kalshi_ws.start()
            print(f"[RealMomentum][Watch] Kalshi WS: {len(new_tickers)} тикеров.")
        except Exception as exc:
            print(f"[RealMomentum][Watch] Kalshi WS недоступен: {exc}")
            self._kalshi_ws = None

    def on_ws_message(self, payload: dict) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self.on_ws_message(item)
            return

        event_type = payload.get("event_type")
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

        side = self.token_to_side.get(asset_id)
        if side and best_ask > 0:
            self.spike_detector.record("polymarket", asset_id, side, best_ask)

        for pair_key in self.pairs_by_asset_id.get(asset_id, set()):
            self._check_for_signals(pair_key)

    def on_kalshi_update(self, ticker: str, top: KalshiTopOfBook) -> None:
        self.live_books_kalshi[ticker] = top
        now = time.time()
        if top.best_yes_ask > 0:
            self.spike_detector.record("kalshi", ticker, "yes", top.best_yes_ask, now)
        if top.best_no_ask > 0:
            self.spike_detector.record("kalshi", ticker, "no", top.best_no_ask, now)
        for pair_key in self.pairs_by_kalshi_ticker.get(ticker, set()):
            self._check_for_signals(pair_key)

    def _check_for_signals(self, pair_key: str) -> None:
        matched = self.watched.get(pair_key)
        if matched is None:
            return

        pm = matched.polymarket
        ka = matched.kalshi
        yes_token = pm.yes_token_id or ""
        no_token = pm.no_token_id or ""
        ka_ticker = ka.market_id

        combos = [
            ("polymarket", yes_token, "kalshi", "yes"),
            ("polymarket", no_token, "kalshi", "no"),
            ("kalshi", ka_ticker, "polymarket", "yes"),
            ("kalshi", ka_ticker, "polymarket", "no"),
        ]

        gap_min = self.engine.strategy.get("gap_signal_min_cents", 9999)

        disable_pm_kalshi = self.engine.strategy.get("disable_pm_to_kalshi", False)
        lookback = self.engine.strategy.get("gap_rising_lookback_seconds", 20.0)
        skip_log_interval = 30.0  # логируем одинаковый скип не чаще раза в 30с

        for leader_venue, leader_id, follower_venue, side in combos:
            if not leader_id:
                continue

            # Пропускаем PM→Kalshi если отключено (до любых вычислений)
            if disable_pm_kalshi and leader_venue == "polymarket" and follower_venue == "kalshi":
                continue

            spike = self.spike_detector.detect_spike(leader_venue, leader_id, side)
            leader_price = self.spike_detector.current_price(leader_venue, leader_id, side)
            if leader_price is None:
                continue

            follower_price = self._get_follower_price(follower_venue, matched, side)
            if follower_price is None or follower_price <= 0:
                continue

            gap_cents = (leader_price - follower_price) * 100

            if spike is None:
                if gap_cents < gap_min:
                    continue
                # Лидер не должен падать за последние N секунд
                baseline = self.spike_detector.baseline_price(leader_venue, leader_id, side, lookback)
                if not self.spike_detector.is_rising(leader_venue, leader_id, side, lookback):
                    skip_key = f"falling:{pair_key}:{leader_venue}:{side}"
                    now_ts = time.time()
                    if now_ts - self._last_skip_log.get(skip_key, 0) >= skip_log_interval:
                        baseline_str = f"{baseline:.4f}" if baseline is not None else "n/a"
                        print(
                            f"[RealMomentum][GAP-SKIP] {pm.symbol} {side.upper()}"
                            f" | leader={leader_venue} now={leader_price:.4f} baseline({lookback:.0f}s)={baseline_str}"
                            f" gap={gap_cents:.1f}c follower={follower_venue} price={follower_price:.4f}"
                            f" | лидер падает"
                        )
                        self._last_skip_log[skip_key] = now_ts
                    continue
                signal_type = "gap"
                spike_val = 0.0
            else:
                if not self.engine.strategy.get("spike_signals_enabled", True):
                    continue
                signal_type = "spike"
                spike_val = spike

            with self._signal_lock:
                reject_reason = self.engine.evaluate_signal(
                    pair_key=pair_key,
                    side=side,
                    leader_venue=leader_venue,
                    follower_venue=follower_venue,
                    leader_price=leader_price,
                    follower_price=follower_price,
                    spike_magnitude=spike_val,
                )
                if reject_reason is not None:
                    _silent = {"outside window", "duplicate position", "opposite side open"}
                    if reject_reason not in _silent:
                        skip_key = f"eval:{pair_key}:{leader_venue}:{side}:{reject_reason}"
                        now_ts = time.time()
                        if now_ts - self._last_skip_log.get(skip_key, 0) >= skip_log_interval:
                            print(
                                f"[RealMomentum][GAP-SKIP] {pm.symbol} {side.upper()}"
                                f" | leader={leader_venue} price={leader_price:.4f}"
                                f" gap={gap_cents:.1f}c follower={follower_venue} price={follower_price:.4f}"
                                f" | {reject_reason}"
                            )
                            self._last_skip_log[skip_key] = now_ts
                    continue

                print(
                    f"[RealMomentum][SIGNAL][{signal_type.upper()}] {pm.symbol} {side.upper()}"
                    f" | leader={leader_venue} price={leader_price:.4f}"
                    + (f" spike={spike_val:.1f}¢" if signal_type == "spike" else f" gap={gap_cents:.1f}¢")
                    + f" | follower={follower_venue} price={follower_price:.4f}"
                )

                # Коллбэк для проверки актуальной цены прямо перед отправкой ордера
                def _current_pm_price(fv=follower_venue, m=matched, s=side):
                    if fv != "polymarket":
                        return None
                    return self._get_follower_price(fv, m, s)

                self.engine.execute_trade(
                    pair_key=pair_key,
                    symbol=pm.symbol,
                    side=side,
                    leader_venue=leader_venue,
                    follower_venue=follower_venue,
                    leader_price=leader_price,
                    follower_price=follower_price,
                    spike_magnitude=spike_val,
                    matched=matched,
                    gap_cents=gap_cents,
                    get_current_price=_current_pm_price,
                )

    def _get_follower_price(self, follower_venue: str, matched: MatchedMarketPair, side: str) -> float | None:
        pm = matched.polymarket
        ka = matched.kalshi
        if follower_venue == "polymarket":
            token_id = pm.yes_token_id if side == "yes" else pm.no_token_id
            if not token_id:
                return None
            book = self.live_books.get(token_id)
            if book and book.best_ask > 0:
                return book.best_ask
            return pm.yes_ask if side == "yes" else pm.no_ask
        else:
            book = self.live_books_kalshi.get(ka.market_id)
            if book:
                return book.best_yes_ask if side == "yes" else book.best_no_ask
            return ka.yes_ask if side == "yes" else ka.no_ask
