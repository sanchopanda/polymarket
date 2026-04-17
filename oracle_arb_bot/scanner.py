from __future__ import annotations

import calendar
import os
import threading
from datetime import datetime, timedelta
from typing import Callable, Optional

from arb_bot.ws import MarketWebSocketClient
from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.polymarket_feed import PolymarketFeed

from oracle_arb_bot.models import OracleMarket


class OracleScanner:
    """
    Получает 5m и 15m PM + Kalshi крипто-рынки через REST.
    Управляет PM и Kalshi WebSocket подписками для real-time ask обновлений.
    Вызывает price_callback(market, side, best_ask) из WS-потока.
    """

    def __init__(self, config: dict) -> None:
        cfg_pm = config["polymarket"]
        cfg_mf = config["market_filter"]

        self._pm_paper: bool = cfg_pm.get("paper", True)
        self._pm_feed = PolymarketFeed(
            base_url=cfg_pm["gamma_base_url"],
            page_size=cfg_pm["page_size"],
            request_delay_ms=cfg_pm["request_delay_ms"],
            market_filter={
                "min_days_to_expiry": cfg_mf["min_days_to_expiry"],
                "max_days_to_expiry": cfg_mf["max_days_to_expiry"],
                "min_volume": cfg_mf["min_volume"],
                "min_liquidity": cfg_mf["min_liquidity"],
                "fee_type": cfg_mf.get("fee_type", "crypto_fees"),
                "symbol": "",  # фильтруем по символу сами ниже
            },
        )
        self._allowed_symbols: set[str] = set(cfg_mf["symbols"])
        self._allowed_intervals: set[int] = set(cfg_mf["interval_minutes"])
        self._pm_ws_url: str = cfg_pm["clob_ws_url"]

        self._active_markets: dict[str, OracleMarket] = {}  # key = market_id
        self._asset_to_market: dict[str, tuple[OracleMarket, str]] = {}
        self._lock = threading.Lock()

        self._pm_ws: Optional[MarketWebSocketClient] = None
        self._pm_subscribed_asset_ids: set[str] = set()

        self._pm_price_callback: Optional[Callable] = None

        # ── Kalshi ────────────────────────────────────────────────────────
        cfg_kalshi = config.get("kalshi", {})
        self._kalshi_paper: bool = cfg_kalshi.get("paper", False)
        self._kalshi_feed: Optional[KalshiFeed] = None
        self._kalshi_ws = None
        self._kalshi_subscribed_tickers: set[str] = set()
        self._kalshi_ws_available: bool = False

        if cfg_kalshi.get("paper") or cfg_kalshi.get("real"):
            self._kalshi_feed = KalshiFeed(
                base_url=cfg_kalshi.get("base_url", "https://api.elections.kalshi.com/trade-api/v2"),
                page_size=cfg_kalshi.get("page_size", 200),
                max_pages=cfg_kalshi.get("max_pages", 10),
                request_timeout_seconds=cfg_kalshi.get("request_timeout_seconds", 20),
                market_filter={
                    "symbol": "",
                    "min_days_to_expiry": cfg_mf["min_days_to_expiry"],
                    "max_days_to_expiry": cfg_mf["max_days_to_expiry"],
                },
                series_tickers=cfg_kalshi.get("series_tickers", []),
            )
            # WS requires auth env vars
            if os.environ.get("KALSHI_API_KEY_ID") and os.environ.get("KALSHI_PRIVATE_KEY_PATH"):
                self._kalshi_ws_available = True
            else:
                print("[scanner] KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH not set — Kalshi WS disabled (REST only)")

    @property
    def kalshi_feed(self) -> Optional[KalshiFeed]:
        return self._kalshi_feed

    def set_pm_price_callback(self, cb: Callable) -> None:
        self._pm_price_callback = cb

    def scan_and_subscribe(self) -> list[OracleMarket]:
        """Fetch markets from REST, evict expired, update WS. Returns newly added markets."""
        now = datetime.utcnow()
        new_markets: list[OracleMarket] = []

        with self._lock:
            # Evict expired
            expired = [k for k, m in self._active_markets.items() if m.expiry <= now]
            for k in expired:
                m = self._active_markets[k]
                print(f"[scanner] evicted {m.symbol} {m.interval_minutes}m [{m.venue}]")
                del self._active_markets[k]

        # ── Polymarket ────────────────────────────────────────────────────
        if self._pm_paper:
            try:
                raw_markets = self._pm_feed.fetch_markets()
            except Exception as exc:
                print(f"[scanner] PM fetch error: {exc}")
                raw_markets = []

            with self._lock:
                for nm in raw_markets:
                    if nm.symbol not in self._allowed_symbols:
                        continue
                    if nm.interval_minutes not in self._allowed_intervals:
                        continue
                    if nm.expiry <= now:
                        continue

                    market_start = nm.expiry - timedelta(minutes=nm.interval_minutes)

                    if nm.market_id in self._active_markets:
                        m = self._active_markets[nm.market_id]
                        m.yes_ask = nm.yes_ask
                        m.no_ask = nm.no_ask
                        continue

                    pm_event_slug = nm.pm_event_slug
                    if pm_event_slug is None:
                        ts = calendar.timegm(market_start.timetuple())
                        pm_event_slug = f"{nm.symbol.lower()}-updown-{nm.interval_minutes}m-{ts}"

                    market = OracleMarket(
                        venue="polymarket",
                        market_id=nm.market_id,
                        title=nm.title,
                        symbol=nm.symbol,
                        interval_minutes=nm.interval_minutes,
                        expiry=nm.expiry,
                        market_start=market_start,
                        volume=nm.volume,
                        yes_ask=nm.yes_ask,
                        no_ask=nm.no_ask,
                        yes_token_id=nm.yes_token_id,
                        no_token_id=nm.no_token_id,
                        pm_event_slug=pm_event_slug,
                    )
                    self._active_markets[nm.market_id] = market
                    new_markets.append(market)
                    print(
                        f"[scanner] new PM: {nm.symbol} {nm.interval_minutes}m "
                        f"start={market_start.strftime('%H:%M')} end={nm.expiry.strftime('%H:%M')}"
                    )

        # ── Kalshi ────────────────────────────────────────────────────────
        if self._kalshi_feed and self._kalshi_paper:
            try:
                kalshi_markets, err = self._kalshi_feed.fetch_markets()
                if err:
                    print(f"[scanner] Kalshi: {err}")
                    kalshi_markets = []
            except Exception as exc:
                print(f"[scanner] Kalshi fetch error: {exc}")
                kalshi_markets = []

            with self._lock:
                for nm in kalshi_markets:
                    if nm.symbol not in self._allowed_symbols:
                        continue
                    if nm.interval_minutes not in self._allowed_intervals:
                        continue
                    if nm.expiry <= now:
                        continue

                    market_start = nm.expiry - timedelta(minutes=nm.interval_minutes)

                    if nm.market_id in self._active_markets:
                        m = self._active_markets[nm.market_id]
                        m.yes_ask = nm.yes_ask
                        m.no_ask = nm.no_ask
                        continue

                    market = OracleMarket(
                        venue="kalshi",
                        market_id=nm.market_id,  # Kalshi ticker
                        title=nm.title,
                        symbol=nm.symbol,
                        interval_minutes=nm.interval_minutes,
                        expiry=nm.expiry,
                        market_start=market_start,
                        volume=nm.volume,
                        yes_ask=nm.yes_ask,
                        no_ask=nm.no_ask,
                        pm_open_price=nm.reference_price,  # floor_strike = open price
                    )
                    self._active_markets[nm.market_id] = market
                    new_markets.append(market)
                    ref = f" ref={nm.reference_price}" if nm.reference_price else ""
                    print(
                        f"[scanner] new Kalshi: {nm.symbol} {nm.interval_minutes}m "
                        f"start={market_start.strftime('%H:%M')} end={nm.expiry.strftime('%H:%M')}{ref}"
                    )

        self._update_pm_ws()
        self._update_kalshi_ws()
        return new_markets

    def all_markets(self) -> list[OracleMarket]:
        with self._lock:
            return list(self._active_markets.values())

    def get_market(self, market_id: str) -> Optional[OracleMarket]:
        with self._lock:
            return self._active_markets.get(market_id)

    def stop(self) -> None:
        if self._pm_ws:
            self._pm_ws.stop()
        if self._kalshi_ws:
            self._kalshi_ws.stop()

    # ── PM WebSocket ──────────────────────────────────────────────────────

    def _update_pm_ws(self) -> None:
        if not self._pm_price_callback:
            return  # PM WS отключён (нет callback)
        with self._lock:
            new_asset_ids: set[str] = set()
            new_asset_map: dict[str, tuple[OracleMarket, str]] = {}
            for m in self._active_markets.values():
                if m.yes_token_id:
                    new_asset_ids.add(m.yes_token_id)
                    new_asset_map[m.yes_token_id] = (m, "yes")
                if m.no_token_id:
                    new_asset_ids.add(m.no_token_id)
                    new_asset_map[m.no_token_id] = (m, "no")

        if new_asset_ids == self._pm_subscribed_asset_ids:
            return

        if self._pm_ws is not None:
            self._pm_ws.stop()
            self._pm_ws = None

        if not new_asset_ids:
            self._pm_subscribed_asset_ids = set()
            return

        self._pm_subscribed_asset_ids = new_asset_ids
        with self._lock:
            self._asset_to_market = new_asset_map

        self._pm_ws = MarketWebSocketClient(
            url=self._pm_ws_url,
            asset_ids=list(new_asset_ids),
            on_message=self._on_pm_message,
        )
        self._pm_ws.start()
        print(f"[scanner] PM WS (re)started: {len(new_asset_ids)} assets")

    def _on_pm_message(self, payload) -> None:
        if isinstance(payload, list):
            for item in payload:
                self._on_pm_message(item)
            return

        changes = payload.get("changes")
        if changes:
            for change in changes:
                self._dispatch_pm_change(change)
            return

        self._dispatch_pm_change(payload)

    def _dispatch_pm_change(self, item: dict) -> None:
        if not self._pm_price_callback:
            return

        asset_id = item.get("asset_id") or item.get("token_id") or ""
        if not asset_id:
            return

        with self._lock:
            entry = self._asset_to_market.get(asset_id)
        if entry is None:
            return

        market, side = entry
        event_type = item.get("event_type") or ""
        best_ask = None

        if event_type == "best_bid_ask":
            try:
                best_ask = float(item.get("best_ask") or 0)
            except (TypeError, ValueError):
                pass
        elif event_type == "book":
            asks = item.get("asks") or []
            if asks:
                try:
                    # Polymarket WS отдаёт asks по убыванию цены — best ask = последний
                    best_ask = min(float(a["price"]) for a in asks)
                except (TypeError, ValueError, KeyError):
                    pass
        elif event_type == "last_trade_price":
            return  # цена последней сделки — не цена спроса, игнорируем
        else:
            # Неизвестный тип — берём только если есть явный best_ask, не price
            raw = item.get("best_ask")
            if raw is not None:
                try:
                    best_ask = float(raw)
                except (TypeError, ValueError):
                    pass
            if best_ask is None:
                asks = item.get("asks") or []
                if asks:
                    try:
                        best_ask = min(float(a["price"]) for a in asks)
                    except Exception:
                        pass

        if best_ask is None or best_ask <= 0:
            return

        # Обновляем цену в market объекте
        with self._lock:
            if market.market_id in self._active_markets:
                if side == "yes":
                    self._active_markets[market.market_id].yes_ask = best_ask
                else:
                    self._active_markets[market.market_id].no_ask = best_ask
                market = self._active_markets[market.market_id]

        self._pm_price_callback(market, side, best_ask)

    # ── Kalshi WebSocket ─────────────────────────────────────────────────

    def _update_kalshi_ws(self) -> None:
        if not self._kalshi_feed or not self._kalshi_ws_available:
            return

        with self._lock:
            kalshi_tickers = [
                m.market_id for m in self._active_markets.values()
                if m.venue == "kalshi"
            ]

        if set(kalshi_tickers) == self._kalshi_subscribed_tickers:
            return

        if self._kalshi_ws is not None:
            self._kalshi_ws.stop()
            self._kalshi_ws = None

        if not kalshi_tickers:
            self._kalshi_subscribed_tickers = set()
            return

        self._kalshi_subscribed_tickers = set(kalshi_tickers)
        try:
            from arb_bot.kalshi_ws import KalshiWebSocketClient
            self._kalshi_ws = KalshiWebSocketClient(
                tickers=kalshi_tickers,
                on_update=self._on_kalshi_update,
            )
            self._kalshi_ws.start()
            print(f"[scanner] Kalshi WS (re)started: {len(kalshi_tickers)} tickers")
        except Exception as exc:
            print(f"[scanner] Kalshi WS start failed: {exc}")
            self._kalshi_ws = None

    def _on_kalshi_update(self, ticker: str, top_of_book) -> None:
        with self._lock:
            market = self._active_markets.get(ticker)
            if not market:
                return
            market.yes_ask = top_of_book.best_yes_ask
            market.no_ask = top_of_book.best_no_ask

        if self._pm_price_callback:
            self._pm_price_callback(market, "yes", top_of_book.best_yes_ask)
