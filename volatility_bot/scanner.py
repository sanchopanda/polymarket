from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from arb_bot.kalshi_ws import KalshiTopOfBook, KalshiWebSocketClient
from arb_bot.ws import MarketWebSocketClient
from cross_arb_bot.kalshi_feed import KalshiFeed
from cross_arb_bot.models import NormalizedMarket
from cross_arb_bot.polymarket_feed import PolymarketFeed

from volatility_bot.models import VolatilityMarket


PriceCallback = Callable[[VolatilityMarket, str, float], None]


class MarketScanner:
    """Fetches markets from REST APIs every scan cycle and manages WebSocket subscriptions.

    Calls price_callback(market, side, best_ask) from WS threads on every price update.
    """

    def __init__(self, config: dict) -> None:
        self._cfg = config
        mf = config["market_filter"]
        pm_cfg = config["polymarket"]
        k_cfg = config["kalshi"]

        self._allowed_intervals: set[int] = set(mf.get("interval_minutes", [5, 15, 60]))

        self._pm_feed = PolymarketFeed(
            base_url=pm_cfg["gamma_base_url"],
            page_size=pm_cfg["page_size"],
            request_delay_ms=pm_cfg["request_delay_ms"],
            market_filter={
                "min_days_to_expiry": mf["min_days_to_expiry"],
                "max_days_to_expiry": mf["max_days_to_expiry"],
                "min_volume": mf["min_volume"],
                "min_liquidity": mf["min_liquidity"],
                "fee_type": mf.get("fee_type", "crypto_fees"),
                "symbol": mf.get("symbol", ""),
            },
        )

        self._kalshi_feed = KalshiFeed(
            base_url=k_cfg["base_url"],
            page_size=k_cfg["page_size"],
            max_pages=k_cfg["max_pages"],
            request_timeout_seconds=k_cfg["request_timeout_seconds"],
            market_filter={
                "symbol": mf.get("symbol", ""),
            },
            series_tickers=k_cfg["series_tickers"],
        )

        # Active markets: key = "{venue}:{market_id}"
        self._active_markets: dict[str, VolatilityMarket] = {}
        self._lock = threading.Lock()

        # WebSocket state
        self._pm_ws: Optional[MarketWebSocketClient] = None
        self._pm_subscribed_asset_ids: set[str] = set()
        # token_id → (market, side)
        self._asset_to_market: dict[str, tuple[VolatilityMarket, str]] = {}

        self._kalshi_ws: Optional[KalshiWebSocketClient] = None
        self._kalshi_subscribed_tickers: set[str] = set()
        # ticker → market
        self._ticker_to_market: dict[str, VolatilityMarket] = {}

        self._price_callback: Optional[PriceCallback] = None
        self._pm_ws_url = pm_cfg.get("clob_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/")

    def set_price_callback(self, cb: PriceCallback) -> None:
        self._price_callback = cb

    def scan_and_subscribe(self) -> list[VolatilityMarket]:
        """Fetch markets, add new ones, restart WS if needed. Returns newly added markets."""
        new_markets: list[VolatilityMarket] = []
        now = datetime.utcnow()

        # ── Polymarket ────────────────────────────────────────────────
        try:
            pm_normalized = self._pm_feed.fetch_markets()
        except Exception as exc:
            print(f"[scanner] PM fetch error: {exc}")
            pm_normalized = []

        for nm in pm_normalized:
            vm = self._normalize_pm(nm)
            if vm is None:
                continue
            key = f"polymarket:{vm.market_id}"
            with self._lock:
                if key not in self._active_markets:
                    self._active_markets[key] = vm
                    new_markets.append(vm)
                else:
                    # refresh prices
                    self._active_markets[key].yes_ask = vm.yes_ask
                    self._active_markets[key].no_ask = vm.no_ask

        # ── Kalshi ───────────────────────────────────────────────────
        try:
            kalshi_normalized, err = self._kalshi_feed.fetch_markets()
        except Exception as exc:
            print(f"[scanner] Kalshi fetch error: {exc}")
            kalshi_normalized, err = [], str(exc)
        if err:
            print(f"[scanner] Kalshi warning: {err}")

        for nm in kalshi_normalized:
            vm = self._normalize_kalshi(nm)
            if vm is None:
                continue
            key = f"kalshi:{vm.market_id}"
            with self._lock:
                if key not in self._active_markets:
                    self._active_markets[key] = vm
                    new_markets.append(vm)
                else:
                    self._active_markets[key].yes_ask = vm.yes_ask
                    self._active_markets[key].no_ask = vm.no_ask

        # ── Evict expired markets ─────────────────────────────────────
        with self._lock:
            expired = [k for k, m in self._active_markets.items() if m.expiry <= now]
            for k in expired:
                del self._active_markets[k]

        # ── Update WebSocket subscriptions ────────────────────────────
        self._update_pm_ws()
        self._update_kalshi_ws()

        print(f"[scanner] scan done | active={len(self._active_markets)} new={len(new_markets)} expired={len(expired)}")
        return new_markets

    # ── Normalization ─────────────────────────────────────────────────────

    def _normalize_pm(self, nm: NormalizedMarket) -> Optional[VolatilityMarket]:
        if nm.interval_minutes not in self._allowed_intervals:
            return None
        if not nm.yes_token_id or not nm.no_token_id:
            return None
        market_start = nm.expiry - timedelta(minutes=nm.interval_minutes)
        return VolatilityMarket(
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
        )

    def _normalize_kalshi(self, nm: NormalizedMarket) -> Optional[VolatilityMarket]:
        if nm.interval_minutes not in self._allowed_intervals:
            return None
        market_start = nm.expiry - timedelta(minutes=nm.interval_minutes)
        return VolatilityMarket(
            venue="kalshi",
            market_id=nm.market_id,
            title=nm.title,
            symbol=nm.symbol,
            interval_minutes=nm.interval_minutes,
            expiry=nm.expiry,
            market_start=market_start,
            volume=nm.volume,
            yes_ask=nm.yes_ask,
            no_ask=nm.no_ask,
        )

    # ── Polymarket WebSocket ──────────────────────────────────────────────

    def _update_pm_ws(self) -> None:
        with self._lock:
            pm_markets = {
                k: m for k, m in self._active_markets.items() if m.venue == "polymarket"
            }

        new_asset_ids: set[str] = set()
        new_asset_map: dict[str, tuple[VolatilityMarket, str]] = {}
        for m in pm_markets.values():
            if m.yes_token_id:
                new_asset_ids.add(m.yes_token_id)
                new_asset_map[m.yes_token_id] = (m, "yes")
            if m.no_token_id:
                new_asset_ids.add(m.no_token_id)
                new_asset_map[m.no_token_id] = (m, "no")

        if new_asset_ids == self._pm_subscribed_asset_ids:
            return  # no change

        # Restart WS with updated asset list
        if self._pm_ws is not None:
            self._pm_ws.stop()
            self._pm_ws = None

        if not new_asset_ids:
            return

        self._pm_subscribed_asset_ids = new_asset_ids
        self._asset_to_market = new_asset_map

        self._pm_ws = MarketWebSocketClient(
            url=self._pm_ws_url,
            asset_ids=list(new_asset_ids),
            on_message=self._on_pm_message,
        )
        self._pm_ws.start()
        print(f"[scanner] PM WS (re)started: {len(new_asset_ids)} assets")

    def _on_pm_message(self, payload) -> None:
        """Called from PM WS thread on each message."""
        if not self._price_callback:
            return

        # Top-level can be a list of events
        if isinstance(payload, list):
            for item in payload:
                self._on_pm_message(item)
            return

        # Handle list of changes (BBA / price_change events)
        changes = payload.get("changes") or []
        if isinstance(changes, list):
            for change in changes:
                self._dispatch_pm_change(change)
            return

        # Single-item payload
        self._dispatch_pm_change(payload)

    def _dispatch_pm_change(self, item: dict) -> None:
        if not self._price_callback:
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
                    best_ask = float(asks[0]["price"])
                except (TypeError, ValueError, KeyError, IndexError):
                    pass
        else:
            # Fallback for unknown formats
            for field in ("best_ask", "price"):
                raw = item.get(field)
                if raw is not None:
                    try:
                        best_ask = float(raw)
                        break
                    except (TypeError, ValueError):
                        pass
            if best_ask is None:
                asks = item.get("asks") or []
                if asks:
                    try:
                        best_ask = float(asks[0]["price"])
                    except Exception:
                        pass

        if best_ask is None or best_ask <= 0:
            return

        # Update stored price
        with self._lock:
            key = f"polymarket:{market.market_id}"
            if key in self._active_markets:
                if side == "yes":
                    self._active_markets[key].yes_ask = best_ask
                else:
                    self._active_markets[key].no_ask = best_ask
                market = self._active_markets[key]

        self._price_callback(market, side, best_ask)

    # ── Kalshi WebSocket ──────────────────────────────────────────────────

    def _update_kalshi_ws(self) -> None:
        with self._lock:
            kalshi_markets = {
                k: m for k, m in self._active_markets.items() if m.venue == "kalshi"
            }

        new_tickers = {m.market_id for m in kalshi_markets.values()}
        new_ticker_map = {m.market_id: m for m in kalshi_markets.values()}

        if new_tickers == self._kalshi_subscribed_tickers:
            return  # no change

        if self._kalshi_ws is not None:
            self._kalshi_ws.stop()
            self._kalshi_ws = None

        if not new_tickers:
            return

        self._kalshi_subscribed_tickers = new_tickers
        self._ticker_to_market = new_ticker_map

        try:
            self._kalshi_ws = KalshiWebSocketClient(
                tickers=list(new_tickers),
                on_update=self._on_kalshi_update,
            )
            self._kalshi_ws.start()
            print(f"[scanner] Kalshi WS (re)started: {len(new_tickers)} tickers")
        except Exception as exc:
            print(f"[scanner] Kalshi WS start error: {exc}")
            self._kalshi_ws = None

    def _on_kalshi_update(self, ticker: str, tob: KalshiTopOfBook) -> None:
        if not self._price_callback:
            return

        with self._lock:
            market = self._ticker_to_market.get(ticker)
            if market is None:
                return
            key = f"kalshi:{ticker}"
            if key in self._active_markets:
                self._active_markets[key].yes_ask = tob.best_yes_ask
                self._active_markets[key].no_ask = tob.best_no_ask
                market = self._active_markets[key]

        if tob.best_yes_ask > 0:
            self._price_callback(market, "yes", tob.best_yes_ask)
        if tob.best_no_ask > 0:
            self._price_callback(market, "no", tob.best_no_ask)

    # ── Accessors ─────────────────────────────────────────────────────────

    def get_market(self, venue: str, market_id: str) -> Optional[VolatilityMarket]:
        with self._lock:
            return self._active_markets.get(f"{venue}:{market_id}")

    def all_markets(self) -> list[VolatilityMarket]:
        with self._lock:
            return list(self._active_markets.values())

    def stop(self) -> None:
        if self._pm_ws:
            self._pm_ws.stop()
        if self._kalshi_ws:
            self._kalshi_ws.stop()
