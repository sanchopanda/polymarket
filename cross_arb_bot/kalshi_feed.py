from __future__ import annotations

import re
from datetime import datetime

import httpx

from cross_arb_bot.models import NormalizedMarket
from src.api.clob import OrderBook, OrderLevel


KALSHI_UPDOWN_RE = re.compile(r"^(?P<symbol>[A-Za-z]+)\s+price\s+up\s+in\s+next\s+(?P<minutes>\d+)\s+mins\?$", re.IGNORECASE)
KALSHI_HOUR_RE = re.compile(r"^(?P<symbol>[A-Za-z]+)\s+price\s+up\s+this\s+hour\?$", re.IGNORECASE)

# Above/below strike-based hourly series (KXBTCD, KXETHD, …)
# Each entry: (binance_symbol, floor_strike_increment, floor_strike_correction, ticker_decimals)
# floor_strike = n * increment - correction, formatted to ticker_decimals decimal places
_ABOVE_BELOW_SERIES: dict[str, tuple[str, str, float, float, int]] = {
    #  series     symbol  binance     increment  correction  decimals
    "KXBTCD": ("BTC",  "BTCUSDT",  100.0,     0.01,       2),
    "KXETHD": ("ETH",  "ETHUSDT",  20.0,      0.01,       2),
    "KXSOLD": ("SOL",  "SOLUSDT",  1.0,       0.0001,     4),
    "KXXRPD": ("XRP",  "XRPUSDT",  0.02,      0.0001,     4),
}
_BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)


def _to_float(raw, default=0.0):
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


class KalshiFeed:
    def __init__(
        self,
        base_url: str,
        page_size: int,
        max_pages: int,
        request_timeout_seconds: int,
        market_filter: dict,
        series_tickers: list[str],
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.max_pages = max_pages
        self.market_filter = market_filter
        self.series_tickers = series_tickers
        self.http = httpx.Client(timeout=request_timeout_seconds)

    def fetch_markets(self) -> tuple[list[NormalizedMarket], str | None]:
        try:
            rows = self._get_markets()
        except httpx.HTTPError as exc:
            return [], f"kalshi fetch failed: {exc}"

        symbol_filter = (self.market_filter.get("symbol") or "").strip().lower()
        result: list[NormalizedMarket] = []

        for row in rows:
            normalized = self._normalize_market(row)
            if normalized is None:
                continue
            if symbol_filter and normalized.symbol.lower() != symbol_filter:
                continue
            result.append(normalized)

        for normalized in self._fetch_hourly_markets():
            if symbol_filter and normalized.symbol.lower() != symbol_filter:
                continue
            result.append(normalized)

        return result, None

    def fetch_market(self, ticker: str) -> tuple[dict | None, str | None]:
        try:
            response = self.http.get(f"{self.base_url}/markets/{ticker}")
            response.raise_for_status()
            return response.json().get("market"), None
        except httpx.HTTPError as exc:
            return None, f"kalshi market fetch failed: {exc}"

    def fetch_orderbook(self, ticker: str) -> tuple[OrderBook | None, str | None]:
        try:
            response = self.http.get(f"{self.base_url}/markets/{ticker}/orderbook")
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            return None, f"kalshi orderbook fetch failed: {exc}"

        orderbook = payload.get("orderbook_fp") or payload.get("orderbook") or {}
        yes_bids_raw = orderbook.get("yes_dollars") or orderbook.get("yes") or []
        no_bids_raw = orderbook.get("no_dollars") or orderbook.get("no") or []

        yes_bids = [self._level_from_pair(item) for item in yes_bids_raw]
        no_bids = [self._level_from_pair(item) for item in no_bids_raw]
        yes_bids = [item for item in yes_bids if item is not None]
        no_bids = [item for item in no_bids if item is not None]

        # Binary market mechanics:
        # buy YES ask = 1 - best NO bid
        # buy NO ask = 1 - best YES bid
        yes_asks = [OrderLevel(price=max(0.0, 1.0 - bid.price), size=bid.size) for bid in no_bids]
        no_asks = [OrderLevel(price=max(0.0, 1.0 - bid.price), size=bid.size) for bid in yes_bids]

        yes_asks.sort(key=lambda x: x.price)
        no_asks.sort(key=lambda x: x.price)
        yes_bids.sort(key=lambda x: x.price, reverse=True)
        no_bids.sort(key=lambda x: x.price, reverse=True)

        # We return asks for YES in asks, and keep YES bids in bids. NO-side asks are fetched separately.
        return OrderBook(bids=yes_bids, asks=yes_asks), None

    def fetch_side_asks(self, ticker: str, side: str) -> tuple[list[OrderLevel] | None, str | None]:
        try:
            response = self.http.get(f"{self.base_url}/markets/{ticker}/orderbook")
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            return None, f"kalshi orderbook fetch failed: {exc}"

        orderbook = payload.get("orderbook_fp") or payload.get("orderbook") or {}
        yes_bids_raw = orderbook.get("yes_dollars") or orderbook.get("yes") or []
        no_bids_raw = orderbook.get("no_dollars") or orderbook.get("no") or []
        yes_bids = [self._level_from_pair(item) for item in yes_bids_raw]
        no_bids = [self._level_from_pair(item) for item in no_bids_raw]
        yes_bids = [item for item in yes_bids if item is not None]
        no_bids = [item for item in no_bids if item is not None]

        if side == "yes":
            asks = [OrderLevel(price=max(0.0, 1.0 - bid.price), size=bid.size) for bid in no_bids]
        else:
            asks = [OrderLevel(price=max(0.0, 1.0 - bid.price), size=bid.size) for bid in yes_bids]
        asks.sort(key=lambda x: x.price)
        return asks, None

    def fetch_side_bids(self, ticker: str, side: str) -> tuple[list[OrderLevel] | None, str | None]:
        """Fetch bid levels for a given side (for selling positions back)."""
        try:
            response = self.http.get(f"{self.base_url}/markets/{ticker}/orderbook")
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            return None, f"kalshi orderbook fetch failed: {exc}"

        orderbook = payload.get("orderbook_fp") or payload.get("orderbook") or {}
        yes_bids_raw = orderbook.get("yes_dollars") or orderbook.get("yes") or []
        no_bids_raw = orderbook.get("no_dollars") or orderbook.get("no") or []

        if side == "yes":
            bids = [self._level_from_pair(item) for item in yes_bids_raw]
        else:
            bids = [self._level_from_pair(item) for item in no_bids_raw]

        bids = [b for b in bids if b is not None]
        bids.sort(key=lambda x: x.price, reverse=True)
        return bids, None

    def _fetch_binance_price(self, binance_symbol: str) -> float | None:
        try:
            resp = self.http.get(_BINANCE_TICKER_URL, params={"symbol": binance_symbol})
            resp.raise_for_status()
            return float(resp.json()["price"])
        except Exception:
            return None

    def _get_events(self, series_ticker: str) -> list[dict]:
        try:
            resp = self.http.get(
                f"{self.base_url}/events",
                params={"series_ticker": series_ticker, "status": "open", "limit": 20},
            )
            resp.raise_for_status()
            return resp.json().get("events", [])
        except Exception:
            return []

    def _atm_tickers(self, event_ticker: str, series: str, price: float) -> list[str]:
        """Return [nearest, next-nearest] market tickers for the given price."""
        _, _, increment, correction, decimals = _ABOVE_BELOW_SERIES[series]
        n = int(price / increment)
        candidates = sorted([n, n + 1], key=lambda k: abs(k * increment - price))
        tickers = []
        for k in candidates:
            if k <= 0:
                continue
            fs = k * increment - correction
            tickers.append(f"{event_ticker}-T{fs:.{decimals}f}")
        return tickers

    def _fetch_hourly_markets(self) -> list[NormalizedMarket]:
        """For each KXBTCD/KXETHD/… event, fetch the ATM sub-market directly by ticker."""
        result: list[NormalizedMarket] = []
        active = [s for s in self.series_tickers if s in _ABOVE_BELOW_SERIES]
        for series in active:
            symbol, binance_sym, _, _, _ = _ABOVE_BELOW_SERIES[series]
            price = self._fetch_binance_price(binance_sym)
            if price is None:
                continue
            for event in self._get_events(series):
                et = event.get("event_ticker", "")
                if not et:
                    continue
                for ticker in self._atm_tickers(et, series, price):
                    try:
                        resp = self.http.get(f"{self.base_url}/markets/{ticker}")
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()
                        market_data = resp.json().get("market") or {}
                        normalized = self._normalize_above_below_market(market_data, symbol)
                        if normalized is not None:
                            result.append(normalized)
                            break  # got a valid market for this event
                    except Exception:
                        continue
        return result

    def _normalize_above_below_market(self, row: dict, symbol: str) -> NormalizedMarket | None:
        expiry = (
            _parse_dt(row.get("close_time"))
            or _parse_dt(row.get("expected_expiration_time"))
            or _parse_dt(row.get("expiration_time"))
        )
        if expiry is None:
            return None
        yes_ask = _to_float(row.get("yes_ask_dollars"))
        no_ask = _to_float(row.get("no_ask_dollars"))
        if yes_ask <= 0 or no_ask <= 0:
            return None
        return NormalizedMarket(
            venue="kalshi",
            market_id=str(row.get("ticker") or ""),
            title=str(row.get("title") or ""),
            symbol=symbol,
            market_kind="updown",
            expiry=expiry,
            yes_label="Above",
            no_label="Below",
            yes_ask=yes_ask,
            no_ask=no_ask,
            yes_bid=_to_float(row.get("yes_bid_dollars")),
            no_bid=_to_float(row.get("no_bid_dollars")),
            yes_depth=_to_float(row.get("yes_ask_size_fp")),
            no_depth=_to_float(row.get("no_ask_size_fp")),
            volume=_to_float(row.get("volume")),
            liquidity=max(_to_float(row.get("yes_ask_size_fp")), _to_float(row.get("no_ask_size_fp"))),
            interval_minutes=60,
            rule_family="price_direction",
            reference_price=_to_float(row.get("floor_strike"), default=None),
            rules_text=str(row.get("rules_primary") or ""),
        )

    def _level_from_pair(self, item) -> OrderLevel | None:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            return None
        try:
            return OrderLevel(price=float(item[0]), size=float(item[1]))
        except (TypeError, ValueError):
            return None

    def _get_markets(self) -> list[dict]:
        rows: list[dict] = []
        for series_ticker in self.series_tickers:
            if series_ticker in _ABOVE_BELOW_SERIES:
                continue  # fetched separately via _fetch_hourly_markets
            cursor: str | None = None
            for _page in range(self.max_pages):
                params = {
                    "status": "open",
                    "limit": self.page_size,
                    "series_ticker": series_ticker,
                }
                if cursor:
                    params["cursor"] = cursor
                response = self.http.get(f"{self.base_url}/markets", params=params)
                response.raise_for_status()
                payload = response.json()
                batch = payload.get("markets", [])
                rows.extend(batch)
                cursor = payload.get("cursor")
                if not cursor or not batch:
                    break
        return rows

    def _normalize_market(self, row: dict) -> NormalizedMarket | None:
        title = str(row.get("title") or "")
        expiry = _parse_dt(row.get("close_time")) or _parse_dt(row.get("expected_expiration_time")) or _parse_dt(row.get("expiration_time"))
        if not title or expiry is None:
            return None

        match = KALSHI_UPDOWN_RE.match(title)
        interval_minutes: int | None = None
        if match:
            symbol = match.group("symbol").upper()
            interval_minutes = int(match.group("minutes"))
        else:
            hour_match = KALSHI_HOUR_RE.match(title)
            if not hour_match:
                return None
            symbol = hour_match.group("symbol").upper()
            interval_minutes = 60

        if interval_minutes is None:
            return None
        yes_ask = _to_float(row.get("yes_ask_dollars"))
        no_ask = _to_float(row.get("no_ask_dollars"))
        if yes_ask <= 0 or no_ask <= 0:
            return None

        return NormalizedMarket(
            venue="kalshi",
            market_id=str(row.get("ticker") or ""),
            title=title,
            symbol=symbol,
            market_kind="updown",
            expiry=expiry,
            yes_label=str(row.get("yes_sub_title") or "Up"),
            no_label=str(row.get("no_sub_title") or "Down"),
            yes_ask=yes_ask,
            no_ask=no_ask,
            yes_bid=_to_float(row.get("yes_bid_dollars")),
            no_bid=_to_float(row.get("no_bid_dollars")),
            yes_depth=_to_float(row.get("yes_ask_size_fp")),
            no_depth=_to_float(row.get("no_ask_size_fp")),
            volume=_to_float(row.get("volume")),
            liquidity=max(_to_float(row.get("yes_ask_size_fp")), _to_float(row.get("no_ask_size_fp"))),
            interval_minutes=interval_minutes,
            rule_family="price_direction",
            reference_price=_to_float(row.get("floor_strike"), default=None),
            rules_text=str(row.get("rules_primary") or ""),
        )
