from __future__ import annotations

import calendar
import re
from datetime import datetime, timedelta, timezone

from src.api.gamma import GammaClient, Market

from cross_arb_bot.models import NormalizedMarket


UPDOWN_RE = re.compile(r"^(?P<symbol>[A-Za-z]+)\s+Up or Down\s+-", re.IGNORECASE)
MINUTE_WINDOW_RE = re.compile(r"(?P<start>\d{1,2}:\d{2}(?:AM|PM))-(?P<end>\d{1,2}:\d{2}(?:AM|PM))\s+ET", re.IGNORECASE)

SYMBOL_MAP = {
    "BITCOIN": "BTC",
    "ETHEREUM": "ETH",
    "SOLANA": "SOL",
    "DOGECOIN": "DOGE",
    "HYPERLIQUID": "HYPE",
}

# Hourly slug: normalized symbol → full name used in Polymarket event slug
HOURLY_SYMBOL_SLUG = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "xrp",
}

# EDT = UTC-4 (Apr–Oct). Same pattern as research_bot/fetch_hourly.py.
# ⚠ Change to timedelta(hours=-5) when DST ends in November.
_ET_OFFSET = timedelta(hours=-4)


def _hourly_slug(slug_name: str, window_start_et: datetime) -> str:
    """Build PM hourly event slug from ET window start datetime.

    Example: _hourly_slug("bitcoin", datetime(2026, 4, 8, 15, 0))
             → "bitcoin-up-or-down-april-8-2026-3pm-et"
    """
    h = window_start_et.hour
    if h == 0:
        time_str = "12am"
    elif h < 12:
        time_str = f"{h}am"
    elif h == 12:
        time_str = "12pm"
    else:
        time_str = f"{h - 12}pm"
    month = window_start_et.strftime("%B").lower()
    return f"{slug_name}-up-or-down-{month}-{window_start_et.day}-{window_start_et.year}-{time_str}-et"

PRICE_TO_BEAT_RE = re.compile(r"Price to beat\s*\$([0-9,]+(?:\.[0-9]+)?)", re.IGNORECASE)
PRICE_ABOVE_RE = re.compile(r"price (?:of )?.*?(?:above|over|greater than)\s*\$([0-9,]+(?:\.[0-9]+)?)", re.IGNORECASE)
PRICE_BELOW_RE = re.compile(r"price (?:of )?.*?(?:below|under|less than)\s*\$([0-9,]+(?:\.[0-9]+)?)", re.IGNORECASE)


class PolymarketFeed:
    def __init__(
        self,
        base_url: str,
        page_size: int,
        request_delay_ms: int,
        market_filter: dict,
    ) -> None:
        self.client = GammaClient(base_url=base_url, page_size=page_size, delay_ms=request_delay_ms)
        self.market_filter = market_filter

    def fetch_markets(self) -> list[NormalizedMarket]:
        raw = self.client.fetch_all_active_markets()

        slug_symbols = self.market_filter.get("slug_symbols") or []
        if slug_symbols:
            now_utc = datetime.now(timezone.utc)
            # Floor to nearest 15-minute window start
            floored_minute = (now_utc.minute // 15) * 15
            window_start = now_utc.replace(minute=floored_minute, second=0, microsecond=0)
            slugs = [
                f"{sym}-updown-15m-{int((window_start + timedelta(minutes=15 * offset)).timestamp())}"
                for offset in range(3)
                for sym in slug_symbols
            ]
            slug_markets = self.client.fetch_markets_by_slugs(slugs)
            existing_ids = {m.id for m in raw}
            for m in slug_markets:
                if m.id not in existing_ids:
                    raw.append(m)
                    existing_ids.add(m.id)

        hourly_slug_symbols = self.market_filter.get("hourly_slug_symbols") or []
        if hourly_slug_symbols:
            now_utc_h = datetime.now(timezone.utc)
            floored_hour = now_utc_h.replace(minute=0, second=0, microsecond=0)
            hourly_slugs = [
                _hourly_slug(
                    HOURLY_SYMBOL_SLUG.get(sym.lower(), sym.lower()),
                    (floored_hour + timedelta(hours=offset) - timedelta(hours=1)) + _ET_OFFSET,
                )
                for offset in range(3)
                for sym in hourly_slug_symbols
            ]
            hourly_markets = self.client.fetch_markets_by_slugs(hourly_slugs)
            existing_ids_h = {m.id for m in raw}
            for m in hourly_markets:
                if m.id not in existing_ids_h:
                    raw.append(m)
                    existing_ids_h.add(m.id)

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        min_expiry = now + timedelta(days=self.market_filter["min_days_to_expiry"])
        max_expiry = now + timedelta(days=self.market_filter["max_days_to_expiry"])
        symbol_filter = (self.market_filter.get("symbol") or "").strip().lower()
        symbols_filter = {
            str(sym).strip().lower()
            for sym in (self.market_filter.get("symbols") or [])
            if str(sym).strip()
        }

        result: list[NormalizedMarket] = []
        for market in raw:
            normalized = self._normalize_market(market)
            if normalized is None:
                continue
            if normalized.expiry < min_expiry or normalized.expiry > max_expiry:
                continue
            if market.volume_num < self.market_filter["min_volume"]:
                continue
            if market.liquidity_num < self.market_filter["min_liquidity"]:
                continue
            fee_type = self.market_filter.get("fee_type") or ""
            if fee_type and not (market.fee_type or "").startswith(fee_type):
                continue
            symbol_lc = normalized.symbol.lower()
            if symbol_filter and symbol_lc != symbol_filter:
                continue
            if symbols_filter and symbol_lc not in symbols_filter:
                continue
            result.append(normalized)
        return result

    def _normalize_market(self, market: Market) -> NormalizedMarket | None:
        if len(market.outcomes) != 2 or len(market.outcome_prices) != 2:
            return None
        if not market.end_date:
            return None

        match = UPDOWN_RE.match(market.question)
        if not match:
            return None

        symbol = SYMBOL_MAP.get(match.group("symbol").upper(), match.group("symbol").upper())
        outcomes = {name.lower(): float(price) for name, price in zip(market.outcomes, market.outcome_prices)}
        up_price = outcomes.get("up")
        down_price = outcomes.get("down")
        if up_price is None or down_price is None:
            return None
        rule_family = self._detect_rule_family(market)
        reference_price = self._extract_reference_price(market.question, market.description)
        interval_minutes = self._extract_interval_minutes(market.question)

        pm_event_slug: str | None = None
        if market.end_date is not None and interval_minutes == 15:
            window_start = market.end_date - timedelta(minutes=15)
            ts = calendar.timegm(window_start.timetuple())
            pm_event_slug = f"{symbol.lower()}-updown-15m-{ts}"
        elif market.end_date is not None and interval_minutes == 60:
            slug_name = HOURLY_SYMBOL_SLUG.get(symbol.lower())
            if slug_name is not None:
                window_start_et = (market.end_date - timedelta(hours=1)) + _ET_OFFSET
                pm_event_slug = _hourly_slug(slug_name, window_start_et)

        return NormalizedMarket(
            venue="polymarket",
            market_id=market.id,
            title=market.question,
            symbol=symbol,
            market_kind="updown",
            expiry=market.end_date,
            yes_label="Up",
            no_label="Down",
            yes_ask=up_price,
            no_ask=down_price,
            yes_bid=max(0.0, 1.0 - down_price),
            no_bid=max(0.0, 1.0 - up_price),
            yes_depth=market.liquidity_num,
            no_depth=market.liquidity_num,
            volume=market.volume_num,
            liquidity=market.liquidity_num,
            interval_minutes=interval_minutes,
            rule_family=rule_family,
            yes_token_id=market.clob_token_ids[0],
            no_token_id=market.clob_token_ids[1],
            reference_price=reference_price,
            rules_text=self._build_rules_text(market),
            pm_event_slug=pm_event_slug,
        )

    def _extract_reference_price(self, question: str, description: str) -> float | None:
        text = f"{question}\n{description}"
        for pattern in (PRICE_TO_BEAT_RE, PRICE_ABOVE_RE, PRICE_BELOW_RE):
            match = pattern.search(text)
            if not match:
                continue
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
        return None

    def _extract_interval_minutes(self, question: str) -> int | None:
        window_match = MINUTE_WINDOW_RE.search(question)
        if window_match:
            start = datetime.strptime(window_match.group("start").upper(), "%I:%M%p")
            end = datetime.strptime(window_match.group("end").upper(), "%I:%M%p")
            delta_minutes = int((end - start).total_seconds() // 60)
            if delta_minutes <= 0:
                delta_minutes += 24 * 60
            return delta_minutes
        if re.search(r"\b15\s*Minutes\b", question, re.IGNORECASE):
            return 15
        if re.search(r"\b5\s*Minutes\b", question, re.IGNORECASE):
            return 5
        if re.search(r"\b1\s*Hour\b|\b60\s*Minutes\b", question, re.IGNORECASE):
            return 60
        return None

    def _detect_rule_family(self, market: Market) -> str:
        text = f"{market.question}\n{market.description}\n{market.resolution_source}".lower()
        if "more green" in text and "more red" in text:
            return "candle_majority"
        if "price to beat" in text or "up or down" in text or "above" in text or "below" in text:
            return "price_direction"
        return "binary_yes_no"

    def _build_rules_text(self, market: Market) -> str:
        parts = [part.strip() for part in [market.description, market.resolution_source] if part and part.strip()]
        return "\n".join(parts)
