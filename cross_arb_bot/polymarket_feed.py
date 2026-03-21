from __future__ import annotations

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
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        min_expiry = now + timedelta(days=self.market_filter["min_days_to_expiry"])
        max_expiry = now + timedelta(days=self.market_filter["max_days_to_expiry"])
        symbol_filter = (self.market_filter.get("symbol") or "").strip().lower()

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
            if fee_type and market.fee_type != fee_type:
                continue
            if symbol_filter and normalized.symbol.lower() != symbol_filter:
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
            interval_minutes=self._extract_interval_minutes(market.question),
            rule_family=rule_family,
            yes_token_id=market.clob_token_ids[0],
            no_token_id=market.clob_token_ids[1],
            reference_price=reference_price,
            rules_text=self._build_rules_text(market),
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
