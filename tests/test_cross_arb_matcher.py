from datetime import datetime

from cross_arb_bot.matcher import build_opportunities, match_markets
from cross_arb_bot.models import NormalizedMarket


def make_market(venue: str, market_id: str, yes_ask: float, no_ask: float) -> NormalizedMarket:
    return NormalizedMarket(
        venue=venue,
        market_id=market_id,
        title="BTC Up or Down",
        symbol="BTC",
        market_kind="updown",
        expiry=datetime(2026, 3, 20, 12, 15),
        yes_label="Up",
        no_label="Down",
        yes_ask=yes_ask,
        no_ask=no_ask,
        yes_bid=0.0,
        no_bid=0.0,
        yes_depth=100.0,
        no_depth=100.0,
        volume=1000.0,
        liquidity=1000.0,
    )


def test_match_markets_matches_same_symbol_and_expiry() -> None:
    pm = make_market("polymarket", "pm1", 0.49, 0.51)
    kalshi = make_market("kalshi", "ka1", 0.48, 0.52)

    matches = match_markets([pm], [kalshi], expiry_tolerance_seconds=120)

    assert len(matches) == 1
    assert matches[0].polymarket.market_id == "pm1"
    assert matches[0].kalshi.market_id == "ka1"


def test_build_opportunities_finds_cross_lock() -> None:
    pm = make_market("polymarket", "pm1", 0.47, 0.53)
    kalshi = make_market("kalshi", "ka1", 0.56, 0.48)

    matches = match_markets([pm], [kalshi], expiry_tolerance_seconds=120)
    opportunities = build_opportunities(matches, min_lock_edge=0.02, max_payout_per_trade=10)

    assert len(opportunities) == 1
    assert opportunities[0].buy_yes_venue == "polymarket"
    assert opportunities[0].buy_no_venue == "kalshi"
    assert opportunities[0].expected_profit == 0.05 * 10
