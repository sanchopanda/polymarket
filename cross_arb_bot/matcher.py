from __future__ import annotations

from cross_arb_bot.models import CrossVenueOpportunity, MatchedMarketPair, NormalizedMarket


def polymarket_crypto_taker_fee(shares: float, price: float) -> float:
    fee_rate = 0.25
    exponent = 2
    return shares * price * fee_rate * ((price * (1 - price)) ** exponent)


def kalshi_taker_fee(shares: float, price: float) -> float:
    raw = 0.07 * shares * price * (1 - price)
    cents = int(raw * 100)
    if raw * 100 > cents:
        cents += 1
    return cents / 100.0


def match_markets(
    polymarket_markets: list[NormalizedMarket],
    kalshi_markets: list[NormalizedMarket],
    expiry_tolerance_seconds: int,
) -> list[MatchedMarketPair]:
    matches: list[MatchedMarketPair] = []
    for pm in polymarket_markets:
        for kalshi in kalshi_markets:
            if pm.symbol != kalshi.symbol:
                continue
            if pm.market_kind != kalshi.market_kind:
                continue
            if pm.rule_family != kalshi.rule_family:
                continue
            if pm.interval_minutes != kalshi.interval_minutes:
                continue

            delta = abs((pm.expiry - kalshi.expiry).total_seconds())
            if delta > expiry_tolerance_seconds:
                continue

            score = 1.0 - min(delta / max(expiry_tolerance_seconds, 1), 1.0) * 0.1
            matches.append(MatchedMarketPair(polymarket=pm, kalshi=kalshi, score=score))

    matches.sort(key=lambda item: (item.polymarket.symbol, item.score), reverse=True)
    return matches


def build_opportunities(
    matches: list[MatchedMarketPair],
    min_lock_edge: float,
    max_lock_edge: float,
    stake_per_pair_usd: float,
) -> list[CrossVenueOpportunity]:
    opportunities: list[CrossVenueOpportunity] = []
    for item in matches:
        pm = item.polymarket
        kalshi = item.kalshi


        legs = [
            ("polymarket", "kalshi", pm.yes_ask, kalshi.no_ask, min(pm.yes_depth, kalshi.no_depth)),
            ("kalshi", "polymarket", kalshi.yes_ask, pm.no_ask, min(kalshi.yes_depth, pm.no_depth)),
        ]
        for yes_venue, no_venue, yes_ask, no_ask, max_shares in legs:
            ask_sum = yes_ask + no_ask
            edge_per_share = 1.0 - ask_sum
            if edge_per_share < min_lock_edge:
                continue
            if edge_per_share > max_lock_edge:
                continue
            if ask_sum <= 0:
                continue
            shares = min(stake_per_pair_usd / ask_sum, max_shares)
            if shares <= 0:
                continue
            capital_used = ask_sum * shares
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
            total_cost = capital_used + total_fee
            expected_payout = shares
            opportunities.append(
                CrossVenueOpportunity(
                    pair_key=f"{pm.market_id}:{kalshi.market_id}",
                    polymarket_market_id=pm.market_id,
                    kalshi_market_id=kalshi.market_id,
                    symbol=pm.symbol,
                    title=f"{pm.title} <> {kalshi.title}",
                    expiry=min(pm.expiry, kalshi.expiry),
                    polymarket_title=pm.title,
                    kalshi_title=kalshi.title,
                    match_score=item.score,
                    expiry_delta_seconds=abs((pm.expiry - kalshi.expiry).total_seconds()),
                    polymarket_reference_price=pm.reference_price,
                    kalshi_reference_price=kalshi.reference_price,
                    polymarket_rules=pm.rules_text,
                    kalshi_rules=kalshi.rules_text,
                    pm_event_slug=pm.pm_event_slug,
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
                    total_cost=total_cost,
                    expected_payout=expected_payout,
                    expected_profit=expected_payout - total_cost,
                )
            )

    opportunities.sort(key=lambda item: item.expected_profit, reverse=True)
    return opportunities
