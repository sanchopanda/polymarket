from __future__ import annotations

from sports_arb_bot.feed_kalshi import KalshiSportsFeed
from sports_arb_bot.feed_polymarket import PolymarketSportsFeed
from sports_arb_bot.matcher import SPORT_TO_KALSHI_SERIES, SportsMatcher
from sports_arb_bot.models import MatchedSportsPair

# Какие спорты скачиваем
DEFAULT_SPORTS = ["wta", "atp", "boxing"]


def _kalshi_series_for(sports: list[str]) -> list[str]:
    seen: set[str] = set()
    out = []
    for s in sports:
        for series in SPORT_TO_KALSHI_SERIES.get(s, []):
            if series not in seen:
                seen.add(series)
                out.append(series)
    return out


def scan(
    sports: list[str] | None = None,
    min_confidence: float = 0.8,
    min_edge: float = 0.0,
    window_hours: float = 24.0,
) -> list[MatchedSportsPair]:
    if sports is None:
        sports = DEFAULT_SPORTS

    print(f"[engine] Скачиваем PM события: {sports} (окно {window_hours}ч)")
    pm_feed = PolymarketSportsFeed()
    pm_events = pm_feed.fetch(sports, window_hours=window_hours)
    print(f"[engine] PM: {len(pm_events)} событий")

    kalshi_series = _kalshi_series_for(sports)
    print(f"[engine] Скачиваем Kalshi серии: {kalshi_series}")
    ka_feed = KalshiSportsFeed()
    ka_events = ka_feed.fetch(kalshi_series)
    print(f"[engine] Kalshi: {len(ka_events)} событий")

    print(f"[engine] Запускаем LLM матчинг...")
    matcher = SportsMatcher(min_confidence=min_confidence)
    matched = matcher.match_all(pm_events, ka_events)
    print(f"[engine] Сматчено: {len(matched)} пар")

    if min_edge > 0.0:
        matched = [p for p in matched if (p.arb_edge() or -1) >= min_edge]

    return matched


def print_results(pairs: list[MatchedSportsPair]) -> None:
    if not pairs:
        print("\nНет сматченных пар.")
        return

    print(f"\n{'='*72}")
    print(f"{'СМАТЧЕННЫЕ ПАРЫ':^72}")
    print(f"{'='*72}")

    for i, pair in enumerate(pairs, 1):
        pm = pair.pm_event
        ka = pair.kalshi_event
        mr = pair.match_result

        print(f"\n[{i}] {pm.sport.upper()} | conf={mr.confidence:.2f}")
        print(f"  PM:  {pm.title}")
        print(f"  KA:  {ka.sub_title or ka.title}  ({ka.competition})")
        print(f"  PM дата: {pm.game_date.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  KA дата: {ka.expected_expiration.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Причина: {mr.reason}")

        # Цены
        pm_prices = dict(zip(pm.players, pm.prices))
        ka_by_ticker = {m.ticker: m for m in ka.markets}
        print(f"  Цены:")
        for pm_player, ka_ticker in mr.outcome_map.items():
            pm_p = pm_prices.get(pm_player, "?")
            ka_m = ka_by_ticker.get(ka_ticker)
            ka_ask = f"{ka_m.yes_ask:.2f}" if ka_m else "?"
            ka_bid = f"{ka_m.yes_bid:.2f}" if ka_m else "?"
            print(f"    {pm_player:<28}  PM={pm_p}  KA ask={ka_ask} bid={ka_bid}  [{ka_ticker}]")

        # Арб
        edge = pair.arb_edge()
        if edge is not None:
            mark = "🔥" if edge > 0.02 else ("~" if edge > 0 else "✗")
            print(f"  ARB edge: {edge:+.4f}  {mark}")

    print(f"\n{'='*72}")
    print(f"Итого пар: {len(pairs)}")
