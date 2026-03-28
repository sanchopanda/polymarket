from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PMSportsEvent:
    slug: str
    title: str
    sport: str              # "wta", "atp", "boxing", ...
    league: str             # eventMetadata.league (пусто у бокса)
    game_date: datetime     # gameStartTime или endDate
    game_id: Optional[int]  # gameId (есть у тенниса)
    players: list[str]      # из outcomes moneyline маркета
    prices: list[float]     # outcomePrices
    token_ids: list[str]    # clobTokenIds
    market_id: str
    end_date: datetime

    def token_id_for_player(self, name: str) -> Optional[str]:
        """Возвращает clobTokenId для указанного игрока."""
        for player, token_id in zip(self.players, self.token_ids):
            if player == name:
                return token_id
        return None


@dataclass
class KalshiMarket:
    ticker: str
    player_name: str        # yes_sub_title
    yes_ask: float
    yes_bid: float
    no_ask: float
    no_bid: float
    volume: float
    open_interest: float


@dataclass
class KalshiMatchEvent:
    event_ticker: str
    series_ticker: str
    title: str
    sub_title: str
    competition: str        # product_metadata.competition
    expected_expiration: datetime
    strike_type: str        # "structured" (теннис) / "custom" (бокс)
    markets: list[KalshiMarket]


@dataclass
class MatchResult:
    is_match: bool
    confidence: float
    reason: str
    # PM outcome name → Kalshi market ticker
    outcome_map: dict[str, str] = field(default_factory=dict)


@dataclass
class MatchedSportsPair:
    pm_event: PMSportsEvent
    kalshi_event: KalshiMatchEvent
    match_result: MatchResult
    sport: str

    def arb_edge(self) -> Optional[float]:
        """
        Считаем lock-арб: покупаем YES игрока A на PM + YES игрока B на Kalshi.
        Если оба YES суммируются < $1 — есть edge.
        Возвращает лучший edge или None.
        """
        if len(self.pm_event.players) != 2:
            return None
        om = self.match_result.outcome_map
        if len(om) < 2:
            return None

        best: Optional[float] = None
        pm_prices = dict(zip(self.pm_event.players, self.pm_event.prices))
        ka_by_ticker = {m.ticker: m for m in self.kalshi_event.markets}

        for pm_player, ka_ticker in om.items():
            ka_market = ka_by_ticker.get(ka_ticker)
            if not ka_market:
                continue
            # Находим другого игрока
            other_players = [p for p in self.pm_event.players if p != pm_player]
            if not other_players:
                continue
            other_player = other_players[0]
            other_ka_ticker = om.get(other_player)
            if not other_ka_ticker:
                continue
            other_ka = ka_by_ticker.get(other_ka_ticker)
            if not other_ka:
                continue

            # Вариант 1: YES pm_player на PM + YES other_player на Kalshi
            pm_ask = pm_prices.get(pm_player, 1.0)
            ka_ask = other_ka.yes_ask
            edge1 = 1.0 - pm_ask - ka_ask
            # Вариант 2: YES other_player на PM + YES pm_player на Kalshi
            pm_ask2 = pm_prices.get(other_player, 1.0)
            ka_ask2 = ka_market.yes_ask
            edge2 = 1.0 - pm_ask2 - ka_ask2

            for e in (edge1, edge2):
                if best is None or e > best:
                    best = e

        return best
