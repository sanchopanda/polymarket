from __future__ import annotations

import re
from datetime import timedelta
from typing import Protocol, runtime_checkable

from sports_arb_bot.feed_kalshi import SERIES_TO_SPORT
from sports_arb_bot.models import KalshiMatchEvent, MatchResult, MatchedSportsPair, PMSportsEvent


@runtime_checkable
class SportMatcherProtocol(Protocol):
    def match(
        self,
        pm_events: list[PMSportsEvent],
        ka_events: list[KalshiMatchEvent],
    ) -> list[MatchedSportsPair]: ...


def _tokens(name: str) -> set[str]:
    """Нормализует имя игрока → набор токенов ≥ 4 символов."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    return {t for t in name.split() if len(t) >= 4}


class TennisMatcher:
    """
    Детерминированный токен-матчинг для тенниса (WTA / ATP).

    Алгоритм:
    1. Pre-filter по дате: |ka.expected_expiration - pm.game_date| ≤ DATE_TOLERANCE
    2. Для каждой пары PM/KA: нормализуем имена → токены ≥ 4 символов
    3. Оба PM-игрока должны найти Kalshi-рынок с непустым пересечением токенов
    4. Если оба матчатся к разным тикерам → MatchedSportsPair
    """

    DATE_TOLERANCE = timedelta(hours=12)

    def match(
        self,
        pm_events: list[PMSportsEvent],
        ka_events: list[KalshiMatchEvent],
    ) -> list[MatchedSportsPair]:
        results: list[MatchedSportsPair] = []
        seen_pm: set[str] = set()
        seen_ka: set[str] = set()

        for pm in pm_events:
            if len(pm.players) != 2:
                continue
            if pm.slug in seen_pm:
                continue

            for ka in ka_events:
                if ka.event_ticker in seen_ka:
                    continue
                if len(ka.markets) < 2:
                    continue

                # Pre-filter по виду спорта
                if SERIES_TO_SPORT.get(ka.series_ticker) != pm.sport:
                    continue

                # Pre-filter по дате
                diff = abs((ka.expected_expiration - pm.game_date).total_seconds())
                if diff > self.DATE_TOLERANCE.total_seconds():
                    continue

                result = self._try_match(pm, ka)
                if result.is_match:
                    results.append(MatchedSportsPair(
                        pm_event=pm,
                        kalshi_event=ka,
                        match_result=result,
                        sport=pm.sport,
                    ))
                    seen_pm.add(pm.slug)
                    seen_ka.add(ka.event_ticker)
                    break

        return results

    def _try_match(self, pm: PMSportsEvent, ka: KalshiMatchEvent) -> MatchResult:
        ka_by_tokens: list[tuple[set[str], str]] = [
            (_tokens(m.player_name), m.ticker)
            for m in ka.markets
        ]

        outcome_map: dict[str, str] = {}
        for pm_player in pm.players:
            pm_toks = _tokens(pm_player)
            if not pm_toks:
                return MatchResult(
                    is_match=False, confidence=0.0,
                    reason=f"empty tokens for PM player '{pm_player}'"
                )
            matched_ticker: str | None = None
            for ka_toks, ka_ticker in ka_by_tokens:
                if pm_toks & ka_toks:
                    matched_ticker = ka_ticker
                    break
            if matched_ticker is None:
                return MatchResult(
                    is_match=False, confidence=0.0,
                    reason=f"no Kalshi match for PM player '{pm_player}'"
                )
            outcome_map[pm_player] = matched_ticker

        if len(set(outcome_map.values())) < 2:
            return MatchResult(
                is_match=False, confidence=0.0,
                reason="both PM players mapped to the same Kalshi ticker"
            )

        return MatchResult(
            is_match=True,
            confidence=1.0,
            reason="token match",
            outcome_map=outcome_map,
        )


# Kalshi yes_sub_title (lowercase) → токены PM-стороны (прозвища команд NBA)
# Kalshi использует города, PM — прозвища; для LA добавляет букву (C/L)
NBA_KALSHI_TO_TOKENS: dict[str, set[str]] = {
    "atlanta":       {"hawks"},
    "boston":        {"celtics"},
    "brooklyn":      {"nets"},
    "charlotte":     {"hornets"},
    "chicago":       {"bulls"},
    "cleveland":     {"cavaliers"},
    "dallas":        {"mavericks"},
    "denver":        {"nuggets"},
    "detroit":       {"pistons"},
    "golden state":  {"warriors"},
    "houston":       {"rockets"},
    "indiana":       {"pacers"},
    "los angeles c": {"clippers"},
    "los angeles l": {"lakers"},
    "memphis":       {"grizzlies"},
    "miami":         {"heat"},
    "milwaukee":     {"bucks"},
    "minnesota":     {"timberwolves"},
    "new orleans":   {"pelicans"},
    "new york":      {"knicks"},
    "oklahoma city": {"thunder"},
    "orlando":       {"magic"},
    "philadelphia":  {"76ers"},
    "phoenix":       {"suns"},
    "portland":      {"trail", "blazers"},
    "sacramento":    {"kings"},
    "san antonio":   {"spurs"},
    "toronto":       {"raptors"},
    "utah":          {"jazz"},
    "washington":    {"wizards"},
}


class NBAMatcher(TennisMatcher):
    """
    Матчер для NBA: Kalshi использует города, PM — прозвища команд.
    Переопределяет токенизацию Kalshi-стороны через словарь NBA_KALSHI_TO_TOKENS.
    """

    def _try_match(self, pm: PMSportsEvent, ka: KalshiMatchEvent) -> MatchResult:
        ka_by_tokens: list[tuple[set[str], str]] = [
            (NBA_KALSHI_TO_TOKENS.get(m.player_name.lower().strip(), _tokens(m.player_name)), m.ticker)
            for m in ka.markets
        ]

        outcome_map: dict[str, str] = {}
        for pm_player in pm.players:
            pm_toks = _tokens(pm_player)
            if not pm_toks:
                return MatchResult(
                    is_match=False, confidence=0.0,
                    reason=f"empty tokens for PM player '{pm_player}'"
                )
            matched_ticker: str | None = None
            for ka_toks, ka_ticker in ka_by_tokens:
                if pm_toks & ka_toks:
                    matched_ticker = ka_ticker
                    break
            if matched_ticker is None:
                return MatchResult(
                    is_match=False, confidence=0.0,
                    reason=f"no Kalshi match for PM player '{pm_player}'"
                )
            outcome_map[pm_player] = matched_ticker

        if len(set(outcome_map.values())) < 2:
            return MatchResult(
                is_match=False, confidence=0.0,
                reason="both PM players mapped to the same Kalshi ticker"
            )

        return MatchResult(
            is_match=True,
            confidence=1.0,
            reason="nba city→nickname match",
            outcome_map=outcome_map,
        )


class LLMSportsMatcher:
    """
    Обёртка над существующим matcher.py для спортов без детерминированного матчинга.
    Активируется через get_matcher(sport, use_llm=True).
    """

    def __init__(self) -> None:
        from sports_arb_bot.matcher import SportsMatcher
        self._llm = SportsMatcher()

    def match(
        self,
        pm_events: list[PMSportsEvent],
        ka_events: list[KalshiMatchEvent],
    ) -> list[MatchedSportsPair]:
        return self._llm.match(pm_events, ka_events)


def get_matcher(sport: str, use_llm: bool = False) -> SportMatcherProtocol:
    """Фабрика матчеров."""
    if use_llm:
        return LLMSportsMatcher()
    if sport == "nba":
        return NBAMatcher()
    return TennisMatcher()
