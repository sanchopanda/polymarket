from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Optional

import httpx

from sports_arb_bot.models import (
    KalshiMatchEvent,
    MatchedSportsPair,
    MatchResult,
    PMSportsEvent,
)

# PM sport → Kalshi series_tickers
SPORT_TO_KALSHI_SERIES: dict[str, list[str]] = {
    "wta":    ["KXWTACHALLENGERMATCH"],
    "atp":    ["KXATPCHALLENGERMATCH"],
    "boxing": ["KXBOXING"],
    "mma":    ["KXMMA"],
}

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "anthropic/claude-haiku-4-5"

DATE_TOLERANCE_DAYS = 2


def _sport_series(sport: str) -> list[str]:
    return SPORT_TO_KALSHI_SERIES.get(sport, [])


def _pre_filter_ka(pm: PMSportsEvent, ka: KalshiMatchEvent) -> bool:
    """Быстрая проверка без LLM — отсеивает явно несовместимые пары."""
    allowed_series = _sport_series(pm.sport)
    if allowed_series and ka.series_ticker not in allowed_series:
        return False
    delta = abs((pm.game_date - ka.expected_expiration).total_seconds())
    if delta > DATE_TOLERANCE_DAYS * 86400:
        return False
    return True


def _batch_llm_match(
    sport: str,
    pm_events: list[PMSportsEvent],
    ka_events: list[KalshiMatchEvent],
    http: httpx.Client,
    api_key: str,
) -> list[dict]:
    """
    Отправляет оба списка событий в LLM одним запросом.
    Возвращает список dict: [{pm_slug, ka_event_ticker, confidence, reason,
                               outcome_map: {pm_player: ka_ticker}}]
    """
    pm_list = [
        {
            "slug": e.slug,
            "title": e.title,
            "tournament": e.league or "",
            "date": e.game_date.strftime("%Y-%m-%d"),
            "players": e.players,
        }
        for e in pm_events
    ]
    ka_list = [
        {
            "event_ticker": e.event_ticker,
            "title": e.sub_title or e.title,
            "competition": e.competition,
            "date": e.expected_expiration.strftime("%Y-%m-%d"),
            "players": {m.player_name: m.ticker for m in e.markets},
        }
        for e in ka_events
    ]

    prompt = f"""You are matching sports prediction market events from two platforms: Polymarket and Kalshi.
Sport: {sport.upper()}

Polymarket events (list A):
{json.dumps(pm_list, ensure_ascii=False, indent=2)}

Kalshi events (list B):
{json.dumps(ka_list, ensure_ascii=False, indent=2)}

Task: Find all pairs where an event from list A and list B refer to the same real-world match.
Note: player names may differ in format (e.g. "Mayweather" vs "Floyd Mayweather Jr.", or transliterations).

For each matched pair, also map each Polymarket player name to the corresponding Kalshi market ticker.

Return a JSON array only (no markdown, no explanation outside JSON):
[
  {{
    "pm_slug": "<slug from list A>",
    "ka_event_ticker": "<event_ticker from list B>",
    "confidence": 0.95,
    "reason": "same players and date",
    "outcome_map": {{
      "<PM player name>": "<Kalshi ticker>",
      "<PM player name 2>": "<Kalshi ticker 2>"
    }}
  }}
]

If no matches found, return empty array: []
Only include matches with confidence > 0.8."""

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 2000,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/polymarket-bot",
    }

    try:
        resp = http.post(OPENROUTER_URL, json=payload, headers=headers, timeout=30.0)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        # Убираем markdown code fences если есть
        if "```" in content:
            parts = content.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                try:
                    return json.loads(part)
                except Exception:
                    continue
        return json.loads(content)
    except Exception as e:
        print(f"[matcher] LLM ошибка для {sport}: {e}")
        return []


class SportsMatcher:
    def __init__(self, min_confidence: float = 0.8) -> None:
        self.min_confidence = min_confidence
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY не задан в окружении")
        self._api_key = api_key
        self._http = httpx.Client(timeout=30.0)

    def match_all(
        self,
        pm_events: list[PMSportsEvent],
        ka_events: list[KalshiMatchEvent],
    ) -> list[MatchedSportsPair]:
        """
        Матчит события по спортам.
        Для каждого спорта — один батч LLM-запрос со всеми событиями.
        """
        # Группируем PM по спорту
        pm_by_sport: dict[str, list[PMSportsEvent]] = {}
        for ev in pm_events:
            pm_by_sport.setdefault(ev.sport, []).append(ev)

        # Индексы для быстрого поиска
        pm_by_slug: dict[str, PMSportsEvent] = {e.slug: e for e in pm_events}
        ka_by_ticker: dict[str, KalshiMatchEvent] = {e.event_ticker: e for e in ka_events}

        matched: list[MatchedSportsPair] = []
        used_ka: set[str] = set()

        for sport, pm_list in pm_by_sport.items():
            # Kalshi события для этого спорта
            allowed_series = _sport_series(sport)
            ka_list = [
                ka for ka in ka_events
                if ka.event_ticker not in used_ka
                and (not allowed_series or ka.series_ticker in allowed_series)
            ]
            if not ka_list:
                print(f"[matcher] {sport}: нет Kalshi событий")
                continue

            # Pre-filter: для каждого PM события оставляем только подходящие по дате KA
            ka_candidates: set[str] = set()
            for pm in pm_list:
                for ka in ka_list:
                    if _pre_filter_ka(pm, ka):
                        ka_candidates.add(ka.event_ticker)

            ka_filtered = [ka for ka in ka_list if ka.event_ticker in ka_candidates]
            if not ka_filtered:
                print(f"[matcher] {sport}: нет KA кандидатов после pre-filter")
                continue

            print(f"[matcher] {sport}: {len(pm_list)} PM + {len(ka_filtered)} KA → LLM батч")

            results = _batch_llm_match(
                sport=sport,
                pm_events=pm_list,
                ka_events=ka_filtered,
                http=self._http,
                api_key=self._api_key,
            )

            for r in results:
                conf = float(r.get("confidence", 0.0))
                if conf < self.min_confidence:
                    continue
                pm_slug = r.get("pm_slug", "")
                ka_ticker = r.get("ka_event_ticker", "")
                pm_ev = pm_by_slug.get(pm_slug)
                ka_ev = ka_by_ticker.get(ka_ticker)
                if not pm_ev or not ka_ev:
                    print(f"[matcher] предупреждение: неизвестный slug/ticker в ответе LLM: {pm_slug} / {ka_ticker}")
                    continue
                if ka_ticker in used_ka:
                    continue

                match_result = MatchResult(
                    is_match=True,
                    confidence=conf,
                    reason=str(r.get("reason", "")),
                    outcome_map=r.get("outcome_map") or {},
                )
                matched.append(MatchedSportsPair(
                    pm_event=pm_ev,
                    kalshi_event=ka_ev,
                    match_result=match_result,
                    sport=sport,
                ))
                used_ka.add(ka_ticker)
                print(f"[matcher] ✓ {pm_slug} ↔ {ka_ticker} (conf={conf:.2f}) — {r.get('reason','')}")

        return matched
