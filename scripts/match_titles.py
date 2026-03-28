"""
Читает data/pm_sports.json и data/kalshi_sports.json,
извлекает question/title и ищет совпадения по именам игроков.

Usage:
    python3 scripts/match_titles.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data"

# PM рынки которые НЕ являются бинарными win/lose матчами
PM_SKIP_PATTERNS = [
    r"\bO/U\b", r"\bOver\b", r"\bUnder\b",
    r"\bSpread\b", r"\bBoth Teams\b", r"\bDraw\b",
    r"\btotal\b", r"\bpoints\b",
]
PM_SKIP_RE = re.compile("|".join(PM_SKIP_PATTERNS), re.IGNORECASE)

# Нормализация имени: убираем лишнее, приводим к нижнему регистру
def _norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\bjr\.?\b|\bsr\.?\b|\bii\b|\biii\b", "", s)
    s = re.sub(r"[^a-z ]", " ", s)
    return " ".join(s.split())

def _name_tokens(s: str) -> set[str]:
    """Набор токенов длиной >= 3 из нормализованного имени."""
    return {t for t in _norm(s).split() if len(t) >= 3}


def load_pm() -> list[dict]:
    raw = json.loads((DATA / "pm_sports.json").read_text())
    result = []
    for m in raw:
        q = m.get("question", "")
        # Пропускаем не-бинарные рынки
        if PM_SKIP_RE.search(q):
            continue
        # Парсим игроков из outcomes
        try:
            outcomes = json.loads(m.get("outcomes") or "[]")
        except Exception:
            outcomes = []
        result.append({
            "slug":       m.get("slug", ""),
            "question":   q,
            "sport":      m.get("sport", ""),
            "game_start": (m.get("game_start_time") or "")[:16],
            "outcomes":   outcomes,
            "tokens":     _name_tokens(q) | {t for o in outcomes for t in _name_tokens(o)},
        })
    return result


def load_ka() -> list[dict]:
    raw = json.loads((DATA / "kalshi_sports.json").read_text())
    # Дедупликация по event_ticker (берём первый маркет события)
    seen: set[str] = set()
    result = []
    for m in raw:
        ev = m.get("event_ticker", "")
        if ev in seen:
            continue
        seen.add(ev)
        title = m.get("title", "")
        yes = m.get("yes_sub_title", "")
        no  = m.get("no_sub_title", "")
        result.append({
            "event_ticker": ev,
            "title":        title,
            "exp_time":     (m.get("expected_expiration_time") or "")[:16],
            "yes":          yes,
            "no":           no,
            "tokens":       _name_tokens(title) | _name_tokens(yes) | _name_tokens(no),
        })
    return result


def match(pm_list: list[dict], ka_list: list[dict]) -> list[tuple[dict, dict, int]]:
    """Возвращает [(pm, ka, overlap_count)] отсортированные по убыванию совпадений."""
    matches = []
    for pm in pm_list:
        best_ka = None
        best_score = 0
        for ka in ka_list:
            overlap = len(pm["tokens"] & ka["tokens"])
            if overlap > best_score:
                best_score = overlap
                best_ka = ka
        if best_ka and best_score >= 2:
            matches.append((pm, best_ka, best_score))
    matches.sort(key=lambda x: -x[2])
    return matches


def main() -> None:
    pm_list = load_pm()
    ka_list = load_ka()

    print(f"PM бинарных рынков: {len(pm_list)}")
    print(f"Kalshi уникальных событий: {len(ka_list)}")
    print()

    # Выводим все заголовки
    print("=" * 72)
    print("PM QUESTIONS (бинарные)")
    print("=" * 72)
    for m in pm_list:
        print(f"  [{m['game_start']}] [{m['sport']}] {m['question']}")

    print()
    print("=" * 72)
    print("KALSHI TITLES")
    print("=" * 72)
    for m in ka_list:
        print(f"  [{m['exp_time']}] {m['title']}")

    print()
    print("=" * 72)
    print("СОВПАДЕНИЯ (overlap >= 2 токенов)")
    print("=" * 72)
    pairs = match(pm_list, ka_list)
    if not pairs:
        print("  Нет совпадений")
    for pm, ka, score in pairs:
        print(f"\n  score={score}")
        print(f"  PM: {pm['question']}")
        print(f"  KA: {ka['title']}")
        print(f"  KA event: {ka['event_ticker']}")


if __name__ == "__main__":
    main()
