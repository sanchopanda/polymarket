"""
Читает data/pm_sports.json и data/kalshi_sports.json,
сохраняет урезанные версии только с заголовками.

Сохраняет:
  data/pm_titles.json     — slug + question (PM)
  data/kalshi_titles.json — event_ticker + title (Kalshi, уникальные события)

Usage:
    python3 scripts/extract_titles.py
"""
from __future__ import annotations

import json
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data"


def main() -> None:
    pm_raw = json.loads((DATA / "pm_sports.json").read_text())
    pm_titles = [
        {"slug": m.get("slug"), "question": m.get("question")}
        for m in pm_raw
    ]
    (DATA / "pm_titles.json").write_text(json.dumps(pm_titles, ensure_ascii=False, indent=2))
    print(f"PM: {len(pm_titles)} → pm_titles.json")

    ka_raw = json.loads((DATA / "kalshi_sports.json").read_text())
    seen: set[str] = set()
    ka_titles = []
    for m in ka_raw:
        ev = m.get("event_ticker", "")
        if ev in seen:
            continue
        seen.add(ev)
        ka_titles.append({"event_ticker": ev, "title": m.get("title")})
    (DATA / "kalshi_titles.json").write_text(json.dumps(ka_titles, ensure_ascii=False, indent=2))
    print(f"Kalshi: {len(ka_titles)} уникальных событий → kalshi_titles.json")


if __name__ == "__main__":
    main()
