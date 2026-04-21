"""CLI: python3 -m weather_bot <command>

Команды:
  watch   — pipeline loop: periodically pull forecast + prices, log to DB
  tick    — один проход и выход (для крона/отладки)
  status  — stats из DB + топ-сигналы в последнем снимке
  scan    — однократно показать signals без записи в DB
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from weather_bot.engine import EngineConfig, WeatherEngine


def load_config(path: str = "weather_bot/config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def cmd_watch(cfg: dict) -> None:
    ec = EngineConfig(
        db_path=cfg["db"]["path"],
        cities=set(cfg["cities"]["allow"]) if cfg["cities"]["allow"] else None,
        interval_seconds=int(cfg["runtime"]["interval_seconds"]),
        user_agent=cfg["nws"]["user_agent"],
    )
    Path(ec.db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = WeatherEngine(ec)
    try:
        engine.run()
    finally:
        engine.close()


def cmd_tick(cfg: dict) -> None:
    ec = EngineConfig(
        db_path=cfg["db"]["path"],
        cities=set(cfg["cities"]["allow"]) if cfg["cities"]["allow"] else None,
        interval_seconds=int(cfg["runtime"]["interval_seconds"]),
        user_agent=cfg["nws"]["user_agent"],
    )
    Path(ec.db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = WeatherEngine(ec)
    try:
        summary = engine.tick()
        print(f"tick done: {summary}")
    finally:
        engine.close()


def cmd_status(cfg: dict) -> None:
    from weather_bot.db import WeatherDB
    db = WeatherDB(cfg["db"]["path"])
    s = db.stats()
    print(f"DB stats: markets={s['markets']} forecasts={s['forecasts']} snapshots={s['snapshots']} resolved={s['resolved']}")
    # Top current signals
    rows = db._conn.execute("""
        SELECT b.title, m.city_slug, m.event_date, s.p_model, s.yes_price, s.edge_yes, s.ts
        FROM snapshots s
        JOIN buckets b ON b.id = s.bucket_id
        JOIN markets m ON m.event_id = b.event_id
        WHERE s.id IN (
            SELECT MAX(id) FROM snapshots GROUP BY bucket_id
        )
        AND ABS(s.edge_yes) > ?
        ORDER BY ABS(s.edge_yes) DESC
        LIMIT 30
    """, (float(cfg["edge"]["min_abs_edge"]),)).fetchall()
    if not rows:
        print("no active signals")
        return
    print(f"\ncurrent signals (|edge|>{cfg['edge']['min_abs_edge']}):")
    print(f"  {'city':>14} {'date':>10} {'bucket':>15}  p_mkt  p_mdl  edge")
    for r in rows:
        print(f"  {r['city_slug']:>14} {r['event_date']:>10} {r['title']:>15}  "
              f"{r['yes_price']:>5.3f}  {r['p_model']:>5.3f}  {r['edge_yes']:>+6.3f}")
    db.close()


def cmd_scan(cfg: dict) -> None:
    """Одноразовый скан без записи в DB."""
    from weather_bot.markets import fetch_active_events
    from weather_bot.model import model_probs, rmse_for_lead
    from weather_bot.nws import NWSClient

    cities = set(cfg["cities"]["allow"]) if cfg["cities"]["allow"] else None
    min_edge = float(cfg["edge"]["min_abs_edge"])
    events = fetch_active_events(cities=cities)
    print(f"fetched {len(events)} active US events")

    with NWSClient(user_agent=cfg["nws"]["user_agent"]) as nws:
        fcache: dict[tuple[str, str], tuple[float, float, float]] = {}
        signals = []
        for ev in events:
            key = (ev.city_slug, ev.event_date)
            if key not in fcache:
                fc = nws.forecast_high(ev.station, ev.event_date)
                if fc is None:
                    continue
                sigma = rmse_for_lead(max(fc.lead_hours, 0))
                fcache[key] = (fc.max_temp_f, sigma, fc.lead_hours)

            mu, sigma, lead = fcache[key]
            probs = model_probs(ev.buckets, mu, sigma)
            for b, pp in zip(ev.buckets, probs):
                edge = pp.p_model - b.yes_price
                if abs(edge) >= min_edge:
                    signals.append((abs(edge), ev.city_slug, ev.event_date, b.title,
                                    b.yes_price, pp.p_model, edge, mu, sigma, lead))

    signals.sort(reverse=True)
    print(f"\nsignals (|edge|>={min_edge}):  [{len(signals)} total]")
    print(f"  {'city':>14} {'date':>10} {'bucket':>15}  p_mkt  p_mdl   edge  mu  sig lead")
    for s in signals[:40]:
        _, city, date, title, yp, pm, edge, mu, sig, lead = s
        print(f"  {city:>14} {date:>10} {title:>15}  {yp:>5.3f}  {pm:>5.3f}  {edge:>+6.3f}  "
              f"{mu:>4.0f} {sig:>3.1f} {lead:>+4.1f}h")


def main() -> int:
    parser = argparse.ArgumentParser("weather_bot")
    parser.add_argument("command", choices=["watch", "tick", "status", "scan"])
    parser.add_argument("--config", default="weather_bot/config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.command == "watch":
        cmd_watch(cfg)
    elif args.command == "tick":
        cmd_tick(cfg)
    elif args.command == "status":
        cmd_status(cfg)
    elif args.command == "scan":
        cmd_scan(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
