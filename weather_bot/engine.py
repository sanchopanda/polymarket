"""Итеративный pipeline без торгов:

  loop:
    1) fetch_active_events() — список US-events с Gamma
    2) для каждого нового — upsert_market()
    3) для каждого уникального (city_slug, event_date) — один запрос в NWS, insert_forecast()
    4) пересчитать p_model по всем bucket'ам, insert_snapshot()
    5) спать N секунд

Запросы в NWS кешируются внутри client-а по grid-point, но forecast сам по себе
перезапрашивается — и это норм, потому что он обновляется ~каждый час.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from weather_bot.db import WeatherDB
from weather_bot.markets import WeatherEvent, fetch_active_events
from weather_bot.model import model_probs, rmse_for_lead
from weather_bot.nws import NWSClient


@dataclass
class EngineConfig:
    db_path: str
    cities: set[str] | None       # None = все US-города
    interval_seconds: int
    user_agent: str


class WeatherEngine:
    def __init__(self, cfg: EngineConfig) -> None:
        self.cfg = cfg
        self.db = WeatherDB(cfg.db_path)
        self.nws = NWSClient(user_agent=cfg.user_agent)

    def close(self) -> None:
        self.nws.close()
        self.db.close()

    def tick(self) -> dict:
        """Один проход. Возвращает сводку для лога."""
        now = datetime.now(timezone.utc)
        events = fetch_active_events(cities=self.cfg.cities)

        # 1) upsert markets
        for ev in events:
            self.db.upsert_market(ev, now)

        # 2) forecasts cache по (city, date) — 1 запрос в NWS на пару
        forecast_cache: dict[tuple[str, str], tuple[int, float, float]] = {}
        # observed_max cache по той же паре — отдельный endpoint, тоже 1 запрос
        obs_cache: dict[tuple[str, str], tuple[float | None, int, float]] = {}
        signals_n = 0
        fc_fails = 0
        obs_fails = 0

        for ev in events:
            key = (ev.city_slug, ev.event_date)

            # forecast
            if key not in forecast_cache:
                try:
                    fc = self.nws.forecast_high(ev.station, ev.event_date)
                except Exception as e:
                    print(f"  [warn] NWS forecast fail for {ev.city_slug} {ev.event_date}: {e}")
                    fc_fails += 1
                    continue
                if fc is None:
                    fc_fails += 1
                    continue
                sigma = rmse_for_lead(max(fc.lead_hours, 0))
                fid = self.db.insert_forecast(
                    ts=now, city_slug=ev.city_slug, event_date=ev.event_date,
                    mu_f=fc.max_temp_f, sigma_f=sigma, lead_hours=fc.lead_hours,
                    source="nws", hours_covered=fc.hours_covered,
                )
                forecast_cache[key] = (fid, fc.max_temp_f, sigma)

            # observed_max (intraday) — только для today и сегодня-в-местном-вчера
            if key not in obs_cache:
                try:
                    obs = self.nws.observed_max_so_far(ev.station, ev.event_date, now)
                except Exception as e:
                    print(f"  [warn] NWS obs fail for {ev.city_slug} {ev.event_date}: {e}")
                    obs = (None, 0, 0.0)
                    obs_fails += 1
                obs_cache[key] = obs
                obs_max, n_obs, hours_remaining = obs
                self.db.insert_obs_max(
                    ts=now, station_icao=ev.station.icao, event_date=ev.event_date,
                    observed_max_f=obs_max, n_obs=n_obs, hours_remaining=hours_remaining,
                )

            fid, mu, sigma = forecast_cache[key]
            obs_max, _n_obs, hours_remaining = obs_cache[key]
            probs = model_probs(ev.buckets, mu, sigma)
            for b, pp in zip(ev.buckets, probs):
                bid = self.db.get_bucket_id(ev.event_id, b.title)
                if bid is None:
                    continue
                self.db.insert_snapshot(
                    ts=now, bucket_id=bid, forecast_id=fid,
                    p_model=pp.p_model, yes_price=b.yes_price, no_price=b.no_price,
                    yes_best_ask=b.yes_best_ask, yes_best_bid=b.yes_best_bid,
                    yes_spread=b.yes_spread, liquidity_num=b.liquidity_num,
                    volume_24h=b.volume_24h, last_trade_price=b.last_trade_price,
                    observed_max_f=obs_max, hours_remaining=hours_remaining,
                )
                if abs(pp.p_model - b.yes_price) > 0.10:
                    signals_n += 1

        return {
            "events": len(events),
            "forecasts": len(forecast_cache),
            "forecast_fails": fc_fails,
            "obs_fails": obs_fails,
            "signals": signals_n,
        }

    def run(self) -> None:
        print(f"[weather_bot] start, interval={self.cfg.interval_seconds}s")
        while True:
            t0 = time.time()
            try:
                summary = self.tick()
                elapsed = time.time() - t0
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"events={summary['events']} fc={summary['forecasts']} "
                    f"fc_fail={summary['forecast_fails']} obs_fail={summary.get('obs_fails', 0)} "
                    f"signals={summary['signals']} elapsed={elapsed:.1f}s"
                )
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[error] tick failed: {e}")

            time.sleep(max(1.0, self.cfg.interval_seconds - (time.time() - t0)))
