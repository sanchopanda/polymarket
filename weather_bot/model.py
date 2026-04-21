"""Гауссова модель вероятностей по бакетам.

Входы:
  mu     — forecast high (°F), point estimate
  sigma  — RMSE прогноза (°F), зависит от lead time
  buckets — список бакетов с [lo, hi] (lo=-inf для левого хвоста, hi=+inf для правого)

Логика: temp ~ Normal(mu, sigma). Пол. вероятность того, что rounded high
попадает в бакет, считаем как вероятность попадания [lo-0.5, hi+0.5], потому что
Polymarket резолвит в целых °F и бакеты типа "64-65°F" включают оба целых градуса
(т.е. любой high ∈ [63.5, 65.5) округляется в 64 или 65).

Это небольшая поправка, но для узких бакетов важна.

RMSE калибруется под lead time по NOAA guidance (Model Performance Statistics).
Дефолты — консервативно слегка выше литературных:
  0–24h : 2.5°F
  24–48h: 3.0°F
  48–72h: 3.5°F
  72–96h: 4.5°F
"""
from __future__ import annotations

from dataclasses import dataclass
from math import erf, sqrt


def rmse_for_lead(lead_hours: float) -> float:
    """RMSE прогноза макс температуры по lead time."""
    if lead_hours <= 24:
        return 2.5
    if lead_hours <= 48:
        return 3.0
    if lead_hours <= 72:
        return 3.5
    if lead_hours <= 96:
        return 4.5
    return 6.0


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def bucket_probability(lo: float, hi: float, mu: float, sigma: float) -> float:
    """P(rounded_high попадает в [lo, hi] °F) при T~Normal(mu, sigma).

    lo=-inf → P(T < hi + 0.5)
    hi=+inf → P(T >= lo - 0.5)
    иначе   → P(lo - 0.5 <= T < hi + 0.5)
    """
    if sigma <= 0:
        # дегенеративный случай: point estimate
        t_rounded = round(mu)
        if lo == float("-inf"):
            return 1.0 if t_rounded <= hi else 0.0
        if hi == float("inf"):
            return 1.0 if t_rounded >= lo else 0.0
        return 1.0 if lo <= t_rounded <= hi else 0.0

    if lo == float("-inf"):
        z_hi = (hi + 0.5 - mu) / sigma
        return _norm_cdf(z_hi)
    if hi == float("inf"):
        z_lo = (lo - 0.5 - mu) / sigma
        return 1.0 - _norm_cdf(z_lo)
    z_hi = (hi + 0.5 - mu) / sigma
    z_lo = (lo - 0.5 - mu) / sigma
    return _norm_cdf(z_hi) - _norm_cdf(z_lo)


@dataclass
class BucketProb:
    title: str
    lo: float
    hi: float
    p_model: float


def model_probs(
    buckets: list,  # list of weather_bot.markets.Bucket
    mu: float,
    sigma: float,
) -> list[BucketProb]:
    """Возвращает список (title, lo, hi, p_model) для каждого бакета.

    Сумма p_model ≈ 1 (может слегка не совпадать если бакеты не покрывают все целые,
    но Polymarket покрывает — хвосты уходят в бесконечность).
    """
    return [
        BucketProb(
            title=b.title, lo=b.lo, hi=b.hi,
            p_model=bucket_probability(b.lo, b.hi, mu, sigma),
        )
        for b in buckets
    ]
