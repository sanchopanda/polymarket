from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class EVFilterConfig:
    cache_path: str = "data/backtest_markets.json"
    min_samples: int = 50
    recalc_interval: int = 50


@dataclass
class EVStrategyConfig:
    price_min: float = 0.05
    price_max: float = 0.50
    min_volume: float = 1000.0
    min_liquidity: float = 1000.0
    max_days_to_expiry: float = 0.083  # 2 часа
    fee_type: str = "crypto_fees"


@dataclass
class EVMartingaleConfig:
    initial_bet: float = 1.0
    max_depth: int = 6
    taker_fee: float = 0.02
    starting_balance: float = 50.0


@dataclass
class EVDbConfig:
    path: str = "data/ev_bot.db"


@dataclass
class EVBotConfig:
    ev_filter: EVFilterConfig = field(default_factory=EVFilterConfig)
    strategy: EVStrategyConfig = field(default_factory=EVStrategyConfig)
    martingale: EVMartingaleConfig = field(default_factory=EVMartingaleConfig)
    db: EVDbConfig = field(default_factory=EVDbConfig)


def load_ev_config(path: str = "ev-bot/ev_config.yaml") -> EVBotConfig:
    raw: dict = {}
    p = Path(path)
    if p.exists():
        with open(p) as f:
            raw = yaml.safe_load(f) or {}

    def _get(d: dict, key: str, default):
        return d.get(key, default)

    ef = raw.get("ev_filter", {})
    ev_filter = EVFilterConfig(
        cache_path=_get(ef, "cache_path", "data/backtest_markets.json"),
        min_samples=_get(ef, "min_samples", 50),
        recalc_interval=_get(ef, "recalc_interval", 50),
    )

    s = raw.get("strategy", {})
    strategy = EVStrategyConfig(
        price_min=_get(s, "price_min", 0.05),
        price_max=_get(s, "price_max", 0.50),
        min_volume=_get(s, "min_volume", 1000.0),
        min_liquidity=_get(s, "min_liquidity", 1000.0),
        max_days_to_expiry=_get(s, "max_days_to_expiry", 0.083),
        fee_type=_get(s, "fee_type", "crypto_fees"),
    )

    m = raw.get("martingale", {})
    martingale = EVMartingaleConfig(
        initial_bet=_get(m, "initial_bet", 1.0),
        max_depth=_get(m, "max_depth", 6),
        taker_fee=_get(m, "taker_fee", 0.02),
        starting_balance=_get(m, "starting_balance", 50.0),
    )

    db_raw = raw.get("db", {})
    db = EVDbConfig(path=_get(db_raw, "path", "data/ev_bot.db"))

    return EVBotConfig(ev_filter=ev_filter, strategy=strategy, martingale=martingale, db=db)
