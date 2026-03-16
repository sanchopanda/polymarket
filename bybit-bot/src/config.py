from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml
from dotenv import load_dotenv


@dataclass
class BybitConfig:
    mode: str = "demo"
    api_key: str = ""
    api_secret: str = ""
    symbols: List[str] = field(default_factory=lambda: ["XRPUSDT", "DOGEUSDT", "SOLUSDT"])
    leverage: int = 100


@dataclass
class MartingaleConfig:
    initial_margin_usdt: float = 0.10
    take_profit_pct: float = 1.0
    stop_loss_pct: float = 1.0
    max_series_depth: int = 10
    max_active_series: int = 3
    check_interval_sec: int = 10


@dataclass
class DbConfig:
    path: str = "data/bot.db"


@dataclass
class ReportsConfig:
    starting_balance: float = 50.0
    max_rows: int = 50


@dataclass
class Config:
    bybit: BybitConfig = field(default_factory=BybitConfig)
    martingale: MartingaleConfig = field(default_factory=MartingaleConfig)
    db: DbConfig = field(default_factory=DbConfig)
    reports: ReportsConfig = field(default_factory=ReportsConfig)


def load_config(path: str = "config.yaml") -> Config:
    load_dotenv()

    raw: dict = {}
    if Path(path).exists():
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

    b = raw.get("bybit", {})
    bybit = BybitConfig(
        mode=b.get("mode", "demo"),
        api_key=os.getenv("BYBIT_API_KEY", b.get("api_key", "")),
        api_secret=os.getenv("BYBIT_API_SECRET", b.get("api_secret", "")),
        symbols=b.get("symbols", ["XRPUSDT", "DOGEUSDT", "SOLUSDT"]),
        leverage=b.get("leverage", 100),
    )

    m = raw.get("martingale", {})
    martingale = MartingaleConfig(
        initial_margin_usdt=m.get("initial_margin_usdt", 0.10),
        take_profit_pct=m.get("take_profit_pct", 1.0),
        stop_loss_pct=m.get("stop_loss_pct", 1.0),
        max_series_depth=m.get("max_series_depth", 10),
        max_active_series=m.get("max_active_series", 3),
        check_interval_sec=m.get("check_interval_sec", 10),
    )

    db_raw = raw.get("db", {})
    db = DbConfig(path=db_raw.get("path", "data/bot.db"))

    r = raw.get("reports", {})
    reports = ReportsConfig(
        starting_balance=r.get("starting_balance", 50.0),
        max_rows=r.get("max_rows", 50),
    )

    return Config(bybit=bybit, martingale=martingale, db=db, reports=reports)
