from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml
from dotenv import load_dotenv


@dataclass
class StrategyConfig:
    target_price: float = 0.50
    price_tolerance: float = 0.10
    price_min: float | None = None   # Если заданы — переопределяют target_price ± tolerance
    price_max: float | None = None
    min_volume_24h: float = 1000.0
    min_liquidity: float = 5000.0
    min_days_to_expiry: int = 0
    max_days_to_expiry: int = 180
    categories: List[str] = field(default_factory=list)
    fee_type: str = ""               # "crypto_fees" — только крипто, "none" — только без комиссий, "" — все


@dataclass
class MartingaleConfig:
    initial_bet_size: float = 0.10
    max_series_depth: int = 6
    max_active_series: int = 3
    escalation_multiplier: float | None = None  # None = авто: 2 × (1 + taker_fee)
    starting_balance: float | None = None       # Начальный баланс (только для real, задаётся вручную)


@dataclass
class PaperTradingConfig:
    starting_balance: float = 50.0
    check_liquidity: bool = False
    taker_fee: float = 0.02   # 2% комиссия Polymarket


@dataclass
class ApiConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    request_delay_ms: int = 300
    page_size: int = 100


@dataclass
class DbConfig:
    path: str = "data/bot.db"
    real_path: str = "data/real.db"


@dataclass
class ReportsConfig:
    max_rows: int = 50


@dataclass
class TelegramConfig:
    token: str = ""


@dataclass
class WalletConfig:
    private_key: str = ""
    chain_id: int = 137  # Polygon mainnet
    proxy: str = ""      # Proxy wallet адрес (для Magic.link аккаунтов)


@dataclass
class Config:
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    martingale: MartingaleConfig = field(default_factory=MartingaleConfig)
    real_martingale: MartingaleConfig = field(default_factory=MartingaleConfig)
    paper_trading: PaperTradingConfig = field(default_factory=PaperTradingConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    db: DbConfig = field(default_factory=DbConfig)
    reports: ReportsConfig = field(default_factory=ReportsConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    wallet: WalletConfig = field(default_factory=WalletConfig)


def load_config(path: str | None = None) -> Config:
    load_dotenv()

    if path is None:
        path = os.environ.get("BOT_CONFIG", "config.yaml")

    raw: dict = {}
    config_path = Path(path)
    if config_path.exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    def _get(section: dict, key: str, default):
        return section.get(key, default)

    s = raw.get("strategy", {})
    strategy = StrategyConfig(
        target_price=_get(s, "target_price", 0.50),
        price_tolerance=_get(s, "price_tolerance", 0.10),
        price_min=_get(s, "price_min", None),
        price_max=_get(s, "price_max", None),
        min_volume_24h=_get(s, "min_volume_24h", 1000.0),
        min_liquidity=_get(s, "min_liquidity", 5000.0),
        min_days_to_expiry=_get(s, "min_days_to_expiry", 0),
        max_days_to_expiry=_get(s, "max_days_to_expiry", 180),
        categories=_get(s, "categories", []),
        fee_type=_get(s, "fee_type", ""),
    )

    mg = raw.get("martingale", {})
    martingale = MartingaleConfig(
        initial_bet_size=_get(mg, "initial_bet_size", 0.10),
        max_series_depth=_get(mg, "max_series_depth", 6),
        max_active_series=_get(mg, "max_active_series", 3),
        escalation_multiplier=_get(mg, "escalation_multiplier", None),
    )

    pt = raw.get("paper_trading", {})
    paper_trading = PaperTradingConfig(
        starting_balance=_get(pt, "starting_balance", 50.0),
        check_liquidity=_get(pt, "check_liquidity", False),
        taker_fee=_get(pt, "taker_fee", 0.02),
    )

    a = raw.get("api", {})
    api = ApiConfig(
        gamma_base_url=_get(a, "gamma_base_url", "https://gamma-api.polymarket.com"),
        clob_base_url=_get(a, "clob_base_url", "https://clob.polymarket.com"),
        request_delay_ms=_get(a, "request_delay_ms", 300),
        page_size=_get(a, "page_size", 100),
    )

    db_raw = raw.get("db", {})
    db = DbConfig(
        path=_get(db_raw, "path", "data/bot.db"),
        real_path=_get(db_raw, "real_path", "data/real.db"),
    )

    r = raw.get("reports", {})
    reports = ReportsConfig(max_rows=_get(r, "max_rows", 50))

    tg = raw.get("telegram", {})
    telegram = TelegramConfig(token=os.getenv("TELEGRAM_TOKEN", _get(tg, "token", "")))

    w = raw.get("wallet", {})
    wallet = WalletConfig(
        private_key=os.getenv("WALLET_PRIVATE_KEY", _get(w, "private_key", "")),
        chain_id=_get(w, "chain_id", 137),
        proxy=os.getenv("WALLET_PROXY", _get(w, "proxy", "")),
    )

    rm = raw.get("real_martingale", {})
    real_martingale = MartingaleConfig(
        initial_bet_size=_get(rm, "initial_bet_size", 1.0),
        max_series_depth=_get(rm, "max_series_depth", 3),
        max_active_series=_get(rm, "max_active_series", 2),
        escalation_multiplier=_get(rm, "escalation_multiplier", None),
        starting_balance=_get(rm, "starting_balance", None),
    )

    return Config(
        strategy=strategy, martingale=martingale, real_martingale=real_martingale,
        paper_trading=paper_trading,
        api=api, db=db, reports=reports, telegram=telegram, wallet=wallet,
    )
