from __future__ import annotations

import argparse
import os

import yaml
from dotenv import load_dotenv

load_dotenv()

from real_momentum_bot.db import RealMomentumDB
from real_momentum_bot.engine import RealMomentumEngine
from real_momentum_bot.watch_runner import RealMomentumWatchRunner


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    with open(path or CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Real momentum bot — live orders, virtual budget")
    parser.add_argument("--config", default=CONFIG_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("watch", help="Live monitoring + real order execution")
    subparsers.add_parser("status", help="Positions and PnL")
    subparsers.add_parser("resolve", help="Resolve expired positions")

    args = parser.parse_args()
    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = RealMomentumDB(config["db"]["path"])
    engine = RealMomentumEngine(config, db)

    if args.command == "watch":
        if engine.is_stopped():
            print("[RealMomentum] Стоп-лосс уже достигнут, торговля невозможна.")
            engine.print_status()
            return
        strat = config["strat"] if "strat" in config else config.get("strategy", {})
        print(
            f"[RealMomentum] Запуск v6 | "
            f"max_entry={strat.get('max_entry_price')} "
            f"gap_pm→ka={strat.get('max_price_gap_cents_pm_to_kalshi')}¢ "
            f"gap_ka→pm={strat.get('max_price_gap_cents')}¢ "
            f"min_leader={strat.get('min_leader_price')} "
            f"stake=free/{strat.get('trades_per_budget', 10)}"
        )
        runner = RealMomentumWatchRunner(engine)
        runner.run()

    elif args.command == "status":
        engine.print_status()

    elif args.command == "resolve":
        engine.resolve()
        engine.retry_redeems()
        engine.print_status()


if __name__ == "__main__":
    main()
