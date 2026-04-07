from __future__ import annotations

import argparse
import os

import yaml
from dotenv import load_dotenv

load_dotenv()

from momentum_bot.db import MomentumDB
from momentum_bot.engine import MomentumEngine
from momentum_bot.telegram_notify import MomentumTelegramNotifier
from momentum_bot.watch_runner import MomentumWatchRunner


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    with open(path or CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Momentum follower paper bot")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("watch", help="Live monitoring with spike detection")
    subparsers.add_parser("status", help="Show positions and PnL")
    subparsers.add_parser("resolve", help="Resolve expired positions")

    args = parser.parse_args()
    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = MomentumDB(config["db"]["path"])
    notifier = None
    if args.command == "watch":
        tg_cfg = config.get("telegram", {})
        engine_ref: MomentumEngine | None = None

        def _status() -> str:
            if engine_ref is None:
                return "momentum_bot запускается..."
            return engine_ref.get_status_text()

        notifier = MomentumTelegramNotifier(
            get_status_fn=_status,
            token_env=tg_cfg.get("token_env", "FAST_ARB_BOT_TOKEN"),
            chat_id_file=tg_cfg.get("chat_id_file", "data/.telegram_chat_id"),
        )
        engine = MomentumEngine(config, db, notifier=notifier)
        engine_ref = engine
    else:
        engine = MomentumEngine(config, db)

    if args.command == "watch":
        print("[Momentum] Starting watch mode...")
        if notifier:
            notifier.start()
        runner = MomentumWatchRunner(engine)
        runner.run()

    elif args.command == "status":
        engine.print_status()

    elif args.command == "resolve":
        engine.resolve()
        engine.print_status()


if __name__ == "__main__":
    main()
