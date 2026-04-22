from __future__ import annotations

import argparse
import os

import yaml
from dotenv import load_dotenv

from jump_paper_bot.db import JumpPaperDB
from jump_paper_bot.engine import JumpPaperEngine
from jump_paper_bot.telegram_notify import JumpTelegramNotifier
from jump_paper_bot.watch_runner import JumpPaperWatchRunner

load_dotenv()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    with open(path or CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="PM jump paper bot")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to config")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("watch", help="Watch PM short markets and open paper positions on jump signal")
    sub.add_parser("status", help="Show DB status")
    sub.add_parser("resolve", help="Resolve expired positions")
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = JumpPaperDB(config["db"]["path"])
    notifier = None
    if args.command == "watch":
        tg_cfg = config.get("telegram", {})
        engine_ref: JumpPaperEngine | None = None

        def _status() -> str:
            if engine_ref is None:
                return "jump_paper_bot запускается..."
            return engine_ref.get_status_text()

        notifier = JumpTelegramNotifier(
            get_status_fn=_status,
            token_env="SIMPLE_BOT_TOKEN",
            chat_id_file=tg_cfg.get("chat_id_file", "data/.telegram_chat_id"),
        )
        engine = JumpPaperEngine(config, db, notifier=notifier)
        engine_ref = engine
    else:
        engine = JumpPaperEngine(config, db)

    if args.command == "watch":
        print(
            "[jump] starting watch | "
            f"stake=${float(config['strategy']['paper_stake_usd']):.2f} | "
            f"jump={float(config['strategy']['jump_cents']):.2f} | "
            f"lookback={int(config['strategy']['lookback_seconds'])}s | "
            f"db={config['db']['path']}"
        )
        if notifier:
            notifier.start()
            print("[jump] telegram polling started")
        else:
            print("[jump] telegram disabled")
        JumpPaperWatchRunner(engine).run()
    elif args.command == "status":
        engine.print_status()
    elif args.command == "resolve":
        engine.resolve()
        engine.print_status()


if __name__ == "__main__":
    main()
