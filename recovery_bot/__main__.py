from __future__ import annotations

import argparse
import os

import yaml
from dotenv import load_dotenv

from recovery_bot.db import RecoveryDB
from recovery_bot.engine import RecoveryEngine
from recovery_bot.telegram_notify import RecoveryTelegramNotifier
from recovery_bot.watch_runner import RecoveryWatchRunner

load_dotenv()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    with open(path or CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="PM recovery bot")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to config")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("watch", help="Watch PM 5m/15m markets")
    sub.add_parser("status", help="Show DB status")
    sub.add_parser("resolve", help="Resolve expired positions")
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = RecoveryDB(config["db"]["path"])
    notifier = None
    if args.command == "watch":
        tg_cfg = config.get("telegram", {})
        engine_ref: RecoveryEngine | None = None

        def _status() -> str:
            if engine_ref is None:
                return "recovery_bot запускается..."
            return engine_ref.get_status_text()

        notifier = RecoveryTelegramNotifier(
            get_status_fn=_status,
            token_env=tg_cfg.get("token_env", "TELEGRAM_TOKEN"),
            chat_id_file=tg_cfg.get("chat_id_file", "data/.telegram_chat_id"),
        )
        engine = RecoveryEngine(config, db, notifier=notifier)
        engine_ref = engine
    else:
        engine = RecoveryEngine(config, db)

    if args.command == "watch":
        print(
            "[recovery] starting watch | "
            f"paper={bool(config['strategy'].get('paper_enabled', True))} | "
            f"real={bool(config['strategy'].get('real_enabled', False))} | "
            f"db={config['db']['path']}"
        )
        print(
            "[recovery] strategy | "
            f"5m: bottom<={config['strategy']['five_minute']['bottom_price']:.2f} -> entry={config['strategy']['five_minute']['entry_price']:.2f}"
            f" delay={int(config['strategy']['five_minute']['activation_delay_seconds'])}s | "
            f"15m: bottom<={config['strategy']['fifteen_minute']['bottom_price']:.2f} -> entry={config['strategy']['fifteen_minute']['entry_price']:.2f}"
            f" delay={int(config['strategy']['fifteen_minute']['activation_delay_seconds'])}s"
        )
        print(
            "[recovery] runtime | "
            f"refresh={int(config['runtime']['universe_refresh_seconds'])}s | "
            f"status={int(config['runtime']['status_interval_seconds'])}s"
        )
        if notifier:
            notifier.start()
            print("[recovery] telegram polling started")
        else:
            print("[recovery] telegram disabled")
        RecoveryWatchRunner(engine).run()
    elif args.command == "status":
        engine.print_status()
    elif args.command == "resolve":
        engine.resolve()
        engine.print_status()


if __name__ == "__main__":
    main()
