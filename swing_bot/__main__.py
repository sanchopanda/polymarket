"""
python3 -m swing_bot run
python3 -m swing_bot scan
python3 -m swing_bot positions [--limit 50]
python3 -m swing_bot stats
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

from swing_bot.bot import SwingBot
from swing_bot.db import SwingDB
from swing_bot.telegram_notify import SwingTelegramNotifier


def main() -> None:
    parser = argparse.ArgumentParser(prog="swing_bot")
    parser.add_argument("--config", default="swing_bot/config.yaml")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="Run the bot (continuous)")
    sub.add_parser("scan", help="Single scan cycle")
    p_pos = sub.add_parser("positions", help="Show recent positions")
    p_pos.add_argument("--limit", type=int, default=50)
    sub.add_parser("stats", help="Show stats")

    args = parser.parse_args()
    config = yaml.safe_load(Path(args.config).read_text())
    db = SwingDB(config["db"]["path"])

    if args.cmd == "run":
        tg_cfg = config.get("telegram", {})
        notifier = SwingTelegramNotifier(
            get_status_fn=lambda: bot.get_status_text(),
            token_env=tg_cfg.get("token_env", "FAST_ARB_BOT_TOKEN"),
            chat_id_file=tg_cfg.get("chat_id_file", "data/.telegram_chat_id"),
        )
        bot = SwingBot(config, db, notifier=notifier)
        notifier.start()
        bot.run()

    elif args.cmd == "scan":
        bot = SwingBot(config, db)
        bot.scan_once()

    elif args.cmd == "positions":
        positions = db.get_recent_positions(args.limit)
        if not positions:
            print("Нет позиций")
            return
        print(f"{'Symbol':<6} {'State':<14} {'Entry':>6} {'REST':>6} "
              f"{'Exit':>6} {'ExitR':>6} {'Type':<5} {'PnL':>8} {'Winner':<6}")
        print("-" * 75)
        for p in positions:
            entry = f"{p.entry_price:.3f}" if p.entry_price else "  -  "
            rest = f"{p.entry_price_rest:.3f}" if p.entry_price_rest else "  -  "
            exit_p = f"{p.exit_price:.3f}" if p.exit_price else "  -  "
            exit_r = f"{p.exit_price_rest:.3f}" if p.exit_price_rest else "  -  "
            pnl = f"${p.pnl:+.4f}" if p.pnl is not None else "   -   "
            winner = p.winning_side or "-"
            print(f"{p.symbol:<6} {p.state.value:<14} {entry:>6} {rest:>6} "
                  f"{exit_p:>6} {exit_r:>6} {(p.exit_type or '-'):<5} {pnl:>8} {winner:<6}")

    elif args.cmd == "stats":
        s = db.stats()
        print(f"Total:        {s['total']}")
        print(f"Resolved:     {s['resolved']}")
        print(f"Open:         {s['open']}")
        print(f"Realized PnL: ${s['realized_pnl']:+.4f}")


if __name__ == "__main__":
    main()
