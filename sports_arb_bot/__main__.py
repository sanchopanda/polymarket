"""
python3 -m sports_arb_bot watch [--config config.yaml]
python3 -m sports_arb_bot status [--config config.yaml]
python3 -m sports_arb_bot resolve [--config config.yaml]
python3 -m sports_arb_bot scan [--sports wta atp] [--min-edge 0.0]
python3 -m sports_arb_bot tags [--limit 500]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.yaml"


def _load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


def _make_db(config: dict):
    from sports_arb_bot.db import SportsArbDB
    db_path = Path(config.get("db", {}).get("path", "data/sports_arb_bot.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SportsArbDB(str(db_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sports_arb_bot",
        description="Спортивный арбитражный бот (Polymarket ↔ Kalshi)",
    )
    sub = parser.add_subparsers(dest="cmd")

    # watch
    watch_p = sub.add_parser("watch", help="Запустить бота (paper trading)")
    watch_p.add_argument("--config", default=str(DEFAULT_CONFIG), help="Путь к config.yaml")

    # status
    status_p = sub.add_parser("status", help="Показать баланс и открытые позиции")
    status_p.add_argument("--config", default=str(DEFAULT_CONFIG))

    # resolve
    resolve_p = sub.add_parser("resolve", help="Проверить и зарезолвить завершённые матчи")
    resolve_p.add_argument("--config", default=str(DEFAULT_CONFIG))

    # scan (legacy LLM-based)
    scan_p = sub.add_parser("scan", help="Скачать рынки, сматчить и вывести пары")
    scan_p.add_argument("--sports", nargs="+", default=["wta", "atp"], metavar="SPORT")
    scan_p.add_argument("--min-confidence", type=float, default=0.8)
    scan_p.add_argument("--min-edge", type=float, default=0.0)
    scan_p.add_argument("--window-hours", type=float, default=24.0)

    # tags
    tags_p = sub.add_parser("tags", help="Показать тэги PM рынков")
    tags_p.add_argument("--limit", type=int, default=500)

    args = parser.parse_args()

    if args.cmd == "watch":
        config = _load_config(args.config)
        db = _make_db(config)
        from sports_arb_bot.watch_runner import SportsArbWatchRunner
        runner = SportsArbWatchRunner(config=config, db=db)

        # Telegram (опционально)
        try:
            from sports_arb_bot.telegram_notify import SportsTelegramNotifier
            token_env = config.get("telegram", {}).get("token_env", "TELEGRAM_TOKEN")
            tg = SportsTelegramNotifier(get_status_fn=runner._get_status_text, token_env=token_env)
            runner.tg = tg
            tg.start()
        except RuntimeError as e:
            print(f"[Telegram] Отключён: {e}")
        except Exception as e:
            print(f"[Telegram] Ошибка инициализации: {e}")

        runner.run()

    elif args.cmd == "status":
        config = _load_config(args.config)
        db = _make_db(config)
        from sports_arb_bot.watch_runner import SportsArbWatchRunner
        import re

        runner = SportsArbWatchRunner(config=config, db=db)
        runner._refresh_balances_for_status(force=True)
        clean = re.sub(r"<[^>]+>", "", runner._get_status_text())
        print(clean)

    elif args.cmd == "resolve":
        config = _load_config(args.config)
        db = _make_db(config)
        from sports_arb_bot.watch_runner import SportsArbWatchRunner
        runner = SportsArbWatchRunner(config=config, db=db)
        runner.resolve_expired()

    elif args.cmd == "scan":
        from sports_arb_bot.engine import print_results, scan
        pairs = scan(
            sports=args.sports,
            min_confidence=args.min_confidence,
            min_edge=args.min_edge,
            window_hours=args.window_hours,
        )
        print_results(pairs)

    elif args.cmd == "tags":
        from sports_arb_bot.feed_polymarket import PolymarketSportsFeed
        feed = PolymarketSportsFeed()
        report = feed.fetch_tags_report()
        print(f"\nТэги спортивных рынков Polymarket (moneyline), топ-50:")
        print(f"{'Тэг':<40} {'Кол-во':>8}")
        print("-" * 50)
        for tag, count in list(report.items())[:50]:
            print(f"{tag:<40} {count:>8}")
        print(f"\nВсего уникальных тэгов: {len(report)}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
