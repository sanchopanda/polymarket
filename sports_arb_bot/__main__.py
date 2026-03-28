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
            tg = SportsTelegramNotifier(get_status_fn=runner._get_status_text)
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
        bal = db.get_balance()
        open_pos = db.get_open_positions()
        all_pos = db.get_all_positions()
        resolved = [p for p in all_pos if p["status"] == "resolved"]

        print(f"\n{'='*60}")
        print(f"Виртуальный баланс")
        print(f"{'='*60}")
        print(f"  Начальный:      ${bal['initial_balance']:.2f}")
        print(f"  Текущий:        ${bal['current_balance']:.2f}")
        print(f"  Всего ставок:   ${bal['total_wagered']:.2f}")
        print(f"  Выиграно:       ${bal['total_won']:.2f}")
        print(f"  Проиграно:      ${bal['total_lost']:.2f}")
        net = bal['current_balance'] - bal['initial_balance']
        print(f"  P&L:            {'+'if net>=0 else ''}${net:.2f}")

        print(f"\nОткрытых позиций: {len(open_pos)}")
        for p in open_pos:
            print(f"  {p['id']} | {p['pm_slug']} | "
                  f"edge={p['edge']:.3f} cost={p['cost']:.3f} "
                  f"shares={p['shares']} total=${p['total_cost']:.2f} | "
                  f"opened={p['opened_at'][:16]}")

        print(f"\nЗарезолвленных: {len(resolved)}")
        for p in resolved[-10:]:
            sign = "+" if (p['pnl'] or 0) >= 0 else ""
            print(f"  {p['id']} | {p['pm_slug']} | "
                  f"winner={p['winner']} | pnl={sign}${(p['pnl'] or 0):.2f}")

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
