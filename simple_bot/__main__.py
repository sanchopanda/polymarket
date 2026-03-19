"""
Simple Bot — виртуальный портфель на Polymarket.

Стратегия: ставим на рынки с экспирацией ≤ N дней, price в диапазоне, vol ≥ 1000.
Нет Мартингейла. Каждая ставка независима.

Команды:
  python -m simple_bot scan           # Разовый скан
  python -m simple_bot scan --dry     # Показать кандидатов без ставок
  python -m simple_bot resolve        # Проверить резолюции
  python -m simple_bot run            # Непрерывный режим (resolve + scan)
  python -m simple_bot status         # Текущий счёт
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from simple_bot.db import BotDB
from simple_bot.engine import SimpleBotEngine


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f) or {}
    # Telegram token из env
    tg = cfg.setdefault("telegram", {})
    tg["token"] = os.getenv("SIMPLE_BOT_TOKEN", tg.get("token", ""))
    return cfg


def cmd_scan(args, engine: SimpleBotEngine) -> None:
    engine.run_scan(dry_run=args.dry)


def cmd_resolve(args, engine: SimpleBotEngine) -> None:
    engine.check_resolutions()


def cmd_status(args, engine: SimpleBotEngine) -> None:
    from simple_bot.telegram_bot import build_status
    cfg = engine.config
    text = build_status(engine.db, cfg["trading"]["starting_balance"])
    # Убираем HTML-теги для консоли
    text = text.replace("<b>", "").replace("</b>", "").replace("&amp;", "&")
    print(text)


def cmd_bets(args, engine: SimpleBotEngine) -> None:
    bets = engine.db.get_all_bets()
    if not bets:
        print("Ставок пока нет.")
        return

    status_filter = args.status
    if status_filter:
        bets = [b for b in bets if b.status == status_filter]

    status_icons = {"open": "⏳", "won": "✅", "lost": "❌"}
    print(f"\n{'Статус':<8} {'Исход':<45} {'Цена':>6} {'Сумма':>7} {'P&L':>8} {'Размещена':<16}")
    print("─" * 95)
    for b in bets:
        icon = status_icons.get(b.status, "?")
        pnl_str = f"${b.pnl:+.2f}" if b.pnl is not None else "—"
        placed = b.placed_at.strftime("%m-%d %H:%M")
        print(
            f" {icon} {b.status:<5}"
            f" {b.outcome[:43]:<45}"
            f" {b.entry_price:>6.3f}"
            f" ${b.amount:>5.2f}"
            f" {pnl_str:>8}"
            f" {placed}"
        )
    print("─" * 95)
    stats = engine.db.stats()
    print(
        f"Всего: {stats['total']}"
        f" | Открыто: {stats['open']}"
        f" | Выиграно: {stats['won']}"
        f" | Проиграно: {stats['lost']}"
        f" | P&L: ${stats['realized_pnl']:+.2f}"
    )


def cmd_run(args, engine: SimpleBotEngine, config: dict) -> None:
    interval_sec = args.interval * 60

    tg_token = config.get("telegram", {}).get("token", "")
    tg = None
    if tg_token:
        from simple_bot.telegram_bot import SimpleTelegramBot
        tg = SimpleTelegramBot(tg_token, engine.db, config["trading"]["starting_balance"])
        tg.start()

    print(f"[Run] Запуск. Интервал: {args.interval} мин. Ctrl+C для остановки.")
    cycle = 0
    while True:
        cycle += 1
        print(f"\n[Run] ══ Цикл #{cycle} [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ══")
        engine.check_resolutions()
        engine.run_scan(dry_run=False)

        # Спим до ближайшей экспирации (но не больше interval_sec)
        open_bets = engine.db.get_open_bets()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        future = [b.end_date for b in open_bets if b.end_date > now]
        if future:
            nearest = min(future)
            secs = min((nearest - now).total_seconds() + 10, interval_sec)
        else:
            secs = interval_sec

        next_at = datetime.now(timezone.utc).strftime('%H:%M:%S')
        print(f"\n[Run] Следующий цикл через {secs/60:.1f} мин.")
        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            print("\n[Run] Остановлено.")
            break


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Bot — виртуальный портфель Polymarket")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_p = subparsers.add_parser("scan", help="Сканировать рынки")
    scan_p.add_argument("--dry", action="store_true", help="Без сохранения ставок")

    subparsers.add_parser("resolve", help="Проверить резолюции")
    subparsers.add_parser("status", help="Текущий счёт")

    bets_p = subparsers.add_parser("bets", help="Показать все ставки")
    bets_p.add_argument("--status", choices=["open", "won", "lost"], help="Фильтр по статусу")

    run_p = subparsers.add_parser("run", help="Непрерывный режим")
    run_p.add_argument("--interval", type=float, default=60, help="Интервал в минутах (по умолчанию 60)")

    args = parser.parse_args()
    config = load_config()

    os.makedirs("data", exist_ok=True)
    db = BotDB(config["db"]["path"])
    engine = SimpleBotEngine(config, db)

    if args.command == "scan":
        cmd_scan(args, engine)
    elif args.command == "resolve":
        cmd_resolve(args, engine)
    elif args.command == "status":
        cmd_status(args, engine)
    elif args.command == "bets":
        cmd_bets(args, engine)
    elif args.command == "run":
        cmd_run(args, engine, config)


if __name__ == "__main__":
    main()
