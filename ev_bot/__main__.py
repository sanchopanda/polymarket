#!/usr/bin/env python3
"""
EV-бот — изолированный paper trading бот с адаптивным EV-фильтром.
Анализирует закрытые рынки по бакетам (цена × объём), ставит только туда где +EV.

Использование:
  python -m ev-bot analyze                   # EV-анализ кэша
  python -m ev-bot analyze --min-samples 100 # с большей выборкой
  python -m ev-bot scan --dry                # кандидаты без сохранения
  python -m ev-bot scan                      # один скан
  python -m ev-bot resolve                   # проверить резолюции
  python -m ev-bot run                       # непрерывный режим
  python -m ev-bot dashboard                 # статистика
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Добавляем корень проекта в sys.path чтобы импортировать src.*
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.backtest.fetcher import load_markets
from src.config import load_config

from ev_bot.config import load_ev_config
from ev_bot.db import EVStore
from ev_bot.engine import EVPaperEngine
from ev_bot.filter import EVFilter
from ev_bot.report import show_ev_analysis


def _build(ev_cfg, main_cfg):
    gamma = GammaClient(
        base_url=main_cfg.api.gamma_base_url,
        page_size=main_cfg.api.page_size,
        delay_ms=main_cfg.api.request_delay_ms,
    )
    store = EVStore(ev_cfg.db.path)
    ev_filter = _load_ev_filter(ev_cfg)
    engine = EVPaperEngine(ev_cfg, store, gamma, ev_filter)
    return engine, store, ev_filter


def _load_ev_filter(ev_cfg) -> EVFilter:
    cache = ev_cfg.ev_filter.cache_path
    ev_filter = EVFilter(
        taker_fee=ev_cfg.martingale.taker_fee,
        min_samples=ev_cfg.ev_filter.min_samples,
        recalc_interval=ev_cfg.ev_filter.recalc_interval,
    )
    if Path(cache).exists():
        markets = load_markets(cache)
        ev_filter.load_history(markets)
        print(f"[EV-Bot] Кэш загружен ({len(markets)} рынков). {ev_filter.summary()}")
    else:
        print(f"[EV-Bot] Кэш не найден: {cache}")
        print("[EV-Bot] Загрузите данные: python -m src.main fetch --limit 10000")
    return ev_filter


def cmd_analyze(args, ev_cfg):
    ev_filter = EVFilter(
        taker_fee=ev_cfg.martingale.taker_fee,
        min_samples=args.min_samples,
        recalc_interval=ev_cfg.ev_filter.recalc_interval,
    )
    cache = ev_cfg.ev_filter.cache_path
    if not Path(cache).exists():
        print(f"Кэш не найден: {cache}")
        print("Загрузите: python -m src.main fetch --limit 10000")
        return
    markets = load_markets(cache)
    ev_filter.load_history(markets)
    show_ev_analysis(ev_filter)


def cmd_scan(args, ev_cfg, main_cfg):
    engine, *_ = _build(ev_cfg, main_cfg)
    engine.scan(dry_run=args.dry)


def cmd_resolve(args, ev_cfg, main_cfg):
    engine, *_ = _build(ev_cfg, main_cfg)
    engine.check_resolutions()


def cmd_run(args, ev_cfg, main_cfg):
    max_interval_sec = args.interval * 3600
    print(f"[EV-Bot] Запуск. Интервал: {args.interval*60:.0f} мин. Ctrl+C для остановки.")

    engine, store, ev_filter = _build(ev_cfg, main_cfg)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n[EV-Bot] ═══ Цикл #{cycle} ═══")
        engine.check_resolutions()
        engine.scan()

        # Показываем статистику
        stats = store.get_stats()
        print(
            f"[EV-Bot] Серии: активных={stats['active_series']} "
            f"выиграно={stats['won_series']} брошено={stats['abandoned_series']} "
            f"| P&L: ${stats['realized_pnl']:+.2f}"
        )

        # Умный sleep: до ближайшей экспирации
        open_bets = store.get_open_bets()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        future_dates = [b.market_end_date for b in open_bets if b.market_end_date and b.market_end_date > now]
        nearest = min(future_dates, default=None)
        if nearest:
            secs = min((nearest - now).total_seconds() + 5, max_interval_sec)
        else:
            secs = max_interval_sec

        next_t = datetime.now(timezone.utc).replace(tzinfo=None)
        next_t = next_t.fromtimestamp(next_t.timestamp() + secs)
        label = f"{secs:.0f} сек." if secs < 60 else f"{secs/60:.1f} мин."
        print(f"[EV-Bot] Следующий цикл в {next_t.strftime('%H:%M:%S')} (через {label})")

        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            print("\n[EV-Bot] Остановлено.")
            break


def cmd_dashboard(args, ev_cfg, main_cfg):
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    _, store, ev_filter = _build(ev_cfg, main_cfg)
    stats = store.get_stats()
    console = Console()

    total_bets = stats["won_bets"] + stats["lost_bets"]
    win_rate = (stats["won_bets"] / total_bets * 100) if total_bets > 0 else 0

    console.print(Panel(
        f"[bold]EV-бот  —  paper trading[/bold]\n"
        f"Серии: активных [cyan]{stats['active_series']}[/cyan]  "
        f"выиграно [green]{stats['won_series']}[/green]  "
        f"брошено [red]{stats['abandoned_series']}[/red]\n"
        f"Ставки: открытых [cyan]{stats['open_bets']}[/cyan]  "
        f"выиграно [green]{stats['won_bets']}[/green]  "
        f"проиграно [red]{stats['lost_bets']}[/red]  "
        f"(win rate: {win_rate:.1f}%)\n"
        f"P&L реализованный: [bold]${stats['realized_pnl']:+.2f}[/bold]\n"
        f"EV-фильтр: {ev_filter.summary()}",
        title="Статистика",
    ))

    # Таблица серий
    all_series = store.get_all_series()
    if all_series:
        tbl = Table(title="Последние серии", show_header=True, header_style="bold")
        tbl.add_column("ID", width=8)
        tbl.add_column("Статус", width=10)
        tbl.add_column("Глубина", justify="right", width=8)
        tbl.add_column("Вложено", justify="right", width=10)
        tbl.add_column("P&L", justify="right", width=10)
        for s in all_series[:20]:
            color = {"won": "green", "abandoned": "red", "active": "cyan"}.get(s.status, "white")
            tbl.add_row(
                s.id[:8],
                f"[{color}]{s.status}[/{color}]",
                str(s.current_depth),
                f"${s.total_invested:.2f}",
                f"${s.total_pnl:+.2f}" if s.total_pnl else "—",
            )
        console.print(tbl)


def main():
    parser = argparse.ArgumentParser(
        description="EV-бот — paper trading с адаптивным EV-фильтром",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default="ev-bot/ev_config.yaml", help="Путь к ev_config.yaml")
    parser.add_argument("--main-config", default="config.yaml", help="Путь к основному config.yaml (для API URLs)")

    sub = parser.add_subparsers(dest="command", required=True)

    # analyze
    a = sub.add_parser("analyze", help="EV-анализ бакетов из кэша исторических рынков")
    a.add_argument("--min-samples", type=int, default=None, help="Мин. рынков в бакете (override config)")

    # scan
    s = sub.add_parser("scan", help="Один скан рынков")
    s.add_argument("--dry", action="store_true", help="Не сохранять ставки")

    # resolve
    sub.add_parser("resolve", help="Проверить резолюции + эскалация")

    # run
    r = sub.add_parser("run", help="Непрерывный режим")
    r.add_argument("--interval", type=float, default=0.033, help="Интервал в часах (default: 0.033 = 2 мин)")

    # dashboard
    sub.add_parser("dashboard", help="Статистика")

    args = parser.parse_args()
    ev_cfg = load_ev_config(args.config)
    main_cfg = load_config(args.main_config)

    # Переопределить min_samples если задан через CLI
    if args.command == "analyze" and args.min_samples is not None:
        ev_cfg.ev_filter.min_samples = args.min_samples

    dispatch = {
        "analyze": lambda: cmd_analyze(args, ev_cfg),
        "scan": lambda: cmd_scan(args, ev_cfg, main_cfg),
        "resolve": lambda: cmd_resolve(args, ev_cfg, main_cfg),
        "run": lambda: cmd_run(args, ev_cfg, main_cfg),
        "dashboard": lambda: cmd_dashboard(args, ev_cfg, main_cfg),
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()
