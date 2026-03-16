#!/usr/bin/env python3
"""
Сравнение стратегий ставок на данных бэктеста Polymarket.

Загружает исторические рынки из JSON и прогоняет через несколько стратегий:
1. Мартингейл (текущая) — удвоение при проигрыше
2. Flat — фиксированная ставка
3. Д'Аламбер — линейное увеличение при проигрыше
4. Фибоначчи — ставки по последовательности Фибоначчи
5. Анти-Мартингейл (Парлей) — удвоение при выигрыше
6. Фильтрация + Flat — Flat только на рынках с entry_price <= 0.40

Запуск:
    python scripts/compare_strategies.py
    python scripts/compare_strategies.py --balance 200 --bet 2.0 --depth 8
"""

import argparse
import json
import random
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.table import Table


# ---------------------------------------------------------------------------
# Модель рынка
# ---------------------------------------------------------------------------

@dataclass
class Market:
    market_id: str
    question: str
    outcome: str
    token_id: str
    entry_price: float
    final_price: float
    won: bool
    volume_num: float
    liquidity_num: float
    end_date: str


# ---------------------------------------------------------------------------
# Результат стратегии
# ---------------------------------------------------------------------------

@dataclass
class StrategyResult:
    name: str
    starting_balance: float
    final_balance: float
    total_bets: int = 0
    won_count: int = 0
    lost_count: int = 0
    total_invested: float = 0.0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    max_consecutive_losses: int = 0

    @property
    def win_rate(self) -> float:
        if self.total_bets == 0:
            return 0.0
        return self.won_count / self.total_bets * 100

    @property
    def roi(self) -> float:
        if self.total_invested == 0:
            return 0.0
        return self.total_pnl / self.total_invested * 100


# ---------------------------------------------------------------------------
# Утилиты расчёта P&L
# ---------------------------------------------------------------------------

def bet_cost(bet_size: float, taker_fee: float) -> float:
    """Стоимость ставки (покупка контракта + комиссия)."""
    return bet_size * (1 + taker_fee)


def bet_pnl(bet_size: float, entry_price: float, final_price: float,
            won: bool, taker_fee: float) -> float:
    """
    Чистый P&L от ставки.
    Выигрыш: контракты * final_price - комиссия_продажи - стоимость.
    Проигрыш: -стоимость.
    """
    cost = bet_cost(bet_size, taker_fee)
    if won:
        contracts = bet_size / entry_price
        gross = contracts * final_price
        fee = gross * taker_fee
        net = gross - fee - cost
        return net
    else:
        return -cost


# ---------------------------------------------------------------------------
# Трекер баланса и статистики
# ---------------------------------------------------------------------------

class Tracker:
    """Отслеживает баланс, просадку, серии проигрышей."""

    def __init__(self, starting_balance: float):
        self.balance = starting_balance
        self.starting_balance = starting_balance
        self.peak = starting_balance
        self.max_drawdown = 0.0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0
        self.total_bets = 0
        self.won_count = 0
        self.lost_count = 0
        self.total_invested = 0.0
        self.total_pnl = 0.0

    def can_afford(self, cost: float) -> bool:
        return self.balance >= cost

    def place_bet(self, bet_size: float, entry_price: float,
                  final_price: float, won: bool, taker_fee: float):
        cost = bet_cost(bet_size, taker_fee)
        pnl = bet_pnl(bet_size, entry_price, final_price, won, taker_fee)

        self.balance += pnl
        self.total_bets += 1
        self.total_invested += cost
        self.total_pnl += pnl

        if won:
            self.won_count += 1
            self.consecutive_losses = 0
        else:
            self.lost_count += 1
            self.consecutive_losses += 1
            self.max_consecutive_losses = max(
                self.max_consecutive_losses, self.consecutive_losses
            )

        # Обновляем пик и просадку
        if self.balance > self.peak:
            self.peak = self.balance
        drawdown = self.peak - self.balance
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown

    def result(self, name: str) -> StrategyResult:
        return StrategyResult(
            name=name,
            starting_balance=self.starting_balance,
            final_balance=self.balance,
            total_bets=self.total_bets,
            won_count=self.won_count,
            lost_count=self.lost_count,
            total_invested=self.total_invested,
            total_pnl=self.total_pnl,
            max_drawdown=self.max_drawdown,
            max_consecutive_losses=self.max_consecutive_losses,
        )


# ---------------------------------------------------------------------------
# Стратегии
# ---------------------------------------------------------------------------

def strategy_martingale(markets: list[Market], balance: float,
                        base_bet: float, taker_fee: float,
                        max_depth: int) -> StrategyResult:
    """
    Мартингейл: удвоение ставки при проигрыше.
    Серия: ставим base_bet, при проигрыше следующий рынок — 2x.
    Серия заканчивается при выигрыше или достижении max_depth.
    """
    tracker = Tracker(balance)
    i = 0
    while i < len(markets):
        if not tracker.can_afford(bet_cost(base_bet, taker_fee)):
            break
        current_bet = base_bet
        depth = 0
        while depth < max_depth and i < len(markets):
            cost = bet_cost(current_bet, taker_fee)
            if not tracker.can_afford(cost):
                break
            m = markets[i]
            tracker.place_bet(current_bet, m.entry_price, m.final_price,
                              m.won, taker_fee)
            i += 1
            if m.won:
                break
            current_bet *= 2
            depth += 1
    return tracker.result("Мартингейл")


def strategy_flat(markets: list[Market], balance: float,
                  base_bet: float, taker_fee: float,
                  **_kwargs) -> StrategyResult:
    """Flat: фиксированная ставка на каждый рынок."""
    tracker = Tracker(balance)
    for m in markets:
        cost = bet_cost(base_bet, taker_fee)
        if not tracker.can_afford(cost):
            break
        tracker.place_bet(base_bet, m.entry_price, m.final_price,
                          m.won, taker_fee)
    return tracker.result("Flat")


def strategy_dalembert(markets: list[Market], balance: float,
                       base_bet: float, taker_fee: float,
                       max_depth: int) -> StrategyResult:
    """
    Д'Аламбер: линейный рост при проигрыше.
    Серия: начинаем с base_bet. Проигрыш → bet += base_bet.
    Выигрыш → bet = max(base_bet, bet - base_bet). Серия ≤ max_depth.
    """
    tracker = Tracker(balance)
    i = 0
    while i < len(markets):
        if not tracker.can_afford(bet_cost(base_bet, taker_fee)):
            break
        current_bet = base_bet
        depth = 0
        while depth < max_depth and i < len(markets):
            cost = bet_cost(current_bet, taker_fee)
            if not tracker.can_afford(cost):
                break
            m = markets[i]
            tracker.place_bet(current_bet, m.entry_price, m.final_price,
                              m.won, taker_fee)
            i += 1
            if m.won:
                break
            current_bet += base_bet
            depth += 1
    return tracker.result("Д'Аламбер")


def strategy_fibonacci(markets: list[Market], balance: float,
                       base_bet: float, taker_fee: float,
                       max_depth: int) -> StrategyResult:
    """
    Фибоначчи: ставки по последовательности 1,1,2,3,5,8... × base_bet.
    Серия: на проигрыш — следующее число Фибоначчи.
    Выигрыш или конец последовательности — серия завершена.
    """
    # Генерируем последовательность Фибоначчи нужной длины
    fib = [1, 1]
    for _ in range(max_depth):
        fib.append(fib[-1] + fib[-2])

    tracker = Tracker(balance)
    i = 0
    while i < len(markets):
        if not tracker.can_afford(bet_cost(base_bet * fib[0], taker_fee)):
            break
        fib_idx = 0
        while fib_idx < max_depth and i < len(markets):
            current_bet = base_bet * fib[fib_idx]
            cost = bet_cost(current_bet, taker_fee)
            if not tracker.can_afford(cost):
                break
            m = markets[i]
            tracker.place_bet(current_bet, m.entry_price, m.final_price,
                              m.won, taker_fee)
            i += 1
            if m.won:
                break
            fib_idx += 1
    return tracker.result("Фибоначчи")


def strategy_anti_martingale(markets: list[Market], balance: float,
                             base_bet: float, taker_fee: float,
                             max_streak: int = 3) -> StrategyResult:
    """
    Анти-Мартингейл (Парлей): удвоение при выигрыше.
    На выигрыш — удваиваем (до max_streak подряд). На проигрыш — сброс.
    Нет серий, каждый рынок независим (но ставка зависит от предыдущего результата).
    """
    tracker = Tracker(balance)
    current_bet = base_bet
    streak = 0
    for m in markets:
        cost = bet_cost(current_bet, taker_fee)
        if not tracker.can_afford(cost):
            current_bet = base_bet
            streak = 0
            cost = bet_cost(current_bet, taker_fee)
            if not tracker.can_afford(cost):
                break
        tracker.place_bet(current_bet, m.entry_price, m.final_price,
                          m.won, taker_fee)
        if m.won:
            streak += 1
            if streak < max_streak:
                current_bet *= 2
            else:
                # Достигли max_streak — фиксируем прибыль, сброс
                current_bet = base_bet
                streak = 0
        else:
            current_bet = base_bet
            streak = 0
    return tracker.result("Анти-Мартингейл")


def strategy_filtered_flat(markets: list[Market], balance: float,
                           base_bet: float, taker_fee: float,
                           max_entry_price: float = 0.40,
                           **_kwargs) -> StrategyResult:
    """Фильтрация + Flat: Flat только на рынках с entry_price <= 0.40."""
    tracker = Tracker(balance)
    for m in markets:
        if m.entry_price > max_entry_price:
            continue
        cost = bet_cost(base_bet, taker_fee)
        if not tracker.can_afford(cost):
            break
        tracker.place_bet(base_bet, m.entry_price, m.final_price,
                          m.won, taker_fee)
    return tracker.result("Фильтрация + Flat")


# ---------------------------------------------------------------------------
# Загрузка данных
# ---------------------------------------------------------------------------

def load_markets(path: str) -> list[Market]:
    """Загружает рынки из JSON-файла."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    markets = []
    for item in data:
        markets.append(Market(
            market_id=item["market_id"],
            question=item.get("question", ""),
            outcome=item.get("outcome", ""),
            token_id=item.get("token_id", ""),
            entry_price=item["entry_price"],
            final_price=item["final_price"],
            won=item["won"],
            volume_num=item.get("volume_num", 0.0),
            liquidity_num=item.get("liquidity_num", 0.0),
            end_date=item.get("end_date", ""),
        ))
    return markets


# ---------------------------------------------------------------------------
# Вывод результатов
# ---------------------------------------------------------------------------

def print_results(results: list[StrategyResult], console: Console):
    """Выводит таблицу сравнения стратегий через Rich."""
    table = Table(
        title="Сравнение стратегий ставок",
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
    )

    table.add_column("Стратегия", style="bold", min_width=20)
    table.add_column("Ставки", justify="right")
    table.add_column("Win%", justify="right")
    table.add_column("Баланс", justify="right")
    table.add_column("P&L", justify="right")
    table.add_column("ROI%", justify="right")
    table.add_column("Макс. просадка", justify="right")
    table.add_column("Макс. серия\nпроигрышей", justify="right")

    # Определяем лучшую стратегию по P&L для подсветки
    best_pnl = max(r.total_pnl for r in results)

    for r in results:
        pnl_color = "green" if r.total_pnl >= 0 else "red"
        roi_color = "green" if r.roi >= 0 else "red"
        is_best = r.total_pnl == best_pnl
        name = f"[bold green]★ {r.name}[/]" if is_best else r.name

        table.add_row(
            name,
            str(r.total_bets),
            f"{r.win_rate:.1f}%",
            f"${r.final_balance:.2f}",
            f"[{pnl_color}]${r.total_pnl:+.2f}[/]",
            f"[{roi_color}]{r.roi:+.1f}%[/]",
            f"${r.max_drawdown:.2f}",
            str(r.max_consecutive_losses),
        )

    console.print()
    console.print(table)
    console.print()


def print_summary(markets: list[Market], console: Console):
    """Краткая сводка по входным данным."""
    total = len(markets)
    won = sum(1 for m in markets if m.won)
    prices = [m.entry_price for m in markets]
    console.print(f"[bold]Загружено рынков:[/] {total}")
    console.print(
        f"[bold]Базовый win rate:[/] {won}/{total} "
        f"({won / total * 100:.1f}%)" if total > 0 else ""
    )
    if prices:
        console.print(
            f"[bold]Цены входа:[/] мин={min(prices):.3f}, "
            f"макс={max(prices):.3f}, среднее={sum(prices) / len(prices):.3f}"
        )
    console.print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Сравнение стратегий ставок на данных бэктеста Polymarket"
    )
    parser.add_argument(
        "--data",
        default="data/backtest_markets.json",
        help="Путь к JSON с рынками (default: data/backtest_markets.json)",
    )
    parser.add_argument(
        "--balance",
        type=float,
        default=100.0,
        help="Начальный баланс (default: 100)",
    )
    parser.add_argument(
        "--bet",
        type=float,
        default=1.0,
        help="Базовая ставка (default: 1.0)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=6,
        help="Макс. глубина для серийных стратегий (default: 6)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed для перемешивания рынков (default: случайный)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    console = Console()

    taker_fee = 0.02
    max_streak = 3  # Для анти-Мартингейла

    # Загрузка и перемешивание
    data_path = Path(args.data)
    if not data_path.is_absolute():
        data_path = Path(__file__).resolve().parent.parent / data_path

    console.print(f"[bold]Файл данных:[/] {data_path}")
    markets = load_markets(str(data_path))

    seed = args.seed if args.seed is not None else random.randint(0, 999999)
    random.seed(seed)
    random.shuffle(markets)

    console.print(
        f"[bold]Параметры:[/] баланс=${args.balance}, ставка=${args.bet}, "
        f"глубина={args.depth}, seed={seed}, taker_fee={taker_fee}"
    )
    print_summary(markets, console)

    # Прогон стратегий
    results = [
        strategy_martingale(markets, args.balance, args.bet, taker_fee,
                            args.depth),
        strategy_flat(markets, args.balance, args.bet, taker_fee),
        strategy_dalembert(markets, args.balance, args.bet, taker_fee,
                           args.depth),
        strategy_fibonacci(markets, args.balance, args.bet, taker_fee,
                           args.depth),
        strategy_anti_martingale(markets, args.balance, args.bet, taker_fee,
                                 max_streak),
        strategy_filtered_flat(markets, args.balance, args.bet, taker_fee),
    ]

    print_results(results, console)


if __name__ == "__main__":
    main()
