"""Симуляция роста депозита recovery_bot на BTC-бакете.

Модель:
- stake = clamp(balance / N, floor=$1, cap=$100)
- pnl трейда = stake * pnl_per_dollar (линейное масштабирование)
- 2 варианта источника pnl_per_dollar:
    * bootstrap — рандомный sample из реальной серии (n=275, BTC resolved)
    * parametric — Bernoulli(WR), на win +avg_win, на loss +avg_loss

Частота — ~78 BTC-сделок/день (3.27/ч × 24); 30 дней = 2340 сделок.
N=1000 Монте-Карло на конфиг.

Использование:
    python3 scripts/simulate_recovery_growth.py
"""

import random
import sqlite3
import statistics
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "recovery_bot.db"

INITIAL_BALANCE = 100.0
STAKE_FLOOR = 1.0
STAKE_CAP = 100.0
TRADES_PER_DAY = 78
DAYS = 60
N_SIMS = 1000
N_VALUES = [20, 30, 50]


def load_btc_pnl_per_dollar() -> list[float]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT pnl, total_cost FROM positions "
        "WHERE mode='real' AND status='resolved' AND symbol='BTC' AND total_cost>0"
    )
    units = [pnl / cost for pnl, cost in cur.fetchall()]
    con.close()
    return units


def empirical_params(units: list[float]) -> tuple[float, float, float]:
    wins = [u for u in units if u > 0]
    losses = [u for u in units if u <= 0]
    wr = len(wins) / len(units)
    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses)
    return wr, avg_win, avg_loss


def sim_one(
    units: list[float],
    model: str,
    n_denom: int,
    wr: float,
    avg_win: float,
    avg_loss: float,
    n_trades: int,
    rng: random.Random,
) -> dict:
    """Возвращает dict с балансом на контрольных точках + TTC + DD + bankrupt."""
    balance = INITIAL_BALANCE
    trades_to_cap = -1
    peak = balance
    max_dd = 0.0
    bankrupt = False
    bal_30d = None

    trades_at_30d = TRADES_PER_DAY * 30
    for t in range(n_trades):
        if t == trades_at_30d:
            bal_30d = balance
        if balance < STAKE_FLOOR:
            bankrupt = True
            if bal_30d is None and t >= trades_at_30d:
                bal_30d = balance
            break

        raw = balance / n_denom
        stake = max(STAKE_FLOOR, min(STAKE_CAP, raw))

        if model == "bootstrap":
            u = rng.choice(units)
        else:
            u = avg_win if rng.random() < wr else avg_loss

        balance += stake * u
        if stake >= STAKE_CAP and trades_to_cap == -1:
            trades_to_cap = t + 1
        peak = max(peak, balance)
        dd = peak - balance
        if dd > max_dd:
            max_dd = dd

    if bal_30d is None:
        bal_30d = balance
    return {
        "final": balance,
        "bal_30d": bal_30d,
        "trades_to_cap": trades_to_cap,
        "max_dd": max_dd,
        "bankrupt": bankrupt,
    }


def run_config(
    units: list[float],
    model: str,
    n_denom: int,
    wr: float,
    avg_win: float,
    avg_loss: float,
) -> dict:
    rng = random.Random(42 + n_denom + (0 if model == "bootstrap" else 1000))
    n_trades = TRADES_PER_DAY * DAYS
    bal_30d_list: list[float] = []
    bal_60d_list: list[float] = []
    m2_profit_list: list[float] = []
    times_to_cap: list[int] = []
    bankrupts = 0

    for _ in range(N_SIMS):
        r = sim_one(units, model, n_denom, wr, avg_win, avg_loss, n_trades, rng)
        bal_30d_list.append(r["bal_30d"])
        bal_60d_list.append(r["final"])
        m2_profit_list.append(r["final"] - r["bal_30d"])
        if r["trades_to_cap"] > 0:
            times_to_cap.append(r["trades_to_cap"])
        if r["bankrupt"]:
            bankrupts += 1

    def pct(arr: list[float], p: float) -> float:
        s = sorted(arr)
        return s[min(int(p * len(s)), len(s) - 1)]

    med_ttc_days = (
        statistics.median(times_to_cap) / TRADES_PER_DAY if times_to_cap else None
    )
    return {
        "model": model,
        "N": n_denom,
        "m1_p5": pct(bal_30d_list, 0.05),
        "m1_p50": pct(bal_30d_list, 0.50),
        "m1_p95": pct(bal_30d_list, 0.95),
        "m1_mean": sum(bal_30d_list) / len(bal_30d_list),
        "m2_p5": pct(m2_profit_list, 0.05),
        "m2_p50": pct(m2_profit_list, 0.50),
        "m2_p95": pct(m2_profit_list, 0.95),
        "m2_mean": sum(m2_profit_list) / len(m2_profit_list),
        "cap_reach_rate": len(times_to_cap) / N_SIMS,
        "med_ttc_days": med_ttc_days,
        "bankrupt_rate": bankrupts / N_SIMS,
    }


def fmt(r: dict) -> str:
    ttc = f"{r['med_ttc_days']:.1f}d" if r["med_ttc_days"] is not None else "—"
    return (
        f"{r['model']:<10} N={r['N']:<3} "
        f"M1: p50=${r['m1_p50']:>8.2f} (p5=${r['m1_p5']:>7.2f}, p95=${r['m1_p95']:>8.2f}) | "
        f"M2 profit: p50=${r['m2_p50']:>8.2f} (p5=${r['m2_p5']:>8.2f}, p95=${r['m2_p95']:>8.2f}) | "
        f"TTC={ttc:<6} cap={r['cap_reach_rate']*100:>4.1f}% bust={r['bankrupt_rate']*100:.1f}%"
    )


def main() -> None:
    units = load_btc_pnl_per_dollar()
    wr, avg_win, avg_loss = empirical_params(units)

    print(f"BTC sample: n={len(units)}, mean_pnl/$1={sum(units)/len(units):+.4f}")
    print(f"WR={wr:.3f}, avg_win/$1={avg_win:+.4f}, avg_loss/$1={avg_loss:+.4f}")
    print(
        f"Sim: ${INITIAL_BALANCE:.0f} -> (stake=balance/N, floor ${STAKE_FLOOR:.0f}, "
        f"cap ${STAKE_CAP:.0f}), {TRADES_PER_DAY}/day × {DAYS}d = "
        f"{TRADES_PER_DAY*DAYS} trades, N_SIMS={N_SIMS}"
    )
    print()
    header = (
        "model      N    p5         p50        p95        mean       "
        "cap    med_ttc  med_dd   bust"
    )
    print(header)
    print("-" * len(header))
    for model in ("bootstrap", "parametric"):
        for n_denom in N_VALUES:
            res = run_config(units, model, n_denom, wr, avg_win, avg_loss)
            print(fmt(res))
        print()


if __name__ == "__main__":
    main()
