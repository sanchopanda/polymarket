"""
scripts/fix_real_pnl.py

Ретроспективный фикс real позиций recovery_bot. Проблемы:
  1) entry_price у real был записан как лимит (0.70), а реально fill шёл дешевле.
  2) redeem(market_id) мог забрать чужие winning shares (параллельные боты)
     → pnl раздут.

Решение:
  - entry_price берём у парной paper позиции (тот же market_id+strategy+side)
    — paper заполнялся проходом по реальному стакану, это хороший proxy.
  - total_cost пересчитываем: filled_shares * paper_entry + fee.
  - winner our_payout = min(chain_payout_старый, filled_shares)  (cap $1/share).
  - loser  pnl = -new_total_cost.
  - real_deposit.balance корректируем на сумму delta.

Запуск:
    python3 scripts/fix_real_pnl.py --dry   # только показать
    python3 scripts/fix_real_pnl.py         # применить
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone

DB = "data/recovery_bot.db"


def pm_fee(shares: float, price: float) -> float:
    if shares <= 0 or price <= 0 or price >= 1:
        return 0.0
    return shares * price * 0.25 * ((price * (1 - price)) ** 2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true", help="не применять, только показать")
    parser.add_argument("--db", default=DB)
    opts = parser.parse_args()

    conn = sqlite3.connect(opts.db)
    conn.row_factory = sqlite3.Row

    real_rows = conn.execute(
        "SELECT id, market_id, strategy_name, side, filled_shares, total_cost, "
        "entry_price, fee, pnl, winning_side, symbol, interval_minutes "
        "FROM positions WHERE mode='real' AND status='resolved'"
    ).fetchall()

    paper_rows = conn.execute(
        "SELECT market_id, strategy_name, side, entry_price FROM positions WHERE mode='paper'"
    ).fetchall()
    paper_by_key: dict[tuple, float] = {
        (r["market_id"], r["strategy_name"], r["side"]): float(r["entry_price"]) for r in paper_rows
    }

    header = (
        f"{'sym':>5} {'int':>3} {'side':>4} {'shares':>7}"
        f" {'paper':>7} {'old_cost':>8} {'new_cost':>8}"
        f" {'old_pnl':>10} {'new_pnl':>10} {'delta':>10}"
    )
    print(header)
    print("-" * len(header))

    fixes: list[tuple[str, float, float, float, float]] = []
    total_delta = 0.0
    skipped_no_paper = 0

    for r in real_rows:
        key = (r["market_id"], r["strategy_name"], r["side"])
        paper_entry = paper_by_key.get(key)
        shares = float(r["filled_shares"])
        old_cost = float(r["total_cost"])
        old_pnl = float(r["pnl"] or 0.0)
        old_entry = float(r["entry_price"])
        old_fee = float(r["fee"] or 0.0)

        if paper_entry is None:
            skipped_no_paper += 1
            print(
                f"{r['symbol']:>5} {r['interval_minutes']:>3} {r['side']:>4}"
                f" {shares:>7.3f} {'—':>7} {old_cost:>8.4f} {'(нет paper)':>28}"
            )
            continue

        new_fee = pm_fee(shares, paper_entry)
        new_cost = shares * paper_entry + new_fee

        if r["winning_side"] == r["side"]:
            old_chain_payout = old_pnl + old_cost
            our_payout = min(old_chain_payout, shares)  # cap на наши доли
            new_pnl = our_payout - new_cost
        else:
            new_pnl = -new_cost

        # Изменение баланса: было старое (pnl_old, cost_old), станет (pnl_new, cost_new).
        # В балансе старое отразилось как: -old_cost + old_payout (если winner) + refund(=0).
        # Новое: -new_cost + new_payout. Но balance уже содержит старое, поэтому
        # delta = (new_cost - old_cost: доп. возврат/списание) + (new_payout - old_payout).
        # Проще: delta балансa = (new_pnl - old_pnl) + (old_cost - new_cost)
        #        = new_payout - old_payout (что верно для любой ветки).
        delta_bal = (new_pnl - old_pnl) + (old_cost - new_cost)
        total_delta += delta_bal

        fixes.append((r["id"], paper_entry, new_cost, new_fee, new_pnl))
        mark = " <<<" if abs(delta_bal) > 1e-6 else ""
        print(
            f"{r['symbol']:>5} {r['interval_minutes']:>3} {r['side']:>4}"
            f" {shares:>7.3f} {paper_entry:>7.4f} {old_cost:>8.4f} {new_cost:>8.4f}"
            f" {old_pnl:>+10.4f} {new_pnl:>+10.4f} {delta_bal:>+10.4f}{mark}"
        )

    print(
        f"\nПозиций real resolved: {len(real_rows)} | с парной paper: {len(fixes)}"
        f" | без paper: {skipped_no_paper}"
    )
    print(f"Суммарная коррекция баланса: ${total_delta:+.4f}")

    bal_row = conn.execute("SELECT balance, peak FROM real_deposit WHERE id=1").fetchone()
    if bal_row is None:
        print("real_deposit не инициализирован.")
        return
    old_balance = float(bal_row["balance"])
    old_peak = float(bal_row["peak"])
    new_balance = round(old_balance + total_delta, 6)
    new_peak = max(new_balance, old_peak + total_delta)
    print(f"real_deposit.balance: ${old_balance:.4f} → ${new_balance:.4f}")
    print(f"real_deposit.peak:    ${old_peak:.4f} → ${new_peak:.4f}")

    if opts.dry:
        print("\nDRY RUN — изменения не применены.")
        return
    if not fixes:
        print("\nНечего править.")
        return

    for pid, new_entry, new_cost, new_fee, new_pnl in fixes:
        conn.execute(
            "UPDATE positions SET entry_price=?, total_cost=?, fee=?, pnl=? WHERE id=?",
            (new_entry, new_cost, new_fee, new_pnl, pid),
        )
    conn.execute(
        "UPDATE real_deposit SET balance=?, peak=?, updated_at=? WHERE id=1",
        (new_balance, new_peak, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    print("\nПрименено.")


if __name__ == "__main__":
    main()
