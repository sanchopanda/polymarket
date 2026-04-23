"""
Миграция CSV данных бэктеста в SQLite.

python3 -m research_bot.migrate_to_db
"""
from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path

from research_bot.backtest_db import get_connection, DB_PATH

DATA_DIR = Path("research_bot/data")
LIVE_DIR = DATA_DIR / "live"


def migrate_markets(conn):
    """Import markets_cache.csv + live/markets_resolved.csv."""
    total = 0
    for path in [DATA_DIR / "markets_cache.csv", LIVE_DIR / "markets_resolved.csv"]:
        if not path.exists():
            continue
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            batch = []
            for r in reader:
                if not r.get("market_id"):
                    continue
                batch.append((
                    r["market_id"],
                    r.get("condition_id", ""),
                    r["symbol"],
                    int(r["interval_minutes"]),
                    r["market_start"],
                    r["market_end"],
                    r.get("winning_side") or None,
                ))
            conn.executemany(
                """INSERT OR REPLACE INTO markets
                   (market_id, condition_id, symbol, interval_minutes,
                    market_start, market_end, winning_side)
                   VALUES (?,?,?,?,?,?,?)""",
                batch,
            )
            conn.commit()
            total += len(batch)
            print(f"  markets from {path.name}: {len(batch)}")
    return total


def migrate_binance_1s(conn):
    """Import binance_1s_*.csv from both data/ and data/live/."""
    total = 0
    for directory in [DATA_DIR, LIVE_DIR]:
        for path in sorted(directory.glob("binance_1s_*.csv")):
            sym = path.stem.replace("binance_1s_", "")
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                batch = []
                for r in reader:
                    try:
                        batch.append((sym, int(r["sec_ts"]), float(r["close"])))
                    except (ValueError, KeyError):
                        continue
                    if len(batch) >= 10000:
                        conn.executemany(
                            "INSERT OR IGNORE INTO binance_1s (symbol, sec_ts, close) VALUES (?,?,?)",
                            batch,
                        )
                        conn.commit()
                        total += len(batch)
                        batch.clear()
                if batch:
                    conn.executemany(
                        "INSERT OR IGNORE INTO binance_1s (symbol, sec_ts, close) VALUES (?,?,?)",
                        batch,
                    )
                    conn.commit()
                    total += len(batch)
            count = conn.execute(
                "SELECT COUNT(*) FROM binance_1s WHERE symbol=?", (sym,)
            ).fetchone()[0]
            print(f"  binance_1s {sym} from {directory.name}/: total in DB = {count}")
    return total


def migrate_trades(conn):
    """Import trades/*.csv from both data/ and data/live/."""
    total = 0
    for directory in [DATA_DIR / "trades", LIVE_DIR / "trades"]:
        if not directory.exists():
            continue
        files = list(directory.glob("*.csv"))
        print(f"  trades from {directory}: {len(files)} files...", end=" ", flush=True)
        t0 = time.time()
        for path in files:
            market_id = path.stem
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                batch = []
                for r in reader:
                    try:
                        raw_size = r.get("size")
                        size = float(raw_size) if raw_size not in (None, "") else None
                        batch.append((market_id, int(r["timestamp"]), r["outcome"], float(r["price"]), size))
                    except (ValueError, KeyError):
                        continue
                if batch:
                    conn.executemany(
                        "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
                        batch,
                    )
                    total += len(batch)
        conn.commit()
        print(f"{total} rows, {time.time()-t0:.1f}s")
    return total


def main():
    print(f"Миграция в {DB_PATH}")
    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / 1024 / 1024
        print(f"  DB уже существует ({size_mb:.1f} MB), дополняем")

    conn = get_connection()

    print("\n1. Markets")
    migrate_markets(conn)

    print("\n2. Binance 1s")
    migrate_binance_1s(conn)

    print("\n3. PM trades")
    migrate_trades(conn)

    # Stats
    print("\n=== Итого в DB ===")
    for table in ["markets", "binance_1s", "pm_trades"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    conn.close()
    size_mb = DB_PATH.stat().st_size / 1024 / 1024
    print(f"\n  Размер: {size_mb:.1f} MB")
    print("Готово!")


if __name__ == "__main__":
    main()
