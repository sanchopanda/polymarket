"""
research_bot/backtest_db.py

Единая SQLite база для данных бэктеста.
Используется ботом (live запись), скриптами загрузки и бэктестом (чтение).

DB path: data/backtest.db
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Optional

DB_PATH = Path("data/backtest.db")


def get_connection(path: Optional[Path] = None) -> sqlite3.Connection:
    p = path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS binance_1s (
            symbol  TEXT    NOT NULL,
            sec_ts  INTEGER NOT NULL,
            close   REAL    NOT NULL,
            PRIMARY KEY (symbol, sec_ts)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS pm_trades (
            market_id TEXT    NOT NULL,
            ts        INTEGER NOT NULL,
            outcome   TEXT    NOT NULL,
            price     REAL    NOT NULL,
            size      REAL
        );
        CREATE INDEX IF NOT EXISTS idx_pm_trades_market
            ON pm_trades(market_id, ts);

        CREATE TABLE IF NOT EXISTS binance_5s (
            symbol    TEXT    NOT NULL,
            bucket_ts INTEGER NOT NULL,
            close     REAL    NOT NULL,
            PRIMARY KEY (symbol, bucket_ts)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS markets (
            market_id        TEXT PRIMARY KEY,
            condition_id     TEXT,
            symbol           TEXT    NOT NULL,
            interval_minutes INTEGER NOT NULL,
            market_start     TEXT    NOT NULL,
            market_end        TEXT    NOT NULL,
            winning_side     TEXT
        );
    """)
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(pm_trades)").fetchall()}
    if "size" not in cols:
        conn.execute("ALTER TABLE pm_trades ADD COLUMN size REAL")


class BacktestDB:
    """Thread-safe writer for live data collection."""

    def __init__(self, path: Optional[Path] = None) -> None:
        self._conn = get_connection(path)
        self._lock = threading.Lock()
        self._batch_1s: list[tuple] = []
        self._batch_trades: list[tuple] = []

    # ── Binance 5s (exact bucket closes from bot) ──────────────────

    def write_5s(self, symbol: str, bucket_ts: int, close: float) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO binance_5s (symbol, bucket_ts, close) VALUES (?,?,?)",
                (symbol, bucket_ts, close),
            )
            self._conn.commit()

    # ── Binance 1s ────────────────────────────────────────────────────

    def write_1s(self, symbol: str, sec_ts: int, close: float) -> None:
        with self._lock:
            self._batch_1s.append((symbol, sec_ts, close))
            if len(self._batch_1s) >= 100:
                self._flush_1s()

    def _flush_1s(self) -> None:
        if not self._batch_1s:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO binance_1s (symbol, sec_ts, close) VALUES (?,?,?)",
            self._batch_1s,
        )
        self._conn.commit()
        self._batch_1s.clear()

    # ── PM trades ─────────────────────────────────────────────────────

    def write_trade(
        self,
        market_id: str,
        ts: int,
        outcome: str,
        price: float,
        size: float | None = None,
    ) -> None:
        with self._lock:
            self._batch_trades.append((market_id, ts, outcome, price, size))
            if len(self._batch_trades) >= 50:
                self._flush_trades()

    def _flush_trades(self) -> None:
        if not self._batch_trades:
            return
        self._conn.executemany(
            "INSERT INTO pm_trades (market_id, ts, outcome, price, size) VALUES (?,?,?,?,?)",
            self._batch_trades,
        )
        self._conn.commit()
        self._batch_trades.clear()

    # ── Markets ───────────────────────────────────────────────────────

    def write_market(self, market_id: str, condition_id: str, symbol: str,
                     interval_minutes: int, market_start: str, market_end: str,
                     winning_side: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO markets
                   (market_id, condition_id, symbol, interval_minutes,
                    market_start, market_end, winning_side)
                   VALUES (?,?,?,?,?,?,?)""",
                (market_id, condition_id, symbol, interval_minutes,
                 market_start, market_end, winning_side),
            )
            if winning_side is not None:
                self._conn.execute(
                    "UPDATE markets SET winning_side=? WHERE market_id=? AND winning_side IS NULL",
                    (winning_side, market_id),
                )
            self._conn.commit()

    def get_unresolved_markets(self, before_ts: int) -> list[dict]:
        """Рынки без winning_side у которых market_end <= before_ts."""
        before_str = __import__("datetime").datetime.utcfromtimestamp(before_ts).strftime("%Y-%m-%d %H:%M:%S")
        rows = self._conn.execute(
            """SELECT market_id, symbol, interval_minutes, market_start, market_end
               FROM markets
               WHERE winning_side IS NULL AND market_end <= ?""",
            (before_str,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Flush ─────────────────────────────────────────────────────────

    def flush(self) -> None:
        with self._lock:
            self._flush_1s()
            self._flush_trades()

    def close(self) -> None:
        self.flush()
        self._conn.close()


# ── Read helpers (for backtest) ───────────────────────────────────────────

def load_markets(conn: sqlite3.Connection, symbols: Optional[list[str]] = None,
                 intervals: Optional[list[int]] = None) -> list[dict]:
    query = "SELECT * FROM markets WHERE winning_side IS NOT NULL"
    params: list = []
    if symbols:
        query += f" AND symbol IN ({','.join('?' for _ in symbols)})"
        params.extend(symbols)
    if intervals:
        query += f" AND interval_minutes IN ({','.join('?' for _ in intervals)})"
        params.extend(intervals)
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def load_5s(conn: sqlite3.Connection, symbol: str,
            start_ts: int, end_ts: int) -> dict[int, float]:
    """Load exact 5s bucket closes saved by bot."""
    rows = conn.execute(
        "SELECT bucket_ts, close FROM binance_5s WHERE symbol=? AND bucket_ts BETWEEN ? AND ?",
        (symbol, start_ts, end_ts),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def load_1s(conn: sqlite3.Connection, symbol: str,
            start_ts: int, end_ts: int) -> dict[int, float]:
    rows = conn.execute(
        "SELECT sec_ts, close FROM binance_1s WHERE symbol=? AND sec_ts BETWEEN ? AND ?",
        (symbol, start_ts, end_ts),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def load_trades(conn: sqlite3.Connection, market_id: str) -> dict[str, list[tuple[int, float]]]:
    rows = conn.execute(
        "SELECT ts, outcome, price FROM pm_trades WHERE market_id=? ORDER BY ts",
        (market_id,),
    ).fetchall()
    result: dict[str, list[tuple[int, float]]] = {"Up": [], "Down": []}
    for r in rows:
        if r[1] in result:
            result[r[1]].append((r[0], r[2]))
    return result


def has_trades(conn: sqlite3.Connection, market_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pm_trades WHERE market_id=? LIMIT 1", (market_id,)
    ).fetchone()
    return row is not None
