from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime

from jump_paper_bot.models import JumpPosition, JumpSignalRecord


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


class JumpPaperDB:
    def __init__(self, path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    title TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    signal_bucket_seconds INTEGER NOT NULL,
                    signal_level REAL NOT NULL,
                    signal_price REAL NOT NULL,
                    signal_avg_prev_10s REAL NOT NULL,
                    limit_price REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    filled_shares REAL NOT NULL,
                    total_cost REAL NOT NULL,
                    depth_usd REAL NOT NULL,
                    opened_at TEXT NOT NULL,
                    market_end TEXT NOT NULL,
                    status TEXT NOT NULL,
                    winning_side TEXT,
                    pnl REAL,
                    telegram_message_id INTEGER
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    signal_bucket_seconds INTEGER NOT NULL,
                    signal_level REAL NOT NULL,
                    signal_price REAL NOT NULL,
                    signal_avg_prev_10s REAL NOT NULL,
                    limit_price REAL NOT NULL,
                    status TEXT NOT NULL,
                    skip_reason TEXT,
                    position_id TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(market_id, side, signal_bucket_seconds)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    price REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status, opened_at)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signals_market ON signals(market_id, side, signal_bucket_seconds)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_history_market ON price_history(market_id, side, ts)"
            )
            self._conn.commit()

    def insert_price_point(self, market_id: str, side: str, ts: datetime, price: float) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO price_history (market_id, side, ts, price) VALUES (?, ?, ?, ?)",
                (market_id, side, _iso(ts), float(price)),
            )
            self._conn.commit()

    def try_record_signal(
        self,
        *,
        market_id: str,
        symbol: str,
        interval_minutes: int,
        side: str,
        signal_bucket_seconds: int,
        signal_level: float,
        signal_price: float,
        signal_avg_prev_10s: float,
        limit_price: float,
        status: str,
        skip_reason: str | None = None,
        position_id: str | None = None,
    ) -> int | None:
        created_at = datetime.utcnow()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT OR IGNORE INTO signals (
                    market_id, symbol, interval_minutes, side, signal_bucket_seconds,
                    signal_level, signal_price, signal_avg_prev_10s, limit_price,
                    status, skip_reason, position_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market_id,
                    symbol,
                    interval_minutes,
                    side,
                    signal_bucket_seconds,
                    signal_level,
                    signal_price,
                    signal_avg_prev_10s,
                    limit_price,
                    status,
                    skip_reason,
                    position_id,
                    _iso(created_at),
                ),
            )
            self._conn.commit()
            if cur.rowcount == 0:
                return None
            return int(cur.lastrowid)

    def open_position(
        self,
        *,
        market_id: str,
        symbol: str,
        title: str,
        interval_minutes: int,
        side: str,
        signal_bucket_seconds: int,
        signal_level: float,
        signal_price: float,
        signal_avg_prev_10s: float,
        limit_price: float,
        entry_price: float,
        filled_shares: float,
        total_cost: float,
        depth_usd: float,
        market_end: datetime,
    ) -> JumpPosition:
        pos_id = str(uuid.uuid4())
        opened_at = datetime.utcnow()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO positions (
                    id, market_id, symbol, title, interval_minutes, side,
                    signal_bucket_seconds, signal_level, signal_price, signal_avg_prev_10s,
                    limit_price, entry_price, filled_shares, total_cost, depth_usd,
                    opened_at, market_end, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
                """,
                (
                    pos_id,
                    market_id,
                    symbol,
                    title,
                    interval_minutes,
                    side,
                    signal_bucket_seconds,
                    signal_level,
                    signal_price,
                    signal_avg_prev_10s,
                    limit_price,
                    entry_price,
                    filled_shares,
                    total_cost,
                    depth_usd,
                    _iso(opened_at),
                    _iso(market_end),
                ),
            )
            self._conn.commit()
        return JumpPosition(
            id=pos_id,
            market_id=market_id,
            symbol=symbol,
            title=title,
            interval_minutes=interval_minutes,
            side=side,
            signal_bucket_seconds=signal_bucket_seconds,
            signal_level=signal_level,
            signal_price=signal_price,
            signal_avg_prev_10s=signal_avg_prev_10s,
            limit_price=limit_price,
            entry_price=entry_price,
            filled_shares=filled_shares,
            total_cost=total_cost,
            depth_usd=depth_usd,
            opened_at=opened_at,
            market_end=market_end,
            status="open",
        )

    def set_open_message_id(self, position_id: str, message_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE positions SET telegram_message_id=? WHERE id=?",
                (int(message_id), position_id),
            )
            self._conn.commit()

    def resolve_position(self, position_id: str, winning_side: str, pnl: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE positions
                SET status='resolved', winning_side=?, pnl=?
                WHERE id=?
                """,
                (winning_side, float(pnl), position_id),
            )
            self._conn.commit()

    def attach_signal_position(self, signal_id: int, position_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE signals SET position_id=? WHERE id=?",
                (position_id, int(signal_id)),
            )
            self._conn.commit()

    def get_open_positions(self) -> list[JumpPosition]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM positions WHERE status='open' ORDER BY opened_at DESC"
            ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def get_recent_positions(self, limit: int = 20) -> list[JumpPosition]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM positions ORDER BY opened_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def stats(self) -> dict:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_count,
                    SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                    SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                    SUM(CASE WHEN status='resolved' AND pnl <= 0 THEN 1 ELSE 0 END) AS lost_count,
                    COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS realized_pnl,
                    COUNT(*) AS total_count
                FROM positions
                """
            ).fetchone()
            signal_row = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS total_signals,
                    SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS opened_signals,
                    SUM(CASE WHEN status!='open' THEN 1 ELSE 0 END) AS skipped_signals
                FROM signals
                """
            ).fetchone()
        return {
            "open_count": int(row["open_count"] or 0),
            "resolved_count": int(row["resolved_count"] or 0),
            "won_count": int(row["won_count"] or 0),
            "lost_count": int(row["lost_count"] or 0),
            "realized_pnl": float(row["realized_pnl"] or 0.0),
            "total_count": int(row["total_count"] or 0),
            "total_signals": int(signal_row["total_signals"] or 0),
            "opened_signals": int(signal_row["opened_signals"] or 0),
            "skipped_signals": int(signal_row["skipped_signals"] or 0),
        }

    def breakdown_by_symbol(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT
                    symbol,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                    SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                    COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS pnl
                FROM positions
                GROUP BY symbol
                ORDER BY symbol
                """
            ).fetchall()

    def breakdown_by_bucket(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT
                    signal_bucket_seconds,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                    SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                    COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS pnl
                FROM positions
                GROUP BY signal_bucket_seconds
                ORDER BY signal_bucket_seconds DESC
                """
            ).fetchall()

    def get_signal(self, market_id: str, side: str, signal_bucket_seconds: int) -> JumpSignalRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM signals
                WHERE market_id=? AND side=? AND signal_bucket_seconds=?
                """,
                (market_id, side, signal_bucket_seconds),
            ).fetchone()
        if row is None:
            return None
        return JumpSignalRecord(
            id=int(row["id"]),
            market_id=row["market_id"],
            symbol=row["symbol"],
            interval_minutes=int(row["interval_minutes"]),
            side=row["side"],
            signal_bucket_seconds=int(row["signal_bucket_seconds"]),
            signal_level=float(row["signal_level"]),
            signal_price=float(row["signal_price"]),
            signal_avg_prev_10s=float(row["signal_avg_prev_10s"]),
            limit_price=float(row["limit_price"]),
            status=row["status"],
            skip_reason=row["skip_reason"],
            created_at=_dt(row["created_at"]) or datetime.utcnow(),
            position_id=row["position_id"],
        )

    def _row_to_position(self, row: sqlite3.Row) -> JumpPosition:
        return JumpPosition(
            id=row["id"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            title=row["title"],
            interval_minutes=int(row["interval_minutes"]),
            side=row["side"],
            signal_bucket_seconds=int(row["signal_bucket_seconds"]),
            signal_level=float(row["signal_level"]),
            signal_price=float(row["signal_price"]),
            signal_avg_prev_10s=float(row["signal_avg_prev_10s"]),
            limit_price=float(row["limit_price"]),
            entry_price=float(row["entry_price"]),
            filled_shares=float(row["filled_shares"]),
            total_cost=float(row["total_cost"]),
            depth_usd=float(row["depth_usd"]),
            opened_at=_dt(row["opened_at"]) or datetime.utcnow(),
            market_end=_dt(row["market_end"]) or datetime.utcnow(),
            status=row["status"],
            winning_side=row["winning_side"],
            pnl=float(row["pnl"]) if row["pnl"] is not None else None,
            telegram_message_id=int(row["telegram_message_id"]) if row["telegram_message_id"] is not None else None,
        )
