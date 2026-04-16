from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

from recovery_bot.models import RecoveryPosition


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


class RecoveryDB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                title TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL,
                market_start TEXT NOT NULL,
                market_end TEXT NOT NULL,
                side TEXT NOT NULL,
                mode TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                touch_ts TEXT NOT NULL,
                armed_ts TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                touch_price REAL NOT NULL,
                trigger_price REAL NOT NULL,
                entry_price REAL NOT NULL,
                requested_shares REAL NOT NULL,
                filled_shares REAL NOT NULL,
                total_cost REAL NOT NULL,
                fee REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                pm_token_id TEXT,
                pm_order_id TEXT,
                note TEXT,
                resolved_at TEXT,
                winning_side TEXT,
                pnl REAL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS real_deposit (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                balance REAL NOT NULL,
                peak REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def has_market_record(self, market_id: str, strategy_name: str, mode: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM positions WHERE market_id=? AND strategy_name=? AND mode=? LIMIT 1",
            (market_id, strategy_name, mode),
        ).fetchone()
        return row is not None

    def open_position(
        self,
        *,
        market_id: str,
        symbol: str,
        title: str,
        interval_minutes: int,
        market_start: datetime,
        market_end: datetime,
        side: str,
        mode: str,
        strategy_name: str,
        touch_ts: datetime,
        armed_ts: datetime,
        touch_price: float,
        trigger_price: float,
        entry_price: float,
        requested_shares: float,
        filled_shares: float,
        total_cost: float,
        fee: float,
        status: str,
        pm_token_id: str | None = None,
        pm_order_id: str | None = None,
        note: str | None = None,
    ) -> RecoveryPosition:
        pos_id = str(uuid.uuid4())
        opened_at = datetime.utcnow()
        self._conn.execute(
            """
            INSERT INTO positions (
                id, market_id, symbol, title, interval_minutes, market_start, market_end,
                side, mode, strategy_name, touch_ts, armed_ts, opened_at,
                touch_price, trigger_price, entry_price, requested_shares, filled_shares,
                total_cost, fee, status, pm_token_id, pm_order_id, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pos_id, market_id, symbol, title, interval_minutes, _iso(market_start), _iso(market_end),
                side, mode, strategy_name, _iso(touch_ts), _iso(armed_ts), _iso(opened_at),
                touch_price, trigger_price, entry_price, requested_shares, filled_shares,
                total_cost, fee, status, pm_token_id, pm_order_id, note,
            ),
        )
        self._conn.commit()
        return RecoveryPosition(
            id=pos_id,
            market_id=market_id,
            symbol=symbol,
            title=title,
            interval_minutes=interval_minutes,
            market_start=market_start,
            market_end=market_end,
            side=side,
            mode=mode,
            strategy_name=strategy_name,
            touch_ts=touch_ts,
            armed_ts=armed_ts,
            opened_at=opened_at,
            touch_price=touch_price,
            trigger_price=trigger_price,
            entry_price=entry_price,
            requested_shares=requested_shares,
            filled_shares=filled_shares,
            total_cost=total_cost,
            fee=fee,
            status=status,
            pm_token_id=pm_token_id,
            pm_order_id=pm_order_id,
            note=note,
        )

    def get_open_positions(self) -> list[RecoveryPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions WHERE status='open' ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def get_working_positions(self) -> list[RecoveryPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions WHERE status='working' ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def get_recent_positions(self, limit: int = 20) -> list[RecoveryPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def get_all_positions(self) -> list[RecoveryPosition]:
        rows = self._conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_position(row) for row in rows]

    def resolve_position(self, position_id: str, winning_side: str, pnl: float) -> None:
        self._conn.execute(
            """
            UPDATE positions
            SET status='resolved', resolved_at=?, winning_side=?, pnl=?
            WHERE id=?
            """,
            (_iso(datetime.utcnow()), winning_side, pnl, position_id),
        )
        self._conn.commit()

    def mark_position_open(
        self,
        position_id: str,
        *,
        entry_price: float,
        filled_shares: float,
        total_cost: float,
        fee: float,
        note: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            UPDATE positions
            SET status='open', entry_price=?, filled_shares=?, total_cost=?, fee=?, note=?
            WHERE id=?
            """,
            (entry_price, filled_shares, total_cost, fee, note, position_id),
        )
        self._conn.commit()

    def mark_position_unfilled(self, position_id: str, note: str | None = None) -> None:
        self._conn.execute(
            "UPDATE positions SET status='unfilled', note=? WHERE id=?",
            (note, position_id),
        )
        self._conn.commit()

    def stats(self) -> dict[str, float | int]:
        row = self._conn.execute(
            """
            SELECT
                SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN status='working' THEN 1 ELSE 0 END) AS working_count,
                SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                SUM(CASE WHEN status='resolved' AND pnl <= 0 THEN 1 ELSE 0 END) AS lost_count,
                SUM(CASE WHEN status='unfilled' THEN 1 ELSE 0 END) AS unfilled_count,
                SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_count,
                COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS realized_pnl
            FROM positions
            """
        ).fetchone()
        return {
            "open_count": int(row["open_count"] or 0),
            "working_count": int(row["working_count"] or 0),
            "resolved_count": int(row["resolved_count"] or 0),
            "won_count": int(row["won_count"] or 0),
            "lost_count": int(row["lost_count"] or 0),
            "unfilled_count": int(row["unfilled_count"] or 0),
            "error_count": int(row["error_count"] or 0),
            "realized_pnl": float(row["realized_pnl"] or 0.0),
        }

    def stats_by_mode(self, mode: str) -> dict[str, float | int]:
        row = self._conn.execute(
            """
            SELECT
                SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_count,
                SUM(CASE WHEN status='working' THEN 1 ELSE 0 END) AS working_count,
                SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved_count,
                SUM(CASE WHEN status='resolved' AND pnl > 0 THEN 1 ELSE 0 END) AS won_count,
                SUM(CASE WHEN status='resolved' AND pnl <= 0 THEN 1 ELSE 0 END) AS lost_count,
                SUM(CASE WHEN status='unfilled' THEN 1 ELSE 0 END) AS unfilled_count,
                SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_count,
                COALESCE(SUM(CASE WHEN status='resolved' THEN pnl ELSE 0 END), 0) AS realized_pnl
            FROM positions
            WHERE mode=?
            """,
            (mode,),
        ).fetchone()
        return {
            "open_count": int(row["open_count"] or 0),
            "working_count": int(row["working_count"] or 0),
            "resolved_count": int(row["resolved_count"] or 0),
            "won_count": int(row["won_count"] or 0),
            "lost_count": int(row["lost_count"] or 0),
            "unfilled_count": int(row["unfilled_count"] or 0),
            "error_count": int(row["error_count"] or 0),
            "realized_pnl": float(row["realized_pnl"] or 0.0),
        }

    def init_real_deposit(self, amount: float) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO real_deposit (id, balance, peak, updated_at) VALUES (1, ?, ?, ?)",
            (amount, amount, _iso(datetime.utcnow())),
        )
        self._conn.commit()

    def get_real_deposit(self) -> tuple[float, float]:
        row = self._conn.execute(
            "SELECT balance, peak FROM real_deposit WHERE id=1"
        ).fetchone()
        if row is None:
            return 0.0, 0.0
        return float(row["balance"]), float(row["peak"])

    def has_real_deposit(self) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM real_deposit WHERE id=1"
        ).fetchone()
        return row is not None

    def deduct_real_deposit(self, amount: float) -> None:
        balance, peak = self.get_real_deposit()
        self._conn.execute(
            "UPDATE real_deposit SET balance=?, peak=?, updated_at=? WHERE id=1",
            (round(balance - amount, 6), peak, _iso(datetime.utcnow())),
        )
        self._conn.commit()

    def add_real_deposit(self, amount: float) -> None:
        balance, peak = self.get_real_deposit()
        new_balance = balance + amount
        self._conn.execute(
            "UPDATE real_deposit SET balance=?, peak=?, updated_at=? WHERE id=1",
            (round(new_balance, 6), max(peak, new_balance), _iso(datetime.utcnow())),
        )
        self._conn.commit()

    def _row_to_position(self, row: sqlite3.Row) -> RecoveryPosition:
        return RecoveryPosition(
            id=row["id"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            title=row["title"],
            interval_minutes=int(row["interval_minutes"]),
            market_start=_dt(row["market_start"]) or datetime.utcnow(),
            market_end=_dt(row["market_end"]) or datetime.utcnow(),
            side=row["side"],
            mode=row["mode"],
            strategy_name=row["strategy_name"],
            touch_ts=_dt(row["touch_ts"]) or datetime.utcnow(),
            armed_ts=_dt(row["armed_ts"]) or datetime.utcnow(),
            opened_at=_dt(row["opened_at"]) or datetime.utcnow(),
            touch_price=float(row["touch_price"]),
            trigger_price=float(row["trigger_price"]),
            entry_price=float(row["entry_price"]),
            requested_shares=float(row["requested_shares"]),
            filled_shares=float(row["filled_shares"]),
            total_cost=float(row["total_cost"]),
            fee=float(row["fee"] or 0.0),
            status=row["status"],
            pm_token_id=row["pm_token_id"],
            pm_order_id=row["pm_order_id"],
            note=row["note"],
            resolved_at=_dt(row["resolved_at"]),
            winning_side=row["winning_side"],
            pnl=float(row["pnl"]) if row["pnl"] is not None else None,
        )
