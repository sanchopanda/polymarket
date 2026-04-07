from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from swing_bot.models import SwingPosition, SwingState


_ISO = "%Y-%m-%dT%H:%M:%S.%f"


def _dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    for fmt in (_ISO, "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _iso(dt: datetime | None) -> Optional[str]:
    return dt.isoformat() if dt else None


class SwingDB:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id              TEXT PRIMARY KEY,
                market_id       TEXT NOT NULL,
                symbol          TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL,
                market_start    TEXT NOT NULL,
                market_end      TEXT NOT NULL,
                yes_token_id    TEXT NOT NULL,
                no_token_id     TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'watching',
                entry_side      TEXT NOT NULL DEFAULT 'yes',
                entry_price     REAL,
                entry_price_rest REAL,
                stake_usd       REAL,
                shares          REAL,
                opened_at       TEXT,
                exit_type       TEXT,
                exit_price      REAL,
                exit_price_rest REAL,
                exited_at       TEXT,
                hold_reason     TEXT,
                flip_shares     REAL,
                winning_side    TEXT,
                pnl             REAL,
                resolved_at     TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                event_type  TEXT NOT NULL,
                position_id TEXT,
                details     TEXT
            );
        """)
        cols = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(positions)").fetchall()
        }
        if "entry_side" not in cols:
            self.conn.execute(
                "ALTER TABLE positions ADD COLUMN entry_side TEXT NOT NULL DEFAULT 'yes'"
            )
        if "hold_reason" not in cols:
            self.conn.execute(
                "ALTER TABLE positions ADD COLUMN hold_reason TEXT"
            )
        self.conn.execute(
            """
            UPDATE positions
            SET
                state = ?,
                pnl = COALESCE(
                    pnl,
                    (COALESCE(exit_price_rest, 0) - COALESCE(entry_price_rest, 0)) * COALESCE(shares, 0)
                ),
                resolved_at = COALESCE(resolved_at, exited_at, ?)
            WHERE state = ? AND exit_type = 'sell'
            """,
            (SwingState.RESOLVED.value, _iso(datetime.utcnow()), SwingState.SOLD.value),
        )
        self.conn.commit()

    # ── write ────────────────────────────────────────────────────

    def open_position(self, pos: SwingPosition) -> None:
        self.conn.execute(
            """INSERT INTO positions
               (id, market_id, symbol, interval_minutes, market_start, market_end,
                yes_token_id, no_token_id, state, entry_side,
                entry_price, entry_price_rest, stake_usd, shares, opened_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                pos.id, pos.market_id, pos.symbol, pos.interval_minutes,
                _iso(pos.market_start), _iso(pos.market_end),
                pos.yes_token_id, pos.no_token_id, pos.state.value, pos.entry_side,
                pos.entry_price, pos.entry_price_rest,
                pos.stake_usd, pos.shares, _iso(pos.opened_at),
            ),
        )
        self.conn.commit()

    def update_state(self, pos_id: str, state: SwingState, **fields) -> None:
        sets = ["state = ?"]
        vals: list = [state.value]
        for k, v in fields.items():
            if isinstance(v, datetime):
                v = _iso(v)
            sets.append(f"{k} = ?")
            vals.append(v)
        vals.append(pos_id)
        self.conn.execute(
            f"UPDATE positions SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        self.conn.commit()

    def resolve_position(
        self,
        pos_id: str,
        winning_side: str,
        pnl: float,
        hold_reason: str | None = None,
    ) -> None:
        self.conn.execute(
            """UPDATE positions
               SET state = ?, winning_side = ?, pnl = ?, resolved_at = ?, hold_reason = ?
               WHERE id = ?""",
            (SwingState.RESOLVED.value, winning_side, round(pnl, 6),
             _iso(datetime.utcnow()), hold_reason, pos_id),
        )
        self.conn.commit()

    def audit(self, event_type: str, pos_id: str | None, details: dict | None = None) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, position_id, details) VALUES (?,?,?,?)",
            (_iso(datetime.utcnow()), event_type, pos_id,
             json.dumps(details) if details else None),
        )
        self.conn.commit()

    # ── read ─────────────────────────────────────────────────────

    def get_open_positions(self) -> list[SwingPosition]:
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE state != 'resolved'"
        ).fetchall()
        return [self._row_to_pos(r) for r in rows]

    def get_position_by_market(self, market_id: str) -> SwingPosition | None:
        row = self.conn.execute(
            "SELECT * FROM positions WHERE market_id = ? ORDER BY opened_at DESC LIMIT 1",
            (market_id,),
        ).fetchone()
        return self._row_to_pos(row) if row else None

    def get_recent_positions(self, limit: int = 50) -> list[SwingPosition]:
        rows = self.conn.execute(
            "SELECT * FROM positions ORDER BY market_start DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_pos(r) for r in rows]

    def stats(self) -> dict:
        row = self.conn.execute("""
            SELECT
                COUNT(*),
                COUNT(CASE WHEN state = 'resolved' THEN 1 END),
                COUNT(CASE WHEN state NOT IN ('resolved', 'sold') THEN 1 END),
                COALESCE(SUM(
                    CASE
                        WHEN state = 'resolved' THEN pnl
                        WHEN state = 'sold' THEN (COALESCE(exit_price_rest, 0) - COALESCE(entry_price_rest, 0)) * COALESCE(shares, 0)
                        WHEN state = 'arbed' THEN (1.0 - COALESCE(entry_price_rest, 0) - COALESCE(exit_price_rest, 0)) * COALESCE(shares, 0)
                    END
                ), 0)
            FROM positions
        """).fetchone()
        total, resolved, open_count = row[0], row[1], row[2]
        return {
            "total": total,
            "resolved": resolved,
            "open": open_count,
            "realized_pnl": round(row[3], 4),
        }

    # ── internal ─────────────────────────────────────────────────

    def _row_to_pos(self, row: sqlite3.Row) -> SwingPosition:
        return SwingPosition(
            id=row["id"],
            market_id=row["market_id"],
            symbol=row["symbol"],
            interval_minutes=row["interval_minutes"],
            market_start=_dt(row["market_start"]),
            market_end=_dt(row["market_end"]),
            yes_token_id=row["yes_token_id"],
            no_token_id=row["no_token_id"],
            state=SwingState(row["state"]),
            entry_side=row["entry_side"],
            entry_price=row["entry_price"],
            entry_price_rest=row["entry_price_rest"],
            stake_usd=row["stake_usd"],
            shares=row["shares"],
            opened_at=_dt(row["opened_at"]),
            exit_type=row["exit_type"],
            exit_price=row["exit_price"],
            exit_price_rest=row["exit_price_rest"],
            exited_at=_dt(row["exited_at"]),
            hold_reason=row["hold_reason"] if "hold_reason" in row.keys() else None,
            flip_shares=row["flip_shares"],
            winning_side=row["winning_side"],
            pnl=row["pnl"],
            resolved_at=_dt(row["resolved_at"]),
        )

    def close(self) -> None:
        self.conn.close()
