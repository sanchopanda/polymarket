from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional


class SportsArbDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._migrate()

    def _commit(self) -> None:
        """Commit only if a transaction is active (safe across DDL/executescript)."""
        if self.conn.in_transaction:
            self.conn.commit()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                sport TEXT NOT NULL,
                pm_slug TEXT NOT NULL,
                pm_title TEXT NOT NULL,
                pm_market_id TEXT NOT NULL,
                ka_event_ticker TEXT NOT NULL,
                ka_title TEXT NOT NULL,
                match_confidence REAL,
                player_a TEXT NOT NULL,
                player_b TEXT NOT NULL,
                leg_pm_player TEXT NOT NULL,
                leg_pm_token_id TEXT NOT NULL,
                leg_pm_price REAL NOT NULL,
                leg_ka_player TEXT NOT NULL,
                leg_ka_ticker TEXT NOT NULL,
                leg_ka_price REAL NOT NULL,
                cost REAL NOT NULL,
                edge REAL NOT NULL,
                shares INTEGER NOT NULL,
                total_cost REAL NOT NULL,
                expected_profit REAL NOT NULL,
                game_date TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                winner TEXT,
                pm_result TEXT,
                ka_result TEXT,
                pnl REAL,
                lock_valid INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                sport TEXT NOT NULL,
                pm_slug TEXT NOT NULL,
                pm_token_id TEXT NOT NULL,
                pm_player TEXT NOT NULL,
                pm_best_ask REAL,
                pm_ask_depth_usd REAL,
                ka_ticker TEXT NOT NULL,
                ka_player TEXT NOT NULL,
                ka_yes_ask REAL,
                ka_ask_depth_usd REAL
            );

            CREATE TABLE IF NOT EXISTS matched_pairs (
                pair_key TEXT PRIMARY KEY,
                sport TEXT NOT NULL,
                pm_slug TEXT NOT NULL,
                pm_title TEXT NOT NULL,
                ka_event_ticker TEXT NOT NULL,
                ka_title TEXT NOT NULL,
                player_a TEXT NOT NULL,
                player_b TEXT NOT NULL,
                match_confidence REAL NOT NULL,
                game_date TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS virtual_balance (
                id INTEGER PRIMARY KEY,
                initial_balance REAL NOT NULL,
                current_balance REAL NOT NULL,
                total_wagered REAL NOT NULL DEFAULT 0,
                total_won REAL NOT NULL DEFAULT 0,
                total_lost REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                position_id TEXT,
                details TEXT
            );
        """)
        # Add columns idempotently
        for col, definition in [
            ("is_paper", "INTEGER NOT NULL DEFAULT 1"),
            ("execution_status", "TEXT"),
            ("ka_order_id", "TEXT"),
            ("ka_fill_price", "REAL"),
            ("ka_fill_shares", "REAL"),
            ("pm_order_id", "TEXT"),
            ("pm_fill_price", "REAL"),
            ("pm_fill_shares", "REAL"),
            ("initially_one_legged", "TEXT"),  # e.g. "pm" or "ka" — which leg was filled first
            ("pm_ask_depth_usd", "REAL DEFAULT 0"),
            ("ka_ask_depth_usd", "REAL DEFAULT 0"),
        ]:
            try:
                self.conn.execute(f"ALTER TABLE positions ADD COLUMN {col} {definition}")
                self._commit()
            except sqlite3.OperationalError:
                pass  # column already exists

        try:
            self.conn.execute("ALTER TABLE matched_pairs ADD COLUMN max_edge_seen REAL")
            self._commit()
        except sqlite3.OperationalError:
            pass  # column already exists

        try:
            self.conn.execute("ALTER TABLE positions ADD COLUMN market_max_edge REAL")
            self._commit()
        except sqlite3.OperationalError:
            pass  # column already exists

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            "INSERT OR IGNORE INTO virtual_balance "
            "(id, initial_balance, current_balance, total_wagered, total_won, total_lost, updated_at) "
            "VALUES (1, 10000.0, 10000.0, 0, 0, 0, ?)",
            (now,),
        )
        self._commit()

    def open_position(
        self,
        sport: str,
        pm_slug: str,
        pm_title: str,
        pm_market_id: str,
        ka_event_ticker: str,
        ka_title: str,
        match_confidence: float,
        player_a: str,
        player_b: str,
        leg_pm_player: str,
        leg_pm_token_id: str,
        leg_pm_price: float,
        leg_ka_player: str,
        leg_ka_ticker: str,
        leg_ka_price: float,
        cost: float,
        edge: float,
        shares: int,
        game_date: datetime,
        lock_valid: bool = True,
        market_max_edge: float = 0.0,
        pm_ask_depth_usd: float = 0.0,
        ka_ask_depth_usd: float = 0.0,
    ) -> str:
        pos_id = str(uuid.uuid4())[:8]
        total_cost = shares * cost
        expected_profit = shares * edge
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO positions (
                id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date, opened_at, status, lock_valid, market_max_edge,
                pm_ask_depth_usd, ka_ask_depth_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)""",
            (
                pos_id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date.isoformat(), now, int(lock_valid), market_max_edge,
                pm_ask_depth_usd, ka_ask_depth_usd,
            ),
        )
        # Deduct stake from balance
        self.conn.execute(
            "UPDATE virtual_balance SET "
            "current_balance = current_balance - ?, "
            "total_wagered = total_wagered + ?, "
            "updated_at = ? WHERE id = 1",
            (total_cost, total_cost, now),
        )
        self._commit()
        return pos_id

    def open_one_legged_position(
        self,
        sport: str,
        pm_slug: str,
        pm_title: str,
        pm_market_id: str,
        ka_event_ticker: str,
        ka_title: str,
        match_confidence: float,
        player_a: str,
        player_b: str,
        leg_pm_player: str,
        leg_pm_token_id: str,
        leg_pm_price: float,
        leg_ka_player: str,
        leg_ka_ticker: str,
        leg_ka_price: float,
        cost: float,
        edge: float,
        shares: int,
        game_date: datetime,
        filled_leg: str,  # "pm" or "ka"
        market_max_edge: float = 0.0,
        pm_ask_depth_usd: float = 0.0,
        ka_ask_depth_usd: float = 0.0,
    ) -> str:
        """Open a one-legged paper position where only one venue was liquid."""
        pos_id = str(uuid.uuid4())[:8]
        filled_price = leg_pm_price if filled_leg == "pm" else leg_ka_price
        total_cost = shares * filled_price
        expected_profit = shares * edge
        exec_status = "paper_one_legged_pm" if filled_leg == "pm" else "paper_one_legged_ka"
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO positions (
                id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date, opened_at, status, lock_valid,
                is_paper, execution_status, market_max_edge,
                pm_ask_depth_usd, ka_ask_depth_usd, initially_one_legged
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 0, 1, ?, ?, ?, ?, ?)""",
            (
                pos_id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date.isoformat(), now, exec_status, market_max_edge,
                pm_ask_depth_usd, ka_ask_depth_usd, filled_leg,
            ),
        )
        self.conn.execute(
            "UPDATE virtual_balance SET "
            "current_balance = current_balance - ?, "
            "total_wagered = total_wagered + ?, "
            "updated_at = ? WHERE id = 1",
            (total_cost, total_cost, now),
        )
        self._commit()
        return pos_id

    def complete_paper_one_legged(
        self,
        pos_id: str,
        missing_leg: str,  # "pm" or "ka"
        fill_price: float,
        shares: int,
    ) -> None:
        """Fill the missing leg of a one-legged paper position."""
        additional_cost = shares * fill_price
        now = datetime.now(tz=timezone.utc).isoformat()
        # Record which leg was filled first (the one that was NOT missing)
        first_leg = "ka" if missing_leg == "pm" else "pm"
        if missing_leg == "pm":
            self.conn.execute(
                "UPDATE positions SET execution_status='paper_both_filled', "
                "lock_valid=1, leg_pm_price=?, initially_one_legged=?, "
                "cost=leg_ka_price + ?, edge=1.0 - leg_ka_price - ?, "
                "total_cost=total_cost + ?, expected_profit=shares * (1.0 - leg_ka_price - ?) "
                "WHERE id=?",
                (fill_price, first_leg, fill_price, fill_price, additional_cost, fill_price, pos_id),
            )
        else:
            self.conn.execute(
                "UPDATE positions SET execution_status='paper_both_filled', "
                "lock_valid=1, leg_ka_price=?, initially_one_legged=?, "
                "cost=leg_pm_price + ?, edge=1.0 - leg_pm_price - ?, "
                "total_cost=total_cost + ?, expected_profit=shares * (1.0 - leg_pm_price - ?) "
                "WHERE id=?",
                (fill_price, first_leg, fill_price, fill_price, additional_cost, fill_price, pos_id),
            )
        self.conn.execute(
            "UPDATE virtual_balance SET "
            "current_balance = current_balance - ?, "
            "total_wagered = total_wagered + ?, "
            "updated_at = ? WHERE id = 1",
            (additional_cost, additional_cost, now),
        )
        self._commit()

    def resolve_one_legged_position(
        self,
        pos_id: str,
        pm_winner: Optional[str],
        ka_result: Optional[str],
    ) -> float:
        """Resolve a one-legged paper position."""
        pos = self.conn.execute(
            "SELECT shares, total_cost, execution_status, "
            "leg_pm_player, leg_ka_player, leg_pm_price, leg_ka_price, is_paper "
            "FROM positions WHERE id = ?",
            (pos_id,),
        ).fetchone()
        if pos is None:
            return 0.0

        shares = int(pos["shares"])
        total_cost = float(pos["total_cost"])
        exec_status = pos["execution_status"]
        is_paper = bool(pos["is_paper"] if pos["is_paper"] is not None else 1)

        if exec_status == "paper_one_legged_pm":
            leg_won = pm_winner == pos["leg_pm_player"]
            winner = pos["leg_pm_player"] if leg_won else "opponent"
        else:  # paper_one_legged_ka
            leg_won = ka_result == "yes"
            winner = pos["leg_ka_player"] if leg_won else "opponent"

        payout = float(shares) if leg_won else 0.0
        pnl = payout - total_cost

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                status='resolved', resolved_at=?, winner=?,
                pm_result=?, ka_result=?, pnl=?, lock_valid=0
            WHERE id=?""",
            (now, winner, pm_winner or "unknown", ka_result or "unknown", pnl, pos_id),
        )
        if is_paper:
            self.conn.execute(
                "UPDATE virtual_balance SET "
                "current_balance = current_balance + ?, "
                "total_won = total_won + ?, "
                "updated_at = ? WHERE id = 1",
                (payout, payout, now),
            )
            if pnl < 0:
                self.conn.execute(
                    "UPDATE virtual_balance SET total_lost = total_lost + ?, updated_at = ? WHERE id = 1",
                    (abs(pnl), now),
                )
        self._commit()
        return pnl

    def resolve_real_one_legged_position(
        self,
        pos_id: str,
        exec_status: str,  # "one_legged_kalshi" | "one_legged_polymarket"
        pm_winner: Optional[str],
        ka_result: Optional[str],
    ) -> float:
        """Резолв реальной одноногой позиции по фактически заполненной ноге."""
        pos = self.conn.execute(
            "SELECT leg_pm_player, leg_ka_player, "
            "ka_fill_price, ka_fill_shares, pm_fill_price, pm_fill_shares "
            "FROM positions WHERE id = ?",
            (pos_id,),
        ).fetchone()
        if pos is None:
            return 0.0

        if exec_status == "one_legged_kalshi":
            # Ka заполнен, PM нет → пayout за Ka YES
            actual_cost = float(pos["ka_fill_price"] or 0) * float(pos["ka_fill_shares"] or 0)
            shares = float(pos["ka_fill_shares"] or 0)
            leg_won = ka_result == "yes"
            winner = pos["leg_ka_player"] if leg_won else pm_winner or "opponent"
        else:
            # PM заполнен, Ka нет → payout за PM YES
            actual_cost = float(pos["pm_fill_price"] or 0) * float(pos["pm_fill_shares"] or 0)
            shares = float(pos["pm_fill_shares"] or 0)
            leg_won = pm_winner == pos["leg_pm_player"]
            winner = pm_winner if leg_won else "opponent"

        payout = shares if leg_won else 0.0
        pnl = payout - actual_cost

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                status='resolved', resolved_at=?, winner=?,
                pm_result=?, ka_result=?, pnl=?,
                total_cost=?, lock_valid=0
            WHERE id=?""",
            (now, winner, pm_winner or "unknown", ka_result or "unknown",
             pnl, actual_cost, pos_id),
        )
        self._commit()
        self.audit("real_one_legged_resolved", pos_id, {
            "exec_status": exec_status, "pnl": pnl, "actual_cost": actual_cost,
        })
        return pnl

    def open_real_position(
        self,
        sport: str,
        pm_slug: str,
        pm_title: str,
        pm_market_id: str,
        ka_event_ticker: str,
        ka_title: str,
        match_confidence: float,
        player_a: str,
        player_b: str,
        leg_pm_player: str,
        leg_pm_token_id: str,
        leg_pm_price: float,
        leg_ka_player: str,
        leg_ka_ticker: str,
        leg_ka_price: float,
        cost: float,
        edge: float,
        shares: int,
        game_date: datetime,
        execution_status: str,
        ka_order_id: str = "",
        ka_fill_price: float = 0.0,
        ka_fill_shares: float = 0.0,
        pm_order_id: str = "",
        pm_fill_price: float = 0.0,
        pm_fill_shares: float = 0.0,
        pm_ask_depth_usd: float = 0.0,
        ka_ask_depth_usd: float = 0.0,
    ) -> str:
        """Record a real (non-paper) trade. Does not touch virtual_balance."""
        pos_id = str(uuid.uuid4())[:8]
        total_cost = shares * cost
        expected_profit = shares * edge
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO positions (
                id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date, opened_at, status, lock_valid,
                is_paper, execution_status,
                ka_order_id, ka_fill_price, ka_fill_shares,
                pm_order_id, pm_fill_price, pm_fill_shares,
                pm_ask_depth_usd, ka_ask_depth_usd, initially_one_legged
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pos_id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date.isoformat(), now,
                execution_status,
                ka_order_id, ka_fill_price, ka_fill_shares,
                pm_order_id, pm_fill_price, pm_fill_shares,
                pm_ask_depth_usd, ka_ask_depth_usd,
                "kalshi" if execution_status == "one_legged_kalshi" else
                "pm" if execution_status == "one_legged_polymarket" else None,
            ),
        )
        self._commit()
        self.audit("real_position_opened", pos_id, {
            "execution_status": execution_status,
            "ka_order_id": ka_order_id,
            "ka_fill_shares": ka_fill_shares,
        })
        return pos_id

    def update_pm_filled(
        self,
        pos_id: str,
        pm_order_id: str,
        pm_fill_price: float,
        pm_fill_shares: float,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                execution_status = 'both_filled',
                pm_order_id = ?,
                pm_fill_price = ?,
                pm_fill_shares = ?
            WHERE id = ?""",
            (pm_order_id, pm_fill_price, pm_fill_shares, pos_id),
        )
        self._commit()
        self.audit("pm_leg_filled", pos_id, {
            "pm_order_id": pm_order_id,
            "pm_fill_price": pm_fill_price,
            "pm_fill_shares": pm_fill_shares,
        })

    def mark_orphaned(self, pos_id: str, reason: str) -> None:
        self.conn.execute(
            "UPDATE positions SET execution_status = 'orphaned_kalshi' WHERE id = ?",
            (pos_id,),
        )
        self._commit()
        self.audit("position_orphaned", pos_id, {"reason": reason})

    def count_real_positions_for_pair(self, pair_key: str) -> int:
        """Количество реальных (не paper) позиций по паре рынков."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions WHERE ka_event_ticker=? AND is_paper=0",
            (pair_key,),
        ).fetchone()
        return int(row["cnt"] or 0)

    def count_all_positions_for_pair(self, pair_key: str) -> int:
        """Количество paper позиций по паре рынков."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions WHERE ka_event_ticker=? AND (is_paper=1 OR is_paper IS NULL)",
            (pair_key,),
        ).fetchone()
        return int(row["cnt"] or 0)

    def has_open_one_legged(self, ka_event_ticker: str) -> bool:
        """Есть ли открытая одноногая позиция (любого типа) для этого матча (paper)."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions "
            "WHERE ka_event_ticker=? AND status='open' "
            "AND execution_status IN ('paper_one_legged_pm', 'paper_one_legged_ka')",
            (ka_event_ticker,),
        ).fetchone()
        return int(row["cnt"] or 0) > 0

    def has_open_one_legged_real(self, ka_event_ticker: str) -> bool:
        """Есть ли открытая реальная одноногая позиция для этого матча."""
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions "
            "WHERE ka_event_ticker=? AND status='open' AND is_paper=0 "
            "AND execution_status IN ('one_legged_kalshi', 'one_legged_polymarket')",
            (ka_event_ticker,),
        ).fetchone()
        return int(row["cnt"] or 0) > 0

    def get_total_real_pnl(self) -> float:
        """Суммарный P&L по закрытым реальным позициям (отрицательный = потери)."""
        row = self.conn.execute(
            "SELECT SUM(pnl) AS total FROM positions "
            "WHERE status IN ('resolved','cancelled') AND is_paper=0"
        ).fetchone()
        return float(row["total"] or 0.0)

    def get_paper_pnl(self) -> float:
        """Суммарный P&L по закрытым paper позициям."""
        row = self.conn.execute(
            "SELECT SUM(pnl) AS total FROM positions "
            "WHERE status IN ('resolved','cancelled') AND (is_paper=1 OR is_paper IS NULL)"
        ).fetchone()
        return float(row["total"] or 0.0)

    def resolve_cancelled_position(
        self,
        pos_id: str,
        pm_result: str,   # "n/a" или имя победителя (PM зарезолвился нормально)
        ka_result: str,   # "void" | "yes" | "no" | "scalar"
        ka_settlement: float | None = None,  # settlement value for scalar
    ) -> float:
        """Рассчитывает P&L при отмене/void рынков.
        PM N/A → 50¢/токен. Kalshi void → полный возврат. Kalshi scalar → settlement_value/шару.
        """
        pos = self.conn.execute(
            "SELECT shares, leg_pm_player, leg_pm_price, leg_ka_price, "
            "total_cost, is_paper FROM positions WHERE id = ?",
            (pos_id,),
        ).fetchone()
        if pos is None:
            return 0.0

        shares = int(pos["shares"])
        leg_pm_price = float(pos["leg_pm_price"])
        leg_ka_price = float(pos["leg_ka_price"])
        leg_pm_player = pos["leg_pm_player"]
        total_cost = float(pos["total_cost"])
        is_paper = bool(pos["is_paper"] if pos["is_paper"] is not None else 1)

        # PM payout
        if pm_result == "n/a":
            pm_payout = 0.50 * shares
        else:
            pm_payout = float(shares) if pm_result == leg_pm_player else 0.0

        # Kalshi payout
        if ka_result == "void":
            ka_payout = leg_ka_price * shares  # полный возврат
        elif ka_result == "scalar" and ka_settlement is not None:
            ka_payout = ka_settlement * shares  # partial settlement
        else:
            ka_payout = float(shares) if ka_result == "yes" else 0.0

        total_return = pm_payout + ka_payout
        pnl = total_return - total_cost

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                status = 'cancelled', resolved_at = ?, winner = 'cancelled',
                pm_result = ?, ka_result = ?, pnl = ?
            WHERE id = ?""",
            (now, pm_result, ka_result, pnl, pos_id),
        )
        if is_paper:
            self.conn.execute(
                "UPDATE virtual_balance SET "
                "current_balance = current_balance + ?, "
                "total_won = total_won + ?, "
                "updated_at = ? WHERE id = 1",
                (total_return, total_return, now),
            )
            if pnl < 0:
                self.conn.execute(
                    "UPDATE virtual_balance SET total_lost = total_lost + ?, updated_at = ? WHERE id = 1",
                    (abs(pnl), now),
                )
        self._commit()
        return pnl

    def audit(self, event_type: str, position_id: Optional[str], details: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, position_id, details) VALUES (?,?,?,?)",
            (datetime.now(tz=timezone.utc).isoformat(), event_type, position_id, json.dumps(details)),
        )
        self._commit()

    def resolve_position(
        self,
        pos_id: str,
        winner: Optional[str],
        pm_result: Optional[str],
        ka_result: Optional[str],
    ) -> float:
        """Resolve position. Fetches shares/total_cost from DB. Returns pnl."""
        pos = self.conn.execute(
            "SELECT shares, total_cost, lock_valid, is_paper FROM positions WHERE id = ?", (pos_id,)
        ).fetchone()
        if pos is None:
            return 0.0

        shares = int(pos["shares"])
        total_cost = float(pos["total_cost"])
        lock_valid = bool(pos["lock_valid"])
        is_paper = bool(pos["is_paper"] if pos["is_paper"] is not None else 1)

        # Lock arb: exactly one of the two YES legs wins → payout = shares * $1
        payout = float(shares) if lock_valid else 0.0
        pnl = payout - total_cost

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                status = 'resolved', resolved_at = ?, winner = ?,
                pm_result = ?, ka_result = ?, pnl = ?
            WHERE id = ?""",
            (now, winner, pm_result, ka_result, pnl, pos_id),
        )
        # Touch virtual_balance only for paper positions
        if is_paper:
            self.conn.execute(
                "UPDATE virtual_balance SET "
                "current_balance = current_balance + ?, "
                "total_won = total_won + ?, "
                "updated_at = ? WHERE id = 1",
                (payout, payout, now),
            )
            if not lock_valid:
                self.conn.execute(
                    "UPDATE virtual_balance SET total_lost = total_lost + ?, updated_at = ? WHERE id = 1",
                    (total_cost, now),
                )
        self._commit()
        return pnl

    def get_open_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at"
        ).fetchall()

    def get_all_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC"
        ).fetchall()

    def save_orderbook_snapshot(
        self,
        sport: str,
        pm_slug: str,
        pm_token_id: str,
        pm_player: str,
        pm_best_ask: Optional[float],
        pm_ask_depth_usd: Optional[float],
        ka_ticker: str,
        ka_player: str,
        ka_yes_ask: Optional[float],
        ka_ask_depth_usd: Optional[float],
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO orderbook_snapshots (
                captured_at, sport,
                pm_slug, pm_token_id, pm_player, pm_best_ask, pm_ask_depth_usd,
                ka_ticker, ka_player, ka_yes_ask, ka_ask_depth_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                now, sport, pm_slug, pm_token_id, pm_player, pm_best_ask, pm_ask_depth_usd,
                ka_ticker, ka_player, ka_yes_ask, ka_ask_depth_usd,
            ),
        )
        self._commit()

    def get_balance(self) -> sqlite3.Row:
        return self.conn.execute("SELECT * FROM virtual_balance WHERE id = 1").fetchone()

    def upsert_matched_pair(
        self,
        pair_key: str,
        sport: str,
        pm_slug: str,
        pm_title: str,
        ka_event_ticker: str,
        ka_title: str,
        player_a: str,
        player_b: str,
        match_confidence: float,
        game_date: datetime,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO matched_pairs (
                pair_key, sport, pm_slug, pm_title, ka_event_ticker, ka_title,
                player_a, player_b, match_confidence, game_date,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_key) DO UPDATE SET last_seen_at = excluded.last_seen_at""",
            (
                pair_key, sport, pm_slug, pm_title, ka_event_ticker, ka_title,
                player_a, player_b, match_confidence, game_date.isoformat(),
                now, now,
            ),
        )
        self._commit()

    def update_max_edge_seen(self, pair_key: str, edge: float) -> None:
        """Обновляет max_edge_seen если новый edge больше текущего."""
        self.conn.execute(
            "UPDATE matched_pairs SET max_edge_seen = MAX(COALESCE(max_edge_seen, 0), ?) "
            "WHERE pair_key = ?",
            (edge, pair_key),
        )
        self._commit()

    def get_max_edge_seen(self, pair_key: str) -> float:
        row = self.conn.execute(
            "SELECT max_edge_seen FROM matched_pairs WHERE pair_key = ?",
            (pair_key,),
        ).fetchone()
        return float(row["max_edge_seen"] or 0.0) if row else 0.0

    def get_matched_pairs(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM matched_pairs ORDER BY last_seen_at DESC"
        ).fetchall()
