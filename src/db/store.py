from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from src.db.models import BetSeries, PortfolioSnapshot, RedeemRecord, ScanLog, SimulatedBet, WalletSnapshot


class Store:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._migrate()

    def _create_tables(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS simulated_bets (
                id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                market_question TEXT NOT NULL,
                outcome TEXT NOT NULL,
                token_id TEXT NOT NULL,
                entry_price REAL NOT NULL,
                amount_usd REAL NOT NULL,
                fee_usd REAL NOT NULL DEFAULT 0,
                shares REAL NOT NULL,
                score REAL NOT NULL,
                placed_at TEXT NOT NULL,
                market_end_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                exit_price REAL,
                pnl REAL,
                series_id TEXT,
                series_depth INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS bet_series (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                current_depth INTEGER NOT NULL DEFAULT 0,
                initial_bet_size REAL NOT NULL,
                total_invested REAL NOT NULL DEFAULT 0,
                total_pnl REAL NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS scan_logs (
                id TEXT PRIMARY KEY,
                scanned_at TEXT NOT NULL,
                total_markets INTEGER NOT NULL,
                candidates_found INTEGER NOT NULL,
                bets_placed INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS redeems (
                id TEXT PRIMARY KEY,
                bet_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                market_question TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                tx_hash TEXT NOT NULL,
                redeemed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wallet_snapshots (
                id TEXT PRIMARY KEY,
                balance_usdc REAL NOT NULL,
                recorded_at TEXT NOT NULL
            );
        """)
        self.conn.commit()

    def _migrate(self) -> None:
        """Идемпотентные миграции для существующих БД."""
        for table, col, typ, default in [
            ("simulated_bets", "series_id", "TEXT", None),
            ("simulated_bets", "series_depth", "INTEGER", 0),
            ("scan_logs", "skipped_limit", "INTEGER", 0),
            ("simulated_bets", "order_id", "TEXT", "''"),
        ]:
            try:
                default_clause = f" DEFAULT {default}" if default is not None else ""
                self.conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {col} {typ}{default_clause}"
                )
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # Колонка уже есть

    # --- Серии ---

    def create_series(self, series: BetSeries) -> None:
        self.conn.execute("""
            INSERT INTO bet_series
            (id, status, current_depth, initial_bet_size, total_invested, total_pnl, started_at, finished_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            series.id, series.status, series.current_depth, series.initial_bet_size,
            series.total_invested, series.total_pnl, series.started_at.isoformat(),
            series.finished_at.isoformat() if series.finished_at else None,
        ))
        self.conn.commit()

    def get_active_series(self) -> List[BetSeries]:
        rows = self.conn.execute(
            "SELECT * FROM bet_series WHERE status IN ('active', 'waiting') ORDER BY started_at"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_all_series(self) -> List[BetSeries]:
        rows = self.conn.execute(
            "SELECT * FROM bet_series ORDER BY started_at DESC"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_series_by_id(self, series_id: str) -> Optional[BetSeries]:
        row = self.conn.execute(
            "SELECT * FROM bet_series WHERE id = ?", (series_id,)
        ).fetchone()
        return self._row_to_series(row) if row else None

    def set_series_waiting(self, series_id: str) -> None:
        self.conn.execute("UPDATE bet_series SET status = 'waiting' WHERE id = ?", (series_id,))
        self.conn.commit()

    def get_waiting_series(self) -> List[BetSeries]:
        rows = self.conn.execute(
            "SELECT * FROM bet_series WHERE status = 'waiting' ORDER BY started_at"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def finish_series(self, series_id: str, status: str, total_pnl: float) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.conn.execute(
            "UPDATE bet_series SET status = ?, total_pnl = ?, finished_at = ? WHERE id = ?",
            (status, total_pnl, now, series_id),
        )
        self.conn.commit()

    def update_series_depth(self, series_id: str, new_depth: int, added_investment: float) -> None:
        self.conn.execute("""
            UPDATE bet_series
            SET current_depth = ?, total_invested = total_invested + ?
            WHERE id = ?
        """, (new_depth, added_investment, series_id))
        self.conn.commit()

    def get_series_pending_escalation(self) -> List[BetSeries]:
        """Активные серии без открытых ставок — ждут эскалации."""
        rows = self.conn.execute("""
            SELECT bs.* FROM bet_series bs
            WHERE bs.status = 'active'
              AND NOT EXISTS (
                  SELECT 1 FROM simulated_bets sb
                  WHERE sb.series_id = bs.id AND sb.status = 'open'
              )
            ORDER BY bs.started_at
        """).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_series_bets(self, series_id: str) -> List[SimulatedBet]:
        rows = self.conn.execute(
            "SELECT * FROM simulated_bets WHERE series_id = ? ORDER BY series_depth",
            (series_id,),
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    # --- Ставки ---

    def save_bet(self, bet: SimulatedBet) -> None:
        self.conn.execute("""
            INSERT OR REPLACE INTO simulated_bets
            (id, market_id, market_question, outcome, token_id, entry_price,
             amount_usd, fee_usd, shares, score, placed_at, market_end_date, status,
             resolved_at, exit_price, pnl, series_id, series_depth, order_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            bet.id, bet.market_id, bet.market_question, bet.outcome, bet.token_id,
            bet.entry_price, bet.amount_usd, bet.fee_usd, bet.shares, bet.score,
            bet.placed_at.isoformat(), bet.market_end_date.isoformat(),
            bet.status, bet.resolved_at.isoformat() if bet.resolved_at else None,
            bet.exit_price, bet.pnl, bet.series_id, bet.series_depth, bet.order_id,
        ))
        self.conn.commit()

    def get_open_bets(self) -> List[SimulatedBet]:
        rows = self.conn.execute(
            "SELECT * FROM simulated_bets WHERE status = 'open' ORDER BY placed_at"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_all_bets(self) -> List[SimulatedBet]:
        rows = self.conn.execute(
            "SELECT * FROM simulated_bets ORDER BY placed_at DESC"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def resolve_bet(self, bet_id: str, exit_price: float) -> None:
        """P&L = выручка - полная стоимость (ставка + комиссия)."""
        status = "won" if exit_price >= 0.9 else "lost"
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.conn.execute("""
            UPDATE simulated_bets
            SET status = ?, resolved_at = ?, exit_price = ?,
                pnl = (exit_price_val * shares) - amount_usd - fee_usd
            WHERE id = ?
        """.replace("exit_price_val", str(exit_price)),
        (status, now, exit_price, bet_id))
        self.conn.commit()

    def already_bet(self, market_id: str, outcome: str) -> bool:
        """Возвращает True если уже есть открытая ставка на ЛЮБОЙ исход этого рынка."""
        row = self.conn.execute(
            "SELECT id FROM simulated_bets WHERE market_id=? AND status='open'",
            (market_id,)
        ).fetchone()
        return row is not None

    # --- Статистика ---

    def get_portfolio_stats(self) -> PortfolioSnapshot:
        row = self.conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='open' THEN amount_usd ELSE 0 END), 0)      AS deployed,
                COALESCE(SUM(CASE WHEN status='open' THEN fee_usd ELSE 0 END), 0)          AS open_fees,
                COALESCE(SUM(fee_usd), 0)                                                   AS total_fees,
                COALESCE(SUM(CASE WHEN status IN ('won','lost') THEN pnl ELSE 0 END), 0)   AS realized_pnl,
                COUNT(CASE WHEN status='open' THEN 1 END)                                   AS open_pos,
                COUNT(CASE WHEN status='won' THEN 1 END)                                    AS wins,
                COUNT(CASE WHEN status='lost' THEN 1 END)                                   AS losses
            FROM simulated_bets
        """).fetchone()

        active_series = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM bet_series WHERE status IN ('active', 'waiting')"
        ).fetchone()

        return PortfolioSnapshot(
            total_deployed=row["deployed"],
            total_fees_paid=row["total_fees"],
            total_pnl_realized=row["realized_pnl"],
            open_positions=row["open_pos"],
            win_count=row["wins"],
            loss_count=row["losses"],
            active_series_count=active_series["cnt"],
        )

    def get_total_invested_in_active_series(self) -> float:
        """Сумма total_invested по всем активным и ожидающим сериям."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(total_invested), 0) AS total FROM bet_series WHERE status IN ('active', 'waiting')"
        ).fetchone()
        return row["total"]

    # --- Scan logs ---

    def save_scan_log(self, log: ScanLog) -> None:
        self.conn.execute("""
            INSERT INTO scan_logs (id, scanned_at, total_markets, candidates_found, bets_placed, skipped_limit)
            VALUES (?,?,?,?,?,?)
        """, (log.id, log.scanned_at.isoformat(), log.total_markets, log.candidates_found, log.bets_placed, log.skipped_limit))
        self.conn.commit()

    def get_avg_skipped_limit(self) -> float:
        """Среднее кол-во упущенных кандидатов из-за лимита серий (по всем сканам)."""
        row = self.conn.execute(
            "SELECT AVG(skipped_limit) FROM scan_logs"
        ).fetchone()
        return row[0] or 0.0

    # --- Redeems ---

    def save_redeem(self, r: RedeemRecord) -> None:
        self.conn.execute(
            "INSERT INTO redeems (id, bet_id, market_id, market_question, amount_usd, tx_hash, redeemed_at) VALUES (?,?,?,?,?,?,?)",
            (r.id, r.bet_id, r.market_id, r.market_question, r.amount_usd, r.tx_hash, r.redeemed_at.isoformat()),
        )
        self.conn.commit()

    def get_redeems(self) -> List[RedeemRecord]:
        rows = self.conn.execute("SELECT * FROM redeems ORDER BY redeemed_at DESC").fetchall()
        return [RedeemRecord(
            id=r["id"], bet_id=r["bet_id"], market_id=r["market_id"],
            market_question=r["market_question"], amount_usd=r["amount_usd"],
            tx_hash=r["tx_hash"], redeemed_at=datetime.fromisoformat(r["redeemed_at"]),
        ) for r in rows]

    def already_redeemed(self, bet_id: str) -> bool:
        return self.conn.execute(
            "SELECT id FROM redeems WHERE bet_id = ?", (bet_id,)
        ).fetchone() is not None

    # --- Wallet snapshots ---

    def save_wallet_snapshot(self, s: WalletSnapshot) -> None:
        self.conn.execute(
            "INSERT INTO wallet_snapshots (id, balance_usdc, recorded_at) VALUES (?,?,?)",
            (s.id, s.balance_usdc, s.recorded_at.isoformat()),
        )
        self.conn.commit()

    def get_wallet_snapshots(self, limit: int = 50) -> List[WalletSnapshot]:
        rows = self.conn.execute(
            "SELECT * FROM wallet_snapshots ORDER BY recorded_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [WalletSnapshot(id=r["id"], balance_usdc=r["balance_usdc"],
                               recorded_at=datetime.fromisoformat(r["recorded_at"])) for r in rows]

    # --- Helpers ---

    def _row_to_bet(self, r: sqlite3.Row) -> SimulatedBet:
        keys = r.keys()
        return SimulatedBet(
            id=r["id"],
            market_id=r["market_id"],
            market_question=r["market_question"],
            outcome=r["outcome"],
            token_id=r["token_id"],
            entry_price=r["entry_price"],
            amount_usd=r["amount_usd"],
            fee_usd=r["fee_usd"] if "fee_usd" in keys else 0.0,
            shares=r["shares"],
            score=r["score"],
            placed_at=datetime.fromisoformat(r["placed_at"]),
            market_end_date=datetime.fromisoformat(r["market_end_date"]),
            status=r["status"],
            resolved_at=datetime.fromisoformat(r["resolved_at"]) if r["resolved_at"] else None,
            exit_price=r["exit_price"],
            pnl=r["pnl"],
            series_id=r["series_id"] if "series_id" in keys else None,
            series_depth=r["series_depth"] if "series_depth" in keys else 0,
            order_id=r["order_id"] if "order_id" in keys else "",
        )

    def _row_to_series(self, r: sqlite3.Row) -> BetSeries:
        return BetSeries(
            id=r["id"],
            status=r["status"],
            current_depth=r["current_depth"],
            initial_bet_size=r["initial_bet_size"],
            total_invested=r["total_invested"],
            total_pnl=r["total_pnl"],
            started_at=datetime.fromisoformat(r["started_at"]),
            finished_at=datetime.fromisoformat(r["finished_at"]) if r["finished_at"] else None,
        )
