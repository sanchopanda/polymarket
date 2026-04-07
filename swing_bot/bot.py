"""
swing_bot/bot.py

Paper-trading бот для swing-стратегии на 5-мин PM крипто-рынках.

Стратегия:
  1. На входе покупаем более дешёвую сторону если min_entry_price <= ask <= max_entry_price
  2. Выход: sell стороны входа >= sell_target ИЛИ buy opposite <= arb_target
  3. Если opposite не дали дешево, на cutoff докупаем opposite по рынку
  3. Все входы/выходы с 1с задержкой (REST-верификация стакана)
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta

from arb_bot.ws import MarketWebSocketClient, TopOfBook
from cross_arb_bot.models import NormalizedMarket
from cross_arb_bot.polymarket_feed import PolymarketFeed
from oracle_arb_bot.resolver import check_polymarket_result
from src.api.clob import ClobClient

from swing_bot.db import SwingDB
from swing_bot.models import SwingPosition, SwingState
from swing_bot.telegram_notify import SwingTelegramNotifier


class SwingBot:
    def __init__(self, config: dict, db: SwingDB, notifier: SwingTelegramNotifier | None = None) -> None:
        self.cfg = config
        self.db = db

        strat = config["strategy"]
        self.interval_minutes = strat["interval_minutes"]
        self.interval_minutes_list = sorted({
            int(x) for x in (strat.get("interval_minutes_list") or [self.interval_minutes])
        })
        self.min_entry_price = strat["min_entry_price"]
        self.max_entry_price = strat["max_entry_price"]
        self.sell_target = strat["sell_target"]
        self.arb_target = strat["arb_target"]
        self.max_flip_price = strat["max_flip_price"]
        self.cutoff_pct = strat["cutoff_pct"]
        self.stake_usd = strat["stake_usd"]
        self.verify_delay = strat["verification_delay_seconds"]

        pm_cfg = config["polymarket"]
        self.feed = PolymarketFeed(
            base_url=pm_cfg["gamma_base_url"],
            page_size=pm_cfg["page_size"],
            request_delay_ms=pm_cfg["request_delay_ms"],
            market_filter=config["market_filter"],
        )
        self.clob = ClobClient(
            base_url=pm_cfg["clob_base_url"],
            delay_ms=pm_cfg["request_delay_ms"],
        )
        self.ws_url = pm_cfg["clob_ws_url"]
        self.scan_interval = config["runtime"]["scan_interval_seconds"]

        # state
        self.markets: dict[str, NormalizedMarket] = {}      # market_id → market
        self.positions: dict[str, SwingPosition] = {}        # market_id → position
        self.live_prices: dict[str, TopOfBook] = {}          # asset_id → top of book
        self.asset_to_market: dict[str, str] = {}            # asset_id → market_id
        self._pending_timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self._ws: MarketWebSocketClient | None = None
        self._last_scan_ts = 0.0
        self._last_cleanup_ts = 0.0
        self._resolve_attempts: dict[str, float] = {}
        self.notifier = notifier

    @staticmethod
    def _side_label(side: str) -> str:
        return "Up" if side == "yes" else "Down"

    @staticmethod
    def _opposite_side(side: str) -> str:
        return "no" if side == "yes" else "yes"

    @staticmethod
    def _token_id_for_side(obj: NormalizedMarket | SwingPosition, side: str) -> str:
        return obj.yes_token_id if side == "yes" else obj.no_token_id

    @staticmethod
    def _top_for_side(yes_top: TopOfBook | None, no_top: TopOfBook | None, side: str) -> TopOfBook | None:
        return yes_top if side == "yes" else no_top

    # ── main loop ────────────────────────────────────────────────

    def run(self) -> None:
        print("[swing] запуск бота")
        # restore open positions from DB
        for pos in self.db.get_open_positions():
            self.positions[pos.market_id] = pos
        if self.positions:
            print(f"[swing] восстановлено {len(self.positions)} позиций из DB")

        try:
            while True:
                now = time.time()

                if now - self._last_scan_ts >= self.scan_interval:
                    self._refresh_markets()
                    self._last_scan_ts = time.time()

                self._check_cutoffs()
                self._resolve_expired()
                if now - self._last_cleanup_ts >= 60:
                    self._cleanup_stale()
                    self._last_cleanup_ts = now

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[swing] остановка")
        finally:
            if self._ws:
                self._ws.stop()

    def scan_once(self) -> None:
        """Single scan cycle for debugging."""
        self._refresh_markets()
        self._print_status()

    # ── market discovery ─────────────────────────────────────────

    def _refresh_markets(self) -> None:
        try:
            raw = self.feed.fetch_markets()
        except Exception as exc:
            print(f"[swing] ошибка fetch_markets: {exc}")
            return

        # filter for target interval only
        filtered = [
            m for m in raw
            if m.interval_minutes in self.interval_minutes_list
        ]

        old_ids = set(self.markets.keys())
        new_ids = set()
        for m in filtered:
            self.markets[m.market_id] = m
            new_ids.add(m.market_id)

        added = new_ids - old_ids
        for mid in old_ids - new_ids:
            self.markets.pop(mid, None)

        if added:
            print(f"[swing] новых рынков: {len(added)} | всего: {len(self.markets)}")

        # rebuild asset → market mapping
        self.asset_to_market.clear()
        asset_ids: list[str] = []
        for m in self.markets.values():
            if m.yes_token_id:
                self.asset_to_market[m.yes_token_id] = m.market_id
                asset_ids.append(m.yes_token_id)
            if m.no_token_id:
                self.asset_to_market[m.no_token_id] = m.market_id
                asset_ids.append(m.no_token_id)

        # reconnect WS if asset set changed
        current_ws_ids = set(self._ws.asset_ids) if self._ws else set()
        if set(asset_ids) != current_ws_ids:
            if self._ws:
                self._ws.stop()
            if asset_ids:
                self._ws = MarketWebSocketClient(
                    url=self.ws_url,
                    asset_ids=asset_ids,
                    on_message=self._on_ws_message,
                )
                self._ws.start()
                print(f"[swing] WS подписка: {len(asset_ids)} токенов")

        self._print_status()

    # ── WS callback ──────────────────────────────────────────────

    def _on_ws_message(self, payload: dict | list[dict]) -> None:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._on_ws_message(item)
            return
        if not isinstance(payload, dict):
            return

        asset_id = payload.get("asset_id", "")
        if not asset_id:
            return

        # update live prices
        event_type = payload.get("event_type", "")
        if event_type in ("price_change", "best_bid_ask"):
            try:
                best_bid = float(payload.get("best_bid", 0))
                best_ask = float(payload.get("best_ask", 0))
            except (TypeError, ValueError):
                return
            self.live_prices[asset_id] = TopOfBook(
                best_bid=best_bid,
                best_ask=best_ask,
                updated_at_ms=int(time.time() * 1000),
            )
        elif event_type == "book":
            asks = payload.get("asks", [])
            bids = payload.get("bids", [])
            try:
                best_ask = float(asks[0]["price"]) if asks else 0.0
                best_bid = float(bids[0]["price"]) if bids else 0.0
            except (TypeError, ValueError, KeyError, IndexError):
                return
            self.live_prices[asset_id] = TopOfBook(
                best_bid=best_bid,
                best_ask=best_ask,
                updated_at_ms=int(time.time() * 1000),
            )
        else:
            return

        market_id = self.asset_to_market.get(asset_id)
        if not market_id:
            return

        self._evaluate_market(market_id)

    # ── signal evaluation ────────────────────────────────────────

    def _evaluate_market(self, market_id: str) -> None:
        market = self.markets.get(market_id)
        if not market:
            return

        with self._lock:
            pos = self.positions.get(market_id)
            pos_state = pos.state if pos else None
            yes_top = self.live_prices.get(market.yes_token_id or "")
            no_top = self.live_prices.get(market.no_token_id or "")

        if pos_state is None or pos_state == SwingState.WATCHING:
            candidates: list[tuple[str, float]] = []
            if (
                yes_top and
                self.min_entry_price <= yes_top.best_ask <= self.max_entry_price
            ):
                candidates.append(("yes", yes_top.best_ask))
            if (
                no_top and
                self.min_entry_price <= no_top.best_ask <= self.max_entry_price
            ):
                candidates.append(("no", no_top.best_ask))
            if candidates:
                entry_side, entry_price = min(candidates, key=lambda item: item[1])
                self._trigger_entry(market_id, market, entry_side, entry_price)
            return

        if pos_state == SwingState.HOLDING:
            entry_side = pos.entry_side
            exit_side = self._opposite_side(entry_side)
            entry_top = self._top_for_side(yes_top, no_top, entry_side)
            exit_top = self._top_for_side(yes_top, no_top, exit_side)
            if entry_top and entry_top.best_bid >= self.sell_target:
                self._trigger_sell(market_id, market, entry_top.best_bid)
                return
            if exit_top and exit_top.best_ask > 0 and exit_top.best_ask <= self.arb_target:
                self._trigger_arb(market_id, market, exit_top.best_ask)
                return

    # ── entry ────────────────────────────────────────────────────

    def _trigger_entry(self, market_id: str, market: NormalizedMarket, entry_side: str, ws_price: float) -> None:
        key = f"entry:{market_id}"
        with self._lock:
            if key in self._pending_timers:
                return
            # check not already positioned
            if market_id in self.positions and self.positions[market_id].state != SwingState.WATCHING:
                return

            # create position placeholder
            now = datetime.utcnow()
            pos = SwingPosition(
                id=str(uuid.uuid4())[:12],
                market_id=market_id,
                symbol=market.symbol,
                interval_minutes=market.interval_minutes or self.interval_minutes,
                market_start=market.expiry - timedelta(minutes=self.interval_minutes),
                market_end=market.expiry,
                yes_token_id=market.yes_token_id or "",
                no_token_id=market.no_token_id or "",
                state=SwingState.PENDING_ENTRY,
                entry_side=entry_side,
                entry_price=ws_price,
                stake_usd=self.stake_usd,
                opened_at=now,
            )
            self.positions[market_id] = pos

            print(f"[swing] ENTRY сигнал {market.symbol} | "
                  f"{self._side_label(entry_side)} ask={ws_price:.3f} | "
                  f"ждём {self.verify_delay}с для REST проверки")

            t = threading.Timer(self.verify_delay, self._verify_entry, args=[market_id, ws_price])
            t.daemon = True
            self._pending_timers[key] = t
            t.start()

    def _verify_entry(self, market_id: str, ws_price: float) -> None:
        self._pending_timers.pop(f"entry:{market_id}", None)

        with self._lock:
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.PENDING_ENTRY:
                return

            # REST orderbook check
            book = self.clob.get_orderbook(self._token_id_for_side(pos, pos.entry_side))
            rest_ask = book.asks[0].price if book and book.asks else None

            if rest_ask is not None and self.min_entry_price <= rest_ask <= self.max_entry_price:
                pos.entry_price_rest = rest_ask
                pos.shares = self.stake_usd / rest_ask
                pos.state = SwingState.HOLDING
                pos.hold_reason = None
                pos.opened_at = datetime.utcnow()

                self.db.open_position(pos)
                self.db.audit("entry_confirmed", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_ask,
                    "shares": round(pos.shares, 4),
                })
                print(f"[swing] ENTRY подтверждён {pos.symbol} | "
                      f"{self._side_label(pos.entry_side)} | ws={ws_price:.3f} rest={rest_ask:.3f} | "
                      f"shares={pos.shares:.2f} @ ${self.stake_usd}")
                if self.notifier:
                    self.notifier.notify_entry(
                        pos.symbol,
                        self._side_label(pos.entry_side),
                        ws_price,
                        rest_ask,
                        pos.shares,
                        self.stake_usd,
                    )
            else:
                # rejected — revert to watching
                actual = rest_ask if rest_ask is not None else "N/A"
                print(f"[swing] ENTRY отклонён {pos.symbol} | "
                      f"{self._side_label(pos.entry_side)} | "
                      f"ws={ws_price:.3f} rest={actual} вне диапазона "
                      f"[{self.min_entry_price:.2f}, {self.max_entry_price:.2f}]")
                self.db.audit("entry_rejected", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_ask,
                    "entry_side": pos.entry_side,
                })
                pos.state = SwingState.WATCHING

    # ── sell exit ────────────────────────────────────────────────

    def _trigger_sell(self, market_id: str, market: NormalizedMarket, ws_price: float) -> None:
        key = f"sell:{market_id}"
        with self._lock:
            if key in self._pending_timers:
                return
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.HOLDING:
                return
            pos.state = SwingState.PENDING_SELL

            print(f"[swing] SELL сигнал {market.symbol} | "
                  f"{self._side_label(pos.entry_side)} bid={ws_price:.3f} | "
                  f"ждём {self.verify_delay}с")

            t = threading.Timer(self.verify_delay, self._verify_sell, args=[market_id, ws_price])
            t.daemon = True
            self._pending_timers[key] = t
            t.start()

    def _verify_sell(self, market_id: str, ws_price: float) -> None:
        self._pending_timers.pop(f"sell:{market_id}", None)

        with self._lock:
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.PENDING_SELL:
                return

            book = self.clob.get_orderbook(self._token_id_for_side(pos, pos.entry_side))
            rest_bid = book.bids[0].price if book and book.bids else None

            if rest_bid is not None and rest_bid >= self.sell_target:
                now = datetime.utcnow()
                pos.exit_type = "sell"
                pos.exit_price = ws_price
                pos.exit_price_rest = rest_bid
                pos.exited_at = now
                pos.state = SwingState.RESOLVED
                pos.hold_reason = None

                pnl = (rest_bid - pos.entry_price_rest) * pos.shares
                pos.pnl = pnl
                pos.resolved_at = now
                self.db.update_state(pos.id, SwingState.RESOLVED,
                                     exit_type="sell", exit_price=ws_price,
                                     exit_price_rest=rest_bid,
                                     exited_at=now,
                                     hold_reason=None,
                                     pnl=round(pnl, 6),
                                     resolved_at=now)
                self.db.audit("sell_confirmed", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_bid,
                    "pnl_estimate": round(pnl, 4),
                })
                print(f"[swing] SELL подтверждён {pos.symbol} | "
                      f"{self._side_label(pos.entry_side)} | "
                      f"entry={pos.entry_price_rest:.3f} exit={rest_bid:.3f} | "
                      f"PnL≈${pnl:.4f}")
                if self.notifier:
                    self.notifier.notify_sell(
                        pos.symbol,
                        self._side_label(pos.entry_side),
                        pos.entry_price_rest,
                        rest_bid,
                        pnl,
                    )
                self.positions.pop(market_id, None)
            else:
                actual = rest_bid if rest_bid is not None else "N/A"
                print(f"[swing] SELL отклонён {pos.symbol} | "
                      f"{self._side_label(pos.entry_side)} | "
                      f"ws={ws_price:.3f} rest={actual} < {self.sell_target}")
                self.db.audit("sell_rejected", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_bid,
                })
                pos.state = SwingState.HOLDING

    # ── arb exit ─────────────────────────────────────────────────

    def _trigger_arb(self, market_id: str, market: NormalizedMarket, ws_price: float) -> None:
        key = f"arb:{market_id}"
        with self._lock:
            if key in self._pending_timers:
                return
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.HOLDING:
                return
            pos.state = SwingState.PENDING_ARB
            exit_side = self._opposite_side(pos.entry_side)

            print(f"[swing] ARB сигнал {market.symbol} | "
                  f"{self._side_label(exit_side)} ask={ws_price:.3f} | "
                  f"ждём {self.verify_delay}с")

            t = threading.Timer(self.verify_delay, self._verify_arb, args=[market_id, ws_price])
            t.daemon = True
            self._pending_timers[key] = t
            t.start()

    def _verify_arb(self, market_id: str, ws_price: float) -> None:
        self._pending_timers.pop(f"arb:{market_id}", None)

        with self._lock:
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.PENDING_ARB:
                return

            exit_side = self._opposite_side(pos.entry_side)
            book = self.clob.get_orderbook(self._token_id_for_side(pos, exit_side))
            rest_ask = book.asks[0].price if book and book.asks else None

            if rest_ask is not None and rest_ask <= self.arb_target:
                now = datetime.utcnow()
                pos.exit_type = "arb"
                pos.exit_price = ws_price
                pos.exit_price_rest = rest_ask
                pos.exited_at = now
                pos.state = SwingState.ARBED
                pos.hold_reason = None

                pnl = 1.0 - pos.entry_price_rest - rest_ask
                pnl_usd = pnl * pos.shares

                self.db.update_state(pos.id, SwingState.ARBED,
                                     exit_type="arb", exit_price=ws_price,
                                     exit_price_rest=rest_ask,
                                     exited_at=now,
                                     hold_reason=None)
                self.db.audit("arb_confirmed", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_ask,
                    "pnl_estimate": round(pnl_usd, 4),
                })
                print(f"[swing] ARB подтверждён {pos.symbol} | "
                      f"{self._side_label(pos.entry_side)}={pos.entry_price_rest:.3f} "
                      f"+ {self._side_label(exit_side)}={rest_ask:.3f} | "
                      f"edge={pnl:.3f} PnL≈${pnl_usd:.4f}")
                if self.notifier:
                    self.notifier.notify_arb(
                        pos.symbol,
                        self._side_label(pos.entry_side),
                        pos.entry_price_rest,
                        self._side_label(exit_side),
                        rest_ask,
                        pnl_usd,
                    )
            else:
                actual = rest_ask if rest_ask is not None else "N/A"
                print(f"[swing] ARB отклонён {pos.symbol} | "
                      f"{self._side_label(exit_side)} | "
                      f"ws={ws_price:.3f} rest={actual} > {self.arb_target}")
                self.db.audit("arb_rejected", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_ask,
                })
                pos.state = SwingState.HOLDING

    # ── flip (cutoff) ────────────────────────────────────────────

    def _check_cutoffs(self) -> None:
        now = datetime.utcnow()
        for market_id, pos in list(self.positions.items()):
            if pos.state != SwingState.HOLDING:
                continue
            cutoff_time = pos.market_start + timedelta(
                seconds=pos.interval_minutes * 60 * self.cutoff_pct
            )
            if now >= cutoff_time:
                self._trigger_flip(market_id, pos)

    def _trigger_flip(self, market_id: str, pos: SwingPosition) -> None:
        key = f"flip:{market_id}"
        with self._lock:
            if key in self._pending_timers:
                return
            if pos.state != SwingState.HOLDING:
                return
            pos.state = SwingState.PENDING_FLIP

            flip_side = self._opposite_side(pos.entry_side)
            flip_top = self._top_for_side(
                self.live_prices.get(pos.yes_token_id),
                self.live_prices.get(pos.no_token_id),
                flip_side,
            )
            ws_price = flip_top.best_ask if flip_top and flip_top.best_ask > 0 else 0.50

            print(f"[swing] FLIP сигнал {pos.symbol} | cutoff {self.cutoff_pct*100:.0f}% | "
                  f"{self._side_label(flip_side)} ask≈{ws_price:.3f} | ждём {self.verify_delay}с")

            t = threading.Timer(self.verify_delay, self._verify_flip, args=[market_id, ws_price])
            t.daemon = True
            self._pending_timers[key] = t
            t.start()

    def _verify_flip(self, market_id: str, ws_price: float) -> None:
        self._pending_timers.pop(f"flip:{market_id}", None)

        with self._lock:
            pos = self.positions.get(market_id)
            if not pos or pos.state != SwingState.PENDING_FLIP:
                return

            flip_side = self._opposite_side(pos.entry_side)
            book = self.clob.get_orderbook(self._token_id_for_side(pos, flip_side))
            rest_ask = book.asks[0].price if book and book.asks else None

            if rest_ask is not None and 0 < rest_ask <= self.max_flip_price:
                now = datetime.utcnow()
                flip_shares = self.stake_usd / rest_ask
                pos.exit_type = "flip"
                pos.exit_price = ws_price
                pos.exit_price_rest = rest_ask
                pos.flip_shares = flip_shares
                pos.exited_at = now
                pos.state = SwingState.FLIPPED
                pos.hold_reason = None

                self.db.update_state(pos.id, SwingState.FLIPPED,
                                     exit_type="flip", exit_price=ws_price,
                                     exit_price_rest=rest_ask,
                                     flip_shares=flip_shares,
                                     exited_at=now,
                                     hold_reason=None)
                self.db.audit("flip_confirmed", pos.id, {
                    "ws_price": ws_price, "rest_price": rest_ask,
                    "flip_shares": round(flip_shares, 4),
                })
                print(f"[swing] FLIP подтверждён {pos.symbol} | "
                      f"{self._side_label(flip_side)}@{rest_ask:.3f} shares={flip_shares:.2f}")
                if self.notifier:
                    self.notifier.notify_flip(
                        pos.symbol,
                        self._side_label(flip_side),
                        rest_ask,
                        flip_shares,
                    )
            else:
                # can't flip — forced hold
                actual = f"{rest_ask:.3f}" if rest_ask is not None else "N/A"
                if rest_ask is None or rest_ask <= 0:
                    reason = "no_flip_liquidity"
                    detail = "стакан пуст"
                else:
                    reason = "flip_price_too_high"
                    detail = f"цена {actual} > {self.max_flip_price:.2f}"
                print(f"[swing] FLIP не удался {pos.symbol} | {detail}, держим")
                self.db.audit("flip_failed", pos.id, {
                    "ws_price": ws_price,
                    "rest_price": rest_ask,
                    "reason": reason,
                })
                pos.hold_reason = reason
                self.db.update_state(pos.id, SwingState.HOLDING, hold_reason=pos.hold_reason)
                pos.state = SwingState.HOLDING

    # ── resolution ───────────────────────────────────────────────

    def _resolve_expired(self) -> None:
        now = datetime.utcnow()
        resolve_after = timedelta(seconds=30)  # wait 30s after expiry

        for market_id, pos in list(self.positions.items()):
            if pos.state == SwingState.RESOLVED:
                continue
            if pos.market_end is None:
                continue
            if now < pos.market_end + resolve_after:
                continue
            # only resolve terminal or holding states
            if pos.state in (SwingState.WATCHING, SwingState.PENDING_ENTRY):
                # never entered — just clean up
                self.positions.pop(market_id, None)
                continue
            if pos.state.value.startswith("pending_"):
                continue  # wait for timer to finish

            self._resolve_one(pos)

    def _resolve_one(self, pos: SwingPosition) -> None:
        last = self._resolve_attempts.get(pos.market_id, 0.0)
        if time.time() - last < 10:
            return
        self._resolve_attempts[pos.market_id] = time.time()

        winning_side = check_polymarket_result(pos.market_id)
        if winning_side is None:
            return

        if pos.exit_type is None and not pos.hold_reason:
            pos.hold_reason = "no_exit_signal"

        pnl = self._calc_pnl(pos, winning_side)
        pos.winning_side = winning_side
        pos.pnl = pnl
        pos.state = SwingState.RESOLVED
        pos.resolved_at = datetime.utcnow()

        self._resolve_attempts.pop(pos.market_id, None)
        self.db.resolve_position(pos.id, winning_side, pnl, hold_reason=pos.hold_reason)
        self.db.audit("resolved", pos.id, {
            "winning_side": winning_side,
            "exit_type": pos.exit_type,
            "hold_reason": pos.hold_reason,
            "pnl": round(pnl, 4),
        })

        stats = self.db.stats()
        hold_note = f" | reason={pos.hold_reason}" if pos.exit_type is None and pos.hold_reason else ""
        print(f"[swing] RESOLVED {pos.symbol} | winner={winning_side} | "
              f"exit={pos.exit_type or 'hold'}{hold_note} | PnL=${pnl:+.4f} | "
              f"cumulative=${stats['realized_pnl']:+.4f} ({stats['resolved']} trades)")
        if self.notifier:
            self.notifier.notify_resolve(
                pos.symbol,
                pos.exit_type,
                pos.hold_reason,
                winning_side,
                pnl,
                stats["realized_pnl"],
            )

    def _calc_pnl(self, pos: SwingPosition, winning_side: str) -> float:
        entry_cost = pos.entry_price_rest * pos.shares if pos.entry_price_rest else 0.0

        if pos.exit_type == "sell":
            # sold entered side for exit_price_rest → PnL = (sell - buy) * shares
            sell_revenue = (pos.exit_price_rest or 0.0) * pos.shares
            return sell_revenue - entry_cost

        if pos.exit_type == "arb":
            arb_cost = (pos.exit_price_rest or 0.0) * pos.shares
            return pos.shares - entry_cost - arb_cost

        if pos.exit_type == "flip":
            entry_won = winning_side == pos.entry_side
            entry_pnl = (pos.shares - entry_cost) if entry_won else -entry_cost
            flip_shares = pos.flip_shares or 0.0
            flip_cost = (pos.exit_price_rest or 0.50) * flip_shares
            flip_won = winning_side == self._opposite_side(pos.entry_side)
            flip_pnl = (flip_shares - flip_cost) if flip_won else -flip_cost
            return entry_pnl + flip_pnl

        won = winning_side == pos.entry_side
        return (pos.shares - entry_cost) if won else -entry_cost

    # ── cleanup ──────────────────────────────────────────────────

    def _cleanup_stale(self) -> None:
        """Remove resolved positions from memory after a while."""
        now = datetime.utcnow()
        stale = []
        for mid, pos in self.positions.items():
            if pos.state == SwingState.RESOLVED and pos.resolved_at:
                if now - pos.resolved_at > timedelta(minutes=10):
                    stale.append(mid)
        for mid in stale:
            self.positions.pop(mid, None)

    # ── status ───────────────────────────────────────────────────

    def _print_status(self) -> None:
        stats = self.db.stats()
        n_markets = len(self.markets)
        n_holding = sum(1 for p in self.positions.values()
                        if p.state in (SwingState.HOLDING, SwingState.PENDING_SELL,
                                       SwingState.PENDING_ARB, SwingState.PENDING_FLIP))
        n_pending = sum(1 for p in self.positions.values()
                        if p.state == SwingState.PENDING_ENTRY)
        print(f"[swing] рынков={n_markets} | holding={n_holding} pending={n_pending} | "
              f"resolved={stats['resolved']} PnL=${stats['realized_pnl']:+.4f}")

    def get_status_text(self) -> str:
        stats = self.db.stats()
        lines = [
            f"Swing Bot Status",
            f"Markets tracked: {len(self.markets)}",
            f"Positions: {stats['open']} open, {stats['resolved']} resolved",
            f"Realized PnL: ${stats['realized_pnl']:+.4f}",
            f"Stake: ${self.stake_usd}",
        ]
        for pos in self.positions.values():
            if pos.state != SwingState.RESOLVED:
                ep = pos.entry_price_rest or pos.entry_price or 0.0
                extra = f" reason={pos.hold_reason}" if pos.hold_reason else ""
                lines.append(
                    f"  {pos.symbol} [{pos.state.value}] {self._side_label(pos.entry_side)} entry={ep:.3f}{extra}"
                )
        return "\n".join(lines)
