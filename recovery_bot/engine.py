from __future__ import annotations

import math
import threading
from datetime import datetime, timedelta, timezone

from cross_arb_bot.polymarket_feed import PolymarketFeed
from oracle_arb_bot.models import OracleMarket
from oracle_arb_bot.scanner import OracleScanner
from real_arb_bot.clients import PolymarketTrader

from recovery_bot.db import RecoveryDB
from recovery_bot.models import RecoveryConfig, RecoveryPosition, TrackedRecovery
from recovery_bot.telegram_notify import RecoveryTelegramNotifier


class RecoveryEngine:
    def __init__(
        self,
        config: dict,
        db: RecoveryDB,
        notifier: RecoveryTelegramNotifier | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.strategy = config["strategy"]
        self.runtime = config["runtime"]
        self._lock = threading.Lock()
        self._states: dict[tuple[str, str], TrackedRecovery] = {}
        self._configs_by_interval: dict[int, RecoveryConfig] = {
            5: RecoveryConfig(
                name="5m_base",
                interval_minutes=5,
                bottom_price=float(self.strategy["five_minute"]["bottom_price"]),
                entry_price=float(self.strategy["five_minute"]["entry_price"]),
                activation_delay_seconds=int(self.strategy["five_minute"]["activation_delay_seconds"]),
                paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                real_stake_usd=float(self.strategy["real_stake_usd"]),
            ),
            15: RecoveryConfig(
                name="15m_wait30",
                interval_minutes=15,
                bottom_price=float(self.strategy["fifteen_minute"]["bottom_price"]),
                entry_price=float(self.strategy["fifteen_minute"]["entry_price"]),
                activation_delay_seconds=int(self.strategy["fifteen_minute"]["activation_delay_seconds"]),
                paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                real_stake_usd=float(self.strategy["real_stake_usd"]),
            ),
        }
        self.pm_feed = PolymarketFeed(
            base_url=config["polymarket"]["gamma_base_url"],
            page_size=config["polymarket"]["page_size"],
            request_delay_ms=config["polymarket"]["request_delay_ms"],
            market_filter=config["market_filter"],
        )
        self.scanner = OracleScanner(config)
        self.scanner.set_pm_price_callback(self.on_pm_price)
        self.pm_trader = PolymarketTrader() if self.strategy.get("real_enabled", False) else None
        if self.strategy.get("real_enabled", False):
            self.db.init_real_deposit(float(self.strategy.get("initial_real_deposit_usd", 15.0)))

    def scan_markets(self) -> list[OracleMarket]:
        return self.scanner.scan_and_subscribe()

    def stop(self) -> None:
        self.scanner.stop()

    def on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        if market.venue != "polymarket" or side != "no":
            return
        cfg = self._configs_by_interval.get(market.interval_minutes)
        if cfg is None:
            return
        self._process_working_paper(market, cfg, best_ask)
        state_key = (market.market_id, cfg.name)
        now = datetime.utcnow()
        should_place = False
        touch_ts = None
        armed_ts = None
        touch_price = None
        with self._lock:
            state = self._states.get(state_key)
            if state is None:
                state = TrackedRecovery(
                    market_id=market.market_id,
                    config_name=cfg.name,
                    symbol=market.symbol,
                    interval_minutes=market.interval_minutes,
                )
                self._states[state_key] = state
            if state.done:
                return
            if state.touch_ts is None and best_ask <= cfg.bottom_price:
                state.touch_ts = now
                state.touch_price = best_ask
                print(
                    f"[recovery] touch {market.symbol} {market.interval_minutes}m"
                    f" | no={best_ask:.3f} <= {cfg.bottom_price:.2f}"
                )
            if state.touch_ts is None:
                return
            if state.armed_ts is None:
                arm_at = state.touch_ts + timedelta(seconds=cfg.activation_delay_seconds)
                if now >= arm_at:
                    if best_ask < cfg.entry_price:
                        state.armed_ts = now
                        print(
                            f"[recovery] armed {market.symbol} {market.interval_minutes}m"
                            f" [{cfg.name}] | no={best_ask:.3f}"
                        )
                        if not state.orders_placed:
                            state.orders_placed = True
                            should_place = True
                            touch_ts = state.touch_ts
                            armed_ts = state.armed_ts
                            touch_price = float(state.touch_price or best_ask)
                    else:
                        state.done = True
                        state.note = "recovered_before_activation"
                if state.armed_ts is None:
                    return
        if not should_place:
            return
        thread = threading.Thread(
            target=self._place_orders,
            args=(market, cfg, touch_ts, armed_ts, touch_price, best_ask),
            daemon=True,
            name=f"recovery-open-{market.symbol}-{market.interval_minutes}",
        )
        thread.start()

    def _place_orders(
        self,
        market: OracleMarket,
        cfg: RecoveryConfig,
        touch_ts: datetime,
        armed_ts: datetime,
        touch_price: float,
        trigger_price: float,
    ) -> None:
        token_id = market.no_token_id
        if not token_id:
            return
        if self.strategy.get("paper_enabled", True) and not self.db.has_market_record(market.market_id, cfg.name, "paper"):
            stake = cfg.paper_stake_usd
            shares = stake / cfg.entry_price
            total_cost = shares * cfg.entry_price
            self.db.open_position(
                market_id=market.market_id,
                symbol=market.symbol,
                title=market.title,
                interval_minutes=market.interval_minutes,
                market_start=market.market_start,
                market_end=market.expiry,
                side="no",
                mode="paper",
                strategy_name=cfg.name,
                touch_ts=touch_ts,
                armed_ts=armed_ts,
                touch_price=touch_price,
                trigger_price=trigger_price,
                entry_price=cfg.entry_price,
                requested_shares=shares,
                filled_shares=0.0,
                total_cost=total_cost,
                fee=0.0,
                status="working",
                pm_token_id=token_id,
                note=f"paper resting from {trigger_price:.3f}",
            )
            print(
                f"[recovery] PAPER WORKING {market.symbol} {market.interval_minutes}m NO"
                f" [{cfg.name}] | touch={touch_price:.3f} | armed={trigger_price:.3f}"
                f" | limit={cfg.entry_price:.3f}"
            )

        if self.strategy.get("real_enabled", False) and not self.db.has_market_record(market.market_id, cfg.name, "real"):
            if self.pm_trader is None:
                return
            requested_shares = self._round_shares(cfg.real_stake_usd / cfg.entry_price)
            if requested_shares <= 0:
                return
            reserved_cost = requested_shares * cfg.entry_price
            if reserved_cost > self.real_balance():
                print(
                    f"[recovery] skip real {market.symbol} {market.interval_minutes}m:"
                    f" reserve=${reserved_cost:.2f} > virtual=${self.real_balance():.2f}"
                )
                return
            result = self.pm_trader.place_limit_buy_order(
                token_id=token_id,
                price=cfg.entry_price,
                size=requested_shares,
                wait_seconds=float(self.strategy.get("real_order_wait_seconds", 0.5)),
            )
            if not result.order_id:
                self.db.open_position(
                    market_id=market.market_id,
                    symbol=market.symbol,
                    title=market.title,
                    interval_minutes=market.interval_minutes,
                    market_start=market.market_start,
                    market_end=market.expiry,
                    side="no",
                    mode="real",
                    strategy_name=cfg.name,
                    touch_ts=touch_ts,
                    armed_ts=armed_ts,
                    touch_price=touch_price,
                    trigger_price=trigger_price,
                    entry_price=cfg.entry_price,
                    requested_shares=requested_shares,
                    filled_shares=0.0,
                    total_cost=0.0,
                    fee=0.0,
                    status="error",
                    pm_token_id=token_id,
                    note=result.status or "order_submit_failed",
                )
                return
            pos = self.db.open_position(
                market_id=market.market_id,
                symbol=market.symbol,
                title=market.title,
                interval_minutes=market.interval_minutes,
                market_start=market.market_start,
                market_end=market.expiry,
                side="no",
                mode="real",
                strategy_name=cfg.name,
                touch_ts=touch_ts,
                armed_ts=armed_ts,
                touch_price=touch_price,
                trigger_price=trigger_price,
                entry_price=cfg.entry_price,
                requested_shares=requested_shares,
                filled_shares=0.0,
                total_cost=reserved_cost,
                fee=0.0,
                status="working",
                pm_token_id=token_id,
                pm_order_id=result.order_id,
                note=result.status,
            )
            self.db.deduct_real_deposit(reserved_cost)
            print(
                f"[recovery] REAL WORKING {market.symbol} {market.interval_minutes}m NO"
                f" [{cfg.name}] | touch={touch_price:.3f} | armed={trigger_price:.3f}"
                f" | limit={cfg.entry_price:.3f} | reserve=${reserved_cost:.2f}"
            )
            self._sync_working_position(pos, cancel_if_partial=False)

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._poll_working_orders(now=now)
        for position in self.db.get_open_positions():
            if position.market_end > now:
                continue
            winning_side = self._resolve_polymarket(position.market_id)
            if winning_side is None:
                continue
            pnl = position.filled_shares - position.total_cost if winning_side == "no" else -position.total_cost
            self.db.resolve_position(position.id, winning_side=winning_side, pnl=pnl)
            if position.mode == "real" and winning_side == "no":
                self.db.add_real_deposit(position.filled_shares)
            print(
                f"[recovery] RESOLVE {position.symbol} {position.interval_minutes}m"
                f" [{position.mode}] | winner={winning_side} | pnl=${pnl:+.2f}"
            )
            if self.notifier:
                self.notifier.notify_resolve(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    pnl=pnl,
                    winning_side=winning_side,
                )

    def _resolve_polymarket(self, market_id: str) -> str | None:
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None or len(market.outcomes) != len(market.outcome_prices):
            return None
        try:
            up_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "up")
            down_idx = next(i for i, outcome in enumerate(market.outcomes) if outcome.lower() == "down")
        except StopIteration:
            return None
        if market.outcome_prices[up_idx] >= 0.9:
            return "yes"
        if market.outcome_prices[down_idx] >= 0.9:
            return "no"
        return None

    def get_status_text(self) -> str:
        paper = self.db.stats_by_mode("paper")
        real = self.db.stats_by_mode("real")
        active = self._active_state_counts()
        lines = ["<b>Recovery Bot</b>"]
        lines.append(self._format_mode_status("PAPER", paper))
        lines.append(self._format_mode_status("REAL", real))
        if self.db.has_real_deposit():
            balance, peak = self.db.get_real_deposit()
            lines.append(f"real deposit: ${balance:.2f} / peak ${peak:.2f}")
        lines.append(f"tracking: touch={active['touched']} armed={active['armed']}")
        return "\n".join(lines)

    def print_status(self) -> None:
        print(self.get_status_text())
        for pos in self.db.get_working_positions()[:10]:
            print(
                f"  WORKING {pos.symbol} {pos.interval_minutes}m NO"
                f" [{pos.strategy_name}] [real] @ {pos.entry_price:.3f}"
                f" req={pos.requested_shares:.2f} order={pos.pm_order_id or '-'}"
            )
        for pos in self.db.get_open_positions()[:10]:
            print(
                f"  OPEN {pos.symbol} {pos.interval_minutes}m NO"
                f" [{pos.strategy_name}] [{pos.mode}] @ {pos.entry_price:.3f}"
                f" shares={pos.filled_shares:.2f} cost=${pos.total_cost:.2f}"
            )

    def _active_state_counts(self) -> dict[str, int]:
        touched = 0
        armed = 0
        with self._lock:
            for state in self._states.values():
                if state.done:
                    continue
                if state.touch_ts is not None:
                    touched += 1
                if state.armed_ts is not None:
                    armed += 1
        return {"touched": touched, "armed": armed}

    @staticmethod
    def _round_shares(value: float) -> float:
        return math.floor(value * 100) / 100.0

    def real_balance(self) -> float:
        balance, _ = self.db.get_real_deposit()
        return balance

    def can_place_real_bet(self) -> bool:
        if not self.strategy.get("real_enabled", False):
            return False
        return self.real_balance() >= float(self.strategy.get("real_stake_usd", 1.0))

    @staticmethod
    def _format_mode_status(label: str, stats: dict[str, float | int]) -> str:
        resolved = int(stats["resolved_count"])
        won = int(stats["won_count"])
        lost = int(stats["lost_count"])
        wr = (won / resolved * 100.0) if resolved else 0.0
        return (
            f"{label}: pnl=${float(stats['realized_pnl']):+.2f} | "
            f"working={int(stats['working_count'])} open={int(stats['open_count'])} resolved={resolved} | "
            f"won={won} lost={lost} | "
            f"unfilled={int(stats['unfilled_count'])} | "
            f"wr={wr:.1f}%"
        )

    def _poll_working_orders(self, now: datetime) -> None:
        for position in self.db.get_working_positions():
            if position.mode != "real":
                continue
            if position.market_end <= now:
                self._sync_working_position(position, cancel_if_partial=True, cancel_if_unfilled=True)
                continue
            seconds_left = (position.market_end - now).total_seconds()
            cancel_before = float(self.strategy.get("cancel_before_expiry_seconds", 15))
            cancel_if_partial = seconds_left <= cancel_before
            self._sync_working_position(position, cancel_if_partial=cancel_if_partial)

    def _sync_working_position(
        self,
        position: RecoveryPosition,
        *,
        cancel_if_partial: bool,
        cancel_if_unfilled: bool = False,
    ) -> None:
        if self.pm_trader is None or not position.pm_order_id:
            return
        try:
            info = self.pm_trader.fetch_order(position.pm_order_id)
        except Exception as exc:
            print(f"[recovery] order sync failed {position.symbol}: {exc}")
            return

        status = str(info.get("status", "")).lower()
        requested = float(info.get("original_size", position.requested_shares) or position.requested_shares)
        matched = float(info.get("size_matched", 0) or 0.0)
        price = float(info.get("price", position.entry_price) or position.entry_price)
        fee = matched * price * 0.25 * ((price * (1 - price)) ** 2) if matched > 0 else 0.0
        actual_cost = matched * price + fee
        reserved_cost = position.total_cost

        is_full = matched >= requested - 1e-9 and matched > 0
        is_partial = 0 < matched < requested - 1e-9
        is_dead = status in {"cancelled", "canceled", "expired", "unmatched", "live_expired"}

        if is_partial and cancel_if_partial:
            self.pm_trader.cancel_order(position.pm_order_id)
            try:
                info = self.pm_trader.fetch_order(position.pm_order_id)
                status = str(info.get("status", status)).lower()
                matched = float(info.get("size_matched", matched) or matched)
                price = float(info.get("price", price) or price)
                fee = matched * price * 0.25 * ((price * (1 - price)) ** 2) if matched > 0 else 0.0
                actual_cost = matched * price + fee
                is_full = matched >= requested - 1e-9 and matched > 0
                is_partial = 0 < matched < requested - 1e-9
                is_dead = status in {"cancelled", "canceled", "expired", "unmatched", "live_expired"}
            except Exception:
                pass

        if is_full or (is_partial and (cancel_if_partial or is_dead)):
            refund = max(0.0, reserved_cost - actual_cost)
            if refund > 0:
                self.db.add_real_deposit(refund)
            self.db.mark_position_open(
                position.id,
                entry_price=price,
                filled_shares=matched,
                total_cost=actual_cost,
                fee=fee,
                note=status or position.note,
            )
            updated = self.db.get_open_positions()[0] if self.db.get_open_positions() else None
            if self.notifier:
                self.notifier.notify_open(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    strategy_name=position.strategy_name,
                    touch_price=position.touch_price,
                    trigger_price=position.trigger_price,
                    entry_price=price,
                    filled_shares=matched,
                    total_cost=actual_cost,
                )
            print(
                f"[recovery] FILLED {position.symbol} {position.interval_minutes}m"
                f" | shares={matched:.2f}/{requested:.2f} @ {price:.3f} | cost=${actual_cost:.2f}"
            )
            return

        if (matched <= 0 and cancel_if_unfilled) or (matched <= 0 and is_dead):
            if not is_dead and cancel_if_unfilled:
                self.pm_trader.cancel_order(position.pm_order_id)
            self.db.add_real_deposit(reserved_cost)
            self.db.mark_position_unfilled(position.id, note=status or "unfilled")
            print(
                f"[recovery] UNFILLED {position.symbol} {position.interval_minutes}m"
                f" | released=${reserved_cost:.2f}"
            )

    def _process_working_paper(self, market: OracleMarket, cfg: RecoveryConfig, best_ask: float) -> None:
        slippage = float(self.strategy.get("paper_fill_max_slippage", 0.02))
        for position in self.db.get_working_positions():
            if position.mode != "paper":
                continue
            if position.market_id != market.market_id or position.strategy_name != cfg.name:
                continue
            if best_ask < position.entry_price:
                continue
            if best_ask > position.entry_price + slippage:
                continue
            shares = position.requested_shares
            total_cost = shares * position.entry_price
            self.db.mark_position_open(
                position.id,
                entry_price=position.entry_price,
                filled_shares=shares,
                total_cost=total_cost,
                fee=0.0,
                note=f"paper observed at {best_ask:.3f}",
            )
            print(
                f"[recovery] PAPER FILLED {market.symbol} {market.interval_minutes}m"
                f" [{cfg.name}] | touch={position.touch_price:.3f}"
                f" | observed={best_ask:.3f} | entry={position.entry_price:.3f}"
            )
            if self.notifier:
                self.notifier.notify_open(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    strategy_name=position.strategy_name,
                    touch_price=position.touch_price,
                    trigger_price=best_ask,
                    entry_price=position.entry_price,
                    filled_shares=shares,
                    total_cost=total_cost,
                )
            return
