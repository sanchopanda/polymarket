from __future__ import annotations

import calendar
import math
import threading
from datetime import datetime, timedelta, timezone

from cross_arb_bot.polymarket_feed import PolymarketFeed
from oracle_arb_bot.models import OracleMarket
from oracle_arb_bot.scanner import OracleScanner
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from real_arb_bot.clients import PolymarketTrader
from src.api.clob import ClobClient

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
        self._states: dict[tuple[str, str, str], TrackedRecovery] = {}
        self._placed_markets: set[tuple[str, str]] = set()  # (market_id, cfg.name) — один ордер на рынок
        self._configs_by_interval: dict[int, RecoveryConfig] = {
            5: RecoveryConfig(
                name="5m_base",
                interval_minutes=5,
                bottom_price=float(self.strategy["five_minute"]["bottom_price"]),
                entry_price=float(self.strategy["five_minute"]["entry_price"]),
                top_price=float(self.strategy["five_minute"]["top_price"]),
                activation_delay_seconds=int(self.strategy["five_minute"]["activation_delay_seconds"]),
                paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                real_stake_usd=float(self.strategy["real_stake_usd"]),
            ),
            15: RecoveryConfig(
                name="15m_wait30",
                interval_minutes=15,
                bottom_price=float(self.strategy["fifteen_minute"]["bottom_price"]),
                entry_price=float(self.strategy["fifteen_minute"]["entry_price"]),
                top_price=float(self.strategy["fifteen_minute"]["top_price"]),
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
        self._clob = ClobClient(base_url=config["polymarket"]["clob_base_url"])
        if self.strategy.get("real_enabled", False):
            self.db.init_real_deposit(float(self.strategy.get("initial_real_deposit_usd", 15.0)))

    def scan_markets(self) -> list[OracleMarket]:
        return self.scanner.scan_and_subscribe()

    def stop(self) -> None:
        self.scanner.stop()

    def on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        if market.venue != "polymarket" or side not in ("yes", "no"):
            return
        cfg = self._configs_by_interval.get(market.interval_minutes)
        if cfg is None:
            return
        now = datetime.utcnow()
        seconds_left = (market.expiry - now).total_seconds()
        # Нужно достаточно времени для recovery + исполнения ордера
        min_seconds = 90 if market.interval_minutes == 5 else 120
        if seconds_left < min_seconds:
            return
        state_key = (market.market_id, cfg.name, side)
        market_key = (market.market_id, cfg.name)
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
                    side=side,
                )
                self._states[state_key] = state
            if state.done:
                return
            # 1. Ждём касания дна
            if state.touch_ts is None and best_ask <= cfg.bottom_price:
                state.touch_ts = now
                state.touch_price = best_ask
                print(
                    f"[recovery] touch {market.symbol} {market.interval_minutes}m {side.upper()}"
                    f" | ask={best_ask:.3f} <= {cfg.bottom_price:.2f}"
                    f" | {int(seconds_left)}s left"
                )
            if state.touch_ts is None:
                return
            # 2. Ждём восстановления в зону [entry_price, top_price] после задержки
            if state.armed_ts is None:
                elapsed = (now - state.touch_ts).total_seconds()
                if elapsed < cfg.activation_delay_seconds:
                    return  # слишком быстрое восстановление, ждём
                if best_ask > cfg.top_price:
                    state.done = True
                    if best_ask >= 0.9:
                        state.note = "resolved_spike"
                        print(
                            f"[recovery] resolved {market.symbol} {market.interval_minutes}m {side.upper()}"
                            f" | ask={best_ask:.3f} (resolution spike)"
                        )
                    else:
                        state.note = "overshot_top_price"
                        print(
                            f"[recovery] overshot {market.symbol} {market.interval_minutes}m {side.upper()}"
                            f" | ask={best_ask:.3f} > {cfg.top_price:.2f}"
                        )
                    return
                if best_ask >= cfg.entry_price:
                    state.armed_ts = now
                    print(
                        f"[recovery] armed {market.symbol} {market.interval_minutes}m {side.upper()}"
                        f" [{cfg.name}] | ask={best_ask:.3f} (trigger>={cfg.entry_price:.2f})"
                    )
                    if market_key in self._placed_markets:
                        state.done = True
                        state.note = "other_side_placed"
                        print(
                            f"[recovery] skip {market.symbol} {market.interval_minutes}m {side.upper()}"
                            f" — ордер уже размещён на другой стороне"
                        )
                        return
                    if not state.orders_placed:
                        state.orders_placed = True
                        self._placed_markets.add(market_key)
                        should_place = True
                        touch_ts = state.touch_ts
                        armed_ts = state.armed_ts
                        touch_price = float(state.touch_price or best_ask)
                if state.armed_ts is None:
                    return
        if not should_place:
            return
        thread = threading.Thread(
            target=self._place_orders,
            args=(market, cfg, touch_ts, armed_ts, touch_price, best_ask, side),
            daemon=True,
            name=f"recovery-open-{market.symbol}-{market.interval_minutes}-{side}",
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
        side: str,
    ) -> None:
        token_id = market.no_token_id if side == "no" else market.yes_token_id
        side_upper = side.upper()

        if self.strategy.get("real_enabled", False) and not self.db.has_market_record(market.market_id, cfg.name, "real", side=side):
            if self.pm_trader is None:
                return
            if not token_id:
                print(f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}: token_id missing")
                return
            requested_shares = self._compute_order_size(self._scaled_stake_usd(), cfg.top_price)
            if requested_shares <= 0:
                return
            reserved_cost = requested_shares * cfg.entry_price
            if reserved_cost > self.real_balance():
                print(
                    f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}:"
                    f" reserve=${reserved_cost:.2f} > virtual=${self.real_balance():.2f}"
                )
                return
            try:
                result = self.pm_trader.place_limit_buy_order(
                    token_id=token_id,
                    price=cfg.top_price,
                    size=requested_shares,
                    wait_seconds=float(self.strategy.get("real_order_wait_seconds", 0.5)),
                )
            except Exception as exc:
                exc_text = str(exc)
                if "lower than the minimum" in exc_text:
                    # рынок требует минимум > нашего размера — молча скипаем
                    print(
                        f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}:"
                        f" min size слишком большой для нашего stake"
                    )
                    return
                print(f"[recovery] order error {market.symbol} {market.interval_minutes}m {side_upper}: {exc}")
                self.db.open_position(
                    market_id=market.market_id,
                    symbol=market.symbol,
                    title=market.title,
                    interval_minutes=market.interval_minutes,
                    market_start=market.market_start,
                    market_end=market.expiry,
                    side=side,
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
                    note=exc_text,
                )
                return
            if not result.order_id:
                self.db.open_position(
                    market_id=market.market_id,
                    symbol=market.symbol,
                    title=market.title,
                    interval_minutes=market.interval_minutes,
                    market_start=market.market_start,
                    market_end=market.expiry,
                    side=side,
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
                side=side,
                mode="real",
                strategy_name=cfg.name,
                touch_ts=touch_ts,
                armed_ts=armed_ts,
                touch_price=touch_price,
                trigger_price=trigger_price,
                entry_price=cfg.top_price,
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
                f"[recovery] REAL WORKING {market.symbol} {market.interval_minutes}m {side_upper}"
                f" [{cfg.name}] | touch={touch_price:.3f} | trigger={trigger_price:.3f}"
                f" | limit={cfg.top_price:.3f} | reserve=${reserved_cost:.2f}"
            )
            self._sync_working_position(pos, cancel_if_partial=False)

        if self.strategy.get("paper_enabled", True) and not self.db.has_market_record(market.market_id, cfg.name, "paper", side=side):
            self._place_paper_via_book(
                market=market,
                cfg=cfg,
                touch_ts=touch_ts,
                armed_ts=armed_ts,
                touch_price=touch_price,
                trigger_price=trigger_price,
                side=side,
                token_id=token_id,
            )

        # Пост-патч real entry из paper: если первый sync пометил real как open
        # c entry=limit (API не отдал реальные fill'ы), а paper уже собрал цену
        # по стакану — берём её как оценку реальной цены входа.
        self._maybe_patch_real_from_paper(market=market, cfg=cfg, side=side)

    def _maybe_patch_real_from_paper(
        self, *, market: OracleMarket, cfg: RecoveryConfig, side: str
    ) -> None:
        if not self.strategy.get("real_enabled", False):
            return
        real_pos = self.db.get_real_working_or_open(market.market_id, cfg.name, side)
        if real_pos is None or real_pos.status != "open":
            return
        if abs(real_pos.entry_price - cfg.top_price) > 1e-6:
            return
        paper_entry = self.db.get_paper_entry_price(market.market_id, cfg.name, side)
        if paper_entry is None or not (0.0 < paper_entry < 1.0):
            return
        shares = real_pos.filled_shares
        if shares <= 0:
            return
        new_fee = 0.0
        new_total_cost = shares * paper_entry
        reserved = real_pos.requested_shares * cfg.entry_price
        old_debit = min(reserved, real_pos.total_cost)
        new_debit = min(reserved, new_total_cost)
        balance_delta = old_debit - new_debit  # >0 → вернуть на баланс
        patched = self.db.patch_real_fill(
            real_pos.id,
            entry_price=paper_entry,
            total_cost=new_total_cost,
            fee=new_fee,
            note=(real_pos.note or "") + " | entry<-paper",
        )
        if not patched:
            return
        if balance_delta > 0:
            self.db.add_real_deposit(balance_delta)
        elif balance_delta < 0:
            self.db.deduct_real_deposit(-balance_delta)
        print(
            f"[recovery] PATCH {market.symbol} {market.interval_minutes}m {side.upper()}"
            f" entry={real_pos.entry_price:.4f}->{paper_entry:.4f}"
            f" | cost=${real_pos.total_cost:.4f}->${new_total_cost:.4f}"
            f" | delta_bal=${balance_delta:+.4f}"
        )

    def _place_paper_via_book(
        self,
        *,
        market: OracleMarket,
        cfg: RecoveryConfig,
        touch_ts: datetime,
        armed_ts: datetime,
        touch_price: float,
        trigger_price: float,
        side: str,
        token_id: str | None,
    ) -> None:
        """Paper заполняется по реальному стакану: идём по asks до top_price.
        Требуется 2× paper_stake_usd доступной глубины, иначе пропускаем."""
        side_upper = side.upper()
        if not token_id:
            print(f"[recovery] skip paper {market.symbol} {market.interval_minutes}m {side_upper}: token_id missing")
            return
        book = self._clob.get_orderbook(token_id)
        if book is None or not book.asks:
            print(f"[recovery] skip paper {market.symbol} {market.interval_minutes}m {side_upper}: orderbook empty")
            return
        stake = cfg.paper_stake_usd
        required_depth = 2.0 * stake
        # asks возрастают по цене; считаем глубину в зоне [best, top_price]
        depth_usd = 0.0
        for lvl in book.asks:
            if lvl.price > cfg.top_price:
                break
            depth_usd += lvl.price * lvl.size
        if depth_usd < required_depth:
            print(
                f"[recovery] skip paper {market.symbol} {market.interval_minutes}m {side_upper}:"
                f" depth=${depth_usd:.2f} < 2×stake=${required_depth:.2f} в [<={cfg.top_price:.2f}]"
            )
            return
        # Симулируем fill по stake_usd
        remaining = stake
        total_shares = 0.0
        total_cost = 0.0
        for lvl in book.asks:
            if lvl.price > cfg.top_price:
                break
            level_usd = lvl.price * lvl.size
            spend = min(remaining, level_usd)
            total_shares += spend / lvl.price
            total_cost += spend
            remaining -= spend
            if remaining < 1e-6:
                break
        if total_shares <= 0:
            return
        avg_fill = total_cost / total_shares
        pos = self.db.open_position(
            market_id=market.market_id,
            symbol=market.symbol,
            title=market.title,
            interval_minutes=market.interval_minutes,
            market_start=market.market_start,
            market_end=market.expiry,
            side=side,
            mode="paper",
            strategy_name=cfg.name,
            touch_ts=touch_ts,
            armed_ts=armed_ts,
            touch_price=touch_price,
            trigger_price=trigger_price,
            entry_price=avg_fill,
            requested_shares=total_shares,
            filled_shares=total_shares,
            total_cost=total_cost,
            fee=0.0,
            status="open",
            pm_token_id=token_id,
            note=f"paper via book: avg={avg_fill:.4f} depth=${depth_usd:.2f}",
        )
        print(
            f"[recovery] PAPER FILLED {market.symbol} {market.interval_minutes}m {side_upper}"
            f" [{cfg.name}] | touch={touch_price:.3f} | avg={avg_fill:.4f}"
            f" | shares={total_shares:.2f} | cost=${total_cost:.2f} | depth=${depth_usd:.2f}"
        )
        if self.notifier:
            market_url = market.pm_event_slug and f"https://polymarket.com/event/{market.pm_event_slug}"
            msg_id = self.notifier.notify_open(
                symbol=market.symbol,
                interval_minutes=market.interval_minutes,
                mode="paper",
                strategy_name=cfg.name,
                side=side,
                touch_price=touch_price,
                trigger_price=trigger_price,
                entry_price=avg_fill,
                filled_shares=total_shares,
                total_cost=total_cost,
                market_url=market_url,
            )
            if msg_id is not None:
                self.db.set_open_message_id(pos.id, msg_id)

    def resolve(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self._poll_working_orders(now=now)
        for position in self.db.get_open_positions():
            if position.market_end > now:
                continue
            winning_side = self._resolve_polymarket(position.market_id)
            if winning_side is None:
                continue

            if position.mode == "real" and winning_side == position.side and self.pm_trader is not None:
                # Сначала редим — resolved только после успешного redeem
                pending_tx = position.pending_redeem_tx or ""
                try:
                    result = self.pm_trader.redeem(position.market_id, pending_tx_hash=pending_tx)
                except Exception as exc:
                    print(f"[recovery] REDEEM ERROR {position.symbol} {position.interval_minutes}m: {exc}")
                    continue
                if result.pending:
                    self.db.set_pending_redeem_tx(position.id, result.tx_hash)
                    print(
                        f"[recovery] REDEEM PENDING {position.symbol} {position.interval_minutes}m"
                        f" | tx={result.tx_hash[:16]}..."
                    )
                    continue
                if not result.success:
                    print(
                        f"[recovery] REDEEM FAILED {position.symbol} {position.interval_minutes}m"
                        f" | {result.error}"
                    )
                    continue
                # Берём реальный payout с чейна как источник истины для PnL и баланса.
                # Если на нашем кошельке есть чужие CTF на этот market — они абсорбятся
                # сюда и учтутся; это цена того, что других ботов мы выключили.
                our_payout = result.payout_usdc
                pnl = our_payout - position.total_cost
                self.db.resolve_position(position.id, winning_side=winning_side, pnl=pnl)
                self.db.add_real_deposit(our_payout)
                print(
                    f"[recovery] RESOLVE {position.symbol} {position.interval_minutes}m"
                    f" [real] | winner={winning_side}"
                    f" | payout=${our_payout:.2f} cost=${position.total_cost:.2f} | pnl=${pnl:+.2f}"
                )
            else:
                # paper или проигравшая real позиция — redeem не нужен
                pnl = position.filled_shares - position.total_cost if winning_side == position.side else -position.total_cost
                self.db.resolve_position(position.id, winning_side=winning_side, pnl=pnl)
                print(
                    f"[recovery] RESOLVE {position.symbol} {position.interval_minutes}m"
                    f" [{position.mode}] | winner={winning_side} | pnl=${pnl:+.2f}"
                )

            if self.notifier:
                self.notifier.notify_resolve(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    side=position.side,
                    pnl=pnl,
                    winning_side=winning_side,
                    reply_to_message_id=position.tg_open_message_id,
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
        paper_recent = self.db.stats_by_mode_recent("paper", hours=12)
        real_recent = self.db.stats_by_mode_recent("real", hours=12)
        active = self._active_state_counts()
        lines = ["<b>Recovery Bot</b>"]
        lines.append(self._format_mode_status("PAPER", paper, recent=paper_recent))
        lines.append(self._format_mode_status("REAL", real, recent=real_recent))
        if self.db.has_real_deposit():
            balance, peak = self.db.get_real_deposit()
            clob_line = f"💳 реальный: депозит ${balance:.2f} (peak ${peak:.2f} | доступно ${balance:.2f})"
            if self.pm_trader is not None:
                try:
                    resp = self.pm_trader._client.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    )
                    clob_bal = float(resp.get("balance", 0)) / 1e6
                    clob_line += f"  (PM CLOB ${clob_bal:.2f})"
                except Exception:
                    pass
            lines.append(clob_line)
        markets = self.scanner.all_markets()
        n_5m = sum(1 for m in markets if m.interval_minutes == 5)
        n_15m = sum(1 for m in markets if m.interval_minutes == 15)
        lines.append(f"рынков: {len(markets)} (5m={n_5m} 15m={n_15m}) | touch={active['touched']} armed={active['armed']}")
        return "\n".join(lines)

    def print_status(self) -> None:
        print(self.get_status_text())
        for pos in self.db.get_working_positions()[:10]:
            print(
                f"  WORKING {pos.symbol} {pos.interval_minutes}m {pos.side.upper()}"
                f" [{pos.strategy_name}] [real] @ {pos.entry_price:.3f}"
                f" req={pos.requested_shares:.2f} order={pos.pm_order_id or '-'}"
            )
        for pos in self.db.get_open_positions()[:10]:
            print(
                f"  OPEN {pos.symbol} {pos.interval_minutes}m {pos.side.upper()}"
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

    @staticmethod
    def _polymarket_url(symbol: str, interval_minutes: int, market_start: datetime) -> str:
        ts = calendar.timegm(market_start.timetuple())
        return f"https://polymarket.com/event/{symbol.lower()}-updown-{interval_minutes}m-{ts}"

    def _extract_avg_fill(
        self,
        info: dict,
        *,
        fallback: float,
        order_id: str | None = None,
        token_id: str | None = None,
    ) -> float:
        """Средняя цена fill. В `info["price"]` — лимит ордера, не цена исполнения.
        Порядок источников:
          1) associate_trades с dict-элементами → avg = Σp·s / Σs
          2) info["average_price"]
          3) get_trades по asset_id, фильтр по order_id → avg из найденных трейдов
          4) fallback (лимит)."""
        trades = info.get("associate_trades") or []
        if trades:
            total_size = 0.0
            total_cost = 0.0
            for t in trades:
                if not isinstance(t, dict):
                    continue
                try:
                    sz = float(t.get("size", 0) or 0)
                    pr = float(t.get("price", 0) or 0)
                except (TypeError, ValueError):
                    continue
                total_size += sz
                total_cost += pr * sz
            if total_size > 0:
                return total_cost / total_size
        avg = info.get("average_price")
        if avg is not None:
            try:
                val = float(avg)
                if val > 0:
                    return val
            except (TypeError, ValueError):
                pass
        if order_id and self.pm_trader is not None:
            fills = self._fetch_order_fills(order_id, token_id)
            if fills:
                total_size = sum(f["size"] for f in fills)
                total_cost = sum(f["price"] * f["size"] for f in fills)
                if total_size > 0:
                    return total_cost / total_size
        return fallback

    def _paper_fallback_if_limit(
        self, position: RecoveryPosition, avg_fill: float, limit_price: float
    ) -> float:
        """Если _extract_avg_fill дотянул до лимита (fallback), и для этого
        (market,strategy,side) уже есть paper с entry_price — берём его как
        оценку реальной цены входа. Paper ходил по реальному стакану, это
        лучше, чем 0.70."""
        if position.mode != "real":
            return avg_fill
        if abs(avg_fill - limit_price) > 1e-6:
            return avg_fill
        paper_entry = self.db.get_paper_entry_price(
            position.market_id, position.strategy_name, position.side
        )
        if paper_entry is None or paper_entry <= 0 or paper_entry >= 1:
            return avg_fill
        return paper_entry

    def _fetch_order_fills(self, order_id: str, token_id: str | None) -> list[dict]:
        """Возвращает список {price, size} трейдов для указанного order_id.
        Использует get_trades — трейды возвращаются одной страницей (последние)."""
        if self.pm_trader is None:
            return []
        try:
            from py_clob_client.clob_types import TradeParams
            params = TradeParams(asset_id=token_id) if token_id else TradeParams()
            resp = self.pm_trader._client.get_trades(params)
        except Exception as exc:
            print(f"[recovery] get_trades failed for order {order_id[:16]}: {exc}")
            return []
        trades = resp.get("data", []) if isinstance(resp, dict) else resp
        if not isinstance(trades, list):
            return []
        out: list[dict] = []
        for t in trades:
            if not isinstance(t, dict):
                continue
            if t.get("taker_order_id") != order_id and t.get("maker_order_id") != order_id:
                # maker_orders может быть списком {order_id, price, size} — проверим и там
                makers = t.get("maker_orders") or []
                if not any(isinstance(m, dict) and m.get("order_id") == order_id for m in makers):
                    continue
            try:
                out.append({
                    "price": float(t.get("price", 0) or 0),
                    "size": float(t.get("size", 0) or 0),
                })
            except (TypeError, ValueError):
                continue
        return out

    @staticmethod
    def _compute_order_size(stake_usd: float, limit_price: float) -> float:
        """Размер должен быть кратен size_step так, чтобы price*size имело ≤2 знака,
        а notional >= $1 (минимум Polymarket). Округляем ВВЕРХ."""
        from decimal import Decimal, ROUND_DOWN, ROUND_UP
        _QUANT = Decimal("0.01")
        _MIN_NOTIONAL = Decimal("1.00")
        price_dec = Decimal(str(limit_price)).quantize(_QUANT, rounding=ROUND_DOWN)
        if price_dec <= 0:
            return 0.0
        price_cents = int(price_dec * 100)
        g = math.gcd(price_cents, 100)
        size_step = Decimal(100 // g) / Decimal(100)
        target = max(_MIN_NOTIONAL, Decimal(str(stake_usd)))
        raw_size = target / price_dec
        size_dec = (raw_size / size_step).to_integral_value(rounding=ROUND_UP) * size_step
        return float(size_dec)

    def real_balance(self) -> float:
        balance, _ = self.db.get_real_deposit()
        return balance

    def _scaled_stake_usd(self) -> float:
        """balance / N, но не ниже real_stake_usd (минимум $1)."""
        n = float(self.strategy.get("stake_scale_n", 35))
        floor = float(self.strategy.get("real_stake_usd", 1.0))
        return max(floor, self.real_balance() / n)

    def can_place_real_bet(self) -> bool:
        if not self.strategy.get("real_enabled", False):
            return False
        return self.real_balance() >= float(self.strategy.get("real_stake_usd", 1.0))

    @staticmethod
    def _format_mode_status(
        label: str,
        stats: dict[str, float | int],
        *,
        recent: dict[str, float | int] | None = None,
    ) -> str:
        resolved = int(stats["resolved_count"])
        won = int(stats["won_count"])
        lost = int(stats["lost_count"])
        wr = (won / resolved * 100.0) if resolved else 0.0
        line = (
            f"{label}: pnl=${float(stats['realized_pnl']):+.2f} | "
            f"working={int(stats['working_count'])} open={int(stats['open_count'])} resolved={resolved} | "
            f"won={won} lost={lost} | "
            f"unfilled={int(stats['unfilled_count'])} | avg_entry={float(stats.get('avg_entry_price', 0.0)):.3f} | "
            f"wr={wr:.1f}%"
        )
        if recent is not None:
            recent_resolved = int(recent["resolved_count"])
            recent_won = int(recent["won_count"])
            recent_wr = (recent_won / recent_resolved * 100.0) if recent_resolved else 0.0
            line += (
                f" | 12h: avg_entry={float(recent.get('avg_entry_price', 0.0)):.3f}"
                f" wr={recent_wr:.1f}% pnl=${float(recent['realized_pnl']):+.2f}"
            )
        return line

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
        limit_price = float(info.get("price", position.entry_price) or position.entry_price)
        avg_fill = self._extract_avg_fill(
            info,
            fallback=limit_price,
            order_id=position.pm_order_id,
            token_id=position.pm_token_id,
        )
        avg_fill = self._paper_fallback_if_limit(position, avg_fill, limit_price)
        fee = 0.0
        actual_cost = matched * avg_fill
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
                limit_price = float(info.get("price", limit_price) or limit_price)
                avg_fill = self._extract_avg_fill(
                    info,
                    fallback=limit_price,
                    order_id=position.pm_order_id,
                    token_id=position.pm_token_id,
                )
                avg_fill = self._paper_fallback_if_limit(position, avg_fill, limit_price)
                fee = 0.0
                actual_cost = matched * avg_fill
                is_full = matched >= requested - 1e-9 and matched > 0
                is_partial = 0 < matched < requested - 1e-9
                is_dead = status in {"cancelled", "canceled", "expired", "unmatched", "live_expired"}
            except Exception:
                pass

        if is_full or (is_partial and (cancel_if_partial or is_dead)):
            won_race = self.db.try_mark_position_open(
                position.id,
                entry_price=avg_fill,
                filled_shares=matched,
                total_cost=actual_cost,
                fee=fee,
                note=status or position.note,
            )
            if not won_race:
                # позиция уже переведена в open другим тиком — без шума
                return
            delta = reserved_cost - actual_cost
            if delta > 0:
                self.db.add_real_deposit(delta)
            elif delta < 0:
                self.db.deduct_real_deposit(-delta)
            if self.notifier:
                market_url = self._polymarket_url(position.symbol, position.interval_minutes, position.market_start)
                msg_id = self.notifier.notify_open(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    strategy_name=position.strategy_name,
                    side=position.side,
                    touch_price=position.touch_price,
                    trigger_price=position.trigger_price,
                    entry_price=avg_fill,
                    filled_shares=matched,
                    total_cost=actual_cost,
                    market_url=market_url,
                )
                if msg_id is not None:
                    self.db.set_open_message_id(position.id, msg_id)
            print(
                f"[recovery] FILLED {position.symbol} {position.interval_minutes}m {position.side.upper()}"
                f" | shares={matched:.2f}/{requested:.2f} @ {avg_fill:.4f} (limit={limit_price:.3f})"
                f" | cost=${actual_cost:.2f}"
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
