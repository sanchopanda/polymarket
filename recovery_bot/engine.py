from __future__ import annotations

import calendar
import json
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

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
        self._placed_markets_ts: dict[tuple[str, str], datetime] = {}  # когда был первый armed
        self._repeat_placed: set[tuple[str, str, str]] = set()  # (market_id, cfg.name, side) — repeat bet уже выставлен
        self._paper_only_names: set[str] = set()
        self._real_only_names: set[str] = set()
        self._real_stats_exclude: set[str] = set(self.strategy.get("real_stats_exclude") or [])
        self._real_disabled_intervals: set[int] = {int(i) for i in self.strategy.get("real_disabled_intervals") or []}
        self._latest_prices: dict[tuple[str, str], float] = {}
        self._price_log_ts: dict[tuple[str, str], float] = {}
        self._meta_fetched: set[str] = set()
        self._depth_fetched: set[tuple[str, str]] = set()
        self._gamma_base_url = config["polymarket"]["gamma_base_url"]
        self._clob_balance_cache: float = 0.0
        self._clob_balance_ts: float = 0.0
        _interval_keys = {5: "five_minute", 15: "fifteen_minute"}
        self._configs_by_interval: dict[int, list[RecoveryConfig]] = {
            5: [RecoveryConfig(
                name="5m_base",
                interval_minutes=5,
                bottom_price=float(self.strategy["five_minute"]["bottom_price"]),
                entry_price=float(self.strategy["five_minute"]["entry_price"]),
                top_price=float(self.strategy["five_minute"]["top_price"]),
                activation_delay_seconds=int(self.strategy["five_minute"]["activation_delay_seconds"]),
                paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                real_stake_usd=float(self.strategy["real_stake_usd"]),
                paper_only=bool(self.strategy["five_minute"].get("paper_only", False)),
                max_seconds_to_expiry=int(self.strategy["five_minute"]["max_seconds_to_expiry"]) if "max_seconds_to_expiry" in self.strategy["five_minute"] else None,
            )],
            15: [RecoveryConfig(
                name="15m_wait30",
                interval_minutes=15,
                bottom_price=float(self.strategy["fifteen_minute"]["bottom_price"]),
                entry_price=float(self.strategy["fifteen_minute"]["entry_price"]),
                top_price=float(self.strategy["fifteen_minute"]["top_price"]),
                activation_delay_seconds=int(self.strategy["fifteen_minute"]["activation_delay_seconds"]),
                paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                real_stake_usd=float(self.strategy["real_stake_usd"]),
                paper_only=bool(self.strategy["fifteen_minute"].get("paper_only", False)),
                max_seconds_to_expiry=int(self.strategy["fifteen_minute"]["max_seconds_to_expiry"]) if "max_seconds_to_expiry" in self.strategy["fifteen_minute"] else None,
            )],
        }
        for extra in self.strategy.get("paper_extras", []):
            for interval, ikey in _interval_keys.items():
                icfg = extra.get(ikey, {})
                if not icfg:
                    continue
                main_icfg = self.strategy[ikey]
                self._configs_by_interval[interval].append(RecoveryConfig(
                    name=extra["name"],
                    interval_minutes=interval,
                    bottom_price=float(icfg.get("bottom_price", main_icfg["bottom_price"])),
                    entry_price=float(icfg.get("entry_price", main_icfg["entry_price"])),
                    top_price=float(icfg.get("top_price", main_icfg["top_price"])),
                    activation_delay_seconds=int(icfg.get("activation_delay_seconds", main_icfg["activation_delay_seconds"])),
                    paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                    real_stake_usd=float(self.strategy["real_stake_usd"]),
                    paper_only=True,
                    max_seconds_to_expiry=int(icfg["max_seconds_to_expiry"]) if "max_seconds_to_expiry" in icfg else None,
                ))
                self._paper_only_names.add(extra["name"])
        real_cfg = self.strategy.get("real_config")
        if real_cfg:
            rname = real_cfg.get("name", "real_v2")
            for interval, ikey in _interval_keys.items():
                icfg = real_cfg.get(ikey)
                if not icfg:
                    continue
                main_icfg = self.strategy[ikey]
                self._configs_by_interval[interval].append(RecoveryConfig(
                    name=rname,
                    interval_minutes=interval,
                    bottom_price=float(icfg.get("bottom_price", main_icfg["bottom_price"])),
                    entry_price=float(icfg.get("entry_price", main_icfg["entry_price"])),
                    top_price=float(icfg.get("top_price", main_icfg["top_price"])),
                    activation_delay_seconds=int(icfg.get("activation_delay_seconds", main_icfg.get("activation_delay_seconds", 0))),
                    paper_stake_usd=float(self.strategy["paper_stake_usd"]),
                    real_stake_usd=float(self.strategy["real_stake_usd"]),
                    real_only=True,
                ))
            self._real_only_names.add(rname)
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
        # Per-symbol drawdown auto-disable
        self._drawdown_threshold_usd: float = float(self.strategy.get("symbol_drawdown_threshold_usd", 3.0))
        self._drawdown_threshold_by_symbol: dict[str, float] = {
            str(k): float(v)
            for k, v in (self.strategy.get("symbol_drawdown_threshold_usd_by_symbol") or {}).items()
        }
        data_dir = Path(config.get("db", {}).get("path", "data/recovery_bot.db")).parent
        self._disabled_symbols_path: Path = data_dir / "disabled_symbols.json"
        self._baseline_path: Path = data_dir / "symbol_pnl_baseline.json"
        self._disabled_symbols: set[str] = self._load_disabled_symbols()
        self._symbol_pnl_baseline: dict[str, float] = self._load_or_init_baseline()
        self._symbol_pnl_current: dict[str, float] = self.db.pnl_by_symbol_real(exclude_strategies=self._real_stats_exclude or None)
        _rb = self.strategy.get("repeat_bet") or {}
        self._repeat_bet_enabled: bool = bool(_rb.get("enabled", False))
        self._repeat_bet_touch_max: float = float(_rb.get("touch_max", 0.38))
        self._repeat_bet_gap_min: float = float(_rb.get("gap_min_seconds", 30))

    def _load_disabled_symbols(self) -> set[str]:
        try:
            if self._disabled_symbols_path.exists():
                data = json.loads(self._disabled_symbols_path.read_text())
                return set(data) if isinstance(data, list) else set()
        except Exception as exc:
            print(f"[recovery] failed to load disabled symbols: {exc}")
        return set()

    def _save_disabled_symbols(self) -> None:
        try:
            self._disabled_symbols_path.parent.mkdir(parents=True, exist_ok=True)
            self._disabled_symbols_path.write_text(json.dumps(sorted(self._disabled_symbols)))
        except Exception as exc:
            print(f"[recovery] failed to save disabled symbols: {exc}")

    def _load_or_init_baseline(self) -> dict[str, float]:
        """Baseline PnL per symbol — persistent across restarts. Manual reset: delete the file."""
        try:
            if self._baseline_path.exists():
                data = json.loads(self._baseline_path.read_text())
                if isinstance(data, dict):
                    return {str(k): float(v) for k, v in data.items()}
        except Exception as exc:
            print(f"[recovery] failed to load baseline: {exc}")
        baseline = self.db.pnl_by_symbol_real(exclude_strategies=self._real_stats_exclude or None)
        try:
            self._baseline_path.parent.mkdir(parents=True, exist_ok=True)
            self._baseline_path.write_text(json.dumps(baseline, indent=2, sort_keys=True))
            print(f"[recovery] baseline initialized: {baseline}")
        except Exception as exc:
            print(f"[recovery] failed to save baseline: {exc}")
        return baseline


    def _check_symbol_drawdown(self) -> None:
        """Обновить текущий PnL по символам и отключить те, что просели от baseline более чем на threshold."""
        self._symbol_pnl_current = self.db.pnl_by_symbol_real(exclude_strategies=self._real_stats_exclude or None)
        for sym, current in self._symbol_pnl_current.items():
            if sym in self._disabled_symbols:
                continue
            baseline = self._symbol_pnl_baseline.get(sym, current)
            drop = current - baseline
            threshold = self._drawdown_threshold_by_symbol.get(sym, self._drawdown_threshold_usd)
            if drop <= -threshold:
                self._disabled_symbols.add(sym)
                self._save_disabled_symbols()
                msg = (
                    f"[recovery] SYMBOL DISABLED {sym}: drop={drop:+.2f}$"
                    f" (baseline={baseline:+.2f}$ → current={current:+.2f}$, threshold=${threshold:.2f})"
                )
                print(msg)
                if self.notifier is not None:
                    try:
                        self.notifier.send(f"🔒 <b>DISABLED</b> {sym}\n"
                                           f"baseline={baseline:+.2f}$ → now={current:+.2f}$ (drop={drop:+.2f}$)")
                    except Exception:
                        pass

    def scan_markets(self) -> list[OracleMarket]:
        return self.scanner.scan_and_subscribe()

    def stop(self) -> None:
        self.scanner.stop()

    @staticmethod
    def _reset_signal_cycle(state: TrackedRecovery) -> None:
        state.touch_ts = None
        state.touch_price = None
        state.armed_ts = None
        state.orders_placed = False
        state.done = False
        state.note = None

    def on_pm_price(self, market: OracleMarket, side: str, best_ask: float) -> None:
        if market.venue != "polymarket" or side not in ("yes", "no"):
            return
        self._latest_prices[(market.market_id, side)] = float(best_ask)
        now_mono = time.monotonic()
        now = datetime.utcnow()
        seconds_left = (market.expiry - now).total_seconds()
        log_key = (market.market_id, side)
        if seconds_left <= 300 and now_mono - self._price_log_ts.get(log_key, 0.0) >= 1.0:
            self._price_log_ts[log_key] = now_mono
            try:
                self.db.insert_price_history(
                    market_id=market.market_id,
                    symbol=market.symbol,
                    side=side,
                    ts=datetime.utcnow(),
                    price=float(best_ask),
                )
            except Exception as exc:
                print(f"[recovery] price_history insert failed ({market.symbol} {side}): {exc}")
        cfgs = self._configs_by_interval.get(market.interval_minutes)
        if not cfgs:
            return
        for cfg in cfgs:
            if cfg.max_seconds_to_expiry is None:
                min_seconds = 90 if market.interval_minutes == 5 else 120
                if seconds_left < min_seconds:
                    continue
            state_key = (market.market_id, cfg.name, side)
            market_key = (market.market_id, cfg.name)
            should_place = False
            touch_ts = None
            armed_ts = None
            touch_price = None
            signal_to_log: tuple[datetime, float] | None = None
            reset_after_signal = False
            signal_only_data: dict | None = None
            time_zone_skip_data: dict | None = None
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
                # 1. Ждём касания дна (touch ловится только пока нет done и нет touch)
                if not state.done and state.touch_ts is None and best_ask <= cfg.bottom_price:
                    state.touch_ts = now
                    state.touch_price = best_ask
                    print(
                        f"[recovery] touch {market.symbol} {market.interval_minutes}m {side.upper()}"
                        f" | ask={best_ask:.3f} <= {cfg.bottom_price:.2f}"
                        f" [{cfg.name}] | {int(seconds_left)}s left"
                    )
                if state.touch_ts is None:
                    continue
                # Signal detection — upward crossing entry_price после touch.
                # Работает ВСЕГДА (armed/done неважно), чтобы ловить все сигналы включая
                # повторные и реверс на противоположной стороне для аналитики.
                # После каждого сигнала цикл touch->signal сбрасывается: следующий сигнал
                # должен иметь собственный новый touch.
                now_above = best_ask >= cfg.entry_price
                if now_above and not state.last_ask_above_entry:
                    signal_to_log = (now, float(best_ask))
                    reset_after_signal = True
                state.last_ask_above_entry = now_above
                if state.done:
                    if reset_after_signal:
                        self._reset_signal_cycle(state)
                    continue
                # 2. Ждём восстановления в зону [entry_price, top_price] после задержки
                if state.armed_ts is None:
                    elapsed = (now - state.touch_ts).total_seconds()
                    if elapsed < cfg.activation_delay_seconds:
                        if reset_after_signal:
                            self._reset_signal_cycle(state)
                        continue
                    if best_ask > cfg.top_price:
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
                                f" [{cfg.name}] | ask={best_ask:.3f} > {cfg.top_price:.2f}"
                            )
                        self._reset_signal_cycle(state)
                        continue
                    if best_ask >= cfg.entry_price:
                        time_blocks_order = False
                        time_zone_skip_note: str | None = None
                        if cfg.max_seconds_to_expiry is not None and seconds_left > cfg.max_seconds_to_expiry:
                            time_blocks_order = True
                        if not time_blocks_order:
                            min_expiry_by_sym = self.strategy.get("min_seconds_to_expiry_by_symbol") or {}
                            min_expiry = min_expiry_by_sym.get(market.symbol)
                            if min_expiry is not None and seconds_left < float(min_expiry):
                                time_blocks_order = True
                        if not time_blocks_order:
                            skip_zones = self.strategy.get("skip_seconds_to_expiry_zones_by_symbol") or {}
                            zone = skip_zones.get(market.symbol)
                            if zone and float(zone[0]) <= seconds_left < float(zone[1]):
                                time_blocks_order = True
                                time_zone_skip_note = (
                                    f"reason=time_zone_filter secs_left={int(seconds_left)}"
                                    f" zone=[{zone[0]},{zone[1]})"
                                )
                                if not self.db.has_market_record(market.market_id, cfg.name, "real", side=side):
                                    time_zone_skip_data = dict(
                                        market_id=market.market_id,
                                        symbol=market.symbol,
                                        title=market.title,
                                        interval_minutes=market.interval_minutes,
                                        market_start=market.market_start,
                                        market_end=market.expiry,
                                        side=side,
                                        mode="real",
                                        strategy_name=cfg.name,
                                        touch_ts=state.touch_ts,
                                        armed_ts=now,
                                        touch_price=float(state.touch_price or best_ask),
                                        trigger_price=cfg.entry_price,
                                        entry_price=cfg.top_price,
                                        requested_shares=self._compute_order_size(
                                            self._scaled_stake_usd(market.symbol), cfg.top_price
                                        ),
                                        note=time_zone_skip_note,
                                    )
                        if not time_blocks_order:
                            state.armed_ts = now
                            print(
                                f"[recovery] armed {market.symbol} {market.interval_minutes}m {side.upper()}"
                                f" [{cfg.name}] | ask={best_ask:.3f} (trigger>={cfg.entry_price:.2f})"
                            )
                            if market_key in self._placed_markets:
                                repeat_key = (market.market_id, cfg.name, side)
                                orig_armed_ts = self._placed_markets_ts.get(market_key)
                                gap_seconds = (now - orig_armed_ts).total_seconds() if orig_armed_ts else None
                                cur_touch = float(state.touch_price or best_ask)
                                repeat_allowed = (
                                    self._repeat_bet_enabled
                                    and repeat_key not in self._repeat_placed
                                    and cur_touch < self._repeat_bet_touch_max
                                    and gap_seconds is not None
                                    and gap_seconds >= self._repeat_bet_gap_min
                                )
                                if repeat_allowed:
                                    self._repeat_placed.add(repeat_key)
                                    should_place = True
                                    touch_ts = state.touch_ts
                                    armed_ts = state.armed_ts
                                    touch_price = cur_touch
                                    print(
                                        f"[recovery] REPEAT BET {market.symbol} {market.interval_minutes}m"
                                        f" {side.upper()} [{cfg.name}]"
                                        f" | touch={cur_touch:.3f} gap={gap_seconds:.0f}s"
                                    )
                                else:
                                    print(
                                        f"[recovery] signal-only {market.symbol} {market.interval_minutes}m"
                                        f" {side.upper()} [{cfg.name}] — повторный цикл без нового ордера"
                                    )
                                signal_only_data = dict(
                                    market_id=market.market_id,
                                    symbol=market.symbol,
                                    title=market.title,
                                    interval_minutes=market.interval_minutes,
                                    market_start=market.market_start,
                                    market_end=market.expiry,
                                    side=side,
                                    mode="real",
                                    strategy_name=cfg.name,
                                    touch_ts=state.touch_ts,
                                    armed_ts=state.armed_ts,
                                    touch_price=cur_touch,
                                    trigger_price=cfg.entry_price,
                                    entry_price=cfg.top_price,
                                    requested_shares=self._compute_order_size(
                                        self._scaled_stake_usd(market.symbol), cfg.top_price
                                    ),
                                )
                            elif not state.orders_placed:
                                state.orders_placed = True
                                self._placed_markets.add(market_key)
                                self._placed_markets_ts[market_key] = now
                                should_place = True
                                touch_ts = state.touch_ts
                                armed_ts = state.armed_ts
                                touch_price = float(state.touch_price or best_ask)
                if reset_after_signal:
                    self._reset_signal_cycle(state)
            if signal_to_log is not None:
                ref_ts, _ = signal_to_log
                self._start_price_probe(
                    kind="signal",
                    market_id=market.market_id,
                    strategy_name=cfg.name,
                    side=side,
                    ref_ts=ref_ts,
                )
            if signal_only_data is not None:
                try:
                    self.db.open_position(
                        **signal_only_data,
                        filled_shares=0.0,
                        total_cost=0.0,
                        fee=0.0,
                        status="signal_only",
                        pm_token_id=market.no_token_id if side == "no" else market.yes_token_id,
                    )
                except Exception as e:
                    print(f"[recovery] signal_only insert failed ({market.symbol} {side}): {e}")
            if time_zone_skip_data is not None:
                try:
                    self.db.open_position(
                        **time_zone_skip_data,
                        filled_shares=0.0,
                        total_cost=0.0,
                        fee=0.0,
                        status="skipped_filter",
                        pm_token_id=market.no_token_id if side == "no" else market.yes_token_id,
                    )
                    print(
                        f"[recovery] time_zone skip {market.symbol} {market.interval_minutes}m {side.upper()}"
                        f" [{cfg.name}] | {time_zone_skip_data['note']}"
                    )
                except Exception as e:
                    print(f"[recovery] time_zone_skip insert failed ({market.symbol} {side}): {e}")
            if not should_place:
                continue
            thread = threading.Thread(
                target=self._place_orders,
                args=(market, cfg, touch_ts, armed_ts, touch_price, best_ask, side),
                daemon=True,
                name=f"recovery-open-{market.symbol}-{market.interval_minutes}-{cfg.name}-{side}",
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
        market_key = (market.market_id, cfg.name)

        def _release() -> None:
            # Убираем рынок из _placed_markets если реальный ордер так и не был выставлен.
            # Следующий сигнал сможет попробовать снова.
            with self._lock:
                self._placed_markets.discard(market_key)
                self._placed_markets_ts.pop(market_key, None)
                self._repeat_placed.discard((market.market_id, cfg.name, side))

        real_allowed = (
            self.strategy.get("real_enabled", False)
            and not cfg.paper_only
            and cfg.interval_minutes not in self._real_disabled_intervals
            and market.symbol not in self._disabled_symbols
        )
        if self.strategy.get("real_enabled", False) and market.symbol in self._disabled_symbols:
            print(f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}: symbol disabled by drawdown")
        if real_allowed and not self.db.has_market_record(market.market_id, cfg.name, "real", side=side):
            if self.pm_trader is None:
                _release()
                return
            if not token_id:
                print(f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}: token_id missing")
                _release()
                return
            requested_shares = self._compute_order_size(self._scaled_stake_usd(market.symbol), cfg.top_price)
            if requested_shares <= 0:
                _release()
                return
            reserved_cost = requested_shares * cfg.entry_price
            if reserved_cost > self.real_balance():
                print(
                    f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}:"
                    f" reserve=${reserved_cost:.2f} > virtual=${self.real_balance():.2f}"
                )
                _release()
                return
            confirm_delay = float(self.strategy.get("real_confirm_delay_seconds") or 0.0)
            delay_by_sym = self.strategy.get("real_confirm_delay_seconds_by_symbol") or {}
            if market.symbol in delay_by_sym:
                confirm_delay = float(delay_by_sym[market.symbol])
            confirm_min = self.strategy.get("real_confirm_min_price")
            by_symbol = self.strategy.get("real_confirm_min_price_by_symbol") or {}
            if market.symbol in by_symbol:
                confirm_min = by_symbol[market.symbol]
            hold_cfg = (self.strategy.get("real_hold_test_by_symbol") or {}).get(market.symbol)
            skip_reason = None
            note = None

            if hold_cfg:
                window_ms = float(hold_cfg.get("window_ms", 2000))
                hold_min = float(hold_cfg.get("min_price", 0.65))
                interval_ms = float(hold_cfg.get("sample_interval_ms", 200))
                samples: list[float] = []
                elapsed = 0.0
                while elapsed < window_ms:
                    step = min(interval_ms, window_ms - elapsed)
                    time.sleep(step / 1000.0)
                    elapsed += step
                    px = self._latest_prices.get((market.market_id, side))
                    if px is not None:
                        samples.append(float(px))
                current_ask = samples[-1] if samples else None
                min_ask = min(samples) if samples else None
                ask_text = "none" if current_ask is None else f"{current_ask:.3f}"
                min_text = "none" if min_ask is None else f"{min_ask:.3f}"
                if min_ask is None or min_ask < hold_min:
                    skip_reason = "hold_test_fail"
                    print(
                        f"[recovery] HOLD TEST FAIL skip real {market.symbol} {market.interval_minutes}m"
                        f" {side_upper} [{cfg.name}] | min_ask over {window_ms:.0f}ms ="
                        f" {min_text} < {hold_min:.2f} (last={ask_text}, n={len(samples)})"
                    )
                if skip_reason is not None:
                    note = (
                        f"reason={skip_reason} min_ask_{window_ms:.0f}ms={min_text}"
                        f" last={ask_text} hold_min={hold_min:.2f}"
                    )
            elif confirm_delay > 0.0 and confirm_min is not None:
                time.sleep(confirm_delay)
                current_ask = self._latest_prices.get((market.market_id, side))
                ask_text = "none" if current_ask is None else f"{current_ask:.3f}"
                if current_ask is None or current_ask < float(confirm_min):
                    skip_reason = "fake_drop"
                    print(
                        f"[recovery] FAKE SIGNAL skip real {market.symbol} {market.interval_minutes}m"
                        f" {side_upper} [{cfg.name}] | ask after {confirm_delay:.1f}s ="
                        f" {ask_text} < {float(confirm_min):.2f}"
                    )
                if skip_reason is not None:
                    note = (
                        f"reason={skip_reason} ask_after_{confirm_delay:.1f}s={ask_text}"
                        f" min={float(confirm_min):.2f}"
                    )

            if skip_reason is not None:
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
                    status="skipped_filter",
                    pm_token_id=token_id,
                    note=note,
                )
                _release()
                return
            attempt_start_ts = datetime.utcnow()
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
                    import re as _re
                    m = _re.search(r"minimum[^0-9]*([0-9]+(?:\.[0-9]+)?)", exc_text)
                    min_req = m.group(1) if m else "?"
                    our_notional = requested_shares * cfg.top_price
                    print(
                        f"[recovery] skip real {market.symbol} {market.interval_minutes}m {side_upper}:"
                        f" min size слишком большой (our={requested_shares:.2f} shares @ {cfg.top_price:.2f} = ${our_notional:.2f}, min={min_req}) | err={exc_text}"
                    )
                    if self.notifier is not None:
                        try:
                            self.notifier.send(
                                f"⚠️ <b>skip min</b> {market.symbol} {market.interval_minutes}m {side_upper}\n"
                                f"our={requested_shares:.2f}sh @ {cfg.top_price:.2f} = ${our_notional:.2f}\n"
                                f"market min = {min_req}"
                            )
                        except Exception:
                            pass
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
                        status="skipped_min_size",
                        pm_token_id=token_id,
                        note=(
                            f"reason=min_size our_shares={requested_shares:.4f}"
                            f" our_notional=${our_notional:.2f} market_min={min_req}"
                        ),
                    )
                    _release()
                    return
                print(f"[recovery] order error {market.symbol} {market.interval_minutes}m {side_upper}: {exc}")
                # Request exception / 500 — возможно ордер прошёл на биржу, но ответ потерян.
                # Пробуем найти реальный трейд и подхватить его.
                is_network_like = (
                    "Request exception" in exc_text
                    or "status_code=None" in exc_text
                    or "status_code=500" in exc_text
                )
                recovered = None
                if is_network_like:
                    try:
                        recovered = self._recover_orphan_trade(
                            token_id=token_id,
                            requested_shares=requested_shares,
                            top_price=cfg.top_price,
                            attempt_start_ts=attempt_start_ts,
                        )
                    except Exception as rec_exc:
                        print(f"[recovery] recovery attempt failed: {rec_exc}")
                if recovered is not None:
                    try:
                        fill_size = float(recovered.get("size", 0) or 0)
                        fill_price = float(recovered.get("price", 0) or 0)
                        fee_bps = float(recovered.get("fee_rate_bps", 0) or 0)
                    except (TypeError, ValueError):
                        fill_size = fill_price = fee_bps = 0.0
                    fee_usd = fill_size * fill_price * fee_bps / 10000.0
                    total_cost = fill_size * fill_price
                    order_id = recovered.get("taker_order_id") or ""
                    print(
                        f"[recovery] RECOVERED LOST FILL {market.symbol} {market.interval_minutes}m"
                        f" {side_upper} | size={fill_size} @ {fill_price:.4f} | cost=${total_cost:.2f}"
                        f" | tx={str(recovered.get('transaction_hash',''))[:14]}"
                    )
                    rec_pos = self.db.open_position(
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
                        entry_price=fill_price,
                        requested_shares=requested_shares,
                        filled_shares=fill_size,
                        total_cost=total_cost,
                        fee=fee_usd,
                        status="open",
                        pm_token_id=token_id,
                        pm_order_id=order_id,
                        note=f"recovered from network error | orig_exc={exc_text[:120]}",
                    )
                    if self.notifier is not None:
                        try:
                            market_url = self._polymarket_url(
                                market.symbol, market.interval_minutes, market.market_start
                            )
                            msg_id = self.notifier.notify_open(
                                symbol=market.symbol,
                                interval_minutes=market.interval_minutes,
                                mode="real",
                                strategy_name=cfg.name,
                                side=side,
                                touch_price=touch_price,
                                trigger_price=trigger_price,
                                entry_price=fill_price,
                                filled_shares=fill_size,
                                total_cost=total_cost,
                                market_url=market_url,
                            )
                            if msg_id is not None:
                                self.db.set_open_message_id(rec_pos.id, msg_id)
                        except Exception:
                            pass
                    return
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
                _release()
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
                _release()
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
            self._start_price_probe(
                kind="order",
                market_id=market.market_id,
                strategy_name=cfg.name,
                side=side,
                ref_ts=datetime.utcnow(),
            )
            print(
                f"[recovery] REAL WORKING {market.symbol} {market.interval_minutes}m {side_upper}"
                f" [{cfg.name}] | touch={touch_price:.3f} | trigger={trigger_price:.3f}"
                f" | limit={cfg.top_price:.3f} | reserve=${reserved_cost:.2f}"
            )
            self._sync_working_position(pos, cancel_if_partial=False)

        if self.strategy.get("paper_enabled", True) and not cfg.real_only and not self.db.has_market_record(market.market_id, cfg.name, "paper", side=side):
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

        # После всех ордеров — фоновый фетч volume/liquidity и снэпшот стакана.
        self._fetch_market_meta_async(market.market_id)
        self._fetch_depth_async(market.market_id, token_id, side, cfg.top_price)

    def _fetch_market_meta_async(self, market_id: str) -> None:
        """Фоново тянем volume/liquidity из Gamma и сохраняем в positions.
        Вызывается ПОСЛЕ размещения ордеров — не блокирует hot path."""
        if market_id in self._meta_fetched:
            return
        self._meta_fetched.add(market_id)
        thread = threading.Thread(
            target=self._run_market_meta_fetch,
            args=(market_id,),
            daemon=True,
            name=f"meta-{market_id[:8]}",
        )
        thread.start()

    def _run_market_meta_fetch(self, market_id: str) -> None:
        try:
            r = requests.get(f"{self._gamma_base_url}/markets/{market_id}", timeout=5)
            if r.status_code != 200:
                return
            data = r.json()
            volume = data.get("volumeNum")
            liquidity = data.get("liquidityNum")
            if volume is None and liquidity is None:
                return
            self.db.set_market_meta(market_id, volume, liquidity)
        except Exception:
            pass

    def _fetch_depth_async(self, market_id: str, token_id: str, side: str, cap_price: float) -> None:
        """Снэпшот ask-ladder до cap_price. Вызывается ПОСЛЕ ордера — не блокирует hot path."""
        if not token_id:
            return
        key = (market_id, side)
        if key in self._depth_fetched:
            return
        self._depth_fetched.add(key)
        thread = threading.Thread(
            target=self._run_depth_fetch,
            args=(market_id, token_id, side, cap_price),
            daemon=True,
            name=f"depth-{market_id[:8]}-{side}",
        )
        thread.start()

    def _run_depth_fetch(self, market_id: str, token_id: str, side: str, cap_price: float) -> None:
        try:
            book = self._clob.get_orderbook(token_id)
            if book is None:
                return
            asks = [[lvl.price, lvl.size] for lvl in book.asks if lvl.price <= cap_price]
            self.db.set_market_depth(market_id, side, json.dumps(asks))
        except Exception:
            pass

    def _start_price_probe(
        self,
        *,
        kind: str,
        market_id: str,
        strategy_name: str,
        side: str,
        ref_ts: datetime,
    ) -> None:
        """Stream best_ask raz v 0.5s в течение 5s после ref_ts → price_probes."""
        thread = threading.Thread(
            target=self._run_price_probe,
            args=(kind, market_id, strategy_name, side, ref_ts),
            daemon=True,
            name=f"probe-{kind}-{market_id[:8]}-{side}",
        )
        thread.start()

    def _run_price_probe(
        self,
        kind: str,
        market_id: str,
        strategy_name: str,
        side: str,
        ref_ts: datetime,
    ) -> None:
        ref_mono = time.monotonic()
        offsets_ms = [200, 500, 700, 1000, 1200, 1500, 1700, 2000]
        for off in offsets_ms:
            target = ref_mono + off / 1000.0
            wait = target - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            price = self._latest_prices.get((market_id, side))
            if price is None:
                continue
            try:
                self.db.insert_price_probe(
                    market_id=market_id,
                    strategy_name=strategy_name,
                    side=side,
                    kind=kind,
                    ref_ts=ref_ts,
                    offset_ms=off,
                    price=float(price),
                )
            except Exception as exc:
                print(f"[recovery] probe insert failed ({kind} {market_id[:8]} {side}): {exc}")
                return

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
        if self.notifier and cfg.name not in self._paper_only_names:
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
        self._check_symbol_drawdown()
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
                print(
                    f"[recovery] RESOLVE {position.symbol} {position.interval_minutes}m"
                    f" [real] | winner={winning_side}"
                    f" | payout=${our_payout:.2f} cost=${position.total_cost:.2f} | pnl=${pnl:+.2f}"
                )
            else:
                # paper или проигравшая real позиция — redeem не нужен
                pnl = position.filled_shares * 0.98 - position.total_cost if winning_side == position.side else -position.total_cost
                self.db.resolve_position(position.id, winning_side=winning_side, pnl=pnl)
                print(
                    f"[recovery] RESOLVE {position.symbol} {position.interval_minutes}m"
                    f" [{position.mode}] | winner={winning_side} | pnl=${pnl:+.2f}"
                )

            if self.notifier and position.strategy_name not in self._paper_only_names:
                self.notifier.notify_resolve(
                    symbol=position.symbol,
                    interval_minutes=position.interval_minutes,
                    mode=position.mode,
                    side=position.side,
                    pnl=pnl,
                    winning_side=winning_side,
                    reply_to_message_id=position.tg_open_message_id,
                )

        for skip in self.db.get_unresolved_skipped_filter():
            if skip.market_end > now:
                continue
            winning_side = self._resolve_polymarket(skip.market_id)
            if winning_side is None:
                continue
            cost = skip.requested_shares * skip.entry_price
            if winning_side == skip.side:
                hypo_pnl = skip.requested_shares * 0.98 - cost
            else:
                hypo_pnl = -cost
            self.db.resolve_skipped_position(skip.id, winning_side=winning_side, pnl=hypo_pnl)
            verdict = "AVOIDED LOSS" if hypo_pnl < 0 else "MISSED WIN"
            print(
                f"[recovery] SKIP-RESOLVE {skip.symbol} {skip.interval_minutes}m"
                f" {skip.side.upper()} [{skip.strategy_name}] | winner={winning_side}"
                f" | hypo_pnl=${hypo_pnl:+.2f} | {verdict}"
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
        _excl_paper = self._paper_only_names or None
        _excl_real = (self._real_only_names | self._real_stats_exclude) or None
        paper = self.db.stats_by_mode("paper", exclude_strategies=_excl_paper)
        real = self.db.stats_by_mode("real", exclude_strategies=_excl_real)
        paper_windows = [
            ("6h",  self.db.stats_by_mode_recent("paper", hours=6,  exclude_strategies=_excl_paper)),
            ("12h", self.db.stats_by_mode_recent("paper", hours=12, exclude_strategies=_excl_paper)),
            ("24h", self.db.stats_by_mode_recent("paper", hours=24, exclude_strategies=_excl_paper)),
            ("48h", self.db.stats_by_mode_recent("paper", hours=48, exclude_strategies=_excl_paper)),
        ]
        real_windows = [
            ("6h",  self.db.stats_by_mode_recent("real", hours=6,  exclude_strategies=_excl_real)),
            ("12h", self.db.stats_by_mode_recent("real", hours=12, exclude_strategies=_excl_real)),
            ("24h", self.db.stats_by_mode_recent("real", hours=24, exclude_strategies=_excl_real)),
            ("48h", self.db.stats_by_mode_recent("real", hours=48, exclude_strategies=_excl_real)),
        ]
        active = self._active_state_counts()
        lines = ["<b>Recovery Bot</b>", ""]
        lines.append(self._format_mode_status("PAPER", paper, recent_windows=paper_windows))
        lines.append("")
        lines.append(self._format_mode_status("REAL", real, recent_windows=real_windows))
        lines.append("")
        if self.pm_trader is not None and self.strategy.get("real_enabled", False):
            lines.append(f"💳 CLOB: ${self.real_balance():.2f}")
            lines.append("")
        btc = self.db.stats_by_symbol("BTC", mode="real", interval_minutes=5)
        if btc["resolved_count"] > 0:
            wr = btc["won_count"] / btc["resolved_count"]
            avg_entry = float(btc["avg_entry_price"])
            avg_shares = float(btc.get("avg_filled_shares") or 0.0)
            pnl_per_fill = avg_shares * (wr * 0.98 - avg_entry) if avg_shares > 0 else 0.0
            lines.append(
                f"📊 BTC: {int(btc['resolved_count'])} ставок"
                f" | WR={wr*100:.1f}%"
                f" | entry={avg_entry:.3f}"
                f" | PnL/fill=${pnl_per_fill:+.3f}"
                f" | total=${float(btc['realized_pnl']):+.2f}"
            )
            lines.append("")
        # Показываем только активные real-extras (self._real_only_names).
        # real_stats_exclude держим отдельно — оно исключает исторические имена
        # из основной статистики, но отображать их в блоке extras не нужно.
        if self._real_only_names:
            seen: set[str] = set()
            for name in sorted(self._real_only_names):
                if name in seen:
                    continue
                seen.add(name)
                s = self.db.stats_by_strategy(name)
                if s["resolved_count"] == 0 and s["open_count"] == 0 and s["working_count"] == 0:
                    continue
                s_recent = self.db.stats_by_strategy_recent(name, hours=12)
                lines.append(self._format_mode_status(name.upper(), s, recent=s_recent))
            lines.append("")
        if self._paper_only_names:
            lines.append("── paper extras ──")
            seen: set[str] = set()
            for cfgs in self._configs_by_interval.values():
                for cfg in cfgs:
                    if cfg.name not in self._paper_only_names or cfg.name in seen:
                        continue
                    seen.add(cfg.name)
                    s = self.db.stats_by_strategy(cfg.name)
                    resolved = int(s["resolved_count"])
                    won = int(s["won_count"])
                    wr = (won / resolved * 100.0) if resolved else 0.0
                    lines.append(
                        f"  {cfg.name}: pnl=${float(s['realized_pnl']):+.2f}"
                        f" | resolved={resolved} wr={wr:.1f}%"
                        f" | avg_entry={float(s['avg_entry_price']):.3f}"
                    )
            lines.append("")
        # Per-symbol PnL + drawdown guard
        all_syms = set(self._symbol_pnl_current) | set(self._symbol_pnl_baseline) | self._disabled_symbols
        if all_syms:
            lines.append("── символы ──")
            for sym in sorted(all_syms):
                cur = self._symbol_pnl_current.get(sym, 0.0)
                base = self._symbol_pnl_baseline.get(sym, cur)
                delta = cur - base
                flag = " 🔒DISABLED" if sym in self._disabled_symbols else ""
                lines.append(f"  {sym}: ${cur:+.2f} (Δ{delta:+.2f}){flag}")
            default_thr = self._drawdown_threshold_usd
            if self._drawdown_threshold_by_symbol:
                overrides = ", ".join(
                    f"{s}=${t:.2f}" for s, t in sorted(self._drawdown_threshold_by_symbol.items())
                )
                lines.append(f"  threshold drop: ${default_thr:.2f} (override: {overrides})")
            else:
                lines.append(f"  threshold drop: ${default_thr:.2f}")
            lines.append("")
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

    def _recover_orphan_trade(
        self,
        *,
        token_id: str,
        requested_shares: float,
        top_price: float,
        attempt_start_ts: datetime,
        wait_seconds: float = 2.0,
        window_seconds: float = 30.0,
    ) -> dict | None:
        """После Request exception: ищем реальный трейд на PM.
        Возвращает trade dict или None. Матчит по asset_id + side=BUY + size≤req
        + price≤top_price+0.02 + match_time в окне ±window_seconds от attempt."""
        if self.pm_trader is None:
            return None
        from py_clob_client.clob_types import TradeParams
        retry_delays = [wait_seconds, 2.0, 2.0]
        trades: list = []
        for attempt, delay in enumerate(retry_delays):
            time.sleep(delay)
            try:
                resp = self.pm_trader._client.get_trades(TradeParams(asset_id=token_id))
                trades = resp.get("data", []) if isinstance(resp, dict) else resp
                if not isinstance(trades, list):
                    trades = []
            except Exception as exc:
                print(f"[recovery] recovery get_trades failed (attempt {attempt + 1}): {exc}")
                continue
            if trades:
                break
            print(f"[recovery] recovery get_trades empty (attempt {attempt + 1}), retrying...")
        if not trades:
            return None
        best: dict | None = None
        best_dt = None
        for t in trades:
            if not isinstance(t, dict):
                continue
            if t.get("asset_id") != token_id or t.get("side") != "BUY":
                continue
            if t.get("status") != "CONFIRMED":
                continue
            try:
                sz = float(t.get("size", 0) or 0)
                px = float(t.get("price", 0) or 0)
                mt = int(t.get("match_time", 0) or 0)
            except (TypeError, ValueError):
                continue
            if sz <= 0 or sz > requested_shares + 0.01:
                continue
            if px > top_price + 0.02:
                continue
            tm_utc = datetime.fromtimestamp(mt, tz=timezone.utc).replace(tzinfo=None)
            delta = abs((tm_utc - attempt_start_ts).total_seconds())
            if delta > window_seconds:
                continue
            if best is None or delta < best_dt:
                best = t
                best_dt = delta
        return best

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
        if self.pm_trader is None:
            return 0.0
        if time.monotonic() - self._clob_balance_ts > 30:
            try:
                resp = self.pm_trader._client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                self._clob_balance_cache = float(resp.get("balance", 0)) / 1e6
                self._clob_balance_ts = time.monotonic()
            except Exception:
                pass
        return self._clob_balance_cache

    def _scaled_stake_usd(self, symbol: str) -> float:
        """Для символов из stake_scale_symbols — balance/N (не ниже real_stake_usd).
        Для остальных — фикс real_stake_usd."""
        floor = float(self.strategy.get("real_stake_usd", 1.0))
        scale_list = self.strategy.get("stake_scale_symbols") or []
        if symbol not in set(scale_list):
            return floor
        n = float(self.strategy.get("stake_scale_n", 35))
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
        recent_windows: list[tuple[str, dict[str, float | int]]] | None = None,
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
        if recent_windows is not None:
            parts = []
            for lbl, ws in recent_windows:
                ws_resolved = int(ws["resolved_count"])
                ws_won = int(ws["won_count"])
                ws_wr = (ws_won / ws_resolved * 100.0) if ws_resolved else 0.0
                parts.append(f"{lbl}={ws_wr:.1f}%/{ws_resolved}")
            line += "\n  wr: " + " | ".join(parts)
        elif recent is not None:
            recent_resolved = int(recent["resolved_count"])
            recent_won = int(recent["won_count"])
            recent_wr = (recent_won / recent_resolved * 100.0) if recent_resolved else 0.0
            line += (
                f" | 12h: avg_entry={float(recent.get('avg_entry_price', 0.0)):.3f}"
                f" wr={recent_wr:.1f}% pnl=${float(recent['realized_pnl']):+.2f}"
            )
        return line

    def _poll_working_orders(self, now: datetime) -> None:
        cancel_after = float(self.strategy.get("real_order_cancel_after_seconds", 0) or 0)
        ask_floor = float(self.strategy.get("real_order_cancel_if_ask_below", 0) or 0)
        for position in self.db.get_working_positions():
            if position.mode != "real":
                continue
            if position.market_end <= now:
                self._sync_working_position(position, cancel_if_partial=True, cancel_if_unfilled=True)
                continue
            seconds_left = (position.market_end - now).total_seconds()
            cancel_before = float(self.strategy.get("cancel_before_expiry_seconds", 15))
            cancel_if_partial = seconds_left <= cancel_before
            cancel_if_unfilled = False
            if cancel_after > 0:
                age = (now - position.opened_at).total_seconds()
                if age >= cancel_after:
                    cancel_if_unfilled = True
            if ask_floor > 0 and not cancel_if_unfilled:
                cur_ask = self._latest_prices.get((position.market_id, position.side))
                if cur_ask is not None and cur_ask < ask_floor:
                    cancel_if_unfilled = True
                    print(
                        f"[recovery] CANCEL ask-floor {position.symbol} {position.interval_minutes}m"
                        f" {position.side.upper()} | ask={cur_ask:.3f} < {ask_floor:.2f}"
                    )
            self._sync_working_position(
                position,
                cancel_if_partial=cancel_if_partial,
                cancel_if_unfilled=cancel_if_unfilled,
            )

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
            self.db.mark_position_unfilled(position.id, note=status or "unfilled")
            print(
                f"[recovery] UNFILLED {position.symbol} {position.interval_minutes}m"
                f" | released=${reserved_cost:.2f}"
            )
