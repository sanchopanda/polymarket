from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderType

from src.api.gamma import GammaClient
from src.config import Config
from src.db.models import BetSeries, RedeemRecord, ScanLog, SimulatedBet, WalletSnapshot
from src.db.store import Store
from src.strategy.scanner import MarketScanner
from src.strategy.scorer import CandidateScorer, ScoredCandidate
from src.strategy.sizing import PositionSizer, compute_dynamic_base_bet, compute_max_deep_slots, compute_dynamic_active_series

CLOB_HOST = "https://clob.polymarket.com"
BUY_SIDE = "BUY"

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e на Polygon
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Tokens Framework
ERC20_BALANCE_ABI = [{"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}]
REDEEM_ABI = [{"inputs": [{"name": "collateralToken", "type": "address"}, {"name": "parentCollectionId", "type": "bytes32"}, {"name": "conditionId", "type": "bytes32"}, {"name": "indexSets", "type": "uint256[]"}], "name": "redeemPositions", "outputs": [], "stateMutability": "nonpayable", "type": "function"}]
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]


def _build_clob_client(private_key: str, chain_id: int, creds: ApiCreds | None = None, funder: str = "") -> PyClobClient:
    """Magic.link: signature_type=EOA(0), funder для баланса."""
    return PyClobClient(
        host=CLOB_HOST,
        chain_id=chain_id,
        key=private_key,
        creds=creds,
        funder=funder or None,
    )


class RealTradingEngine:
    """
    Движок реальной торговли на Polymarket через CLOB API.
    Использует те же стратегии/сайзер, что и PaperTradingEngine,
    но размещает настоящие ордера.
    """

    def __init__(self, config: Config, store: Store, gamma: GammaClient) -> None:
        self.config = config
        self.mg = config.real_martingale
        self.store = store
        self.gamma = gamma
        self.scanner = MarketScanner(gamma, config.strategy)
        self.scorer = CandidateScorer(config.strategy)
        self.sizer = PositionSizer(self.mg, config.paper_trading.taker_fee)
        self._clob: PyClobClient | None = None
        self._creds: ApiCreds | None = None
        self.alert_fn = None  # опционально: callable(str) для TG-уведомлений

    def _get_clob(self) -> PyClobClient:
        """Возвращает авторизованный CLOB-клиент (L2), инициализируя при первом вызове."""
        if self._clob is not None:
            return self._clob

        pk = self.config.wallet.private_key
        chain_id = self.config.wallet.chain_id
        if not pk:
            raise RuntimeError("WALLET_PRIVATE_KEY не задан в .env")

        # L1-клиент для получения API-ключей (без funder — EOA режим)
        l1_client = _build_clob_client(pk, chain_id)
        print("[Real] Получаем L2 API-ключи...")
        self._creds = l1_client.create_or_derive_api_creds()
        print(f"[Real] API-ключи получены: {self._creds.api_key[:8]}...")

        # Если задан proxy-кошелёк — используем его как funder для ордеров и баланса
        funder = self.config.wallet.proxy
        if funder:
            self._clob = _build_clob_client(pk, chain_id, self._creds, funder=funder)
        else:
            self._clob = _build_clob_client(pk, chain_id, self._creds)
        self._clob_balance = self._clob

        return self._clob

    def check_balance(self) -> float:
        """Возвращает баланс USDC.e кошелька напрямую из блокчейна."""
        from web3 import Web3
        from eth_account import Account

        pk = self.config.wallet.private_key
        if not pk:
            raise RuntimeError("WALLET_PRIVATE_KEY не задан в .env")
        address = Account.from_key(pk).address

        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(USDC_E),
                    abi=ERC20_BALANCE_ABI,
                )
                raw = contract.functions.balanceOf(Web3.to_checksum_address(address)).call()
                balance = raw / 1e6  # USDC.e: 6 decimals
                print(f"[Real] Баланс USDC.e: ${balance:.2f} (адрес: {address})")
                self.store.save_wallet_snapshot(WalletSnapshot(balance_usdc=balance))
                return balance
            except Exception as e:
                print(f"[Real] RPC {rpc} недоступен: {e}")

        raise RuntimeError("Все RPC недоступны, не удалось получить баланс")

    def run_scan(self, dry_run: bool = False) -> int:
        """Сканирует рынки и создаёт новые серии с реальными ордерами."""
        clob = self._get_clob()

        active_series = self.store.get_active_series()
        active_count = len(active_series)

        real_balance = self.check_balance()
        total_invested = self.store.get_total_invested_in_active_series()
        total_capital = real_balance + total_invested

        deep_slots = compute_max_deep_slots(
            total_capital,
            self.mg.initial_bet_size,
            self.mg.max_series_depth,
        )
        max_series = compute_dynamic_active_series(deep_slots)

        slots_full = active_count >= max_series
        if slots_full:
            print(f"[Scan] Все слоты заняты ({active_count}/{max_series})")

        print(f"[Scan] Загружаем рынки с Gamma API... (слоты: {active_count}/{max_series}, капитал: ${total_capital:.2f})")
        candidates = self.scanner.scan()
        ranked = self.scorer.rank(candidates, top_n=50)

        # Ретрай эскалаций: active-серии с проигранными ставками, но без открытых
        for series in self.store.get_series_pending_escalation():
            bets = self.store.get_series_bets(series.id)
            last_depth = max((b.series_depth for b in bets), default=0)
            print(f"[Scan] Повторная эскалация серии {series.id[:8]} depth={last_depth}")
            spent = self._escalate_series(series.id, last_depth, ranked, available_balance=real_balance)
            real_balance -= spent

        price_min = self.config.strategy.price_min or (self.config.strategy.target_price - self.config.strategy.price_tolerance)
        price_max = self.config.strategy.price_max or (self.config.strategy.target_price + self.config.strategy.price_tolerance)
        print(
            f"[Scan] Кандидатов: {len(ranked)} "
            f"(диапазон ${price_min:.2f}–${price_max:.2f})"
        )
        current_base_bet = self.mg.initial_bet_size
        print(f"[Scan] Базовая ставка: ${current_base_bet:.2f}")

        bets_placed = 0
        skipped_limit = 0
        for sc in ranked:
            if active_count >= max_series:
                if not self.store.already_bet(sc.market.id, sc.outcome):
                    skipped_limit += 1
                continue

            if self.store.already_bet(sc.market.id, sc.outcome):
                continue

            size = self.sizer.calculate(0, initial_bet_size=current_base_bet)

            if real_balance < size:
                print(f"[Scan] Недостаточно средств для новой серии (баланс ${real_balance:.2f} < ${size:.2f})")
                break

            fee = round(size * self.config.paper_trading.taker_fee, 6)
            shares = round(size / sc.price, 4)  # для записи в БД

            mode = "DRY" if dry_run else "ORDER"
            print(
                f"[{mode}] {sc.market.question[:60]}\n"
                f"         Исход: {sc.outcome} | Цена: ${sc.price:.4f} | "
                f"Мульт.: {1/sc.price:.1f}x | Скор: {sc.total_score:.3f}"
            )
            print(f"         Серия: новая (depth=0) | Ставка: ${size:.2f} + комиссия ${fee:.4f}")

            if not dry_run:
                order_id = ""
                try:
                    order_args = MarketOrderArgs(
                        token_id=sc.token_id,
                        amount=size,  # точная сумма в USDC
                        side=BUY_SIDE,
                    )
                    signed_order = clob.create_market_order(order_args)
                    resp = clob.post_order(signed_order, orderType=OrderType.FOK)
                    order_id = resp.get("orderID", "") if resp else ""
                    status = resp.get("status", "") if resp else ""
                    print(f"         order_id: {order_id} | status: {status}")
                    if status not in ("MATCHED", "FILLED", "matched", "filled"):
                        print(f"[Scan] Ордер не исполнен (status={status}) — пропускаем")
                        continue
                except Exception as e:
                    print(f"[Scan] Ошибка ордера: {e}")
                    continue

                total_cost = size + fee
                series = BetSeries(
                    initial_bet_size=current_base_bet,
                    total_invested=total_cost,
                )
                self.store.create_series(series)
                bet = SimulatedBet(
                    market_id=sc.market.id,
                    market_question=sc.market.question,
                    outcome=sc.outcome,
                    token_id=sc.token_id,
                    entry_price=sc.price,
                    amount_usd=size,
                    fee_usd=fee,
                    shares=shares,
                    score=sc.total_score,
                    placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    market_end_date=sc.market.end_date,
                    series_id=series.id,
                    series_depth=0,
                    order_id=order_id,
                )
                self.store.save_bet(bet)
                bets_placed += 1
                active_count += 1
                real_balance -= total_cost

        if skipped_limit > 0:
            print(f"[Scan] Пропущено из-за лимита серий: {skipped_limit}")

        log = ScanLog(
            scanned_at=datetime.now(timezone.utc).replace(tzinfo=None),
            total_markets=len(candidates),
            candidates_found=len(candidates),
            bets_placed=bets_placed,
            skipped_limit=skipped_limit,
        )
        if not dry_run:
            self.store.save_scan_log(log)

        skipped_str = f" | Пропущено из-за лимита: {skipped_limit}" if skipped_limit > 0 else ""
        print(f"[Scan] Готово. Кандидатов: {len(ranked)} | Новых серий: {bets_placed}{skipped_str}")
        return bets_placed

    def check_resolutions(self) -> int:
        """Проверяет открытые ставки через Gamma API. При победе — серия завершена, при проигрыше — эскалация."""
        # Убираем настоящие orphan-серии: active, ни одной ставки в принципе
        for series in self.store.get_active_series():
            if series.status != "active":
                continue
            bets = self.store.get_series_bets(series.id)
            if not bets:
                self.store.finish_series(series.id, "cancelled", 0.0)
                print(f"[Resolve] Серия {series.id[:8]} отменена (ни одной ставки)")

        open_bets = self.store.get_open_bets()
        if not open_bets:
            print("[Resolve] Открытых позиций нет.")
            return 0

        print(f"[Resolve] Проверяем {len(open_bets)} открытых позиций...")
        resolved_count = 0
        series_to_escalate = []

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for bet in open_bets:
            market = self.gamma.fetch_market(bet.market_id)
            if market is None:
                continue

            expired = bet.market_end_date and bet.market_end_date <= now
            prices_settled = any(p >= 0.95 or p <= 0.05 for p in market.outcome_prices)
            if not market.closed and not (expired and prices_settled):
                continue

            try:
                idx = market.outcomes.index(bet.outcome)
                exit_price = market.outcome_prices[idx]
            except (ValueError, IndexError):
                continue

            status = "won" if exit_price >= 0.9 else "lost"
            pnl = (exit_price * bet.shares) - bet.amount_usd - bet.fee_usd
            print(
                f"[Resolve] {'✓ ВЫИГРЫШ' if status == 'won' else '✗ ПРОИГРЫШ'}: "
                f"{bet.market_question[:60]} / {bet.outcome} | P&L: ${pnl:+.2f}"
            )

            if status == "won":
                if not market.closed:
                    print(f"[Redeem] Ждём финализации оракула (closed=False) — повторим в следующем цикле")
                    continue  # ставка остаётся open, без blockchain вызова
                redeemed = self._redeem_won_bet(bet)
                if not redeemed:
                    print(f"[Redeem] Повторим в следующем цикле — серия не закрывается")
                    continue  # ставка остаётся open, retry в следующем цикле
            self.store.resolve_bet(bet.id, exit_price)
            resolved_count += 1

            if bet.series_id:
                if status == "won":
                    self._finish_series_won(bet.series_id)
                else:
                    series_to_escalate.append((bet.series_id, bet.series_depth))

        # Получаем баланс кошелька один раз для всех эскалаций
        try:
            available_balance = self.check_balance()
        except Exception:
            available_balance = None

        total_invested = self.store.get_total_invested_in_active_series()
        total_capital = (available_balance or 0) + total_invested

        ranked = []
        if series_to_escalate:
            print("[Resolve] Загружаем рынки для эскалации...")
            candidates = self.scanner.scan()
            ranked = self.scorer.rank(candidates, top_n=50)
            already_used = sum(
                1 for sc in ranked if self.store.already_bet(sc.market.id, sc.outcome)
            )
            free_candidates = len(ranked) - already_used
            print(f"[Resolve] Кандидатов: {len(ranked)} | Свободных (не в портфеле): {free_candidates} | Нужно эскалаций: {len(series_to_escalate)}")
            for series_id, depth in series_to_escalate:
                self._escalate_series(series_id, depth, ranked, available_balance=available_balance)

        # После эскалаций: активируем waiting серии под свободные глубокие слоты
        active_series_now = self.store.get_active_series()
        current_deep = len([s for s in active_series_now if s.current_depth >= 2])
        max_deep = compute_max_deep_slots(
            total_capital,
            self.mg.initial_bet_size,
            self.mg.max_series_depth,
        )
        free_deep_slots = max_deep - current_deep
        if free_deep_slots > 0:
            waiting = self.store.get_waiting_series()
            for w in waiting[:free_deep_slots]:
                self.store.conn.execute(
                    "UPDATE bet_series SET status = 'active' WHERE id = ?", (w.id,)
                )
                self.store.conn.commit()
                print(f"[Resolve] ▶ Серия {w.id[:8]} из очереди активирована (глубоких: {current_deep+1}/{max_deep})")
                if not ranked:
                    candidates = self.scanner.scan()
                    ranked = self.scorer.rank(candidates, top_n=50)
                self._escalate_series(w.id, w.current_depth, ranked, available_balance=available_balance)
                current_deep += 1

        print(f"[Resolve] Закрыто позиций: {resolved_count}")
        return resolved_count

    def _redeem_won_bet(self, bet: SimulatedBet) -> bool:
        """Вызывает redeemPositions на CTF контракте.
        Возвращает True при успехе (серию можно закрывать), False — нужно повторить."""
        import httpx
        from web3 import Web3
        from eth_account import Account
        from datetime import datetime, timezone

        if self.store.already_redeemed(bet.id):
            return True  # уже выкупали

        pk = self.config.wallet.private_key
        address = Account.from_key(pk).address

        try:
            data = httpx.get(
                f"https://gamma-api.polymarket.com/markets/{bet.market_id}", timeout=10
            ).json()
            condition_id = data.get("conditionId", "")
            neg_risk = data.get("negRisk", False)
        except Exception as e:
            print(f"[Redeem] Ошибка получения conditionId: {e}")
            return False

        if not condition_id:
            print(f"[Redeem] conditionId не найден для рынка {bet.market_id}")
            return False

        if neg_risk:
            # neg-risk рынки — redeem через другой контракт, пока пропускаем
            print(f"[Redeem] Neg-risk рынок — пропускаем")
            return True  # не блокируем серию

        condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        amount = round((bet.exit_price or 1.0) * bet.shares, 4)

        tx_hash = self._send_redeem_tx(address, pk, condition_bytes)
        if tx_hash is None:
            return False
        self.store.save_redeem(RedeemRecord(
            bet_id=bet.id,
            market_id=bet.market_id,
            market_question=bet.market_question,
            amount_usd=amount,
            tx_hash=tx_hash,
            redeemed_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ))
        return True

    def redeem_all_winning_positions(self) -> None:
        """Запрашивает все выигранные позиции аккаунта через Polymarket API и выкупает их."""
        import httpx
        from web3 import Web3
        from eth_account import Account
        from datetime import datetime, timezone

        pk = self.config.wallet.private_key
        address = Account.from_key(pk).address

        try:
            data = httpx.get(
                f"https://data-api.polymarket.com/positions?user={address}&sizeThreshold=0.01",
                timeout=15,
            ).json()
        except Exception as e:
            print(f"[Redeem] Ошибка получения позиций: {e}")
            return

        winning = [p for p in data if p.get("curPrice", 0) >= 0.9 and not p.get("negativeRisk", False)]
        total_value = sum(p["currentValue"] for p in winning)
        print(f"[Redeem] Найдено выигранных позиций: {len(winning)} на сумму ${total_value:.2f}")

        for pos in winning:
            condition_id = pos["conditionId"]
            title = pos["title"]
            amount = pos["currentValue"]
            print(f"[Redeem] ${amount:.2f} | {title[:55]}")

            condition_bytes = bytes.fromhex(condition_id.replace("0x", ""))
            tx_hash = self._send_redeem_tx(address, pk, condition_bytes)
            if tx_hash:
                self.store.save_redeem(RedeemRecord(
                    bet_id="external",
                    market_id=pos.get("asset", ""),
                    market_question=title,
                    amount_usd=amount,
                    tx_hash=tx_hash,
                    redeemed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                ))
            else:
                print(f"[Redeem] Не удалось выкупить — попробуйте позже")

    def _send_redeem_tx(self, address: str, pk: str, condition_bytes: bytes) -> str | None:
        """Отправляет redeemPositions на CTF контракте. Возвращает tx_hash или None при ошибке."""
        from web3 import Web3

        for rpc in POLYGON_RPCS:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                ctf = w3.eth.contract(
                    address=Web3.to_checksum_address(CTF_ADDRESS), abi=REDEEM_ABI
                )
                addr = Web3.to_checksum_address(address)
                gas_price = w3.eth.gas_price
                confirmed_nonce = w3.eth.get_transaction_count(addr, "latest")
                pending_nonce = w3.eth.get_transaction_count(addr, "pending")

                if pending_nonce > confirmed_nonce:
                    nonce = confirmed_nonce
                    gas_price = max(int(gas_price * 1.3), w3.to_wei(50, "gwei"))
                else:
                    nonce = confirmed_nonce
                    gas_price = max(gas_price, w3.to_wei(50, "gwei"))

                tx = ctf.functions.redeemPositions(
                    Web3.to_checksum_address(USDC_E),
                    bytes(32),
                    condition_bytes,
                    [1, 2],
                ).build_transaction({
                    "from": addr,
                    "nonce": nonce,
                    "gasPrice": gas_price,
                    "gas": 200000,
                    "chainId": self.config.wallet.chain_id,
                })
                signed = w3.eth.account.sign_transaction(tx, pk)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                tx_str = "0x" + tx_hash.hex()
                print(f"[Redeem] Ждём подтверждения транзакции...")
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                if receipt.status == 1:
                    print(f"[Redeem] ✓ Выплата получена | tx: {tx_str[:20]}...")
                    return tx_str
                else:
                    print(f"[Redeem] ✗ Транзакция отклонена контрактом — повторим позже")
                    return None
            except Exception as e:
                err = str(e)
                if "not in the chain" in err or "already known" in err or "nonce too low" in err or "replacement transaction underpriced" in err:
                    print(f"[Redeem] Проблема с транзакцией — повторим позже")
                    return None
                print(f"[Redeem] RPC {rpc} недоступен: {e}")
                continue

        print(f"[Redeem] Все RPC недоступны")
        return None

    def _finish_series_won(self, series_id: str) -> None:
        bets = self.store.get_series_bets(series_id)
        total_pnl = sum(b.pnl or 0 for b in bets)
        self.store.finish_series(series_id, "won", total_pnl)
        series = self.store.get_series_by_id(series_id)
        print(
            f"[Series] ✓ Серия завершена ПОБЕДОЙ | "
            f"Глубина: {series.current_depth} | Вложено: ${series.total_invested:.2f} | "
            f"P&L серии: ${total_pnl:+.2f}"
        )

    def _escalate_series(self, series_id: str, current_depth: int, ranked: List[ScoredCandidate], available_balance: float | None = None) -> float:
        """Эскалация серии: удвоение ставки на новом рынке. Возвращает потраченную сумму (0 если не эскалировали)."""
        clob = self._get_clob()
        series = self.store.get_series_by_id(series_id)
        if not series:
            return 0.0

        next_depth = current_depth + 1

        # Проверяем лимит глубоких слотов (depth≥2)
        if next_depth >= 2:
            active_series = self.store.get_active_series()
            current_deep = len([
                s for s in active_series
                if s.current_depth >= 2 and s.id != series_id
            ])
            total_invested = self.store.get_total_invested_in_active_series()
            total_capital = (available_balance or 0) + total_invested
            max_deep = compute_max_deep_slots(
                total_capital,
                series.initial_bet_size,
                self.mg.max_series_depth,
            )
            if current_deep >= max_deep:
                self.store.set_series_waiting(series_id)
                print(
                    f"[Series] ⏸ Серия {series_id[:8]} в очереди "
                    f"(глубоких: {current_deep}/{max_deep})"
                )
                return 0.0

        if next_depth >= self.mg.max_series_depth:
            bets = self.store.get_series_bets(series_id)
            total_pnl = sum(b.pnl or 0 for b in bets)
            self.store.finish_series(series_id, "abandoned", total_pnl)
            print(
                f"[Series] ✗ Серия БРОШЕНА (лимит глубины {self.mg.max_series_depth}) | "
                f"P&L: ${total_pnl:+.2f}"
            )
            return 0.0

        # Проверяем реальный баланс кошелька
        size_needed = self.sizer.calculate(next_depth, initial_bet_size=series.initial_bet_size)
        if available_balance is not None and available_balance < size_needed:
            self.store.set_series_waiting(series_id)
            print(
                f"[Series] ⏸ Серия {series_id[:8]} ждёт пополнения "
                f"(баланс ${available_balance:.2f} < нужно ${size_needed:.2f})"
            )
            return 0.0

        for sc in ranked:
            if self.store.already_bet(sc.market.id, sc.outcome):
                continue

            size = size_needed
            shares = round(size / sc.price, 4)  # для записи в БД
            order_id = ""

            try:
                order_args = MarketOrderArgs(
                    token_id=sc.token_id,
                    amount=size,  # точная сумма в USDC
                    side=BUY_SIDE,
                )
                signed_order = clob.create_market_order(order_args)
                resp = clob.post_order(signed_order, orderType=OrderType.FOK)
                order_id = resp.get("orderID", "") if resp else ""
                status = resp.get("status", "") if resp else ""
                if status not in ("MATCHED", "FILLED", "matched", "filled"):
                    print(f"[Series] Ордер не исполнен (status={status}) — пробуем следующий рынок")
                    continue
            except Exception as e:
                print(f"[Series] Ошибка ордера при эскалации: {e} — пробуем следующий рынок")
                continue

            fee = round(size * self.config.paper_trading.taker_fee, 6)
            total_cost = size + fee
            bet = SimulatedBet(
                market_id=sc.market.id,
                market_question=sc.market.question,
                outcome=sc.outcome,
                token_id=sc.token_id,
                entry_price=sc.price,
                amount_usd=size,
                fee_usd=fee,
                shares=shares,
                score=sc.total_score,
                placed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                market_end_date=sc.market.end_date,
                series_id=series_id,
                series_depth=next_depth,
                order_id=order_id,
            )
            self.store.save_bet(bet)
            self.store.update_series_depth(series_id, next_depth, total_cost)
            print(
                f"[Series] ↑ Эскалация: depth={next_depth} | Ставка: ${size:.2f} | "
                f"Цена: ${sc.price:.4f} | {sc.market.question[:50]} / {sc.outcome}"
            )
            return total_cost

        print(f"[Series] ⏳ Нет рынка для эскалации depth={next_depth} — повторим в следующем цикле")
        return 0.0
