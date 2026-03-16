from __future__ import annotations

import threading
import time
from typing import Optional

import httpx

from src.db.store import Store
from src.config import Config


API = "https://api.telegram.org/bot{token}/{method}"


def _call(token: str, method: str, **kwargs) -> dict:
    try:
        r = httpx.post(API.format(token=token, method=method), json=kwargs, timeout=30)
        return r.json()
    except Exception:
        return {}


MAIN_KEYBOARD = {
    "inline_keyboard": [[
        {"text": "📊 Paper", "callback_data": "dashboard_paper"},
        {"text": "💰 Real", "callback_data": "dashboard_real"},
        {"text": "🔄 Обновить", "callback_data": "refresh"},
    ]]
}


def _send(token: str, chat_id: int, text: str, keyboard: dict | None = None) -> dict:
    chunks = [text[i:i+4096] for i in range(0, len(text), 4096)]
    result = {}
    for i, chunk in enumerate(chunks):
        kwargs: dict = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
        if keyboard and i == len(chunks) - 1:
            kwargs["reply_markup"] = keyboard
        result = _call(token, "sendMessage", **kwargs)
    return result


def _edit(token: str, chat_id: int, message_id: int, text: str, keyboard: dict | None = None) -> None:
    kwargs: dict = {"chat_id": chat_id, "message_id": message_id,
                    "text": text[:4096], "parse_mode": "HTML"}
    if keyboard:
        kwargs["reply_markup"] = keyboard
    _call(token, "editMessageText", **kwargs)


def _fmt_pnl(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}${val:.2f}"


def build_dashboard_text(store: Store, starting_balance: float, max_series_depth: int, real: bool = False) -> str:
    from src.db.models import BetSeries
    from datetime import datetime, timezone

    stats = store.get_portfolio_stats()
    all_bets = store.get_all_bets()
    all_series = store.get_all_series()

    open_bets = [b for b in all_bets if b.status == "open"]
    open_invested = sum(b.amount_usd + b.fee_usd for b in open_bets)
    open_potential = sum(b.shares for b in open_bets)
    free_cash = starting_balance + stats.total_pnl_realized - open_invested
    portfolio_value = starting_balance + stats.total_pnl_realized
    total_spent = sum(b.amount_usd + b.fee_usd for b in all_bets)

    won_series = sum(1 for s in all_series if s.status == "won")
    abandoned_series = sum(1 for s in all_series if s.status == "abandoned")

    title = "💰 <b>РЕАЛЬНЫЙ ПОРТФЕЛЬ</b>" if real else "📊 <b>ВИРТУАЛЬНЫЙ ПОРТФЕЛЬ</b>"
    lines = [
        title,
        f"  Начальный баланс:  ${starting_balance:.2f}",
        f"  Вложено в ставки:  ${open_invested:.2f}  ({len(open_bets)} поз.)",
        f"  Свободные средства: ${free_cash:.2f}",
        f"  Реализованный P&L: {_fmt_pnl(stats.total_pnl_realized)}",
        f"  Портфель:          ${portfolio_value:.2f}  ({_fmt_pnl(portfolio_value - starting_balance)})",
        f"  Если все выиграют:  ${free_cash + open_potential:.2f}",
        f"  Если все проиграют: ${free_cash:.2f}",
        "",
        "📈 <b>МАРТИНГЕЙЛ</b>",
        f"  Ставок всего:   {len(all_bets)}  (открытых: {stats.open_positions})",
        f"  Потрачено:      ${total_spent:.2f}",
        f"  P&L реализов.:  {_fmt_pnl(stats.total_pnl_realized)}",
        "",
        f"  Серий активных:   {stats.active_series_count}",
        f"  Серий выигранных: {won_series}",
        f"  Серий брошенных:  {abandoned_series}",
        f"  Серий всего:      {len(all_series)}",
    ]

    if stats.total_bets > 0:
        net_closed = sum(b.amount_usd + b.fee_usd for b in all_bets if b.status != "open")
        roi = stats.total_pnl_realized / net_closed * 100 if net_closed > 0 else 0
        lines += [
            "",
            f"  Побед / Поражений: {stats.win_count} / {stats.loss_count}",
            f"  Winrate:           {stats.win_rate * 100:.1f}%",
            f"  ROI:               {_fmt_pnl(stats.total_pnl_realized)} / ${net_closed:.2f} = {roi:.1f}%",
        ]

    # Среднее время серии
    finished = [s for s in all_series if s.finished_at and s.status in ("won", "abandoned")]
    if finished:
        avg_sec = sum((s.finished_at - s.started_at).total_seconds() for s in finished) / len(finished)
        avg_str = f"{avg_sec/60:.0f} мин." if avg_sec < 3600 else f"{avg_sec/3600:.1f} ч."
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        total_hours = max((now_dt - min(s.started_at for s in all_series)).total_seconds() / 3600, 0.01)
        rate = len(finished) / total_hours
        avg_skipped = store.get_avg_skipped_limit()
        lines += [
            "",
            f"  Среднее время серии: {avg_str}",
            f"  Темп:                {rate:.1f} серий/час",
        ]
        if avg_skipped > 0:
            lines.append(f"  Упущено (среднее):   {avg_skipped:.1f} канд./скан")

    # Статистика по глубинам
    won = [s for s in all_series if s.status == "won"]
    if won:
        from collections import Counter
        depth_counts = Counter(s.current_depth for s in won)
        total_won = len(won)
        lines += ["", "🎯 <b>ПОБЕДЫ ПО ГЛУБИНЕ</b>"]
        for d in range(max(depth_counts) + 1):
            count = depth_counts.get(d, 0)
            if count == 0:
                continue
            pct = count / total_won * 100
            lines.append(f"  d{d}: {count} ({pct:.0f}%)")

    # Активные серии
    active = store.get_active_series()
    if active:
        lines += ["", "📋 <b>АКТИВНЫЕ СЕРИИ</b>"]
        for s in active:
            lines.append(f"  d{s.current_depth}  нач.${s.initial_bet_size:.2f}  вложено ${s.total_invested:.2f}  [{s.started_at.strftime('%H:%M')}]")

    return "\n".join(lines)


class TelegramBot:
    def __init__(self, token: str, store: Store, config: Config,
                 real_store: Store | None = None) -> None:
        self.token = token
        self.store = store
        self.real_store = real_store
        self.config = config
        self._offset = 0
        self._stop = threading.Event()
        self.chat_id: int | None = None
        self._last_mode = "paper"  # Запоминаем последний дашборд для кнопки "Обновить"

    def start(self) -> None:
        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()
        print(f"[Telegram] Бот запущен.")

    def stop(self) -> None:
        self._stop.set()

    def send_alert(self, text: str) -> None:
        """Отправить уведомление всем известным чатам."""
        if self.chat_id:
            _send(self.token, self.chat_id, text)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                result = _call(self.token, "getUpdates", offset=self._offset, timeout=20)
                updates = result.get("result", [])
                for upd in updates:
                    self._offset = upd["update_id"] + 1
                    self._handle(upd)
            except Exception as e:
                print(f"[Telegram] Ошибка polling: {e}")
                time.sleep(5)

    def _get_dashboard(self, mode: str) -> str:
        if mode == "real" and self.real_store:
            return build_dashboard_text(
                self.real_store,
                self.config.paper_trading.starting_balance,
                self.config.real_martingale.max_series_depth,
                real=True,
            )
        return build_dashboard_text(
            self.store,
            self.config.paper_trading.starting_balance,
            self.config.martingale.max_series_depth,
            real=False,
        )

    def _handle(self, upd: dict) -> None:
        # Обычное сообщение
        msg = upd.get("message")
        if msg:
            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "").strip().lower()
            if not chat_id or not text:
                return
            self.chat_id = chat_id
            if text in ("/dashboard", "/d", "/start", "/help"):
                reply = self._get_dashboard("paper")
                self._last_mode = "paper"
                _send(self.token, chat_id, reply, keyboard=MAIN_KEYBOARD)
            return

        # Нажатие кнопки
        cb = upd.get("callback_query")
        if cb:
            chat_id = cb.get("message", {}).get("chat", {}).get("id")
            message_id = cb.get("message", {}).get("message_id")
            data = cb.get("data", "")
            cb_id = cb.get("id")

            _call(self.token, "answerCallbackQuery", callback_query_id=cb_id)

            if chat_id:
                self.chat_id = chat_id

            if data == "dashboard_paper":
                self._last_mode = "paper"
            elif data == "dashboard_real":
                self._last_mode = "real"
            # refresh оставляет _last_mode как есть

            if chat_id and message_id:
                reply = self._get_dashboard(self._last_mode)
                _edit(self.token, chat_id, message_id, reply, keyboard=MAIN_KEYBOARD)
