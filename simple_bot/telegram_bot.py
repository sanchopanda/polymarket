"""Простой Telegram-бот для simple_bot."""

from __future__ import annotations

import threading
import time

import httpx

from simple_bot.db import BotDB

API = "https://api.telegram.org/bot{token}/{method}"

KEYBOARD = {
    "inline_keyboard": [[
        {"text": "🔄 Обновить", "callback_data": "refresh"},
    ]]
}


def _call(token: str, method: str, **kwargs) -> dict:
    try:
        r = httpx.post(API.format(token=token, method=method), json=kwargs, timeout=30)
        return r.json()
    except Exception:
        return {}


def _send(token: str, chat_id: int, text: str) -> dict:
    return _call(token, "sendMessage",
                 chat_id=chat_id, text=text, parse_mode="HTML",
                 reply_markup=KEYBOARD)


def _edit(token: str, chat_id: int, message_id: int, text: str) -> None:
    _call(token, "editMessageText",
          chat_id=chat_id, message_id=message_id,
          text=text, parse_mode="HTML", reply_markup=KEYBOARD)


def build_status(db: BotDB, starting_balance: float) -> str:
    stats = db.stats()
    free = starting_balance + stats["realized_pnl"] - stats["open_invested"]
    total_closed = stats["won"] + stats["lost"]
    win_rate = stats["won"] / total_closed * 100 if total_closed > 0 else 0
    net_closed = sum(
        b.amount + b.fee for b in db.get_all_bets() if b.status != "open"
    )
    roi = stats["realized_pnl"] / net_closed * 100 if net_closed > 0 else 0
    pnl_sign = "+" if stats["realized_pnl"] >= 0 else ""

    lines = [
        "📊 <b>ВИРТУАЛЬНЫЙ ПОРТФЕЛЬ</b>",
        "",
        f"  Баланс:        <b>${free:.2f}</b>",
        f"  Начальный:     ${starting_balance:.2f}",
        f"  P&amp;L:            {pnl_sign}${stats['realized_pnl']:.2f}  (ROI {roi:+.1f}%)",
        "",
        f"  Ставок всего:  {stats['total']}",
        f"  Открытых:      {stats['open']}",
        f"  Выиграно:      {stats['won']}",
        f"  Проиграно:     {stats['lost']}",
        f"  Win rate:      {win_rate:.1f}%",
    ]
    return "\n".join(lines)


class SimpleTelegramBot:
    def __init__(self, token: str, db: BotDB, starting_balance: float) -> None:
        self.token = token
        self.db = db
        self.starting_balance = starting_balance
        self._offset = 0
        self._stop = threading.Event()
        self.chat_id: int | None = None

    def start(self) -> None:
        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()
        print("[Telegram] Бот запущен.")

    def stop(self) -> None:
        self._stop.set()

    def send_alert(self, text: str) -> None:
        if self.chat_id:
            _send(self.token, self.chat_id, text)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                result = _call(self.token, "getUpdates", offset=self._offset, timeout=20)
                for upd in result.get("result", []):
                    self._offset = upd["update_id"] + 1
                    self._handle(upd)
            except Exception as e:
                print(f"[Telegram] Ошибка: {e}")
                time.sleep(5)

    def _handle(self, upd: dict) -> None:
        msg = upd.get("message")
        if msg:
            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "").strip().lower()
            if chat_id:
                self.chat_id = chat_id
            if text in ("/start", "/d", "/dashboard", "/status"):
                _send(self.token, chat_id, build_status(self.db, self.starting_balance))
            return

        cb = upd.get("callback_query")
        if cb:
            chat_id = cb.get("message", {}).get("chat", {}).get("id")
            message_id = cb.get("message", {}).get("message_id")
            cb_id = cb.get("id")
            _call(self.token, "answerCallbackQuery", callback_query_id=cb_id)
            if chat_id:
                self.chat_id = chat_id
            if chat_id and message_id:
                _edit(self.token, chat_id, message_id,
                      build_status(self.db, self.starting_balance))
