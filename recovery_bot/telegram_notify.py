from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Статус", callback_data="recovery_status"))
    return kb


class RecoveryTelegramNotifier:
    def __init__(
        self,
        get_status_fn: Callable[[], str],
        token_env: str = "TELEGRAM_TOKEN",
        chat_id_file: str = "data/.telegram_chat_id",
    ) -> None:
        self._get_status = get_status_fn
        self._chat_id_file = chat_id_file
        token = os.environ.get(token_env, "")
        if not token:
            print(f"[recovery_tg] {token_env} не задан — Telegram отключён")
            self._bot: Optional[telebot.TeleBot] = None
            self._chat_id: Optional[int] = None
            return
        self._chat_id = self._load_chat_id()
        self._bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        try:
            with open(self._chat_id_file) as fh:
                return int(fh.read().strip())
        except Exception:
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(self._chat_id_file, "w") as fh:
                fh.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        if not self._bot:
            return
        threading.Thread(
            target=self._bot.infinity_polling,
            kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
            daemon=True,
            name="tg-recovery",
        ).start()
        if self._chat_id:
            self.send("🤖 <b>recovery_bot запущен</b>")

    def _setup_handlers(self) -> None:
        bot = self._bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            self.send("🤖 <b>recovery_bot подключён</b>\n/recovery_status — статистика")

        @bot.message_handler(commands=["recovery_status"])
        def handle_status(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "recovery_status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self.send(self._get_status())
        except Exception as exc:
            print(f"[recovery_tg] status failed: {exc}")

    def send(self, text: str) -> None:
        if not self._bot or not self._chat_id:
            return
        try:
            self._bot.send_message(self._chat_id, text, reply_markup=_kb())
        except Exception as exc:
            print(f"[recovery_tg] send failed: {exc}")

    def notify_open(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        mode: str,
        strategy_name: str,
        touch_price: float,
        trigger_price: float,
        entry_price: float,
        filled_shares: float,
        total_cost: float,
    ) -> None:
        tag = "📝 PAPER" if mode == "paper" else "🔴 REAL"
        self.send(
            f"{tag} <b>OPEN {symbol} {interval_minutes}m NO</b>\n"
            f"strategy={strategy_name}\n"
            f"touch={touch_price:.3f} -> trigger={trigger_price:.3f}\n"
            f"entry={entry_price:.3f} | shares={filled_shares:.2f}\n"
            f"cost=${total_cost:.2f}"
        )

    def notify_resolve(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        mode: str,
        pnl: float,
        winning_side: str,
    ) -> None:
        emoji = "💰" if pnl >= 0 else "❌"
        tag = "📝 PAPER" if mode == "paper" else "🔴 REAL"
        self.send(
            f"{emoji} {tag} <b>RESOLVE {symbol} {interval_minutes}m NO</b>\n"
            f"winner={winning_side}\n"
            f"PnL: <b>${pnl:+.2f}</b>"
        )

