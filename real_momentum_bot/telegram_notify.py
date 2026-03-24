from __future__ import annotations

import os
import threading
from typing import Callable

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _status_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Обновить статус", callback_data="status"))
    return kb


class TelegramNotifier:
    """Telegram-бот статуса для real_momentum_bot."""

    CHAT_ID_FILE = "data/.telegram_chat_id_real_momentum"

    def __init__(self, get_status_fn: Callable[[], str]) -> None:
        token = os.environ.get("TELEGRAM_TOKEN", "")
        if not token:
            raise RuntimeError("TELEGRAM_TOKEN не задан в .env")

        self.chat_id: int | None = self._load_chat_id()
        self._get_status = get_status_fn
        self.bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> int | None:
        try:
            with open(self.CHAT_ID_FILE) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(self.CHAT_ID_FILE, "w") as f:
                f.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        t = threading.Thread(target=self._poll, daemon=True, name="tg-real-momentum-bot")
        t.start()
        if self.chat_id:
            print(f"[Telegram] chat_id={self.chat_id} (из кеша)")
            self._send("🤖 <b>real_momentum_bot перезапущен</b>")
        else:
            print("[Telegram] Ожидаю /start от пользователя...")

    def _poll(self) -> None:
        self.bot.infinity_polling(timeout=10, long_polling_timeout=5, logger_level=None)

    def _setup_handlers(self) -> None:
        bot = self.bot

        @bot.message_handler(commands=["start"])
        def handle_start(message):
            self.chat_id = message.chat.id
            self._save_chat_id(self.chat_id)
            print(f"[Telegram] Подключён chat_id={self.chat_id}")
            self._send("🤖 <b>real_momentum_bot подключён</b>\nКнопкой можно запрашивать текущий статус.")

        @bot.message_handler(commands=["status"])
        def handle_status(message):
            self.chat_id = message.chat.id
            self._save_chat_id(self.chat_id)
            self._send_status(message.chat.id)

        @bot.callback_query_handler(func=lambda c: c.data == "status")
        def handle_callback(call):
            bot.answer_callback_query(call.id, text="Обновляю...")
            self._send_status(call.message.chat.id)

    def _send_status(self, chat_id: int) -> None:
        try:
            text = self._get_status()
            self.bot.send_message(chat_id, text, reply_markup=_status_keyboard())
        except Exception as e:
            print(f"[Telegram] Ошибка статуса: {e}")

    def _send(self, text: str) -> None:
        if self.chat_id is None:
            return
        try:
            self.bot.send_message(self.chat_id, text, reply_markup=_status_keyboard())
        except Exception as e:
            print(f"[Telegram] Ошибка отправки: {e}")
