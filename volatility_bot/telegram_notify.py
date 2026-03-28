from __future__ import annotations

import os
import threading
from typing import Callable

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Статус", callback_data="status"))
    return kb


class TelegramNotifier:
    """Telegram-уведомления для volatility_bot.

    Читает SIMPLE_BOT_TOKEN из env.
    get_status_fn() → str вызывается при /status.
    """

    CHAT_ID_FILE = "data/.telegram_chat_id"

    def __init__(self, get_status_fn: Callable[[], str]) -> None:
        token = os.environ.get("SIMPLE_BOT_TOKEN", "")
        if not token:
            raise RuntimeError("SIMPLE_BOT_TOKEN не задан в .env")
        self._get_status = get_status_fn
        self.chat_id: int | None = self._load_chat_id()
        self.bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> int | None:
        try:
            return int(open(self.CHAT_ID_FILE).read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            open(self.CHAT_ID_FILE, "w").write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        threading.Thread(target=self.bot.infinity_polling,
                         kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
                         daemon=True, name="tg-vol").start()
        if self.chat_id:
            print(f"[Telegram] chat_id={self.chat_id}")
            self._send("🤖 <b>volatility_bot перезапущен</b>")
        else:
            print("[Telegram] Ожидаю /start от пользователя...")

    def _setup_handlers(self) -> None:
        bot = self.bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self.chat_id = msg.chat.id
            self._save_chat_id(self.chat_id)
            print(f"[Telegram] Подключён chat_id={self.chat_id}")
            self._send("🤖 <b>volatility_bot подключён</b>\n/status — текущий статус")

        @bot.message_handler(commands=["status"])
        def handle_status(msg):
            self.chat_id = msg.chat.id
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self._send(self._get_status())
        except Exception as e:
            print(f"[Telegram] Ошибка статуса: {e}")

    def _send(self, text: str) -> None:
        if self.chat_id is None:
            return
        try:
            self.bot.send_message(self.chat_id, text, reply_markup=_kb())
        except Exception as e:
            print(f"[Telegram] Ошибка отправки: {e}")
