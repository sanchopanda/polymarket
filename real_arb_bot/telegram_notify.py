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
    """Telegram-бот для уведомлений real_arb_bot.

    Читает SIMPLE_BOT_TOKEN и TELEGRAM_CHAT_ID из env.
    get_status_fn() → str — вызывается при запросе статуса.
    """

    CHAT_ID_FILE = "data/.telegram_chat_id"

    def __init__(self, get_status_fn: Callable[[], str]) -> None:
        token = os.environ.get("SIMPLE_BOT_TOKEN", "")
        if not token:
            raise RuntimeError("SIMPLE_BOT_TOKEN не задан в .env")

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
        t = threading.Thread(target=self._poll, daemon=True, name="tg-bot")
        t.start()
        if self.chat_id:
            print(f"[Telegram] chat_id={self.chat_id} (из кеша)")
            self._send("🤖 <b>real_arb_bot перезапущен</b>")
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
            self._send("🤖 <b>real_arb_bot подключён</b>\nБуду присылать уведомления об открытии и закрытии позиций.")

        @bot.message_handler(commands=["status"])
        def handle_status(message):
            self.chat_id = message.chat.id
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

    # ── Уведомления ─────────────────────────────────────────────────────

    def notify_open(
        self,
        symbol: str,
        yes_venue: str,
        no_venue: str,
        yes_ask: float,
        no_ask: float,
        ask_sum: float,
        edge: float,
        cost: float,
        expected_profit: float,
        execution_status: str,
        kalshi_fill: float = 0.0,
        pm_fill: float = 0.0,
        is_paper: bool = False,
    ) -> None:
        paper_tag = " [PAPER]" if is_paper else ""
        if is_paper:
            icon = "📝"
            fill_line = "paper-трейд (реальных ордеров нет)"
        elif execution_status == "both_filled":
            icon = "✅"
            fill_line = f"Kalshi fill: {kalshi_fill:.4f} | PM fill: {pm_fill:.4f}"
        elif execution_status == "orphaned_kalshi":
            icon = "⚠️"
            fill_line = "Kalshi исполнен, Polymarket — нет! Одноногая позиция."
        else:
            icon = "❌"
            fill_line = f"Статус: {execution_status}"

        text = (
            f"{icon} <b>ОТКРЫТА{paper_tag}: {symbol}</b>\n"
            f"{yes_venue}:YES @ {yes_ask:.4f} + {no_venue}:NO @ {no_ask:.4f}\n"
            f"ask_sum={ask_sum:.4f} | edge={edge:.4f}\n"
            f"cost=${cost:.2f} | ожид. прибыль=${expected_profit:.2f}\n"
            f"{fill_line}"
        )
        self._send(text)

    def notify_resolve(
        self,
        symbol: str,
        pm_result: str,
        kalshi_result: str,
        pnl: float,
        lock_valid: bool,
        is_paper: bool = False,
    ) -> None:
        paper_tag = " [PAPER]" if is_paper else ""
        if lock_valid:
            icon = "💰" if pnl > 0 else "📉"
            validity = "арбитраж ✓"
        else:
            icon = "⚠️"
            validity = "ложный матч!"

        text = (
            f"{icon} <b>РЕЗОЛВ{paper_tag}: {symbol}</b>\n"
            f"PM={pm_result} | Kalshi={kalshi_result} | {validity}\n"
            f"P&L: <b>${pnl:+.2f}</b>"
        )
        self._send(text)

    def _send(self, text: str) -> None:
        if self.chat_id is None:
            return
        try:
            self.bot.send_message(self.chat_id, text, reply_markup=_status_keyboard())
        except Exception as e:
            print(f"[Telegram] Ошибка отправки: {e}")
