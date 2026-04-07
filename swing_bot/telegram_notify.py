from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Статус", callback_data="swing_status"))
    return kb


class SwingTelegramNotifier:
    """
    Telegram-уведомления для swing_bot.
    Токен: FAST_ARB_BOT_TOKEN.
    chat_id: data/.telegram_chat_id (общий с real_arb_bot).
    """

    def __init__(
        self,
        get_status_fn: Callable[[], str],
        token_env: str = "FAST_ARB_BOT_TOKEN",
        chat_id_file: str = "data/.telegram_chat_id",
    ) -> None:
        self._get_status = get_status_fn
        self._chat_id_file = chat_id_file
        token = os.environ.get(token_env, "")
        if not token:
            print(f"[swing_tg] {token_env} не задан — Telegram отключён")
            self._bot: Optional[telebot.TeleBot] = None
            self._chat_id: Optional[int] = None
            return

        self._chat_id = self._load_chat_id()
        self._bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        try:
            with open(self._chat_id_file) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(self._chat_id_file, "w") as f:
                f.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        if not self._bot:
            return
        threading.Thread(
            target=self._bot.infinity_polling,
            kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
            daemon=True,
            name="tg-swing",
        ).start()
        if self._chat_id:
            print(f"[swing_tg] chat_id={self._chat_id}")
            self.send("🤖 <b>swing_bot запущен</b>")
        else:
            print("[swing_tg] chat_id не найден — отправь /start боту")

    def _setup_handlers(self) -> None:
        bot = self._bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            print(f"[swing_tg] подключён chat_id={self._chat_id}")
            self.send("🤖 <b>swing_bot подключён</b>\n/swing_status — статистика")

        @bot.message_handler(commands=["swing_status"])
        def handle_status(msg):
            self._chat_id = msg.chat.id
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "swing_status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self.send(self._get_status())
        except Exception as e:
            print(f"[swing_tg] ошибка статуса: {e}")

    def send(self, text: str) -> None:
        if not self._bot or not self._chat_id:
            return
        try:
            self._bot.send_message(self._chat_id, text, reply_markup=_kb())
        except Exception as exc:
            print(f"[swing_tg] send failed: {exc}")

    # ── уведомления ──────────────────────────────────────────────

    def notify_entry(
        self,
        symbol: str,
        side_label: str,
        ws_price: float,
        rest_price: float,
        shares: float,
        stake: float,
    ) -> None:
        self.send(
            f"📈 <b>ВХОД: {symbol} {side_label}</b>\n"
            f"ws={ws_price:.3f} → rest={rest_price:.3f}\n"
            f"shares={shares:.2f} @ ${stake:.2f}"
        )

    def notify_sell(self, symbol: str, side_label: str, entry: float, exit_price: float, pnl: float) -> None:
        emoji = "✅" if pnl >= 0 else "📉"
        self.send(
            f"{emoji} <b>ПРОДАЖА: {symbol} {side_label}</b>\n"
            f"entry={entry:.3f} → sell={exit_price:.3f}\n"
            f"PnL: <b>${pnl:+.4f}</b>"
        )

    def notify_arb(
        self,
        symbol: str,
        entry_label: str,
        entry_price: float,
        hedge_label: str,
        hedge_price: float,
        pnl: float,
    ) -> None:
        self.send(
            f"✅ <b>АРБ: {symbol}</b>\n"
            f"{entry_label}={entry_price:.3f} + {hedge_label}={hedge_price:.3f} | "
            f"edge={1 - entry_price - hedge_price:.3f}\n"
            f"PnL: <b>${pnl:+.4f}</b>"
        )

    def notify_flip(self, symbol: str, hedge_label: str, hedge_price: float, flip_shares: float) -> None:
        self.send(
            f"🔄 <b>ФЛИП: {symbol} → {hedge_label}</b>\n"
            f"{hedge_label}@{hedge_price:.3f} | shares={flip_shares:.2f}"
        )

    def notify_resolve(
        self,
        symbol: str,
        exit_type: str | None,
        hold_reason: str | None,
        winning_side: str,
        pnl: float,
        cumulative: float,
    ) -> None:
        emoji = "💰" if pnl >= 0 else "❌"
        exit_label = exit_type or "hold"
        reason_line = f"\nreason={hold_reason}" if exit_type is None and hold_reason else ""
        self.send(
            f"{emoji} <b>РЕЗОЛВ: {symbol}</b>\n"
            f"winner={winning_side} | exit={exit_label}{reason_line}\n"
            f"PnL: <b>${pnl:+.4f}</b> | итого: ${cumulative:+.4f}"
        )
