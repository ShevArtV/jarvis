"""Аккаунт движка, выбранный для топика (колонка ``sessions.account``).

Смена аккаунта сохраняет session_id: транскрипты у профилей общие (см.
engines/accounts.py), сессия продолжается под другим логином. Смена движка
аккаунт сбрасывает — это делает ``bot.sessions.set_engine``.
"""

from __future__ import annotations

from bot.db import _db
from bot.timeutil import utcnow
from engines.accounts import MAIN_ACCOUNT, account_names


def get_account(chat_id: int, thread_id: int) -> str:
    """Аккаунт топика. Неизвестный (убран из JARVIS_ACCOUNTS) — main."""
    with _db() as conn:
        row = conn.execute(
            "SELECT account, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row or not row[0] or row[0] not in account_names(row[1]):
        return MAIN_ACCOUNT
    return row[0]


def set_account(chat_id: int, thread_id: int, account: str) -> None:
    """Записывает аккаунт топика; main хранится как NULL. Запись топика уже
    есть: /engine показывает выбор только для существующего топика."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET account = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (None if account == MAIN_ACCOUNT else account,
             utcnow().isoformat(), chat_id, thread_id),
        )
