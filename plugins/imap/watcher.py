"""Periodic IMAP poller — notifies Manager on new unseen messages.

Env:
  JARVIS_IMAP_ACCOUNTS  — JSON-список объектов аккаунтов (см. ниже)
  JARVIS_IMAP_INTERVAL  — интервал опроса в секундах (default: 120)

Формат одного аккаунта в JARVIS_IMAP_ACCOUNTS:
  {
    "host": "imap.yandex.ru",
    "port": 993,              // default 993
    "user": "user@yandex.ru",
    "password": "...",        // прямо или через password_env
    "password_env": "JARVIS_IMAP_PASS_YANDEX",  // альтернатива password
    "label": "Яндекс",       // отображается в нотисе
    "folder": "INBOX",        // default INBOX
    "ssl": true               // default true
  }

Если JARVIS_IMAP_ACCOUNTS не задан — воркер не запускается.
"""
import asyncio
import email
import imaplib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from email.header import decode_header, make_header

from bot.db import _db
from bot.timeutil import utcnow

logger = logging.getLogger(__name__)


NoticeCallback = Callable[[str, str], Awaitable[None]]


def _mark_seen(account_key: str, uid: int) -> bool:
    """Вставляет UID в imap_state. Возвращает True если UID новый."""
    try:
        with _db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO imap_state(account, uid, seen_at) "
                "VALUES (?, ?, ?)",
                (account_key, uid, utcnow().isoformat()),
            )
            new = conn.execute("SELECT changes()").fetchone()[0]
        return bool(new)
    except Exception:
        logger.exception("imap: _mark_seen failed account=%s uid=%d", account_key, uid)
        return False


def _load_accounts() -> list[dict]:
    raw = os.environ.get("JARVIS_IMAP_ACCOUNTS", "")
    if not raw:
        return []
    try:
        accounts = json.loads(raw)
        if not isinstance(accounts, list):
            raise ValueError("expected JSON array")
        return accounts
    except Exception:
        logger.error("JARVIS_IMAP_ACCOUNTS invalid JSON: %r", raw[:100])
        return []


def _resolve_password(account: dict) -> str:
    if "password_env" in account:
        return os.environ.get(account["password_env"], "")
    return account.get("password", "")


def _fetch_unseen_sync(account: dict) -> list[tuple[int, str, str]]:
    """Синхронный IMAP-запрос; вызывается через run_in_executor."""
    host = account["host"]
    port = int(account.get("port", 993))
    user = account["user"]
    password = _resolve_password(account)
    folder = account.get("folder", "INBOX")
    use_ssl = account.get("ssl", True)

    results: list[tuple[int, str, str]] = []
    try:
        M: imaplib.IMAP4 = (
            imaplib.IMAP4_SSL(host, port) if use_ssl else imaplib.IMAP4(host, port)
        )
        M.login(user, password)
        M.select(folder, readonly=True)

        status, data = M.uid("SEARCH", None, "UNSEEN")  # type: ignore[arg-type]
        if status != "OK":
            M.logout()
            return []

        uid_list = [u for u in (data[0] or b"").split() if u]
        for uid_bytes in uid_list:
            uid = int(uid_bytes)
            st, msg_data = M.uid(  # type: ignore[arg-type]
                "FETCH", uid_bytes,
                "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])",
            )
            if st != "OK" or not msg_data or msg_data[0] is None:
                continue
            raw_part = msg_data[0]
            raw_headers: bytes = raw_part[1] if isinstance(raw_part, tuple) else b""
            msg = email.message_from_bytes(raw_headers)
            subject = str(make_header(decode_header(msg.get("Subject", "(без темы)"))))
            from_addr = str(make_header(decode_header(msg.get("From", ""))))
            results.append((uid, subject, from_addr))

        M.logout()
    except Exception:
        logger.exception("imap: fetch_unseen failed %s@%s", user, host)

    return results


async def _poll_account(account: dict, send_notice: NoticeCallback) -> None:
    key = f"{account.get('host')}/{account.get('user')}"
    loop = asyncio.get_event_loop()
    try:
        messages = await loop.run_in_executor(None, _fetch_unseen_sync, account)
    except Exception:
        logger.exception("imap: executor failed account=%s", key)
        return

    label = account.get("label") or account.get("user", key)
    for uid, subject, from_addr in messages:
        if not _mark_seen(key, uid):
            continue
        text = f"📧 Новое письмо [{label}]\nОт: {from_addr}\nТема: {subject}"
        try:
            await send_notice(text, "imap")
        except Exception:
            logger.exception("imap: send_notice failed uid=%d", uid)


async def run_imap_watcher(send_notice: NoticeCallback) -> None:
    accounts = _load_accounts()
    if not accounts:
        logger.info("JARVIS_IMAP_ACCOUNTS not set — IMAP watcher disabled")
        return

    interval = int(os.environ.get("JARVIS_IMAP_INTERVAL", "120"))
    logger.info("imap_watcher: started (%d accounts, interval=%ds)", len(accounts), interval)

    while True:
        try:
            for account in accounts:
                await _poll_account(account, send_notice)
        except Exception:
            logger.exception("imap_watcher: poll cycle failed")
        await asyncio.sleep(interval)
