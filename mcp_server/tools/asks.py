"""MCP-тул ask_user: вопрос человеку в топик с ожиданием ответа."""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from typing import Any

from bot.timeutil import utcnow
from mcp_server import common
from mcp_server.common import logger, mcp

_TASK_TAG_RE = re.compile(r"#[A-Za-z0-9][A-Za-z0-9._-]*")


def _external_executor_task(chat_id: int, thread_id: int) -> tuple[str, str] | None:
    """Внешняя задача, по которой топик прямо сейчас работает как ИСПОЛНИТЕЛЬ.

    Возвращает ``(тег, source)`` — тег задачи ('#123', или '#?' если тег не
    распознан) и метку интеграции, поднявшей ход. None — обычный ход
    (сообщение человека, триггер Менеджеру, роль неизвестна).

    Роль пишет интегратор в agent_triggers.role; на любой source, а не только
    на доску — контракт «исполнителю вопросы в трекер, не в чат» общий.
    NULL/нет колонки → None: гард не должен включаться вслепую на
    неперезапущенном стеке.
    """
    try:
        with common._connect() as conn:
            row = conn.execute(
                "SELECT text, source FROM agent_triggers "
                "WHERE chat_id = ? AND thread_id = ? "
                "AND status = 'in_progress' AND role = 'executor' "
                "ORDER BY id DESC LIMIT 1",
                (chat_id, thread_id),
            ).fetchone()
    except sqlite3.OperationalError:
        logger.warning("agent_triggers.role is missing — external ask_user guard is off")
        return None
    if row is None:
        return None
    match = _TASK_TAG_RE.search(row["text"] or "")
    return (match.group(0) if match else "#?"), (row["source"] or "external")


def _ask_keyboard(ask_id: int, options: list[str]) -> dict[str, Any]:
    """Inline-клавиатура вариантов. В callback_data идёт ИНДЕКС, а не текст:
    Telegram ограничивает callback_data 64 байтами."""
    return {
        "inline_keyboard": [
            [{"text": opt[:64], "callback_data": f"ask:{ask_id}:{idx}"}]
            for idx, opt in enumerate(options)
        ]
    }


def _touch_ask(ask_id: int) -> None:
    """Пульс ожидания: бот по нему отличает живой вопрос от брошенного."""
    try:
        with common._connect() as conn:
            conn.execute(
                "UPDATE ask_requests SET polled_at = ? WHERE id = ?",
                (utcnow().isoformat(), ask_id),
            )
    except sqlite3.OperationalError:
        # Бот ещё не перезапущен и колонки нет — работаем без пульса.
        pass


def _expire_ask(ask_id: int) -> None:
    with common._connect() as conn:
        conn.execute(
            "UPDATE ask_requests SET status = 'timed_out', answered_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (utcnow().isoformat(), ask_id),
        )


@mcp.tool(
    name="ask_user",
    description=(
        "Ask the human a question in his Telegram topic and BLOCK until he "
        "answers. Use this whenever you need a decision only he can make: "
        "before destructive or irreversible actions (deleting files, DROP/"
        "DELETE, force-push, anything on production), when the task is "
        "ambiguous and guessing would waste work, or when you must choose "
        "between options with real trade-offs.\n\n"
        "Pass `options` to render tappable buttons — prefer this over free-form "
        "questions, it is much faster for him to answer. He can always reply "
        "with text instead, so keep questions answerable either way.\n\n"
        "Returns {status: 'answered', answer, option_index, via} once he "
        "responds, or {status: 'timed_out', answer: <default>} if he does not "
        "answer within `timeout_seconds`. On timeout do NOT assume consent: "
        "treat it as 'no answer' and stop, unless you passed an explicit "
        "`default`.\n\n"
        "Do not use this for questions you can answer yourself by reading the "
        "code, running a command, or checking git — he is not a lookup service.\n\n"
        "NOT available while you work on an external tracker's task as its "
        "assignee: such a call is rejected with {status: 'blocked'}. There the "
        "whole dialogue belongs in the task itself — post your question as a "
        "task comment and end your turn; his reply wakes you again."
    ),
)
async def ask_user(
    question: str,
    thread_id: int,
    options: list[str] | None = None,
    chat_id: int | None = None,
    timeout_seconds: int = 1800,
    default: str | None = None,
    poll_interval: float = 2.0,
) -> dict[str, Any]:
    """Post a question into the topic and wait for the human's answer."""
    question = (question or "").strip()
    if not question:
        return {"status": "error", "error": "question is empty"}
    if timeout_seconds <= 0:
        timeout_seconds = 1800
    elif timeout_seconds > 3600:
        timeout_seconds = 3600
    poll_interval = min(max(poll_interval, 1.0), 30.0)
    options = [str(o).strip() for o in (options or []) if str(o).strip()][:8]
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()

    # Работаешь по внешней задаче как исполнитель — чат не твой канал: ответ в
    # нём осел бы мимо трекера. Отказ вместо вопроса, ничего не отправляем.
    external = _external_executor_task(target_chat_id, thread_id)
    if external is not None:
        task_tag, source = external
        logger.info(
            "ask_user blocked for %s task %s (thread=%s)", source, task_tag, thread_id,
        )
        return {
            "status": "blocked",
            "task": task_tag,
            "source": source,
            "error": (
                f"Ты работаешь над задачей {task_tag} из внешнего трекера "
                f"({source}) — вопросы в чат отключены. Напиши комментарий в "
                "задаче и заверши ход: ответ придёт следующим триггером. "
                "ОБЯЗАТЕЛЬНО упомяни адресата по логину — «@<логин>» в тексте "
                "комментария: комментарий без упоминания никого не будит, и "
                "вопрос повиснет. Логин постановщика виден в карточке. Перед "
                "опасным действием так же остановись и спроси комментарием, не "
                "делай его на своё усмотрение."
            ),
        }

    now = utcnow().isoformat()

    with common._connect() as conn:
        cur = conn.execute(
            "INSERT INTO ask_requests(chat_id, thread_id, question, options_json, "
            "status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (
                target_chat_id, thread_id, question,
                json.dumps(options, ensure_ascii=False) if options else None,
                now,
            ),
        )
        ask_id = cur.lastrowid

    body = f"❓ {question}\n\n"
    body += (
        "Выбери вариант или ответь сообщением."
        if options else "Ответь сообщением в этот топик."
    )
    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": body,
        "message_thread_id": thread_id,
    }
    if options:
        params["reply_markup"] = _ask_keyboard(ask_id, options)

    try:
        sent = common._telegram_api("sendMessage", params)
    except RuntimeError as exc:
        with common._connect() as conn:
            conn.execute(
                "UPDATE ask_requests SET status = 'failed' WHERE id = ?", (ask_id,),
            )
        return {"status": "error", "error": f"failed to post question: {exc}"}

    tg_msg_id = sent.get("message_id")
    with common._connect() as conn:
        conn.execute(
            "UPDATE ask_requests SET telegram_message_id = ? WHERE id = ?",
            (tg_msg_id, ask_id),
        )
    # Бот ищет только status='pending', а истёкший вопрос всё равно остаётся
    # последним сообщением топика — messages_log даёт боту его message_id,
    # чтобы отличить «ответ на протухший вопрос» от обычного сообщения.
    try:
        with common._connect() as conn:
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES (?, ?, 'out', 'ask_user', ?, ?, ?)",
                (target_chat_id, thread_id, question, tg_msg_id, now),
            )
    except Exception as exc:
        logger.warning("ask_user #%s: failed to log question to messages_log: %s",
                       ask_id, exc)
    logger.info("ask_user #%s posted to thread=%s (options=%d)",
                ask_id, thread_id, len(options))

    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            _touch_ask(ask_id)
            with common._connect() as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT status, answer, option_index, via FROM ask_requests WHERE id = ?",
                    (ask_id,),
                ).fetchone()
            if row is not None and row["status"] == "answered":
                logger.info("ask_user #%s answered via %s", ask_id, row["via"])
                return {
                    "status": "answered",
                    "ask_id": ask_id,
                    "answer": row["answer"],
                    "option_index": row["option_index"],
                    "via": row["via"],
                }
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(poll_interval)
    except BaseException:
        # Вызов прерван (клиент бросил его по tool_timeout, процесс гасят) —
        # ответа никто не ждёт, иначе он съест следующее сообщение в топик.
        _expire_ask(ask_id)
        logger.info("ask_user #%s abandoned by the caller", ask_id)
        raise

    # Таймаут: закрываем вопрос, гасим кнопки — чтобы по протухшему нельзя было
    # кликнуть и чтобы следующее сообщение в топик не съелось как «ответ».
    _expire_ask(ask_id)
    if tg_msg_id:
        try:
            common._telegram_api("editMessageText", {
                "chat_id": target_chat_id,
                "message_id": tg_msg_id,
                "text": (
                    f"❓ {question}\n\n⌛ Вопрос истёк — ответа не было. "
                    "Можешь всё равно ответить сообщением: передам агенту "
                    "в ближайшие 15 минут."
                ),
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": []},
            })
        except RuntimeError:
            pass
    logger.info("ask_user #%s timed out after %ss", ask_id, timeout_seconds)
    return {
        "status": "timed_out",
        "ask_id": ask_id,
        "answer": default,
        "timeout_seconds": timeout_seconds,
    }
