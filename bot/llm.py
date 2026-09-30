"""Вызов движка: системный префикс, поток ответа, reply-to контекст.

Системный блок ``[SYSTEM:]`` раньше клеился в тело КАЖДОГО user-сообщения и
копился в транскрипте. Теперь он уходит в системный канал движка (у claude —
``--append-system-prompt``, у codex/opencode системного канала нет, поэтому
префиксом только на новой сессии) — один раз на сеанс, а не на каждый ход.
"""

from __future__ import annotations

import json
import logging

from engines import Engine, ensure_engine_tools

from bot.sessions import (
    get_mcp_playwright,
    reset_session,
    update_actual_model,
    update_session_id,
)
from bot.settings import CLAUDE_CWD
from bot.topics import active_procs, resolve_topic_role, spawn_procs

logger = logging.getLogger(__name__)

def build_system_prefix(
    effective_cwd: str,
    mcp_playwright: bool = False,
    key: tuple[int, int] | None = None,
) -> str:
    """Постоянный [SYSTEM:]-блок для движка.

    Раньше клеился в тело КАЖDОГО user-сообщения и копился в транскрипте.
    Теперь передаётся в системный канал движка (claude --append-system-prompt;
    codex/opencode — префиксом только на новой сессии) — один раз на сессию.
    Строка про браузер добавляется ТОЛЬКО когда Playwright реально подключён
    (mcp_playwright), иначе не зовём модель пользоваться недоступными тулами.

    `key` — (chat_id, thread_id) топика. Нужен движку, чтобы он мог сам поднять
    историю топика через manager_inbox: сессия живёт один сеанс, а переписка
    переживает его в messages_log.
    """
    lines = [
        "[SYSTEM: Сообщение пришло от пользователя через Telegram-бота Jarvis.",
        f"Ты работаешь в проекте {effective_cwd}.",
    ]
    if key is not None:
        lines.append(
            f"Твой топик: chat_id={key[0]}, thread_id={key[1]}. Сессия живёт один "
            "сеанс и не помнит прошлые — но переписка топика сохраняется. Если "
            "нужен контекст прошлых разговоров, подними его сам через MCP-инструмент "
            f"manager_inbox(chat_id={key[0]}, thread_id={key[1]})."
        )
    lines.append(
        "Ответ уходит в Telegram как Rich Message (Markdown): заголовки ##, таблицы, "
        "чек-листы - [x], цитаты >, <details><summary>…</summary>…</details>. Не "
        "начинай строку хэштегом — # в начале строки станет заголовком. Картинки, "
        "приложенные файл-маркерами, встанут в конец ответа коллажем."
    )
    if mcp_playwright:
        lines.append(
            "Если нужно работать с браузером, используй Playwright MCP browser_* tools, "
            "когда они доступны; если MCP недоступен, скажи об этом и выбери рабочий fallback."
        )
    if key is not None:
        lines.append(
            "Пользователь НЕ видит этот ход в реальном времени и не может тебя "
            "перебить — единственный способ что-то у него спросить и дождаться "
            f"ответа: MCP-инструмент ask_user(question, thread_id={key[1]}, "
            "options=[...]). Он блокирует тебя до ответа. Обязательно спрашивай "
            "ПЕРЕД опасными действиями (удаления, DELETE/DROP, sudo, push --force, "
            "что-либо на проде) и когда задача допускает разные толкования, а "
            "угадывание обесценит работу. Давай варианты в options — по ним "
            "отвечать быстрее. Не спрашивай о том, что можешь выяснить сам "
            "(прочитать код, запустить команду, посмотреть git)."
        )
    else:
        lines.append(
            "Опасные действия (удаления, DELETE/DROP, действия на проде, sudo, "
            "push --force) — переспрашивай."
        )
    lines[-1] += "]"
    return "\n".join(lines)


async def call_llm_stream(
    engine: Engine,
    session_id: str,
    prompt: str,
    key: tuple[int, int],
    cwd: str | None,
    on_intermediate,
    spawn_id: str | None = None,
) -> tuple[bool, str, str | None]:
    """Обёртка над engine.call_stream. Обновляет session_id в БД, если движок
    вернул изменённый id (актуально для codex/opencode — они сами назначают
    реальный id при первом запуске). Для spawn'а id в БД не сохраняется.

    Также сохраняет actual_model в sessions — реальное имя модели,
    которым CLI ответил (из stream-events). Это позволяет /session
    показать точную модель, не догадки.

    Возвращает (ok, final_text, session_id_after).
    """
    mcp_ok, mcp_status = ensure_engine_tools(engine)
    if not mcp_ok:
        logger.warning("engine=%s MCP setup issue: %s", engine.name, mcp_status)

    # Браузер — on-demand, флаг per-topic. Системный блок строим под флаг
    # (строка про browser_* только когда Playwright подключён) и передаём в
    # системный канал движка вместо вшивания в каждый prompt.
    mcp_playwright = get_mcp_playwright(*key)
    mcp_topic_role = resolve_topic_role(key)
    effective_cwd = cwd or CLAUDE_CWD
    system_prefix = build_system_prefix(effective_cwd, mcp_playwright, key=key)

    ok, final_text, sid_after, actual_model = await engine.call_stream(
        session_id=session_id,
        prompt=prompt,
        key=key,
        cwd=cwd,
        on_intermediate=on_intermediate,
        active_procs=active_procs,
        spawn_procs=spawn_procs,
        spawn_id=spawn_id,
        system_prefix=system_prefix,
        mcp_playwright=mcp_playwright,
        mcp_topic_role=mcp_topic_role,
    )
    # Recovery: иногда opencode/codex на resume могут вернуть rc=0, но пустой
    # текст. Для постоянной сессии делаем один автоповтор в новой сессии.
    if (
        spawn_id is None
        and (not ok)
        and engine.name in {"opencode", "codex"}
        and "вернул пустой ответ" in (final_text or "")
    ):
        try:
            new_sid, _, _ = reset_session(key[0], key[1])
            logger.warning(
                "engine=%s empty reply on session=%s key=%s; retrying with new session=%s",
                engine.name, session_id, key, new_sid,
            )
            ok2, final_text2, sid_after2, actual_model2 = await engine.call_stream(
                session_id=new_sid,
                prompt=prompt,
                key=key,
                cwd=cwd,
                on_intermediate=on_intermediate,
                active_procs=active_procs,
                spawn_procs=spawn_procs,
                spawn_id=spawn_id,
                system_prefix=system_prefix,
                mcp_playwright=mcp_playwright,
                mcp_topic_role=mcp_topic_role,
            )
            ok, final_text, sid_after, actual_model = ok2, final_text2, sid_after2, actual_model2
            session_id = new_sid
        except Exception:
            logger.exception(
                "recovery retry failed after empty reply: engine=%s key=%s",
                engine.name, key,
            )
    # Для постоянной сессии (не spawn) — если движок отдал новый id, сохраняем.
    if spawn_id is None and sid_after and sid_after != session_id:
        try:
            update_session_id(key[0], key[1], engine.name, sid_after)
            logger.info(
                "session_id updated by engine=%s for key=%s: %s -> %s",
                engine.name, key, session_id, sid_after,
            )
        except Exception:
            logger.exception("failed to persist new session_id from engine")
    # Реальная модель — сохраняем в sessions для /session и manager_topics.
    if spawn_id is None and actual_model:
        update_actual_model(key[0], key[1], engine.name, actual_model)
    return ok, final_text, sid_after


def _build_reply_context_prefix(ctx: dict) -> str:
    parts = []
    t = ctx.get("type")
    created = ctx.get("_created_at", "")
    if t == "claude_response":
        parts.append(f"пользователь отвечает на твой предыдущий ответ (время {created})")
    else:
        parts.append(f"пользователь отвечает на твоё сообщение типа {t!r} (время {created})")
        extras = {k: v for k, v in ctx.items() if k not in ("type", "_created_at")}
        if extras:
            parts.append(f"метаданные: {json.dumps(extras, ensure_ascii=False)}")
    return "[Пользователь отвечает на:] " + "; ".join(parts)
