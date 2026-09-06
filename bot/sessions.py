"""Сеансы топика: session_id, движок, модель, флаги, handoff между движками.

Топик — рабочее место и живёт долго; **сеанс** внутри него — как окно терминала:
открывается первым сообщением, закрывается ``/close`` или сам после простоя.
Топик (cwd, движок, модель) закрытие переживает, контекст LLM-сессии — нет.
Это главный ограничитель расхода: цена хода линейна по размеру контекста, а
короткие сеансы не дают ему расти.

Handoff здесь же: при смене движка контекст не переносится (у каждого CLI свой
формат транскрипта), поэтому в БД кладётся текстовое резюме, которое доедет до
первого промпта нового движка.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from engines import get_engine_by_name
from bot.db import _db
from bot.settings import DEFAULT_ENGINE, DEFAULT_ENGINE_NAME, SESSION_IDLE_MINUTES

logger = logging.getLogger(__name__)

# Файлы инструкций проекта: их правка должна переоткрывать сеанс.
INSTRUCTION_FILES = ("AGENTS.md", "CLAUDE.md")

def get_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Возвращает (session_id, cwd, engine_name) для топика.

    Если записи нет — создаёт новую под дефолтный движок (DEFAULT_ENGINE).
    Engine хранится per-topic; при смене JARVIS_ENGINE существующие топики
    продолжают работать со своим движком, переключение — через /engine.
    """
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id, cwd, engine FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            session_id, cwd, engine_in_db = row
            return session_id, cwd, engine_in_db
        new_id = DEFAULT_ENGINE.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, NULL, ?, ?)",
            (chat_id, thread_id, new_id, DEFAULT_ENGINE_NAME, now),
        )
        return new_id, None, DEFAULT_ENGINE_NAME


def update_session_id(
    chat_id: int, thread_id: int, expected_engine: str, new_session_id: str,
) -> None:
    """Обновляет session_id, только если в БД для топика всё ещё лежит
    `expected_engine`. Используется codex/opencode-движком: при первом запуске
    они отдают реальный id в stream'е, мы подменяем placeholder. Условие
    `engine = expected_engine` защищает от race с командой /engine: если
    пользователь успел переключиться на другой движок, старый стрим не должен
    перезаписывать новый id."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET session_id = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
            (new_session_id, datetime.utcnow().isoformat(),
             chat_id, thread_id, expected_engine),
        )


def reset_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Новый id для движка топика; cwd и engine сохраняются. Если записи
    нет — создаётся под DEFAULT_ENGINE."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            cwd, engine_name = row
        else:
            cwd, engine_name = None, DEFAULT_ENGINE_NAME
        engine = get_engine_by_name(engine_name)
        new_id = engine.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, engine_name, now),
        )
    return new_id, cwd, engine_name


def mark_session_start(chat_id: int, thread_id: int) -> None:
    """Запомнить, когда открыт сеанс. По этой метке видно, не изменились ли с тех
    пор инструкции проекта (движок читает их только при старте сессии)."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET session_started_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def touch_session(chat_id: int, thread_id: int) -> None:
    """Отметить активность в топике — продлевает текущий сеанс."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET last_activity_at = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (now, now, chat_id, thread_id),
        )


def close_session(chat_id: int, thread_id: int) -> bool:
    """Закрыть сеанс топика. Возвращает False, если он и так был закрыт.

    session_id не трогаем — он пересоздастся при следующем сообщении.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None or row[0] is None:
            return False
        conn.execute(
            "UPDATE sessions SET last_activity_at = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )
    return True


def clear_close_request(chat_id: int, thread_id: int) -> None:
    """Погасить флаг close_requested — закрытие, запрошенное Менеджером через
    MCP (manager_close_session), доведено ботом до конца."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET close_requested = NULL "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        )


def _session_is_stale(last_activity_at: str | None) -> bool:
    """Протух ли сеанс: закрыт (NULL) или простаивал дольше порога."""
    if not last_activity_at:
        return True
    if SESSION_IDLE_MINUTES <= 0:
        return False  # 0/отрицательное — авто-закрытие выключено
    try:
        last = datetime.fromisoformat(last_activity_at)
    except ValueError:
        logger.warning("bad last_activity_at=%r, treating session as stale",
                       last_activity_at)
        return True
    return datetime.utcnow() - last > timedelta(minutes=SESSION_IDLE_MINUTES)


def _session_state_line(key: tuple[int, int]) -> str:
    """Человекочитаемое состояние сеанса для /session и /tokens."""
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (key[0], key[1]),
        ).fetchone()
    if row is None or row[0] is None:
        return "закрыт (следующее сообщение откроет новый)"
    if SESSION_IDLE_MINUTES <= 0:
        return "открыт (авто-закрытие выключено)"
    try:
        last = datetime.fromisoformat(row[0])
    except ValueError:
        return "открыт (не удалось прочитать last_activity_at)"
    idle_min = int((datetime.utcnow() - last).total_seconds() // 60)
    left = SESSION_IDLE_MINUTES - idle_min
    if left <= 0:
        return "протух (следующее сообщение откроет новый)"
    return f"открыт, простой {idle_min} мин, закроется через {left} мин"


def _instructions_changed(cwd: str | None, started_at: str | None) -> bool:
    """Изменились ли инструкции проекта с момента открытия сеанса.

    Движки читают AGENTS.md / CLAUDE.md при СТАРТЕ сессии. Пока сеанс жив, правка
    инструкций молча не действует: агент до конца сеанса работает по версии,
    прочитанной когда-то давно. Это тихая ловушка — правишь файл, перезапускаешь
    задачу, а поведение прежнее, и непонятно почему (напоролись 2026-07-11 на
    новостном топике: правки формата дайджеста не доходили до агента).

    Поэтому: инструкции новее сеанса → сеанс переоткрываем.
    """
    if not cwd or not started_at:
        return False
    try:
        # session_started_at пишется в UTC, а getmtime отдаёт epoch. Без явного
        # tzinfo naive-строка трактуется как ЛОКАЛЬНОЕ время — и в UTC+3 сеанс
        # переоткрывался бы на каждом сообщении.
        started = datetime.fromisoformat(started_at).replace(
            tzinfo=timezone.utc).timestamp()
    except ValueError:
        return False
    for name in INSTRUCTION_FILES:
        path = os.path.join(cwd, name)
        try:
            if os.path.getmtime(path) > started:
                logger.info("instructions changed: %s newer than session start", path)
                return True
        except OSError:
            continue
    return False


def ensure_active_session(
    chat_id: int, thread_id: int,
) -> tuple[str, str | None, str, bool]:
    """Сеанс топика для текущего сообщения: (session_id, cwd, engine, opened_new).

    Новый сеанс открывается, если прошлый закрыт, протух ИЛИ если изменились
    инструкции проекта (их движок читает только при старте сессии).
    opened_new=True, чтобы вызывающий сообщил об этом пользователю.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at, cwd, session_started_at FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()

    if row is None:
        # Первое сообщение в топике: get_session заведёт запись.
        session_id, cwd, engine_name = get_session(chat_id, thread_id)
        touch_session(chat_id, thread_id)
        mark_session_start(chat_id, thread_id)
        logger.info("session opened (new topic): key=%s session=%s",
                    (chat_id, thread_id), session_id)
        return session_id, cwd, engine_name, True

    stale = _session_is_stale(row[0])
    fresh_instructions = not stale and _instructions_changed(row[1], row[2])

    if stale or fresh_instructions:
        # Делегированные задачи (jobs) переоткрытию сеанса не мешают: job несёт
        # полный текст задачи и не зависит от контекста прошлого сеанса, а
        # Менеджер поднимает своё состояние через manager_inbox.
        session_id, cwd, engine_name = reset_session(chat_id, thread_id)
        touch_session(chat_id, thread_id)
        mark_session_start(chat_id, thread_id)
        if fresh_instructions:
            reason = "instructions changed"
        else:
            reason = "closed" if row[0] is None else "stale"
        logger.info("session opened (%s): key=%s session=%s",
                    reason, (chat_id, thread_id), session_id)
        return session_id, cwd, engine_name, True

    session_id, cwd, engine_name = get_session(chat_id, thread_id)
    touch_session(chat_id, thread_id)
    return session_id, cwd, engine_name, False


def set_engine(
    chat_id: int, thread_id: int, new_engine_name: str, model: str | None = None,
) -> tuple[str, str | None]:
    """Меняет движок топика: создаёт новый session_id под новый движок, cwd
    сохраняется, model записывается явно (NULL допустим). Если записи не было —
    создаётся. Возвращает (session_id, cwd).

    Engine-проверка (поддерживается ли имя) — на стороне get_engine_by_name."""
    new_engine = get_engine_by_name(new_engine_name)
    new_id = new_engine.new_session_id()
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        cwd = row[0] if row else None
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, model, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, engine=excluded.engine, "
            "model=excluded.model, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, new_engine.name, model, now),
        )
    return new_id, cwd


def get_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает выбранную для топика модель (или None — дефолт движка)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def get_mcp_playwright(chat_id: int, thread_id: int) -> bool:
    """True, если для топика включён браузер (Playwright грузится per-call)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT mcp_playwright FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row[0]) if row and row[0] is not None else False


def set_mcp_playwright(chat_id: int, thread_id: int, enabled: bool) -> None:
    """Выставляет per-topic флаг браузера. Применяется со СЛЕДУЮЩЕГО сообщения:
    набор тулов движка меняется на лету, сессия и контекст сохраняются.
    Если записи топика ещё нет — создаёт под дефолтный движок."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None:
            engine_name = DEFAULT_ENGINE_NAME
            new_id = get_engine_by_name(engine_name).new_session_id()
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "mcp_playwright, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, new_id, None, engine_name,
                 1 if enabled else 0, now),
            )
        else:
            conn.execute(
                "UPDATE sessions SET mcp_playwright = ?, updated_at = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (1 if enabled else 0, now, chat_id, thread_id),
            )


def _persistent_column_for_engine(engine_name: str) -> str | None:
    engine_name = (engine_name or "").strip().lower()
    if engine_name == "claude":
        return "persistent_claude"
    if engine_name == "codex":
        return "persistent_codex"
    return None


def get_persistent_for_engine(chat_id: int, thread_id: int, engine_name: str) -> bool:
    """Persistent включён по умолчанию для движков, которые его поддерживают
    (claude, codex): NULL в колонке и отсутствие строки топика трактуются как
    ВКЛЮЧЕНО. Выключается только явным /persistent off (значение 0). Движки
    без поддержки persistent всегда возвращают False."""
    column = _persistent_column_for_engine(engine_name)
    if column is None:
        return False
    with _db() as conn:
        row = conn.execute(
            f"SELECT {column} FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row[0]) if row and row[0] is not None else True


def set_persistent_for_engine(
    chat_id: int, thread_id: int, engine_name: str, enabled: bool,
) -> None:
    """Set /persistent flag for the selected engine.

    Unsupported engines are ignored on disable and rejected by callers on
    enable. New topic rows are still created under DEFAULT_ENGINE, matching the
    existing flag helpers.
    """
    column = _persistent_column_for_engine(engine_name)
    if column is None:
        if enabled:
            raise RuntimeError(f"persistent is not supported for {engine_name}")
        return
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None:
            engine_name = DEFAULT_ENGINE_NAME
            new_id = get_engine_by_name(engine_name).new_session_id()
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                f"{column}, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, new_id, None, engine_name,
                 1 if enabled else 0, now),
            )
        else:
            conn.execute(
                f"UPDATE sessions SET {column} = ?, updated_at = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (1 if enabled else 0, now, chat_id, thread_id),
            )


def get_persistent_claude(chat_id: int, thread_id: int) -> bool:
    """Backward-compatible helper for existing Claude persistent code."""
    return get_persistent_for_engine(chat_id, thread_id, "claude")


def set_persistent_claude(chat_id: int, thread_id: int, enabled: bool) -> None:
    """Backward-compatible helper for existing Claude persistent code."""
    set_persistent_for_engine(chat_id, thread_id, "claude", enabled)


def update_actual_model(
    chat_id: int, thread_id: int, engine_name: str, model: str | None,
) -> None:
    """Сохранить реальную модель, которой ответил CLI в последнем запуске.

    Engine-проверка как у update_session_id — защита от race с /engine: если
    оператор успел переключиться, прошлый стрим не должен перезаписать
    actual_model нового движка.
    """
    if not model:
        return
    try:
        with _db() as conn:
            conn.execute(
                "UPDATE sessions SET actual_model = ? "
                "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
                (model, chat_id, thread_id, engine_name),
            )
    except Exception:
        logger.exception(
            "update_actual_model failed chat=%s thread=%s engine=%s",
            chat_id, thread_id, engine_name,
        )


def get_actual_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает последнюю реально использованную моделью или None."""
    with _db() as conn:
        row = conn.execute(
            "SELECT actual_model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def update_model_only(chat_id: int, thread_id: int, model: str | None) -> bool:
    """Меняет только модель текущего движка топика, не трогая session_id.

    Используется, когда оператор тыкает в свой же активный движок и
    выбирает другую модель. Контекст сессии сохраняется (тот же jsonl).
    Возвращает True если запись существовала.
    """
    with _db() as conn:
        cur = conn.execute(
            "UPDATE sessions SET model = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (model, datetime.utcnow().isoformat(), chat_id, thread_id),
        )
    return cur.rowcount == 1


def set_pending_summary(chat_id: int, thread_id: int, summary: str) -> None:
    """Сохраняет резюме предыдущей сессии — будет доставлено в первый prompt
    после переключения движка."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET pending_summary = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (summary, datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def get_pending_summary(chat_id: int, thread_id: int) -> str | None:
    """Вернуть pending_summary без очистки.

    Резюме удаляем только после успешного первого хода новой сессии, иначе при
    падении этого хода контекст потеряется без возможности ретрая.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT pending_summary FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row or not row[0]:
        return None
    return row[0]


def clear_pending_summary(chat_id: int, thread_id: int) -> None:
    """Очистить pending_summary после того, как summary уже успешно попало
    в новый ход LLM-сессии."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET pending_summary = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def _transfer_marker(old_engine_name: str) -> str:
    """JSON-маркер «контекст просили перенести». Кладётся в pending_summary при
    смене движка и разворачивается в указание прочитать историю (см.
    _resolve_pending_summary) при первом сообщении новому движку."""
    return json.dumps(
        {"transfer_requested": True, "old_engine": old_engine_name},
        ensure_ascii=False,
    )


def _parse_transfer_marker(pending: str) -> dict | None:
    """Распарсить JSON-маркер {transfer_requested, old_engine, old_session_id}
    или вернуть None, если не маркер / невалидный JSON."""
    try:
        data = json.loads(pending)
    except (json.JSONDecodeError, TypeError):
        return None
    if data.get("transfer_requested") and data.get("old_engine"):
        return data
    return None


def build_context_handoff(key: tuple[int, int], old_engine_name: str) -> str:
    """Указание новому движку поднять контекст самому.

    Раньше здесь старый движок гонялся за резюме — полный проход по всей
    истории, самый дорогой вызов из возможных. Теперь платит только новый
    движок и только за то, что реально прочитал: историю топика он берёт через
    manager_inbox, рабочее состояние — из кода и git.
    """
    chat_id, thread_id = key
    return (
        f"Ты подхватываешь диалог, который до тебя вёл другой агент "
        f"({old_engine_name}). Его контекст тебе НЕ передан.\n"
        f"Прежде чем отвечать, подними историю сам: вызови MCP-инструмент "
        f"manager_inbox(chat_id={chat_id}, thread_id={thread_id}) и прочитай "
        f"последние сообщения — столько, сколько нужно, чтобы понять задачу и "
        f"на чём остановились. Рабочее состояние сверяй с кодом и git, а не с "
        f"пересказом."
    )


def set_cwd(chat_id: int, thread_id: int, cwd: str) -> None:
    """Создаёт запись, если её нет (session_id — новый id для дефолтного движка),
    либо обновляет cwd."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE sessions SET cwd = ?, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
                (cwd, now, chat_id, thread_id),
            )
        else:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, DEFAULT_ENGINE.new_session_id(), cwd,
                 DEFAULT_ENGINE_NAME, now),
            )


def clear_cwd(chat_id: int, thread_id: int) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET cwd = NULL, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )
