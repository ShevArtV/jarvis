"""OpenAI Codex CLI adapter (@openai/codex).

Ключевые отличия от claude:
- session_id (в терминах codex — `thread_id`) генерирует сам codex при первом запуске,
  задать его заранее нельзя. Поэтому мы при первом запуске передаём `None` и
  ловим реальный id из события `{"type":"thread.started","thread_id":"..."}`.
  Адаптер возвращает его из call_stream; вызывающий код обязан обновить БД.
- Resume: `codex exec resume --json <thread_id> [PROMPT]`.
- Stream: `--json` даёт JSONL с типами thread.started / turn.started / item.started /
  item.completed / turn.completed. У item.type бывают: agent_message (с .text),
  command_execution (с .command), reasoning (с .text).
- --append-system-prompt у codex нет; инструкции уходят либо через AGENTS.md,
  либо префиксом в prompt. Мы префиксуем: это работает и для новой сессии, и для resume.
- Нет pidfile-локов, которые нужно чистить.
- Сессии хранятся в ~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ts>-<thread_id>.jsonl.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Awaitable, Callable

from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

CODEX_BIN = os.environ.get("CODEX_BIN", "codex")
CODEX_TIMEOUT = int(os.environ.get("CODEX_TIMEOUT", "3600"))
# Тот же минимальный интервал, что и у claude-адаптера.
INTERMEDIATE_MIN_INTERVAL = 2.0
DEFAULT_CODEX_MODELS = [
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex",
    "gpt-5.2",
]

# Per-call модель. Выставляется через engines.engine_model_scope() из
# telegram_bot.py перед call_stream. Если None — фолбэк на CODEX_MODEL env,
# иначе CLI берёт свою дефолтную модель.
CURRENT_MODEL: ContextVar[str | None] = ContextVar("codex_model", default=None)

# Инструкция про маркер [[FILE: ...]]. Клеится префиксом к пользовательскому
# prompt'у на каждом вызове (чтобы не зависеть от AGENTS.md — тот может быть
# переопределён под проект).
FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR CODEX] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)

# Обёртка вокруг thread_id, сгенерированного нами до первого запуска codex.
# В БД у нас уже лежит UUID в колонке session_id; но при первом вызове мы не
# передаём его codex'у (он всё равно сгенерирует свой), а после получения
# реального thread_id из stream заменяем наш placeholder на него.
_PLACEHOLDER_PREFIX = "placeholder-"


def _is_placeholder(session_id: str) -> bool:
    return session_id.startswith(_PLACEHOLDER_PREFIX)


def _codex_sessions_root() -> Path:
    return Path.home() / ".codex" / "sessions"


def _split_models(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _models_from_codex_cache() -> list[str]:
    cache_path = Path(
        os.environ.get("CODEX_MODELS_CACHE", Path.home() / ".codex" / "models_cache.json")
    )
    if not cache_path.is_file():
        return []
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("cannot read codex models cache: %s", cache_path, exc_info=True)
        return []
    if not isinstance(data, dict):
        return []

    models: list[str] = []
    for item in data.get("models", []):
        if not isinstance(item, dict) or item.get("visibility") != "list":
            continue
        slug = item.get("slug")
        if isinstance(slug, str) and slug and slug not in models:
            models.append(slug)
    return models


CODEX_MODELS = (
    _split_models(os.environ.get("CODEX_MODELS"))
    or _models_from_codex_cache()
    or DEFAULT_CODEX_MODELS
)


class CodexEngine:
    name = "codex"
    bin_path = CODEX_BIN
    models: list[str] = CODEX_MODELS

    # --- Session helpers ---

    def new_session_id(self) -> str:
        """Генерируем placeholder — пометку, что настоящий id нам отдаст codex
        в первом же stream-событии. До тех пор в БД лежит именно placeholder."""
        return f"{_PLACEHOLDER_PREFIX}{uuid.uuid4()}"

    def session_exists(self, session_id: str, cwd: str) -> bool:
        """Ищем файл сессии в ~/.codex/sessions/**/rollout-*-<session_id>.jsonl.

        Для placeholder'ов возвращаем False (реальной сессии ещё нет).
        """
        if _is_placeholder(session_id):
            return False
        root = _codex_sessions_root()
        if not root.is_dir():
            return False
        try:
            # Быстрее, чем rglob, так как структура строго YYYY/MM/DD/.
            for match in root.rglob(f"rollout-*-{session_id}.jsonl"):
                return match.is_file()
        except OSError:
            return False
        return False

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        """Codex не ведёт pidfile-локов (или мы их не знаем). No-op.
        При next call не будет 'already in use'."""
        return None

    # --- Stream ---

    async def call_stream(
        self,
        session_id: str,
        prompt: str,
        key: tuple[int, int],
        cwd: str | None,
        on_intermediate: Callable[[str], Awaitable[None]],
        active_procs: dict,
        spawn_procs: dict,
        spawn_id: str | None = None,
    ) -> tuple[bool, str, str | None]:
        effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))

        is_spawn = spawn_id is not None

        # Собираем итоговый prompt: префикс с инструкцией про [[FILE:]] + сам prompt.
        full_prompt = FILE_MARKER_SYSTEM + "\n\n" + prompt

        # Решаем: новая сессия или resume.
        resume_mode = (
            not is_spawn
            and not _is_placeholder(session_id)
            and self.session_exists(session_id, effective_cwd)
        )

        # ВАЖНО: у `codex exec` и `codex exec resume` разный набор флагов.
        #   exec принимает: --sandbox, --dangerously-bypass-approvals-and-sandbox,
        #                   --skip-git-repo-check, -C/--cd, --ephemeral, --json
        #   resume принимает: --dangerously-bypass-approvals-and-sandbox,
        #                   --skip-git-repo-check, --ephemeral, --json
        #                   (НЕТ --sandbox, НЕТ -C/--cd)
        # Для resume --dangerously-bypass-approvals-and-sandbox уже включает
        # "без сэндбокса" + approval=never, так что --sandbox не нужен.
        # cwd передаём через `cwd=` в subprocess для обоих путей.
        shared_flags = [
            "--json",
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
        ]
        if is_spawn:
            # Одноразовая параллельная сессия: не хотим, чтобы codex её сохранял
            # и потом мешался в списке recent-sessions.
            shared_flags.append("--ephemeral")

        model = CURRENT_MODEL.get() or os.environ.get("CODEX_MODEL")
        model_flags = ["--model", model] if model else []

        if resume_mode:
            # У resume нет --sandbox и -C/--cd. Sandbox-режим уже покрыт
            # --dangerously-bypass-approvals-and-sandbox, а cwd — через subprocess cwd=.
            cmd = [
                CODEX_BIN, "exec", "resume",
                *shared_flags,
                *model_flags,
                session_id, full_prompt,
            ]
        else:
            # Новая сессия: можем (и хотим) явно задать sandbox и cwd.
            cmd = [
                CODEX_BIN, "exec",
                *shared_flags,
                *model_flags,
                "--sandbox", "danger-full-access",
                "-C", effective_cwd,
                full_prompt,
            ]

        logger.info(
            "codex start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=effective_cwd,
                start_new_session=True,
                limit=10 * 1024 * 1024,
            )
        except FileNotFoundError:
            return False, f"`{CODEX_BIN}` не найден в PATH.", session_id, None

        if is_spawn:
            spawn_procs[(key[0], key[1], spawn_id)] = proc
        else:
            active_procs[key] = proc

        final_text = ""
        real_thread_id: str | None = None
        # actual_model: то, что мы сами попросили (через --model) — пока
        # CLI не сообщит точное. Stream-парсер ниже может перезаписать.
        actual_model: str | None = model
        buffer_intermediate: list[str] = []
        last_push = 0.0
        # Ошибки из stream (type=error и type=turn.failed). Codex пишет их
        # в stdout-JSON, НЕ в stderr, и затем exit=1. Если их не ловить —
        # бот отдаёт пользователю «Ошибка codex (rc=1): (пусто)».
        stream_errors: list[str] = []

        async def flush_intermediate(force: bool = False) -> None:
            nonlocal last_push
            if not buffer_intermediate:
                return
            now = time.monotonic()
            if not force and (now - last_push) < INTERMEDIATE_MIN_INTERVAL:
                return
            text = "\n".join(buffer_intermediate)
            buffer_intermediate.clear()
            last_push = now
            try:
                await on_intermediate(text)
            except Exception:
                logger.exception("on_intermediate failed")

        async def read_stream():
            nonlocal final_text, real_thread_id, actual_model
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                raw = line.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Best-effort парсинг модели из любого top-level event'а
                # с полем `model` или nested в `item`/`turn`/`thread`.
                for obj in (ev, ev.get("item"), ev.get("turn"), ev.get("thread")):
                    if isinstance(obj, dict):
                        m = obj.get("model")
                        if isinstance(m, str) and m:
                            actual_model = m
                            break
                etype = ev.get("type")
                if etype == "thread.started":
                    tid = ev.get("thread_id")
                    if isinstance(tid, str) and tid:
                        real_thread_id = tid
                    continue
                if etype == "error":
                    # Верхнеуровневая ошибка codex — например, usage limit,
                    # сетевой сбой до старта turn. Сохраняем, чтобы отдать
                    # пользователю после rc != 0.
                    msg = (ev.get("message") or "").strip()
                    if msg:
                        stream_errors.append(msg)
                    continue
                if etype == "turn.failed":
                    err = ev.get("error") or {}
                    msg = (err.get("message") or "").strip()
                    if msg and msg not in stream_errors:
                        stream_errors.append(msg)
                    continue
                if etype in ("item.started", "item.completed"):
                    item = ev.get("item") or {}
                    itype = item.get("type")
                    if itype == "agent_message":
                        # Финальный текст ответа — обновляем final_text (перезапись:
                        # последнее agent_message — финальный) и показываем как
                        # промежуточный для пользователя.
                        txt = (item.get("text") or "").strip()
                        if not txt:
                            continue
                        if etype == "item.completed":
                            final_text = txt
                            # Промежуточно тоже показываем — но коротко.
                            buffer_intermediate.append(txt[:800])
                            await flush_intermediate()
                    elif itype == "command_execution":
                        # Аналог tool_use в claude.
                        if etype != "item.started":
                            continue
                        cmd_str = item.get("command") or ""
                        if cmd_str:
                            s = cmd_str[:150]
                            buffer_intermediate.append(f"🔧 exec {s}")
                            await flush_intermediate()
                    elif itype == "reasoning":
                        if etype != "item.completed":
                            continue
                        txt = (item.get("text") or "").strip()
                        if txt:
                            # Показываем первую строку рассуждений.
                            first = txt.splitlines()[0][:300]
                            buffer_intermediate.append(f"💭 {first}")
                            await flush_intermediate()
                # turn.started / turn.completed — игнорируем, нам достаточно item.*.

        try:
            try:
                await asyncio.wait_for(read_stream(), timeout=CODEX_TIMEOUT)
            except asyncio.TimeoutError:
                await terminate_process_tree(proc)
                return False, f"Timeout: codex не ответил за {CODEX_TIMEOUT}с.", session_id, actual_model
            await proc.wait()
        except asyncio.CancelledError:
            await terminate_process_tree(proc)
            raise
        finally:
            await flush_intermediate(force=True)
            if is_spawn:
                skey = (key[0], key[1], spawn_id)
                if spawn_procs.get(skey) is proc:
                    spawn_procs.pop(skey, None)
            else:
                if active_procs.get(key) is proc:
                    active_procs.pop(key, None)

        stderr_text = ""
        if proc.stderr is not None:
            try:
                stderr_b = await proc.stderr.read()
                stderr_text = stderr_b.decode("utf-8", errors="replace")
            except Exception:
                pass

        if proc.returncode != 0:
            # Подробный лог, чтобы в следующий раз не гадать, что именно
            # запускали: сама команда, cwd и собранные stream-ошибки.
            # Секреты в cmd попасть не должны — там нет токенов.
            logger.warning(
                "codex rc=%s cmd=%s cwd=%s stderr=%s stream_errors=%s",
                proc.returncode, cmd, effective_cwd,
                stderr_text[:500], stream_errors[:3],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", (real_thread_id or session_id), actual_model
            if not final_text:
                # Приоритет: сообщения из stdout-stream (codex пишет сюда
                # usage-limit и пр.), затем stderr, затем «(пусто)».
                err_body = ""
                if stream_errors:
                    err_body = "\n".join(stream_errors)[:1500]
                elif stderr_text:
                    err_body = stderr_text[:1500]
                else:
                    err_body = "(пусто)"
                return False, (
                    f"Ошибка codex (rc={proc.returncode}): {err_body}"
                ), session_id, actual_model

        logger.info(
            "codex done: key=%s rc=%s final_len=%d real_thread_id=%s model=%s",
            key, proc.returncode, len(final_text), real_thread_id, actual_model,
        )
        if not final_text.strip():
            return False, (
                "codex вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), (real_thread_id or session_id), actual_model

        # Если это была новая постоянная сессия — отдаём новый id наверх.
        # Для spawn'а (ephemeral) — не возвращаем; даже если codex его выдал,
        # мы не хотим его резюмировать (он --ephemeral всё равно не сохранён).
        effective_out_id = session_id
        if not is_spawn and real_thread_id and real_thread_id != session_id:
            effective_out_id = real_thread_id

        return True, final_text, effective_out_id, actual_model
