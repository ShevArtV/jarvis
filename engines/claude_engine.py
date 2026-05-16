"""Claude CLI adapter.

Инкапсулирует текущее поведение (из старого telegram_bot.py):
- stream-json через `claude --print --output-format stream-json --verbose`;
- `--resume` если jsonl сессии уже существует, иначе `--session-id`;
- чистка stale pidfile в `~/.claude/sessions/`;
- `--append-system-prompt` для инструкции про маркер [[FILE:]];
- bypassPermissions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Awaitable, Callable

from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
CLAUDE_TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", "3600"))
INTERMEDIATE_MIN_INTERVAL = 2.0

APPEND_SYSTEM_PROMPT = (
    "Если нужно отправить пользователю файл (скриншот, собранный пакет, "
    "сгенерированный документ и т.п.) — выведи отдельной строкой маркер "
    "[[FILE: /абсолютный/путь]] (опционально с подписью через '|': "
    "[[FILE: /путь | подпись]]). Бот автоматически отправит файл в Telegram. "
    "Используй только для файлов в пределах cwd сессии или явно указанных пользователем."
)

# Per-call модель. Выставляется через engines.engine_model_scope() из
# telegram_bot.py перед call_stream. Если None — фолбэк на CLAUDE_MODEL env,
# иначе --model не передаётся (CLI берёт свою дефолтную).
CURRENT_MODEL: ContextVar[str | None] = ContextVar("claude_model", default=None)


def _sessions_dir_for(cwd: str) -> Path:
    """~/.claude/projects/<encoded-cwd>/  (Claude CLI заменяет '/' и '.' на '-':
    /home/shevartv/projects/visa-center.ru → -home-shevartv-projects-visa-center-ru)."""
    encoded = re.sub(r"[/.]+", "-", cwd)
    return Path.home() / ".claude" / "projects" / encoded


class ClaudeEngine:
    name = "claude"
    bin_path = CLAUDE_BIN
    # CLI принимает алиасы ('opus', 'sonnet', 'haiku') и полные имена
    # ('claude-opus-4-7', 'claude-opus-4-7[1m]', ...). Алиасы держим как
    # дефолт, а вариант 1M-окна добавляем полным именем — короткого алиаса для
    # него нет.
    models: list[str] = [
        "opus",
        "claude-opus-4-7[1m]",
        "sonnet",
        "haiku",
    ]

    # --- Session filesystem helpers ---

    def new_session_id(self) -> str:
        return str(uuid.uuid4())

    def session_exists(self, session_id: str, cwd: str) -> bool:
        return (_sessions_dir_for(cwd) / f"{session_id}.jsonl").exists()

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        """Claude CLI хранит лок-файлы в ~/.claude/sessions/<pid>.json с полем sessionId.
        Если процесс мёртв — удаляем файл, чтобы --resume не упёрся в 'session in use'."""
        sessions_dir = Path.home() / ".claude" / "sessions"
        if not sessions_dir.is_dir():
            return
        for p in sessions_dir.glob("*.json"):
            try:
                data = json.loads(p.read_text())
            except Exception:
                continue
            if data.get("sessionId") != session_id:
                continue
            pid = data.get("pid")
            alive = False
            if isinstance(pid, int):
                try:
                    os.kill(pid, 0)
                    alive = True
                except ProcessLookupError:
                    alive = False
                except PermissionError:
                    alive = True  # чужой процесс, не трогаем
            if not alive:
                try:
                    p.unlink()
                    logger.info(
                        "removed stale session lock %s (pid=%s sid=%s)",
                        p, pid, session_id,
                    )
                except OSError:
                    pass

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
        # cwd берётся из аргумента (если None — caller должен подставить default).
        effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))

        is_spawn = spawn_id is not None
        if is_spawn:
            session_flags = ["--session-id", session_id]
            resume_mode = False
        else:
            resume_mode = self.session_exists(session_id, effective_cwd)
            if resume_mode:
                self.clear_stale_session_pidfile(session_id)
                session_flags = ["--resume", session_id]
            else:
                session_flags = ["--session-id", session_id]

        model = CURRENT_MODEL.get() or os.environ.get("CLAUDE_MODEL")
        model_flags = ["--model", model] if model else []

        cmd = [
            CLAUDE_BIN, "--print",
            "--permission-mode", "bypassPermissions",
            "--input-format", "text",
            "--output-format", "stream-json",
            "--verbose",
            "--append-system-prompt", APPEND_SYSTEM_PROMPT,
            *model_flags,
            *session_flags,
            prompt,
        ]

        logger.info(
            "claude start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
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
            return False, f"`{CLAUDE_BIN}` не найден в PATH.", session_id, None

        if is_spawn:
            spawn_procs[(key[0], key[1], spawn_id)] = proc
        else:
            active_procs[key] = proc

        final_text = ""
        actual_model: str | None = None
        buffer_intermediate: list[str] = []
        last_push = 0.0

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
            nonlocal final_text, actual_model
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
                etype = ev.get("type")
                # 'system' с subtype='init' — первый event, содержит model.
                # Также fallback: message.model в каждом assistant event.
                if etype == "system" and ev.get("subtype") == "init":
                    m = ev.get("model")
                    if isinstance(m, str) and m:
                        actual_model = m
                if etype == "assistant":
                    msg = ev.get("message", {}) or {}
                    if not actual_model:
                        m = msg.get("model")
                        if isinstance(m, str) and m:
                            actual_model = m
                    for block in msg.get("content", []) or []:
                        btype = block.get("type")
                        if btype == "text":
                            txt = (block.get("text") or "").strip()
                            if txt:
                                buffer_intermediate.append(txt[:800])
                        elif btype == "tool_use":
                            name = block.get("name", "?")
                            inp = block.get("input") or {}
                            summary = ""
                            if isinstance(inp, dict):
                                for k in ("command", "file_path", "path",
                                          "pattern", "url", "description"):
                                    if k in inp and inp[k]:
                                        s = str(inp[k])
                                        summary = f" {k}={s[:120]}"
                                        break
                            buffer_intermediate.append(f"🔧 {name}{summary}")
                    await flush_intermediate()
                elif etype == "result":
                    r = ev.get("result")
                    if isinstance(r, str):
                        final_text = r

        try:
            try:
                await asyncio.wait_for(read_stream(), timeout=CLAUDE_TIMEOUT)
            except asyncio.TimeoutError:
                await terminate_process_tree(proc)
                return False, f"Timeout: claude не ответил за {CLAUDE_TIMEOUT}с.", session_id, actual_model
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
            logger.warning("claude rc=%s stderr=%s", proc.returncode, stderr_text[:500])
            if proc.returncode and proc.returncode < 0:
                return False, "", session_id, actual_model
            if not final_text:
                return False, (
                    f"Ошибка claude (rc={proc.returncode}): "
                    f"{stderr_text[:1500] or '(пусто)'}"
                ), session_id, actual_model

        logger.info(
            "claude done: key=%s rc=%s final_len=%d model=%s",
            key, proc.returncode, len(final_text), actual_model,
        )
        if not final_text.strip():
            return False, (
                "claude вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), session_id, actual_model
        return True, final_text, session_id, actual_model
