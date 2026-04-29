"""OpenCode CLI adapter.

OpenCode generates its own session id on first run. We store a placeholder in
Jarvis DB, catch the real ``ses_...`` id from JSONL output, then persist it.
Resume goes through ``opencode run --session <id>``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
OPENCODE_TIMEOUT = int(os.environ.get("OPENCODE_TIMEOUT", "3600"))
INTERMEDIATE_MIN_INTERVAL = 2.0

FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR OPENCODE] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)

_PLACEHOLDER_PREFIX = "placeholder-"


def _is_placeholder(session_id: str) -> bool:
    return session_id.startswith(_PLACEHOLDER_PREFIX)


def _opencode_env() -> dict[str, str]:
    env = os.environ.copy()
    # The bot is a long-running service; autoupdate checks add noise and can
    # unexpectedly change CLI behavior between restarts.
    env.setdefault("OPENCODE_DISABLE_AUTOUPDATE", "true")
    env.setdefault("OPENCODE_CLIENT", "jarvis")
    return env


def _extract_session_id(ev: dict[str, Any]) -> str | None:
    for key in ("sessionID", "sessionId", "session_id"):
        val = ev.get(key)
        if isinstance(val, str) and val:
            return val
    for key in ("session", "part", "message", "info"):
        obj = ev.get(key)
        if not isinstance(obj, dict):
            continue
        for nested_key in ("id", "sessionID", "sessionId", "session_id"):
            val = obj.get(nested_key)
            if isinstance(val, str) and val.startswith("ses_"):
                return val
    props = ev.get("properties")
    if isinstance(props, dict):
        return _extract_session_id(props)
    return None


def _string_from_any(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        chunks: list[str] = []
        for item in value:
            if isinstance(item, str):
                chunks.append(item)
            elif isinstance(item, dict):
                txt = item.get("text") or item.get("content")
                if isinstance(txt, str):
                    chunks.append(txt)
        return "".join(chunks)
    return ""


def _text_from_part(part: Any) -> str:
    if not isinstance(part, dict):
        return ""
    for key in ("text", "delta", "content", "value", "message", "output"):
        txt = _string_from_any(part.get(key))
        if txt:
            return txt
    state = part.get("state")
    if isinstance(state, dict):
        for key in ("text", "content", "output"):
            txt = _string_from_any(state.get(key))
            if txt:
                return txt
    return ""


def _error_message(ev: dict[str, Any]) -> str:
    err = ev.get("error")
    if isinstance(err, str):
        return err.strip()
    if isinstance(err, dict):
        data = err.get("data")
        for obj in (err, data):
            if not isinstance(obj, dict):
                continue
            msg = obj.get("message") or obj.get("error")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
        return json.dumps(err, ensure_ascii=False)[:1500]
    msg = ev.get("message")
    if isinstance(msg, str):
        return msg.strip()
    return ""


def _tool_summary(part: dict[str, Any]) -> str:
    name = part.get("tool") or part.get("name") or "tool"
    state = part.get("state") if isinstance(part.get("state"), dict) else {}
    title = state.get("title") if isinstance(state, dict) else None
    if isinstance(title, str) and title.strip():
        return f"{name} {title.strip()[:140]}"
    inp = state.get("input") if isinstance(state, dict) else part.get("input")
    if isinstance(inp, dict):
        for key in ("command", "file_path", "path", "pattern", "query", "description"):
            val = inp.get(key)
            if val:
                return f"{name} {key}={str(val)[:140]}"
    return str(name)


class OpenCodeEngine:
    name = "opencode"

    def new_session_id(self) -> str:
        return f"{_PLACEHOLDER_PREFIX}{uuid.uuid4()}"

    def session_exists(self, session_id: str, cwd: str) -> bool:
        """OpenCode stores sessions in SQLite and validates ids itself.

        Avoid probing the DB from the bot process: it can be locked by another
        opencode instance. For real ``ses_...`` ids we optimistically resume and
        let the CLI return a clear error if the session was removed.
        """
        return (not _is_placeholder(session_id)) and session_id.startswith("ses_")

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        return None

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
        full_prompt = FILE_MARKER_SYSTEM + "\n\n" + prompt

        resume_mode = (not is_spawn) and self.session_exists(session_id, effective_cwd)

        cmd = [
            OPENCODE_BIN, "run",
            "--format", "json",
            "--dangerously-skip-permissions",
            "--dir", effective_cwd,
        ]
        model = os.environ.get("OPENCODE_MODEL")
        if model:
            cmd.extend(["--model", model])
        agent = os.environ.get("OPENCODE_AGENT")
        if agent:
            cmd.extend(["--agent", agent])
        variant = os.environ.get("OPENCODE_VARIANT")
        if variant:
            cmd.extend(["--variant", variant])
        if resume_mode:
            cmd.extend(["--session", session_id])
        cmd.append(full_prompt)

        logger.info(
            "opencode start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=effective_cwd,
                env=_opencode_env(),
                start_new_session=True,
                limit=10 * 1024 * 1024,
            )
        except FileNotFoundError:
            return False, f"`{OPENCODE_BIN}` не найден в PATH.", session_id

        if is_spawn:
            spawn_procs[(key[0], key[1], spawn_id)] = proc
        else:
            active_procs[key] = proc

        real_session_id: str | None = None
        text_chunks: list[str] = []
        part_texts: dict[str, str] = {}
        final_text = ""
        buffer_intermediate: list[str] = []
        last_push = 0.0
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

        def remember_text(part: Any, fallback: str = "") -> str:
            txt = _text_from_part(part) or fallback
            if not txt:
                return ""
            if isinstance(part, dict):
                pid = part.get("id")
                if isinstance(pid, str) and pid:
                    part_texts[pid] = txt
            return txt

        async def read_stream() -> None:
            nonlocal final_text, real_session_id
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
                    logger.debug("opencode non-json stdout: %s", raw[:300])
                    continue

                sid = _extract_session_id(ev)
                if sid:
                    real_session_id = sid

                etype = ev.get("type")
                props = ev.get("properties") if isinstance(ev.get("properties"), dict) else {}
                part = ev.get("part") if isinstance(ev.get("part"), dict) else props.get("part")

                if etype in ("error", "session.error", "turn.failed"):
                    msg = _error_message(ev) or _error_message(props)
                    if msg and msg not in stream_errors:
                        stream_errors.append(msg)
                    continue

                if etype == "tool_use":
                    if isinstance(part, dict):
                        buffer_intermediate.append(f"🔧 {_tool_summary(part)}")
                        await flush_intermediate()
                    continue

                if etype == "message.part.delta":
                    delta = _string_from_any(ev.get("delta") or props.get("delta")) or _text_from_part(part)
                    if delta:
                        if isinstance(part, dict) and isinstance(part.get("id"), str):
                            pid = part["id"]
                            part_texts[pid] = part_texts.get(pid, "") + delta
                        else:
                            text_chunks.append(delta)
                        final_text = delta
                        buffer_intermediate.append(delta[-800:])
                        await flush_intermediate()
                    continue

                if etype == "message.part.updated":
                    txt = remember_text(part)
                    if txt:
                        final_text = txt
                        buffer_intermediate.append(txt[-800:])
                        await flush_intermediate()
                    continue

                if etype == "text":
                    txt = remember_text(part, _string_from_any(ev.get("text")))
                    if txt:
                        if not isinstance(part, dict) or not part.get("id"):
                            text_chunks.append(txt)
                        final_text = txt
                        buffer_intermediate.append(txt[-800:])
                        await flush_intermediate()
                    continue

                if etype == "step_finish":
                    if isinstance(part, dict) and part.get("reason") == "stop":
                        await flush_intermediate(force=True)
                    continue

                if etype == "message.updated":
                    msg = ev.get("message") if isinstance(ev.get("message"), dict) else props.get("message")
                    txt = _text_from_part(msg)
                    if txt:
                        final_text = txt

        try:
            try:
                await asyncio.wait_for(read_stream(), timeout=OPENCODE_TIMEOUT)
            except asyncio.TimeoutError:
                await terminate_process_tree(proc)
                return False, f"Timeout: opencode не ответил за {OPENCODE_TIMEOUT}с.", session_id
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

        if part_texts:
            final_text = "\n".join(part_texts.values())
        elif text_chunks:
            final_text = "".join(text_chunks)

        if proc.returncode != 0:
            logger.warning(
                "opencode rc=%s cmd=%s cwd=%s stderr=%s stream_errors=%s",
                proc.returncode, cmd, effective_cwd,
                stderr_text[:500], stream_errors[:3],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", (real_session_id or session_id)
            if not final_text:
                err_body = "\n".join(stream_errors)[:1500] if stream_errors else stderr_text[:1500]
                return False, (
                    f"Ошибка opencode (rc={proc.returncode}): {err_body or '(пусто)'}"
                ), session_id

        logger.info(
            "opencode done: key=%s rc=%s final_len=%d real_session_id=%s",
            key, proc.returncode, len(final_text), real_session_id,
        )
        if not final_text.strip():
            return False, (
                "opencode вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), (real_session_id or session_id)

        effective_out_id = session_id
        if not is_spawn and real_session_id and real_session_id != session_id:
            effective_out_id = real_session_id

        return True, final_text, effective_out_id
