"""Persistent OpenCode worker on top of ``opencode serve``.

``opencode run`` is one-shot: one process per message. For a live process we
start the headless HTTP server once per topic and talk to it:

- ``POST /session/{id}/prompt_async`` — send a message; while a turn is running
  the server admits it into the same session instead of rejecting it;
- ``GET /event`` (SSE) — parts, tool calls, permission asks and
  ``session.idle``, which marks the end of the turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from engines.opencode_engine import (
    FILE_MARKER_SYSTEM,
    OPENCODE_BIN,
    OpenCodeEngine,
    _cleanup_tempfile,
    _error_message,
    _opencode_env,
    _opencode_mcp_config,
    _tool_summary,
)
from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

INTERMEDIATE_MIN_INTERVAL = 2.0
SERVER_START_TIMEOUT = 30.0
_URL_RE = re.compile(r"https?://[\w.\-]+:\d+")

# Новой сессии сразу разрешаем всё — аналог `opencode run
# --dangerously-skip-permissions`. Для сессий, созданных раньше через
# `opencode run`, страхует автоответ на permission.asked.
_ALLOW_ALL = [{"permission": "*", "pattern": "*", "action": "allow"}]


def _model_ref(model: str | None) -> dict | None:
    """``provider/model`` → ``{"providerID", "modelID"}`` для prompt_async."""
    model = model or os.environ.get("OPENCODE_MODEL")
    if not model or "/" not in model:
        return None
    provider, model_id = model.split("/", 1)
    return {"providerID": provider, "modelID": model_id}


class PersistentOpenCodeWorker:
    """Live ``opencode serve`` process for one Telegram topic."""

    def __init__(
        self,
        key: tuple[int, int],
        proc: asyncio.subprocess.Process,
        base_url: str,
        session_id: str,
        cwd: str,
        model: str | None,
        system: str,
        config_path: str | None = None,
    ):
        self.key = key
        self.proc = proc
        self.base_url = base_url
        self.session_id = session_id
        self.cwd = cwd
        self.model = model
        self.system = system
        self.busy = False
        self.dead = False
        self.last_activity = time.monotonic()
        self.turn_lock = asyncio.Lock()
        self.pending_future: asyncio.Future | None = None
        self.on_intermediate: Callable[[str], Awaitable[None]] | None = None
        self.reader_task: asyncio.Task | None = None
        self.stderr_task: asyncio.Task | None = None
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
        self._connected = asyncio.Event()
        self._config_path = config_path
        self._buffer: list[str] = []
        self._last_push = 0.0
        self._output_tail: deque[str] = deque(maxlen=80)
        self._reset_turn()

    def _reset_turn(self) -> None:
        self._saw_busy = False
        self._error = ""
        self._roles: dict[str, str] = {}
        self._msg_order: list[str] = []
        self._texts: dict[str, dict[str, str]] = {}
        self._announced: set[str] = set()

    # --- HTTP ---

    async def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        params = {"directory": self.cwd, **kwargs.pop("params", {})}
        resp = await self.client.request(method, path, params=params, **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {resp.status_code} {resp.text[:500]}")
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return None

    async def open_session(self, requested_session_id: str) -> str:
        """Resume the stored ``ses_...`` session or create a new one."""
        if OpenCodeEngine().session_exists(requested_session_id, self.cwd):
            try:
                await self._call("GET", f"/session/{requested_session_id}")
                self.session_id = requested_session_id
                return self.session_id
            except RuntimeError:
                logger.warning(
                    "persistent opencode: session %s not found, creating new key=%s",
                    requested_session_id, self.key,
                )
        body: dict[str, Any] = {"permission": _ALLOW_ALL}
        agent = os.environ.get("OPENCODE_AGENT")
        if agent:
            body["agent"] = agent
        info = await self._call("POST", "/session", json=body)
        sid = info.get("id") if isinstance(info, dict) else None
        if not isinstance(sid, str) or not sid:
            raise RuntimeError(f"opencode serve не вернул id сессии: {info!r}")
        self.session_id = sid
        return sid

    async def _prompt(self, text: str) -> None:
        body: dict[str, Any] = {
            "parts": [{"type": "text", "text": text}],
            "system": self.system,
        }
        model = _model_ref(self.model)
        if model:
            body["model"] = model
        for env_name, field in (("OPENCODE_AGENT", "agent"), ("OPENCODE_VARIANT", "variant")):
            value = os.environ.get(env_name)
            if value:
                body[field] = value
        await self._call("POST", f"/session/{self.session_id}/prompt_async", json=body)

    # --- Turn ---

    async def submit(self, text: str) -> tuple[bool, "asyncio.Future"]:
        """Start a new turn or add the message to the running one.

        Returns ``(is_new_turn, future)`` like the Claude/Codex workers.
        """
        async with self.turn_lock:
            if self.dead or self.proc.returncode is not None:
                raise RuntimeError("persistent opencode process is not running")
            is_new = not self.busy
            if is_new:
                self.busy = True
                self._reset_turn()
                self.pending_future = asyncio.get_running_loop().create_future()
            fut = self.pending_future
            try:
                await self._prompt(text)
            except Exception as exc:
                if is_new:
                    self._resolve(False, f"Ошибка prompt_async: {exc}")
                else:
                    raise
            self.last_activity = time.monotonic()
            return is_new, fut

    def _final_text(self) -> str:
        for mid in reversed(self._msg_order):
            if self._roles.get(mid) != "assistant":
                continue
            text = "\n".join(t for t in self._texts.get(mid, {}).values() if t).strip()
            if text:
                return text
        return ""

    def _resolve(self, ok: bool, text: str) -> None:
        fut = self.pending_future
        self.pending_future = None
        self.busy = False
        self.last_activity = time.monotonic()
        if fut is not None and not fut.done():
            fut.set_result((ok, text))

    async def _flush(self, force: bool = False) -> None:
        if not self._buffer:
            return
        now = time.monotonic()
        if not force and (now - self._last_push) < INTERMEDIATE_MIN_INTERVAL:
            return
        text = "\n".join(self._buffer)
        self._buffer.clear()
        self._last_push = now
        cb = self.on_intermediate
        if cb is None:
            return
        try:
            await cb(text)
        except Exception:
            logger.exception("persistent opencode: on_intermediate failed key=%s", self.key)

    # --- Events ---

    async def _read_loop(self) -> None:
        try:
            async with self.client.stream(
                "GET", "/event", params={"directory": self.cwd}, timeout=None,
            ) as resp:
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:])
                    except json.JSONDecodeError:
                        continue
                    if isinstance(ev, dict):
                        await self._handle_event(ev)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("persistent opencode: event loop crashed key=%s", self.key)
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, f"opencode serve: поток событий оборвался: {exc}")
        finally:
            self.dead = True
            self._connected.set()
            _cleanup_tempfile(self._config_path)
            self._config_path = None
            await self._flush(force=True)
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, f"opencode serve остановился. {await self.read_stderr_tail()}".strip())
            await self.client.aclose()

    async def _handle_event(self, ev: dict) -> None:
        etype = ev.get("type")
        props = ev.get("properties") if isinstance(ev.get("properties"), dict) else {}
        if etype == "server.connected":
            self._connected.set()
            return
        if props.get("sessionID") not in (None, self.session_id):
            return
        if etype in ("session.status", "session.idle"):
            status = (props.get("status") or {}).get("type") if etype == "session.status" else "idle"
            if status in ("busy", "retry"):
                self._saw_busy = True
            elif status == "idle" and self.busy and (self._saw_busy or self._error):
                await self._flush(force=True)
                text = self._final_text()
                if text:
                    self._resolve(True, text)
                else:
                    self._resolve(False, self._error or "opencode вернул пустой ответ.")
            return
        if etype == "message.updated":
            info = props.get("info") if isinstance(props.get("info"), dict) else {}
            mid, role = info.get("id"), info.get("role")
            if isinstance(mid, str) and isinstance(role, str):
                if mid not in self._roles:
                    self._msg_order.append(mid)
                self._roles[mid] = role
                if role == "assistant":
                    self._saw_busy = True
            return
        if etype == "message.part.updated":
            await self._handle_part(props.get("part"))
            return
        if etype == "session.error":
            self._error = _error_message(props) or "opencode: ошибка сессии"
            self._buffer.append(f"⚠️ {self._error[:800]}")
            await self._flush()
            return
        if etype == "permission.asked":
            pid = props.get("id")
            if isinstance(pid, str):
                try:
                    await self._call(
                        "POST", f"/session/{self.session_id}/permissions/{pid}",
                        json={"response": "always"},
                    )
                except Exception:
                    logger.exception("persistent opencode: permission reply failed key=%s", self.key)
            return
        if etype == "question.asked":
            # Спрашивать через TUI-вопросы некому: у пользователя есть ask_user.
            qid = props.get("id")
            if isinstance(qid, str):
                try:
                    await self._call("POST", f"/question/{qid}/reject")
                except Exception:
                    logger.exception("persistent opencode: question reject failed key=%s", self.key)

    async def _handle_part(self, part: Any) -> None:
        if not isinstance(part, dict):
            return
        pid, mid, ptype = part.get("id"), part.get("messageID"), part.get("type")
        if not isinstance(pid, str) or not isinstance(mid, str):
            return
        if mid not in self._roles:
            self._msg_order.append(mid)
            self._roles[mid] = ""
        ended = bool((part.get("time") or {}).get("end"))
        if ptype == "text" and not part.get("synthetic"):
            self._texts.setdefault(mid, {})[pid] = part.get("text") or ""
            if ended and self._roles.get(mid) == "assistant" and pid not in self._announced:
                self._announced.add(pid)
                self._buffer.append((part.get("text") or "")[-800:])
                await self._flush()
        elif ptype == "reasoning" and ended and pid not in self._announced:
            self._announced.add(pid)
            txt = (part.get("text") or "").strip()
            if txt:
                self._buffer.append(f"💭 {txt[:800]}")
                await self._flush()
        elif ptype == "tool" and pid not in self._announced:
            status = (part.get("state") or {}).get("status")
            if status in ("running", "completed", "error"):
                self._announced.add(pid)
                self._buffer.append(f"🔧 {_tool_summary(part)}")
                await self._flush()

    # --- Process output ---

    async def _read_output_loop(self) -> None:
        if self.proc.stdout is None:
            return
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    break
                self._output_tail.append(line.decode("utf-8", errors="replace").rstrip())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("persistent opencode: output loop failed", exc_info=True)

    async def read_stderr_tail(self) -> str:
        return "\n".join(self._output_tail)[-2000:]


async def _wait_for_url(proc: asyncio.subprocess.Process, tail: list[str]) -> str:
    assert proc.stdout is not None
    while True:
        line = await proc.stdout.readline()
        if not line:
            raise RuntimeError("opencode serve завершился при старте: " + "\n".join(tail)[-1500:])
        text = line.decode("utf-8", errors="replace").rstrip()
        tail.append(text)
        match = _URL_RE.search(text)
        if match:
            return match.group(0)


async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentOpenCodeWorker:
    """Start ``opencode serve`` and open/resume the topic session."""
    effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))
    if not os.path.isdir(effective_cwd):
        raise RuntimeError(f"Рабочая папка `{effective_cwd}` не существует.")

    config_path = _opencode_mcp_config(mcp_playwright, mcp_topic_role)
    env = _opencode_env()
    if config_path:
        env["OPENCODE_CONFIG"] = config_path
    cmd = [OPENCODE_BIN, "serve", "--hostname", "127.0.0.1", "--port", "0"]
    logger.info(
        "persistent opencode start: key=%s session=%s cwd=%s model=%s",
        key, session_id, effective_cwd, model,
    )
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=effective_cwd,
            env=env,
            start_new_session=True,
            limit=10 * 1024 * 1024,
        )
    except Exception:
        _cleanup_tempfile(config_path)
        raise

    tail: list[str] = []
    try:
        base_url = await asyncio.wait_for(_wait_for_url(proc, tail), SERVER_START_TIMEOUT)
    except BaseException:
        _cleanup_tempfile(config_path)
        await terminate_process_tree(proc)
        raise

    system = "\n\n".join(p for p in (system_prefix, FILE_MARKER_SYSTEM) if p)
    worker = PersistentOpenCodeWorker(
        key, proc, base_url, session_id, effective_cwd, model, system, config_path,
    )
    worker._output_tail.extend(tail)
    worker.stderr_task = asyncio.create_task(worker._read_output_loop())
    worker.reader_task = asyncio.create_task(worker._read_loop())
    try:
        await asyncio.wait_for(worker._connected.wait(), SERVER_START_TIMEOUT)
        if worker.dead:
            raise RuntimeError("opencode serve: не удалось подписаться на события")
        await worker.open_session(session_id)
    except BaseException:
        worker.dead = True
        worker.reader_task.cancel()
        worker.stderr_task.cancel()
        _cleanup_tempfile(config_path)
        await terminate_process_tree(proc)
        raise
    return worker
