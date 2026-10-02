"""Persistent Codex app-server worker.

This is intentionally separate from :mod:`engines.codex_engine`: the normal
Codex path uses ``codex exec`` as a one-shot subprocess, while true persistent
steering requires the experimental app-server JSON-RPC protocol.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path

from engines.codex_engine import (
    CODEX_BIN,
    FILE_MARKER_SYSTEM,
    CodexEngine,
    _is_placeholder,
)
from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

INTERMEDIATE_MIN_INTERVAL = 2.0


def _mcp_config_overrides(
    mcp_playwright: bool,
    mcp_topic_role: str | None,
) -> tuple[list[str], list[Path]]:
    """App-server MCP flags and temporary files."""
    flags: list[str] = []
    cleanup_paths: list[Path] = []

    if mcp_playwright:
        from engines.playwright_mcp import playwright_command_args, playwright_server_name

        spec = playwright_command_args()
        if spec is None:
            logger.warning("persistent codex: Playwright requested but globally disabled")
        else:
            npx, args = spec
            table = f"mcp_servers.{playwright_server_name()}"
            flags.extend([
                "-c", f"{table}.command={json.dumps(npx, ensure_ascii=False)}",
                "-c", f"{table}.args={json.dumps(args, ensure_ascii=False)}",
                "-c", f"{table}.enabled=true",
            ])

    if mcp_topic_role:
        from engines.topic_mcp import codex_inline_config_flags

        flags.extend(codex_inline_config_flags(mcp_topic_role))

    return flags, cleanup_paths


def _text_input(text: str) -> list[dict]:
    return [{"type": "text", "text": text, "text_elements": []}]


def _sandbox_policy() -> dict:
    return {"type": "dangerFullAccess"}


class PersistentCodexWorker:
    """Live ``codex app-server`` process for one Telegram topic."""

    def __init__(
        self,
        key: tuple[int, int],
        proc: asyncio.subprocess.Process,
        session_id: str,
        cwd: str,
        model: str | None,
        cleanup_paths: list[Path] | None = None,
    ):
        self.key = key
        self.proc = proc
        self.session_id = session_id
        self.cwd = cwd
        self.model = model
        # Живой процесс не сообщает модель в событиях — это та, что запросили.
        self.actual_model = model
        self.busy = False
        # Можно ли дописывать в идущий ход (см. submit(exclusive=...)).
        self.steerable = True
        self.dead = False
        self.last_activity = time.monotonic()
        self.turn_lock = asyncio.Lock()
        self.pending_future: asyncio.Future | None = None
        self.on_intermediate: Callable[[str], Awaitable[None]] | None = None
        self.reader_task: asyncio.Task | None = None
        self.stderr_task: asyncio.Task | None = None
        self._request_lock = asyncio.Lock()
        self._next_request_id = 1
        self._pending_requests: dict[int, asyncio.Future] = {}
        self._buffer: list[str] = []
        self._last_push = 0.0
        self._active_turn_id: str | None = None
        self._final_text = ""
        self._stderr_tail: deque[str] = deque(maxlen=80)
        self._cleanup_paths = cleanup_paths or []

    async def initialize_and_open_thread(
        self,
        requested_session_id: str,
        system_prefix: str | None,
    ) -> str:
        """Initialize JSON-RPC and start/resume the Codex thread.

        Returns the real app-server thread id. For a new Codex session the
        caller must persist it in ``sessions.session_id``.
        """
        await self._request(
            "initialize",
            {
                "clientInfo": {"name": "jarvis", "version": "0"},
                "capabilities": {"experimentalApi": True},
            },
        )

        developer_parts = []
        if system_prefix:
            developer_parts.append(system_prefix)
        developer_parts.append(FILE_MARKER_SYSTEM)
        developer_instructions = "\n\n".join(developer_parts)

        common: dict = {
            "model": self.model,
            "cwd": self.cwd,
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            "developerInstructions": developer_instructions,
        }
        common = {k: v for k, v in common.items() if v is not None}

        if (
            _is_placeholder(requested_session_id)
            or not CodexEngine().session_exists(requested_session_id, self.cwd)
        ):
            result = await self._request("thread/start", common)
        else:
            result = await self._request(
                "thread/resume", {"threadId": requested_session_id, **common}
            )
        thread_id = _extract_thread_id(result) or requested_session_id
        self.session_id = thread_id
        return thread_id

    async def submit(
        self, text: str, *, exclusive: bool = False,
    ) -> tuple[bool, asyncio.Future | None]:
        """Start a new turn or steer the active one.

        Returns ``(is_new_turn, future)``. The future resolves to
        ``(ok, final_text)`` for the turn starter; steering callers only get an
        acknowledgement and must not wait for the same final result.
        ``exclusive`` — only as a separate turn that nobody may steer into;
        ``(False, None)``: a turn is running (or it is exclusive), nothing sent.
        """
        async with self.turn_lock:
            if self.dead or self.proc.returncode is not None:
                raise RuntimeError("persistent codex process is not running")

            is_new = not self.busy
            if not is_new and (exclusive or not self.steerable):
                return False, None
            if is_new:
                self.busy = True
                self.steerable = not exclusive
                self._final_text = ""
                self._active_turn_id = None
                self.pending_future = asyncio.get_running_loop().create_future()
                fut = self.pending_future
                try:
                    result = await self._request(
                        "turn/start",
                        {
                            "threadId": self.session_id,
                            "input": _text_input(text),
                            "cwd": self.cwd,
                            "approvalPolicy": "never",
                            "sandboxPolicy": _sandbox_policy(),
                            "model": self.model,
                        },
                    )
                    self._active_turn_id = _extract_turn_id(result) or self._active_turn_id
                except Exception as exc:
                    self._resolve(False, f"Ошибка turn/start: {exc}")
                self.last_activity = time.monotonic()
                return True, fut

            fut = self.pending_future
            turn_id = self._active_turn_id
            if not turn_id:
                raise RuntimeError("active Codex turn id is not known yet")
            await self._request(
                "turn/steer",
                {
                    "threadId": self.session_id,
                    "input": _text_input(text),
                    "expectedTurnId": turn_id,
                },
            )
            self.last_activity = time.monotonic()
            return False, fut

    async def _request(self, method: str, params: dict | None) -> dict:
        async with self._request_lock:
            rid = self._next_request_id
            self._next_request_id += 1
            fut = asyncio.get_running_loop().create_future()
            self._pending_requests[rid] = fut
            payload = {"jsonrpc": "2.0", "id": rid, "method": method}
            if params is not None:
                payload["params"] = params
            try:
                assert self.proc.stdin is not None
                self.proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
                await self.proc.stdin.drain()
            except Exception:
                self._pending_requests.pop(rid, None)
                raise
        try:
            return await asyncio.wait_for(fut, timeout=30.0)
        except Exception:
            self._pending_requests.pop(rid, None)
            raise

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
            logger.exception("persistent codex: on_intermediate failed key=%s", self.key)

    def _resolve(self, ok: bool, text: str) -> None:
        fut = self.pending_future
        self.pending_future = None
        self.busy = False
        self._active_turn_id = None
        self.last_activity = time.monotonic()
        if fut is not None and not fut.done():
            fut.set_result((ok, text))

    async def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    break
                raw = line.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    logger.debug("persistent codex: non-json stdout line: %r", raw[:300])
                    continue
                if "id" in ev:
                    self._handle_response(ev)
                elif "method" in ev:
                    await self._handle_notification(ev)
        except Exception as exc:
            logger.exception("persistent codex: read loop crashed key=%s", self.key)
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, f"Codex app-server reader crashed: {exc}")
        finally:
            self.dead = True
            self._cleanup_profiles()
            await self._flush(force=True)
            err = await self.read_stderr_tail()
            for fut in list(self._pending_requests.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError(f"Codex app-server stopped. {err}".strip()))
            self._pending_requests.clear()
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, f"Codex app-server stopped. {err}".strip())

    def _handle_response(self, ev: dict) -> None:
        rid = ev.get("id")
        fut = self._pending_requests.pop(rid, None)
        if fut is None or fut.done():
            return
        if "error" in ev and ev["error"] is not None:
            fut.set_exception(RuntimeError(_format_rpc_error(ev["error"])))
        else:
            result = ev.get("result")
            fut.set_result(result if isinstance(result, dict) else {})

    async def _handle_notification(self, ev: dict) -> None:
        method = ev.get("method")
        params = ev.get("params") or {}
        if not isinstance(params, dict):
            return

        if method == "turn/started":
            turn = params.get("turn") or {}
            tid = turn.get("id")
            if isinstance(tid, str) and tid:
                self._active_turn_id = tid
            return

        if method in {"item/started", "item/completed"}:
            item = params.get("item") or {}
            if not isinstance(item, dict):
                return
            itype = item.get("type")
            if itype == "commandExecution" and method == "item/started":
                cmd = (item.get("command") or "").strip()
                if cmd:
                    self._buffer.append(f"🔧 exec {cmd[:150]}")
                    await self._flush()
            elif itype == "reasoning" and method == "item/completed":
                chunks = []
                summary = item.get("summary")
                content = item.get("content")
                if isinstance(summary, list):
                    chunks.extend(str(x) for x in summary if x)
                if isinstance(content, list):
                    chunks.extend(str(x) for x in content if x)
                txt = "\n".join(chunks).strip()
                if txt:
                    self._buffer.append(f"💭 {txt[:800]}")
                    await self._flush()
            elif itype == "agentMessage" and method == "item/completed":
                txt = (item.get("text") or "").strip()
                if txt:
                    self._final_text = txt
                    self._buffer.append(txt[:800])
                    await self._flush()
            return

        if method == "item/agentMessage/delta":
            # Delta events are token/subword fragments. Publishing them directly
            # makes Telegram journal lines like "Од\nобр\nение". The completed
            # agentMessage item below carries the assembled text.
            return

        if method in {"item/reasoning/summaryTextDelta", "item/reasoning/textDelta"}:
            # Same as agentMessage deltas: wait for item/completed, where Codex
            # sends reasoning content as assembled summary/content chunks.
            return

        if method == "turn/completed":
            await self._flush(force=True)
            turn = params.get("turn") or {}
            error = turn.get("error") if isinstance(turn, dict) else None
            if error:
                self._resolve(False, _format_rpc_error(error))
            else:
                self._resolve(True, self._final_text)
            return

        if method == "error":
            message = _format_rpc_error(params)
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, message)
            return

        if method == "warning":
            message = (params.get("message") or params.get("text") or "").strip()
            if message:
                self._buffer.append(f"⚠️ {message[:800]}")
                await self._flush()

    async def _read_stderr_loop(self) -> None:
        if self.proc.stderr is None:
            return
        try:
            while True:
                line = await self.proc.stderr.readline()
                if not line:
                    break
                self._stderr_tail.append(line.decode("utf-8", errors="replace").rstrip())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("persistent codex: stderr loop failed", exc_info=True)

    async def read_stderr_tail(self) -> str:
        return "\n".join(self._stderr_tail)[-2000:]

    def _cleanup_profiles(self) -> None:
        if not self._cleanup_paths:
            return
        from engines.topic_mcp import cleanup_codex_profile

        paths = self._cleanup_paths
        self._cleanup_paths = []
        for path in paths:
            cleanup_codex_profile(path)


def _extract_thread_id(result: dict) -> str | None:
    for obj in (result, result.get("thread")):
        if isinstance(obj, dict):
            for key in ("threadId", "id"):
                value = obj.get(key)
                if isinstance(value, str) and value:
                    return value
    return None


def _extract_turn_id(result: dict) -> str | None:
    for obj in (result, result.get("turn")):
        if isinstance(obj, dict):
            for key in ("turnId", "id"):
                value = obj.get(key)
                if isinstance(value, str) and value:
                    return value
    return None


def _format_rpc_error(error: object) -> str:
    if isinstance(error, str):
        return error
    if not isinstance(error, dict):
        return str(error)
    for key in ("message", "error", "details"):
        value = error.get(key)
        if isinstance(value, str) and value:
            return value
    return json.dumps(error, ensure_ascii=False)[:1500]


async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentCodexWorker:
    """Start app-server and open/resume a Codex thread."""
    effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))
    if not os.path.isdir(effective_cwd):
        raise RuntimeError(f"Рабочая папка `{effective_cwd}` не существует.")

    mcp_flags, mcp_cleanup_paths = _mcp_config_overrides(mcp_playwright, mcp_topic_role)
    cmd = [
        CODEX_BIN,
        "app-server",
        *mcp_flags,
        "--listen",
        "stdio://",
    ]
    logger.info(
        "persistent codex start: key=%s session=%s cwd=%s model=%s",
        key,
        session_id,
        effective_cwd,
        model,
    )
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=effective_cwd,
            start_new_session=True,
            limit=10 * 1024 * 1024,
        )
    except Exception:
        from engines.topic_mcp import cleanup_codex_profile

        for path in mcp_cleanup_paths:
            cleanup_codex_profile(path)
        raise
    worker = PersistentCodexWorker(key, proc, session_id, effective_cwd, model, mcp_cleanup_paths)
    worker.reader_task = asyncio.create_task(worker._read_loop())
    worker.stderr_task = asyncio.create_task(worker._read_stderr_loop())
    try:
        await worker.initialize_and_open_thread(session_id, system_prefix)
    except Exception:
        worker.dead = True
        if worker.reader_task:
            worker.reader_task.cancel()
        if worker.stderr_task:
            worker.stderr_task.cancel()
        worker._cleanup_profiles()
        await terminate_process_tree(proc)
        raise
    return worker
