"""Общий воркер Agent Client Protocol (JSON-RPC по stdio).

Используется cursor-agent acp и kimi acp: ``initialize`` → auth →
``session/new|load`` → ``session/prompt``. Поток хода — ``session/update``,
конец — ответ на ``session/prompt``. У cursor второй prompt посреди хода
прерывает первый (``cancelled``) — реплика в идущий ход. У kimi
(``supports_steer=False``) второй prompt отвергается — ждём конца хода.

Закрытие stdin процесс не гасит — воркер убивают через ``terminate_process_tree``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from engines.common import IntermediateBuffer, iter_json_events
from engines.cursor_events import _acp_tool_step, _format_error

logger = logging.getLogger(__name__)

LOAD_TIMEOUT = 120.0
CALL_TIMEOUT = 30.0


class AcpWorker:
    """Базовый живой ACP-процесс на один Telegram-топик."""

    # Подклассы могут переопределить через class attr или module-level proxy.
    RETRY_LIMIT = 0
    RETRY_DELAY = 5.0
    label = "acp"
    file_marker = ""
    # False — ACP не принимает второй session/prompt в busy (kimi).
    supports_steer = True

    def __init__(
        self,
        key: tuple[int, int],
        proc: asyncio.subprocess.Process,
        session_id: str,
        cwd: str,
        model: str | None,
    ):
        self.key = key
        self.proc = proc
        self.session_id = session_id
        self.cwd = cwd
        self.model = model
        self.actual_model = model
        self.busy = False
        self.steerable = True
        self.dead = False
        self.fresh = False
        self.last_activity = time.monotonic()
        self.turn_lock = asyncio.Lock()
        self.pending_future: asyncio.Future | None = None
        self.on_intermediate: Callable[[str], Awaitable[None]] | None = None
        self.reader_task: asyncio.Task | None = None
        self.stderr_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()
        self._next_id = 1
        self._calls: dict[int, asyncio.Future] = {}
        self._prompt_id: int | None = None
        self._last_prompt = ""
        self._retries = 0
        self._retry_task: asyncio.Task | None = None
        self._loading = False
        self._prefix = ""
        self._journal = IntermediateBuffer(
            f"persistent {self.label}: on_intermediate failed key=%s", key,
        )
        self._text = ""
        self._tail: list[str] = []
        self._tools: dict[str, dict] = {}
        self._stderr_tail: deque[str] = deque(maxlen=80)

    # --- hooks ---

    async def authenticate(self) -> None:
        """Опциональный login после initialize."""

    def session_on_disk(self, session_id: str) -> bool:
        raise NotImplementedError

    def mcp_servers(
        self, mcp_topic_role: str | None, mcp_playwright: bool,
    ) -> list[dict[str, Any]]:
        return []

    def had_print_session(self, session_id: str) -> bool:
        """True, если разовый режим имел сессию, но ACP её не подхватил."""
        return False

    def is_retriable(self, answer: str) -> bool:
        return False

    def retry_system_prefix(self) -> str:
        return (
            f"[SYSTEM: прошлый ход оборвался сбоем связи с сервером {self.label} "
            "— выполни сообщение заново.]\n\n"
        )

    @property
    def retry_limit(self) -> int:
        return self.RETRY_LIMIT

    @property
    def retry_delay(self) -> float:
        return self.RETRY_DELAY

    # --- session ---

    async def open_session(
        self,
        requested_session_id: str,
        system_prefix: str | None,
        mcp_topic_role: str | None = None,
        mcp_playwright: bool = False,
    ) -> str:
        await self._call("initialize", {"protocolVersion": 1, "clientCapabilities": {
            "fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
        await self.authenticate()
        servers = self.mcp_servers(mcp_topic_role, mcp_playwright)
        params: dict[str, Any] = {"cwd": self.cwd, "mcpServers": servers}
        if self.session_on_disk(requested_session_id):
            self._loading = True
            try:
                await self._call(
                    "session/load",
                    {**params, "sessionId": requested_session_id},
                    timeout=LOAD_TIMEOUT,
                )
            finally:
                self._loading = False
            self.session_id = requested_session_id
            return self.session_id
        result = await self._call("session/new", params)
        sid = result.get("sessionId")
        if not isinstance(sid, str) or not sid:
            raise RuntimeError(f"{self.label} session/new не вернул sessionId")
        self.fresh = self.had_print_session(requested_session_id)
        self.session_id = sid
        self._prefix = "\n\n".join(
            p for p in (system_prefix, self.file_marker) if p
        ) + ("\n\n" if system_prefix or self.file_marker else "")
        return sid

    async def submit(
        self, text: str, *, exclusive: bool = False,
    ) -> tuple[bool, asyncio.Future | None]:
        async with self.turn_lock:
            if self.dead or self.proc.returncode is not None:
                raise RuntimeError(f"persistent {self.label} process is not running")
            is_new = not self.busy
            if not is_new and (exclusive or not self.steerable or not self.supports_steer):
                return False, None
            if is_new:
                self.busy = True
                self.steerable = not exclusive and self.supports_steer
                self._text, self._tail, self._tools = "", [], {}
                self._retries = 0
                self.pending_future = asyncio.get_running_loop().create_future()
            else:
                self._text, self._tail = "", []
                self._retries = 0
            fut = self.pending_future
            text, self._prefix = self._prefix + text, ""
            self._last_prompt = text
            await self._prompt(text)
            return is_new, fut

    async def _prompt(self, text: str) -> None:
        rid = self._take_id()
        self._prompt_id = rid
        try:
            await self._send({"jsonrpc": "2.0", "id": rid, "method": "session/prompt", "params": {
                "sessionId": self.session_id, "prompt": [{"type": "text", "text": text}]}})
        except Exception as exc:
            logger.warning(
                "persistent %s: session/prompt failed key=%s", self.label, self.key,
                exc_info=True,
            )
            self._resolve(False, f"Ошибка session/prompt: {exc}")
        self.last_activity = time.monotonic()

    async def _retry(self, rid: int) -> None:
        await asyncio.sleep(self.retry_delay)
        async with self.turn_lock:
            if self.dead or self.pending_future is None or self._prompt_id != rid:
                return
            self._text, self._tail, self._tools = "", [], {}
            await self._prompt(self.retry_system_prefix() + self._last_prompt)

    def _take_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    async def _send(self, payload: dict) -> None:
        async with self._write_lock:
            assert self.proc.stdin is not None
            self.proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
            await self.proc.stdin.drain()

    async def _call(
        self, method: str, params: dict, timeout: float = CALL_TIMEOUT,
    ) -> dict:
        rid = self._take_id()
        fut = asyncio.get_running_loop().create_future()
        self._calls[rid] = fut
        try:
            await self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._calls.pop(rid, None)

    async def _flush(self, force: bool = False) -> None:
        await self._journal.flush(self.on_intermediate, force)

    def _resolve(self, ok: bool, text: str) -> None:
        fut = self.pending_future
        self.pending_future = None
        self._prompt_id = None
        self.busy = False
        self.last_activity = time.monotonic()
        if fut is not None and not fut.done():
            fut.set_result((ok, text))

    async def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        try:
            async for ev in iter_json_events(self.proc.stdout, f"persistent {self.label}:"):
                if not isinstance(ev, dict):
                    continue
                if "id" in ev and "method" in ev:
                    await self._answer_request(ev)
                elif "id" in ev:
                    await self._handle_response(ev)
                elif ev.get("method") == "session/update":
                    await self._handle_update(ev.get("params") or {})
        except Exception as exc:
            logger.exception(
                "persistent %s: read loop crashed key=%s", self.label, self.key,
            )
            self._resolve(False, f"{self.label} acp reader crashed: {exc}")
        finally:
            self.dead = True
            await self._flush(force=True)
            err = f"{self.label} acp stopped. {self.read_stderr_tail()}".strip()
            for fut in list(self._calls.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError(err))
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, err)

    async def _handle_response(self, ev: dict) -> None:
        rid = ev.get("id")
        error = ev.get("error")
        fut = self._calls.get(rid) if isinstance(rid, int) else None
        if fut is not None:
            if fut.done():
                return
            if error:
                fut.set_exception(RuntimeError(_format_error(error)))
            else:
                result = ev.get("result")
                fut.set_result(result if isinstance(result, dict) else {})
            return
        if rid != self._prompt_id:
            return
        self._close_text()
        await self._flush(force=True)
        if error:
            self._resolve(False, f"Ошибка {self.label}: {_format_error(error)}")
            return
        answer = "\n\n".join(self._tail)
        if self.is_retriable(answer):
            if self._retries < self.retry_limit:
                self._retries += 1
                logger.warning(
                    "persistent %s: %s, retry %d key=%s",
                    self.label, answer.strip()[:120], self._retries, self.key,
                )
                self._retry_task = asyncio.create_task(self._retry(rid))
                return
            self._resolve(
                False,
                f"Ошибка {self.label} после {self.retry_limit} повторов: "
                f"{answer.strip()[:300]}",
            )
            return
        stop = (ev.get("result") or {}).get("stopReason")
        if not answer.strip():
            self._resolve(False, f"{self.label} вернул пустой ответ (stopReason={stop}).")
        else:
            self._resolve(True, answer)

    async def _answer_request(self, ev: dict) -> None:
        rid = ev["id"]
        if ev.get("method") != "session/request_permission":
            await self._send({"jsonrpc": "2.0", "id": rid, "error": {
                "code": -32601, "message": "jarvis answers no interactive requests"}})
            return
        options = (ev.get("params") or {}).get("options") or []
        chosen = next(
            (o.get("optionId") for o in options
             if isinstance(o, dict) and o.get("kind") == "allow_once"),
            None,
        )
        outcome = (
            {"outcome": "selected", "optionId": chosen} if chosen
            else {"outcome": "cancelled"}
        )
        await self._send({"jsonrpc": "2.0", "id": rid, "result": {"outcome": outcome}})

    def _close_text(self) -> None:
        text = self._text.strip()
        self._text = ""
        if text:
            self._tail.append(text)
            self._journal.append(text[:800])

    async def _handle_update(self, params: dict) -> None:
        update = params.get("update") or {}
        if self._loading or not isinstance(update, dict):
            return
        kind = update.get("sessionUpdate")
        if kind == "agent_message_chunk":
            content = update.get("content") or {}
            if content.get("type") == "text":
                self._text += content.get("text") or ""
            return
        if kind not in ("tool_call", "tool_call_update"):
            return
        self._close_text()
        if kind == "tool_call":
            self._tail.clear()
        tid = update.get("toolCallId") or ""
        tool = self._tools.setdefault(tid, {"kind": "", "title": "", "logged": False})
        for field in ("kind", "title"):
            if update.get(field):
                tool[field] = update[field]
        raw = update.get("rawInput")
        finished = update.get("status") in ("completed", "failed")
        if not tool["logged"] and ((isinstance(raw, dict) and raw) or finished):
            tool["logged"] = True
            self._journal.append(_acp_tool_step(
                tool["kind"], tool["title"],
                raw if isinstance(raw, dict) else {}, self.cwd,
            ))
        if finished:
            self._tools.pop(tid, None)
        await self._flush()

    async def _read_stderr_loop(self) -> None:
        if self.proc.stderr is None:
            return
        try:
            async for line in self.proc.stderr:
                self._stderr_tail.append(line.decode("utf-8", errors="replace").rstrip())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("persistent %s: stderr loop failed", self.label, exc_info=True)

    def read_stderr_tail(self) -> str:
        return "\n".join(self._stderr_tail)[-2000:]
