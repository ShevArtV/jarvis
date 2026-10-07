"""Persistent Cursor worker over ``cursor-agent acp`` (Agent Client Protocol).

JSON-RPC построчно по stdio, последовательность снята с CLI 2026.10.01:
``initialize`` → ``authenticate`` → ``session/new`` | ``session/load`` →
``session/prompt``. Поток хода — нотификации ``session/update``, ответ на
``session/prompt`` (``stopReason``) и есть конец хода. Второй ``session/prompt``
посреди хода прерывает первый (тот отвечает ``cancelled``) и выполняется сам —
это и есть реплика в идущий ход.

ACP хранит сессии в ``<config>/acp-sessions/<id>/``, отдельно от чатов
``cursor-agent --print`` — сессии живого и разового режимов не пересекаются.

Закрытие stdin процесс не гасит (CLI висит после EOF) — поэтому воркер
не закрывают, а убивают ``terminate_process_tree`` (bot.topics).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from collections.abc import Awaitable, Callable

from engines.common import IntermediateBuffer, iter_json_events, resolve_cwd
from engines.cursor_engine import (
    _RETRIABLE,
    CURSOR_BIN,
    FILE_MARKER_SYSTEM,
    CursorEngine,
    _config_dir,
)
from engines.cursor_events import _acp_tool_step, _format_error
from engines.process_control import spawn, terminate_process_tree
from engines.topic_mcp import acp_mcp_servers

logger = logging.getLogger(__name__)

# session/load проигрывает историю — на длинной сессии это дольше обычного вызова.
_LOAD_TIMEOUT = 120.0
_CALL_TIMEOUT = 30.0
_RETRY_LIMIT = 2
_RETRY_DELAY = 5.0


def acp_session_exists(session_id: str) -> bool:
    return bool(session_id) and (_config_dir() / "acp-sessions" / session_id / "store.db").exists()



class PersistentCursorWorker:
    """Live ``cursor-agent acp`` process for one Telegram topic."""

    def __init__(self, key: tuple[int, int], proc: asyncio.subprocess.Process,
                 session_id: str, cwd: str, model: str | None):
        self.key = key
        self.proc = proc
        self.session_id = session_id
        self.cwd = cwd
        self.model = model
        self.actual_model = model
        self.busy = False
        self.steerable = True
        self.dead = False
        # Сессию пришлось открыть заново, хотя у топика был прежний id.
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
        # id ``session/prompt`` текущего хода; ответ на прежние (cancelled) не закрывает ход.
        self._prompt_id: int | None = None
        # Текст последнего prompt и число его автоповторов (см. _RETRIABLE).
        self._last_prompt = ""
        self._retries = 0
        self._retry_task: asyncio.Task | None = None
        self._loading = False
        # Префикс первого prompt: [SYSTEM:] и FILE-маркер — только в новую сессию.
        self._prefix = ""
        self._journal = IntermediateBuffer("persistent cursor: on_intermediate failed key=%s", key)
        self._text = ""
        # Реплики после последнего tool_call — это и есть ответ.
        self._tail: list[str] = []
        self._tools: dict[str, dict] = {}
        self._stderr_tail: deque[str] = deque(maxlen=80)

    async def open_session(self, requested_session_id: str, system_prefix: str | None,
                           mcp_topic_role: str | None = None) -> str:
        await self._call("initialize", {"protocolVersion": 1, "clientCapabilities": {
            "fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
        # Ключ API CLI берёт из env сам, а cursor_login при ключе уводит во вход через браузер.
        if not os.environ.get("CURSOR_API_KEY"):
            await self._call("authenticate", {"methodId": "cursor_login"})
        # Manager MCP cursor читает из ~/.cursor/mcp.json и в ACP тоже; stdio-серверы
        # полем mcpServers ACP не принимает (только http/sse) — сюда идут http-серверы роли.
        servers = acp_mcp_servers(mcp_topic_role) if mcp_topic_role else []
        params = {"cwd": self.cwd, "mcpServers": servers}
        if acp_session_exists(requested_session_id):
            self._loading = True
            try:
                await self._call("session/load", {**params, "sessionId": requested_session_id},
                                 timeout=_LOAD_TIMEOUT)
            finally:
                self._loading = False
            self.session_id = requested_session_id
            return self.session_id
        result = await self._call("session/new", params)
        sid = result.get("sessionId")
        if not isinstance(sid, str) or not sid:
            raise RuntimeError("cursor session/new не вернул sessionId")
        # Чат разового режима у топика был, но в ACP он не загружается.
        self.fresh = CursorEngine().session_exists(requested_session_id, self.cwd)
        self.session_id = sid
        self._prefix = "\n\n".join(p for p in (system_prefix, FILE_MARKER_SYSTEM) if p) + "\n\n"
        return sid

    async def submit(self, text: str, *, exclusive: bool = False) -> tuple[bool, asyncio.Future | None]:
        """Новый ход либо реплика в идущий (см. PersistentCodexWorker.submit)."""
        async with self.turn_lock:
            if self.dead or self.proc.returncode is not None:
                raise RuntimeError("persistent cursor process is not running")
            is_new = not self.busy
            if not is_new and (exclusive or not self.steerable):
                return False, None
            if is_new:
                self.busy = True
                self.steerable = not exclusive
                self._text, self._tail, self._tools = "", [], {}
                self._retries = 0
                self.pending_future = asyncio.get_running_loop().create_future()
            else:
                # Cursor прервёт идущий ход — его недосказанный текст ответом не станет.
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
            logger.warning("persistent cursor: session/prompt failed key=%s", self.key, exc_info=True)
            self._resolve(False, f"Ошибка session/prompt: {exc}")
        self.last_activity = time.monotonic()

    async def _retry(self, rid: int) -> None:
        await asyncio.sleep(_RETRY_DELAY)
        async with self.turn_lock:
            # За паузу ход могли закрыть, прервать репликой или убить процесс.
            if self.dead or self.pending_future is None or self._prompt_id != rid:
                return
            self._text, self._tail, self._tools = "", [], {}
            await self._prompt("[SYSTEM: прошлый ход оборвался сбоем связи с сервером Cursor "
                               "(RetriableError) — выполни сообщение заново.]\n\n" + self._last_prompt)

    def _take_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    async def _send(self, payload: dict) -> None:
        async with self._write_lock:
            assert self.proc.stdin is not None
            self.proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
            await self.proc.stdin.drain()

    async def _call(self, method: str, params: dict, timeout: float = _CALL_TIMEOUT) -> dict:
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
            async for ev in iter_json_events(self.proc.stdout, "persistent cursor:"):
                if not isinstance(ev, dict):
                    continue
                if "id" in ev and "method" in ev:
                    await self._answer_request(ev)
                elif "id" in ev:
                    await self._handle_response(ev)
                elif ev.get("method") == "session/update":
                    await self._handle_update(ev.get("params") or {})
        except Exception as exc:
            logger.exception("persistent cursor: read loop crashed key=%s", self.key)
            self._resolve(False, f"cursor acp reader crashed: {exc}")
        finally:
            self.dead = True
            await self._flush(force=True)
            err = f"cursor acp stopped. {self.read_stderr_tail()}".strip()
            for fut in list(self._calls.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError(err))
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, err)

    async def _handle_response(self, ev: dict) -> None:
        rid = ev.get("id")
        error = ev.get("error")
        fut = self._calls.get(rid)
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
            return  # прерванный репликой ход: его cancelled ход не закрывает
        self._close_text()
        await self._flush(force=True)
        if error:
            self._resolve(False, f"Ошибка cursor: {_format_error(error)}")
            return
        answer = "\n\n".join(self._tail)
        if _RETRIABLE.match(answer.strip()):
            if self._retries < _RETRY_LIMIT:
                self._retries += 1
                logger.warning("persistent cursor: %s, retry %d key=%s",
                               answer.strip()[:120], self._retries, self.key)
                self._retry_task = asyncio.create_task(self._retry(rid))
                return
            self._resolve(False, f"Ошибка cursor после {_RETRY_LIMIT} повторов: {answer.strip()[:300]}")
            return
        stop = (ev.get("result") or {}).get("stopReason")
        if not answer.strip():
            self._resolve(False, f"cursor вернул пустой ответ (stopReason={stop}).")
        else:
            self._resolve(True, answer)

    async def _answer_request(self, ev: dict) -> None:
        """Неотвеченный запрос агента подвешивает ход — отвечаем на каждый."""
        rid = ev["id"]
        if ev.get("method") != "session/request_permission":
            await self._send({"jsonrpc": "2.0", "id": rid, "error": {
                "code": -32601, "message": "jarvis answers no interactive requests"}})
            return
        # Аналог --force разового режима. allow_always не берём: пишет разрешение в конфиг.
        options = (ev.get("params") or {}).get("options") or []
        chosen = next((o.get("optionId") for o in options
                       if isinstance(o, dict) and o.get("kind") == "allow_once"), None)
        outcome = ({"outcome": "selected", "optionId": chosen} if chosen
                   else {"outcome": "cancelled"})
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
            return  # проигрыш истории session/load — не новый ход
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
        # rawInput CLI присылает не сразу, а в одном из tool_call_update.
        if not tool["logged"] and ((isinstance(raw, dict) and raw) or finished):
            tool["logged"] = True
            self._journal.append(_acp_tool_step(tool["kind"], tool["title"],
                                                raw if isinstance(raw, dict) else {}, self.cwd))
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
            logger.debug("persistent cursor: stderr loop failed", exc_info=True)

    def read_stderr_tail(self) -> str:
        return "\n".join(self._stderr_tail)[-2000:]



async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentCursorWorker:
    """Поднять ``cursor-agent acp`` и открыть/загрузить сессию топика."""
    effective_cwd = resolve_cwd(cwd)
    if not os.path.isdir(effective_cwd):
        raise RuntimeError(f"Рабочая папка `{effective_cwd}` не существует.")
    if mcp_playwright:
        logger.warning("persistent cursor: Playwright MCP не поддержан, key=%s", key)
    model = model or os.environ.get("CURSOR_MODEL")
    cmd = [CURSOR_BIN, *(["--model", model] if model else []), "acp"]
    logger.info("persistent cursor start: key=%s session=%s cwd=%s model=%s",
                key, session_id, effective_cwd, model)
    proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
    worker = PersistentCursorWorker(key, proc, session_id, effective_cwd, model)
    worker.reader_task = asyncio.create_task(worker._read_loop())
    worker.stderr_task = asyncio.create_task(worker._read_stderr_loop())
    try:
        await worker.open_session(session_id, system_prefix, mcp_topic_role)
    except BaseException:
        worker.dead = True
        worker.reader_task.cancel()
        worker.stderr_task.cancel()
        await terminate_process_tree(proc)
        raise
    return worker
