"""Живой процесс ``claude`` под stream-json: один процесс на топик, реплики — в stdin."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable

from engines.claude_cli import _accumulate_assistant_event, _claude_command
from engines.claude_engine import ClaudeEngine
from engines.common import (
    IntermediateBuffer,
    iter_json_events,
)
from engines.process_control import spawn

logger = logging.getLogger(__name__)


class PersistentClaudeWorker:
    """Живой ``claude`` subprocess в ``stream-json``/``stream-json`` режиме,
    держится на весь сеанс топика вместо процесса на сообщение.

    Сообщение, отправленное пока идёт предыдущий ход (между стартом хода и
    его ``result``), не порождает новый subprocess и не ждёт лока — пишется в
    тот же stdin и подхватывается моделью сразу после текущего шага (tool-вызов
    при этом НЕ прерывается, только между шагами). Экспериментально проверено
    2026-07-13 — см. README, раздел «Живой процесс /persistent».
    """

    def __init__(self, key: tuple[int, int], proc: asyncio.subprocess.Process,
                 session_id: str, cwd: str):
        self.key = key
        self.proc = proc
        self.session_id = session_id
        self.cwd = cwd
        self.busy = False
        # Можно ли дописывать в идущий ход (см. submit(exclusive=...)).
        self.steerable = True
        self.dead = False
        self.last_activity = time.monotonic()
        self.turn_lock = asyncio.Lock()
        self.pending_future: asyncio.Future | None = None
        self.on_intermediate: Callable[[str], Awaitable[None]] | None = None
        self.reader_task: asyncio.Task | None = None
        # Модель, которой CLI ответил последний раз (system.init / assistant).
        self.actual_model: str | None = None
        self._journal = IntermediateBuffer(
            "persistent worker: on_intermediate failed key=%s", key,
        )

    def _write_user_message(self, text: str) -> None:
        line = json.dumps({
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        }) + "\n"
        assert self.proc.stdin is not None
        self.proc.stdin.write(line.encode())

    async def submit(
        self, text: str, *, exclusive: bool = False,
    ) -> tuple[bool, asyncio.Future | None]:
        """Отправить реплику живому процессу.

        Возвращает (is_new_turn, future). ``future`` резолвится в
        ``(ok, final_text)`` — ждать её нужно, только если ``is_new_turn``:
        если ход уже шёл, реплика просто дописана в него, и результат придёт
        тому вызову, который этот ход начал.

        ``exclusive`` — только отдельным ходом, в который никто не допишет
        (триггер, которому разрешено промолчать). ``(False, None)`` — реплика
        не отправлена: идёт ход, а дописывать нельзя; ждать его конца."""
        async with self.turn_lock:
            is_new = not self.busy
            if not is_new and (exclusive or not self.steerable):
                return False, None
            if is_new:
                self.busy = True
                self.steerable = not exclusive
                self.pending_future = asyncio.get_running_loop().create_future()
            fut = self.pending_future
            self._write_user_message(text)
            try:
                assert self.proc.stdin is not None
                await self.proc.stdin.drain()
            except Exception:
                logger.exception("persistent worker: stdin.drain failed key=%s", self.key)
        self.last_activity = time.monotonic()
        return is_new, fut

    async def _flush(self, force: bool = False) -> None:
        await self._journal.flush(self.on_intermediate, force)

    def _resolve(self, ok: bool, text: str) -> None:
        fut = self.pending_future
        self.pending_future = None
        self.busy = False
        self.last_activity = time.monotonic()
        if fut is not None and not fut.done():
            fut.set_result((ok, text))

    async def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        try:
            async for ev in iter_json_events(self.proc.stdout, "persistent claude"):
                etype = ev.get("type")
                if etype == "system" and ev.get("subtype") == "init":
                    m = ev.get("model")
                    if isinstance(m, str) and m:
                        self.actual_model = m
                if etype == "assistant":
                    m = (ev.get("message") or {}).get("model")
                    if isinstance(m, str) and m:
                        self.actual_model = m
                    _accumulate_assistant_event(ev, self._journal.lines, self.cwd)
                    await self._flush()
                elif etype == "result":
                    await self._flush(force=True)
                    r = ev.get("result")
                    self._resolve(True, r if isinstance(r, str) else "")
        except Exception:
            logger.exception("persistent worker: read loop crashed key=%s", self.key)
        finally:
            self.dead = True
            await self._flush(force=True)
            # Процесс умер (краш/убили) во время хода — будим ожидающего, а не
            # вешаем его до CLAUDE_TIMEOUT.
            if self.pending_future is not None and not self.pending_future.done():
                self._resolve(False, "")

    async def read_stderr_tail(self) -> str:
        if self.proc.stderr is None:
            return ""
        try:
            data = await self.proc.stderr.read(4096)
            return data.decode("utf-8", errors="replace")
        except Exception:
            logger.debug("persistent worker: cannot read stderr key=%s", self.key, exc_info=True)
            return ""


async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentClaudeWorker:
    """Поднять живой ``claude`` под ``stream-json`` вход/выход. Флаги сессии —
    как в разовом вызове (``--resume``/``--session-id``), но выставляются
    ОДИН раз при старте процесса: все следующие реплики уходят в его stdin,
    без пересоздания."""
    resume_mode, session_flags = ClaudeEngine()._session_flags(session_id, cwd, is_spawn=False)
    cmd = _claude_command(
        session_flags, model, system_prefix, mcp_playwright, mcp_topic_role,
        input_format="stream-json",
    )
    logger.info(
        "persistent claude start: key=%s session=%s mode=%s cwd=%s",
        key, session_id, "resume" if resume_mode else "new", cwd,
    )
    proc = await spawn(cmd, cwd=cwd, stdin=asyncio.subprocess.PIPE)
    worker = PersistentClaudeWorker(key, proc, session_id, cwd)
    worker.reader_task = asyncio.create_task(worker._read_loop())
    return worker
