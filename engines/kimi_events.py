"""Разбор stream-json Kimi Code CLI (print-режим)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from engines.common import IntermediateBuffer, iter_json_events
from engines.tool_steps import _tool_step


def _tool_step_from_kimi(name: str, arguments: Any, cwd: str) -> str | None:
    args: dict[str, Any] = {}
    if isinstance(arguments, str) and arguments.strip():
        try:
            parsed = json.loads(arguments)
            if isinstance(parsed, dict):
                args = parsed
        except json.JSONDecodeError:
            args = {"input": arguments}
    elif isinstance(arguments, dict):
        args = arguments
    if args.get("path") and not args.get("file_path"):
        args = {**args, "file_path": args["path"]}
    return _tool_step(name or "tool", args, cwd)


class KimiStream:
    """Разбор stream-json одного ``kimi -p --output-format stream-json``."""

    def __init__(
        self, cwd: str, model: str | None,
        on_intermediate: Callable[[str], Awaitable[None]],
    ) -> None:
        self.cwd = cwd
        self.on_intermediate = on_intermediate
        self.journal = IntermediateBuffer()
        self.tail_texts: list[str] = []
        self.actual_model = model
        self.session_id: str | None = None
        self.error = ""

    async def flush(self, force: bool = False) -> None:
        await self.journal.flush(self.on_intermediate, force)

    async def read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for ev in iter_json_events(proc.stdout, "kimi"):
            await self.handle_event(ev)

    def _remember_session(self, value: Any) -> None:
        if isinstance(value, str) and value.startswith("session_"):
            self.session_id = value

    async def handle_event(self, ev: Any) -> None:
        if not isinstance(ev, dict):
            return
        for key in ("sessionId", "session_id", "id"):
            if ev.get("role") == "meta" or key in ev:
                self._remember_session(ev.get(key))
        if isinstance(ev.get("session"), dict):
            self._remember_session(ev["session"].get("id"))

        role = ev.get("role")
        if role == "meta":
            m = ev.get("model")
            if isinstance(m, str) and m:
                self.actual_model = m
            return
        if role == "assistant":
            content = ev.get("content")
            text = ""
            if isinstance(content, str):
                text = content.strip()
            elif isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        parts.append(block.get("text") or "")
                    elif isinstance(block, str):
                        parts.append(block)
                text = "".join(parts).strip()
            tool_calls = ev.get("tool_calls")
            if tool_calls:
                self.tail_texts.clear()
                if isinstance(tool_calls, list):
                    for call in tool_calls:
                        if not isinstance(call, dict):
                            continue
                        fn = call.get("function") if isinstance(call.get("function"), dict) else {}
                        step = _tool_step_from_kimi(
                            str(fn.get("name") or call.get("name") or "tool"),
                            fn.get("arguments"),
                            self.cwd,
                        )
                        if step:
                            self.journal.append(step)
                await self.flush()
                return
            if text:
                self.tail_texts.append(text)
                self.journal.append(text[:800])
                await self.flush()
            return
        if role == "tool":
            return

    def answer(self) -> str:
        return "\n\n".join(self.tail_texts)
