"""Идёт ли у живого процесса движка фоновая работа между ходами.

Ход закончился (``busy=False``), а агент ещё ждёт фонового субагента или
фоновую shell-команду — такой процесс reaper убивать не должен: вместе с ним
умрёт и фоновая работа, а уведомление о её конце не придёт никогда
(Тимлид, 04.11.2026 03:31 — субагент убит с ``Exit code 137``).

Порядок проверки:
1. у воркера свой точный сигнал ``has_background_work()`` (claude — по событию
   ``background_tasks_changed`` из stream-json);
2. иначе — общий для всех движков: у процесса есть дочерний shell, которого не
   было при старте воркера. MCP-серверы — node/python и живут с самого старта,
   их не считаем. Работает только на Linux (``/proc``); на других ОС — «нет».
"""

from __future__ import annotations

import glob
import logging
import os

logger = logging.getLogger(__name__)

# Чем движки запускают shell-команды агента.
_SHELLS = frozenset({"bash", "sh", "zsh", "dash", "fish"})


def _child_pids(pid: int) -> set[int]:
    pids: set[int] = set()
    for path in glob.glob(f"/proc/{pid}/task/*/children"):
        try:
            with open(path) as fh:
                pids.update(int(p) for p in fh.read().split())
        except (OSError, ValueError):
            continue
    return pids


def _comm(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/comm") as fh:
            return fh.read().strip()
    except OSError:
        return ""


def shell_children(pid: int | None) -> set[int]:
    """Прямые дочерние shell-процессы ``pid`` (пусто вне Linux)."""
    if not pid or not os.path.isdir("/proc"):
        return set()
    return {c for c in _child_pids(pid) if _comm(c) in _SHELLS}


def capture_baseline(worker) -> None:
    """Запомнить shell-детей сразу после старта воркера — это не фоновая работа."""
    proc = getattr(worker, "proc", None)
    worker.baseline_shells = shell_children(getattr(proc, "pid", None))


def has_background_work(worker) -> bool:
    check = getattr(worker, "has_background_work", None)
    if callable(check):
        return bool(check())
    proc = getattr(worker, "proc", None)
    current = shell_children(getattr(proc, "pid", None))
    return bool(current - getattr(worker, "baseline_shells", set()))
