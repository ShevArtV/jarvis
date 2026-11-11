"""Каждый CLI движка — в своём transient scope systemd (Linux, бот под user-сервисом).

Без этого все сеансы и их дети (тесты, сборки, MCP) живут в cgroup ``jarvis-bot.service``.
Один тяжёлый прогон упирает сервис в MemoryHigh, давление памяти растёт, и systemd-oomd
убивает весь сервис — бот и сеансы всех топиков (11.11.2026, vitest в 11 воркеров).
В своём scope oomd убьёт только виновный сеанс.

``systemd-run --scope`` запускает команду тем же процессом (PID не меняется — группа процессов
и остановка дерева работают как раньше). ``PartOf`` — остановка и рестарт бота гасят сеансы,
как гасил их общий cgroup. Лимиты на сеанс — защита от runaway-процесса (ugrep, 29.07.2026).
"""

from __future__ import annotations

import functools
import logging
import os
import shutil
import subprocess
import sys
import uuid

logger = logging.getLogger(__name__)

LIMITS = ("MemoryHigh=3G", "MemoryMax=4G", "MemorySwapMax=512M")


def _own_service() -> str | None:
    """Имя user-сервиса, в cgroup которого живёт бот, или None."""
    try:
        with open("/proc/self/cgroup", encoding="utf-8") as fh:
            path = fh.read().strip().rsplit(":", 1)[-1]
    except OSError:
        return None
    unit = path.rsplit("/", 1)[-1]
    return unit if "/user@" in path and unit.endswith(".service") else None


@functools.cache
def scope_prefix() -> tuple[str, ...]:
    """Общая часть префикса ``systemd-run`` или пусто, если scope недоступен.
    Проверяется один раз пробным запуском: сбой systemd-run не должен ронять сеансы."""
    if not sys.platform.startswith("linux") or os.environ.get("JARVIS_CLI_SCOPE") == "0":
        return ()
    service, run = _own_service(), shutil.which("systemd-run")
    if not service or not run:
        return ()
    prefix = (run, "--user", "--scope", "--quiet", "--collect", "--expand-environment=no",
              f"--property=PartOf={service}", *(f"--property={p}" for p in LIMITS))
    try:
        probe = subprocess.run([*prefix, "true"], capture_output=True, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("cli scope disabled: probe failed: %s", exc)
        return ()
    if probe.returncode != 0:
        logger.warning("cli scope disabled: rc=%s %s", probe.returncode, probe.stderr[-300:])
        return ()
    logger.info("cli scope enabled: PartOf=%s %s", service, " ".join(LIMITS))
    return prefix


def wrap(argv: list[str]) -> list[str]:
    """argv CLI в отдельном scope (или как есть, если scope недоступен)."""
    prefix = scope_prefix()
    if not prefix:
        return argv
    return [*prefix, f"--unit=jarvis-cli-{uuid.uuid4().hex[:12]}", *argv]
