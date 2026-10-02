"""Запуск CLI-движков и остановка их деревьев процессов — на Linux, macOS и Windows.

На Windows npm ставит CLI как ``codex.cmd``-обёртку. Запуск ``.cmd`` идёт через
cmd.exe, который режет аргумент на переводе строки и раскрывает ``%VAR%``,
поэтому обёртку разворачиваем в ``node <script.js>`` и запускаем node напрямую.
Группы процессов POSIX там нет: дерево гасим через ``taskkill /T``.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import subprocess
import sys

IS_WINDOWS = sys.platform == "win32"

# Строка запуска в обёртке npm cmd-shim: "%dp0%\node_modules\pkg\bin\cli.js" %*
_CMD_SHIM_SCRIPT_RE = re.compile(r'"%~?dp0%?\\([^"]+?\.[cm]?js)"', re.IGNORECASE)


def resolve_command(bin_name: str) -> list[str]:
    """Префикс argv для запуска CLI: ``[path]`` или ``[node, script.js]``.

    Голое имя ищется в PATH (на Windows — с учётом PATHEXT). Не найдено —
    возвращается как есть, чтобы запуск упал привычным FileNotFoundError."""
    path = shutil.which(bin_name) or bin_name
    if IS_WINDOWS and path.lower().endswith((".cmd", ".bat")):
        return unwrap_cmd_shim(path) or [path]
    return [path]


def unwrap_cmd_shim(path: str) -> list[str] | None:
    """``[node, script.js]`` из обёртки npm cmd-shim или None, если формат чужой."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError:
        return None
    match = _CMD_SHIM_SCRIPT_RE.search(text)
    if not match:
        return None
    shim_dir = os.path.dirname(path)
    script = os.path.join(shim_dir, match.group(1))
    if not os.path.isfile(script):
        return None
    local_node = os.path.join(shim_dir, "node.exe")
    node = local_node if os.path.isfile(local_node) else shutil.which("node")
    return [node, script] if node else None


def detached_kwargs() -> dict:
    """Отдельная группа процессов для CLI: её можно погасить целиком, а Ctrl+C
    в консоли бота не долетает до движков."""
    if IS_WINDOWS:
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


async def spawn(cmd: list[str], *, cwd: str | None = None, stdin: int | None = None,
                stderr: int = asyncio.subprocess.PIPE, **kwargs) -> asyncio.subprocess.Process:
    """``create_subprocess_exec`` для CLI движка: argv[0] разворачивается через
    resolve_command, stdout/stderr — в пайпы, лимит строки 10 МБ (stream-json)."""
    argv = [*resolve_command(cmd[0]), *cmd[1:]]
    return await asyncio.create_subprocess_exec(
        *argv,
        stdin=stdin,
        stdout=asyncio.subprocess.PIPE,
        stderr=stderr,
        cwd=cwd,
        limit=10 * 1024 * 1024,
        **detached_kwargs(),
        **kwargs,
    )


async def feed_stdin(proc: asyncio.subprocess.Process, text: str) -> None:
    """Отдать промпт в stdin и закрыть его. Промпт в argv упирается в лимиты
    длины аргумента (128 КБ на Linux, 32 КБ на всю строку в Windows)."""
    assert proc.stdin is not None
    try:
        proc.stdin.write(text.encode("utf-8"))
        await proc.stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass  # CLI умер на старте — причину покажет его stderr и код выхода
    finally:
        proc.stdin.close()


def run_cli(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Синхронный ``subprocess.run`` для коротких служебных вызовов CLI."""
    kwargs.setdefault("encoding", "utf-8")
    kwargs.setdefault("errors", "replace")
    kwargs.setdefault("check", False)
    return subprocess.run([*resolve_command(cmd[0]), *cmd[1:]], **kwargs)


def pid_alive(pid: int) -> bool:
    """Жив ли процесс. Чужой процесс без прав на сигнал считается живым."""
    if IS_WINDOWS:
        return _pid_alive_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_alive_windows(pid: int) -> bool:
    # os.kill(pid, 0) на Windows шлёт CTRL_C_EVENT, а не проверяет процесс.
    import ctypes

    process_query_limited_information = 0x1000
    error_access_denied = 5
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return ctypes.get_last_error() == error_access_denied
    try:
        code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return True
        return code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def signal_process_group(proc: asyncio.subprocess.Process, sig: int) -> None:
    """Послать сигнал группе процессов CLI, при неудаче — самому процессу (POSIX)."""
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    except Exception:
        pgid = None

    if pgid:
        try:
            os.killpg(pgid, sig)
            return
        except ProcessLookupError:
            return
        except Exception:
            pass

    try:
        os.kill(proc.pid, sig)
    except ProcessLookupError:
        return


async def _kill_tree_windows(proc: asyncio.subprocess.Process) -> None:
    killer = await asyncio.create_subprocess_exec(
        "taskkill", "/T", "/F", "/PID", str(proc.pid),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await killer.wait()
    if proc.returncode is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass


async def terminate_process_tree(
    proc: asyncio.subprocess.Process,
    *,
    terminate_timeout: float = 2.0,
    kill_timeout: float = 2.0,
) -> None:
    """Погасить CLI вместе с потомками: SIGTERM группе, через terminate_timeout —
    SIGKILL. На Windows мягкой остановки нет, сразу ``taskkill /T /F``."""
    if proc.returncode is not None:
        return
    if IS_WINDOWS:
        await _kill_tree_windows(proc)
        try:
            await asyncio.wait_for(proc.wait(), timeout=kill_timeout)
        except TimeoutError:
            pass
        return

    signal_process_group(proc, signal.SIGTERM)
    try:
        await asyncio.wait_for(proc.wait(), timeout=terminate_timeout)
        return
    except TimeoutError:
        pass

    signal_process_group(proc, signal.SIGKILL)
    try:
        await asyncio.wait_for(proc.wait(), timeout=kill_timeout)
    except TimeoutError:
        pass
