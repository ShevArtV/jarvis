"""Helpers for terminating CLI subprocess trees."""

from __future__ import annotations

import asyncio
import os
import signal


def signal_process_group(proc: asyncio.subprocess.Process, sig: int) -> None:
    """Send a signal to the subprocess process group, falling back to the PID."""
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
        if sig == signal.SIGTERM:
            proc.terminate()
        elif sig == signal.SIGKILL:
            proc.kill()
        else:
            os.kill(proc.pid, sig)
    except ProcessLookupError:
        return


async def terminate_process_tree(
    proc: asyncio.subprocess.Process,
    *,
    terminate_timeout: float = 2.0,
    kill_timeout: float = 2.0,
) -> None:
    """Terminate a subprocess group with SIGTERM, then SIGKILL if needed."""
    if proc.returncode is not None:
        return
    signal_process_group(proc, signal.SIGTERM)
    try:
        await asyncio.wait_for(proc.wait(), timeout=terminate_timeout)
        return
    except asyncio.TimeoutError:
        pass

    signal_process_group(proc, signal.SIGKILL)
    try:
        await asyncio.wait_for(proc.wait(), timeout=kill_timeout)
    except asyncio.TimeoutError:
        pass
