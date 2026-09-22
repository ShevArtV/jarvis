"""Живая проверка persistent opencode: ход + сообщение посреди хода.

    ./venv/bin/python scripts/smoke_persistent_opencode.py [cwd] [provider/model]
"""

import asyncio
import logging
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engines.persistent_opencode import start_persistent  # noqa: E402
from engines.process_control import terminate_process_tree  # noqa: E402


async def main() -> None:
    cwd = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="oc-smoke-")
    model = sys.argv[2] if len(sys.argv) > 2 else None
    t0 = time.monotonic()

    async def journal(text: str) -> None:
        print(f"[{time.monotonic() - t0:6.1f}] JOURNAL: {text[:200]!r}")

    worker = await start_persistent(
        key=(0, 0), session_id="placeholder-smoke", cwd=cwd, model=model,
        system_prefix="Кодовое слово: ЗЕБРА.", mcp_playwright=False,
    )
    print("session", worker.session_id, worker.base_url)
    worker.on_intermediate = journal
    try:
        is_new, fut = await worker.submit(
            "Выполни в bash команду `sleep 15; echo A` и после этого ответь одним словом: ГОТОВО."
        )
        print("first is_new", is_new)
        await asyncio.sleep(6)
        is_new2, _ = await worker.submit(
            "Дополнение: в финальном ответе также назови кодовое слово и сколько будет 2+2."
        )
        print("second is_new", is_new2)
        ok, text = await asyncio.wait_for(fut, 240)
        print(f"[{time.monotonic() - t0:6.1f}] RESULT ok={ok}: {text!r}")
        print("busy after turn:", worker.busy)

        is_new3, fut3 = await worker.submit("Какое кодовое слово? Ответь одним словом.")
        ok3, text3 = await asyncio.wait_for(fut3, 120)
        print(f"[{time.monotonic() - t0:6.1f}] RESULT2 new={is_new3} ok={ok3}: {text3!r}")
    finally:
        worker.dead = True
        worker.reader_task.cancel()
        worker.stderr_task.cancel()
        await terminate_process_tree(worker.proc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
