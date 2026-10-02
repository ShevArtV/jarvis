"""Webhook receiver for external events (Bitrix24, etc.).

Env:
  JARVIS_WEBHOOK_TOKEN  — обязательный секрет: заголовок X-Jarvis-Token или ?token=...
                          в URL (Битрикс24 умеет только второе)
  JARVIS_WEBHOOK_HOST   — адрес (default: 127.0.0.1; наружу — через reverse proxy с TLS)
  JARVIS_WEBHOOK_PORT   — порт (default: 8765)

Если JARVIS_WEBHOOK_TOKEN не задан — сервер не запускается.
"""
import asyncio
import hmac
import json
import logging
import os
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import datetime
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_state.db")

NoticeCallback = Callable[[str, str], Awaitable[None]]

_BITRIX_EVENTS: dict[str, str] = {
    "ONCRMACTIVITYADD": "новое действие в CRM",
    "ONTASKADD": "новая задача в Битрикс24",
    "ONCALENDARENTRY": "новое событие в календаре",
    "ONCRMDEALUPDATE": "обновление сделки CRM",
    "ONCRMDEALADD": "новая сделка CRM",
}


def _log_event(source: str, event: str, payload: dict) -> None:
    try:
        conn = sqlite3.connect(_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            conn.execute(
                "INSERT INTO webhook_log(source, event, payload, received_at) "
                "VALUES (?, ?, ?, ?)",
                (source, event, json.dumps(payload, ensure_ascii=False),
                 datetime.utcnow().isoformat()),
            )
        conn.close()
    except Exception:
        logger.exception("webhook: log failed source=%s event=%s", source, event)


def _make_starlette_app(send_notice: NoticeCallback, token: str):
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route

    async def bitrix24(request: Request) -> Response:
        given = request.headers.get("x-jarvis-token") or request.query_params.get("token", "")
        if not hmac.compare_digest(given.encode(), token.encode()):
            return Response("Unauthorized", status_code=401)
        try:
            body = await request.body()
            params = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
            data = {k: (v[0] if len(v) == 1 else v) for k, v in params.items()}
            event = data.get("event", "")
        except Exception:
            logger.exception("webhook: failed to parse bitrix24 body")
            return JSONResponse({"ok": False}, status_code=400)

        _log_event("bitrix24", event, data)

        if event in _BITRIX_EVENTS:
            label = _BITRIX_EVENTS[event]
            title = (
                data.get("data[FIELDS][TITLE]")
                or data.get("data[FIELDS][NAME]")
                or data.get("data[FIELDS][SUBJECT]", "")
            )
            text = f"📨 Битрикс24: {label}"
            if title:
                text += f" — {title}"
            asyncio.create_task(send_notice(text, "webhook"))

        return JSONResponse({"ok": True})

    return Starlette(routes=[
        Route("/hook/bitrix24", bitrix24, methods=["POST", "GET"]),
    ])


async def run_webhook_server(send_notice: NoticeCallback) -> None:
    token = os.environ.get("JARVIS_WEBHOOK_TOKEN", "")
    if not token:
        logger.info("JARVIS_WEBHOOK_TOKEN not set — webhook server disabled")
        return

    host = os.environ.get("JARVIS_WEBHOOK_HOST", "127.0.0.1")
    port = int(os.environ.get("JARVIS_WEBHOOK_PORT", "8765"))

    try:
        import uvicorn
    except ImportError:
        logger.error(
            "uvicorn not installed — webhook server disabled "
            "(pip install 'uvicorn[standard]' starlette)"
        )
        return

    app = _make_starlette_app(send_notice, token)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    # Отключаем uvicorn'овский перехват сигналов — в основном event loop
    # сигналы обрабатывает PTB.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    logger.info("webhook_server: starting on %s:%d", host, port)
    try:
        await server.serve()
    except Exception:
        logger.exception("webhook_server: serve() raised")
