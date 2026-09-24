"""Канал QueueWarden «Бот»: long-poll → agent_triggers Тимлиду, дедуп, ack.

HTTP мокается через httpx.MockTransport, БД — временный файл: живую
bot_state.db тесты не трогают.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from bot import db as bot_db
from integrations import queuewarden_bot as qw

TOPIC = (-1001, 77)
BASE = "https://qw.test"
ENV = {
    "QUEUEWARDEN_MCP_TOKEN": "tok",
    "QUEUEWARDEN_URL": BASE,
    "JARVIS_TEAMLEAD_CHAT_ID": str(TOPIC[0]),
    "JARVIS_TEAMLEAD_THREAD_ID": str(TOPIC[1]),
    "JARVIS_QW_NOTIFICATIONS": "1",
}


def _item(n: int, notification_id: int | None = None) -> dict:
    return {
        "id": n,
        "notificationId": notification_id if notification_id is not None else 1000 + n,
        "type": "task.moved",
        "title": f"QW-{n}: Починить корзину",
        "body": "Задача переведена в «В работе»",
        "taskId": 500 + n,
        "projectId": 7,
        "url": f"https://stage.queuewarden.ru/task/{500 + n}",
        "createdAt": "2026-09-23T10:00:00Z",
    }


class _FakeQW:
    """Мок-сервер: GET отдаёт очередную заготовку, ack копит ids."""

    def __init__(self, responses: list) -> None:
        self.responses = list(responses)
        self.acked: list[list] = []
        self.gets = 0
        self.auth: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.auth.append(request.headers.get("Authorization", ""))
        if request.url.path == "/api/bot/notifications/ack":
            ids = json.loads(request.content)["ids"]
            self.acked.append(ids)
            return httpx.Response(200, json={"acked": len(ids)})
        self.gets += 1
        resp = self.responses.pop(0) if self.responses else httpx.Response(200, json={"items": []})
        if isinstance(resp, Exception):
            raise resp
        return resp

    def client(self, token: str = "tok") -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self),
            headers={"Authorization": f"Bearer {token}"},
        )


class _DbCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "bot_state.db")
        self._db_patch = patch.object(bot_db, "DB_PATH", self.db_path)
        self._db_patch.start()
        bot_db.init_db()

    def tearDown(self) -> None:
        self._db_patch.stop()
        self._tmp.cleanup()

    def triggers(self) -> list[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT chat_id, thread_id, text, source, status, role "
                "FROM agent_triggers ORDER BY id"
            ).fetchall()

    def poll(self, server: _FakeQW) -> float | None:
        async def run() -> float | None:
            async with server.client() as client:
                return await qw.poll_once(client, BASE, TOPIC, qw._QuietLog())
        return asyncio.run(run())


class ParseTest(unittest.TestCase):
    def test_parses_items_and_drops_malformed(self) -> None:
        items = qw.parse_notifications({"items": [
            _item(1), {"id": 2}, {"notificationId": 3}, "junk", _item(4),
        ]})
        self.assertEqual([i["id"] for i in items], [1, 4])

    def test_empty_and_bad_payload(self) -> None:
        self.assertEqual(qw.parse_notifications({"items": []}), [])
        with self.assertRaises(ValueError):
            qw.parse_notifications({"error": "x"})
        with self.assertRaises(ValueError):
            qw.parse_notifications([])

    def test_trigger_text_carries_notification_and_instruction(self) -> None:
        text = qw.build_trigger_text(_item(1))
        for part in ("task.moved", "QW-1: Починить корзину", "«В работе»",
                     "https://stage.queuewarden.ru/task/501", "taskId: 501",
                     "projectId: 7", "queuewarden_task_get", "КОРОТКОЕ резюме"):
            self.assertIn(part, text)

    def test_notice_topic_env_override(self) -> None:
        with patch.dict(os.environ, ENV):
            self.assertEqual(qw.resolve_notice_topic(), TOPIC)
            with patch.dict(os.environ, {"JARVIS_QW_NOTICE_CHAT_ID": "-5",
                                         "JARVIS_QW_NOTICE_THREAD_ID": "9"}):
                self.assertEqual(qw.resolve_notice_topic(), (-5, 9))


class PollTest(_DbCase):
    def test_trigger_goes_to_topic_and_is_acked(self) -> None:
        server = _FakeQW([httpx.Response(200, json={"items": [_item(1), _item(2)]})])
        self.assertEqual(self.poll(server), 0.0)
        rows = self.triggers()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row[:2], TOPIC)
            self.assertEqual(row[3], "queuewarden")
            self.assertEqual(row[4], "pending")
            self.assertEqual(row[5], "manager")
        self.assertIn("QW-1", rows[0][2])
        self.assertEqual(server.acked, [[1, 2]])
        self.assertTrue(all(a == "Bearer tok" for a in server.auth))

    def test_duplicate_is_acked_but_not_enqueued_twice(self) -> None:
        # Повторная доставка того же notificationId (новый id доставки).
        server = _FakeQW([
            httpx.Response(200, json={"items": [_item(1)]}),
            httpx.Response(200, json={"items": [_item(2, notification_id=1001)]}),
        ])
        self.poll(server)
        self.poll(server)
        self.assertEqual(len(self.triggers()), 1)
        self.assertEqual(server.acked, [[1], [2]])

    def test_ack_only_after_trigger_is_written(self) -> None:
        server = _FakeQW([httpx.Response(200, json={"items": [_item(1), _item(2)]})])
        real = qw.enqueue_agent_trigger
        seen_at_ack: list[int] = []

        def flaky(chat_id, thread_id, text, source, role=None, seen_key=None):
            if seen_key[2] == 1002:
                raise sqlite3.OperationalError("database is locked")
            return real(chat_id, thread_id, text, source, role=role, seen_key=seen_key)

        orig_call = server.__call__

        def spy(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/ack"):
                seen_at_ack.append(len(self.triggers()))
            return orig_call(request)

        with patch.object(qw, "enqueue_agent_trigger", side_effect=flaky):
            async def run() -> float | None:
                async with httpx.AsyncClient(transport=httpx.MockTransport(spy)) as c:
                    return await qw.poll_once(c, BASE, TOPIC, qw._QuietLog())
            asyncio.run(run())
        # Упавшее не подтверждено (QW доставит снова), записанное — да,
        # и к моменту ack триггер уже в базе.
        self.assertEqual(server.acked, [[1]])
        self.assertEqual(seen_at_ack, [1])

    def test_failed_write_rolls_back_seen_mark(self) -> None:
        # Отметка и триггер — одна транзакция: упал INSERT триггера → отметки
        # нет, и повторная доставка поставит триггер.
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("ALTER TABLE agent_triggers RENAME TO agent_triggers_off")
        with self.assertRaises(sqlite3.OperationalError):
            qw.enqueue_notification(_item(1), TOPIC)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("ALTER TABLE agent_triggers_off RENAME TO agent_triggers")
            seen = conn.execute("SELECT COUNT(*) FROM integration_seen_items").fetchone()[0]
        self.assertEqual(seen, 0)
        self.assertIsNotNone(qw.enqueue_notification(_item(1), TOPIC))

    def test_error_statuses(self) -> None:
        cases = [
            (httpx.Response(404), qw.NOT_DEPLOYED_SLEEP_SECONDS),
            (httpx.Response(401), qw.NOT_DEPLOYED_SLEEP_SECONDS),
            (httpx.Response(403, json={"error": "bot_requires_bridge"}),
             qw.NOT_DEPLOYED_SLEEP_SECONDS),
            (httpx.Response(502), None),
            (httpx.Response(200, json={"items": []}), 0.0),
        ]
        for resp, expected in cases:
            with self.subTest(status=resp.status_code):
                server = _FakeQW([resp])
                self.assertEqual(self.poll(server), expected)
                self.assertEqual(server.acked, [])
        self.assertEqual(self.triggers(), [])


class WorkerLoopTest(_DbCase):
    def _run_worker(self, server: _FakeQW, sleeps_before_stop: int) -> list[float]:
        delays: list[float] = []

        async def fake_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) >= sleeps_before_stop:
                raise asyncio.CancelledError

        with patch.dict(os.environ, ENV), \
                patch.object(qw, "_make_client", side_effect=lambda t: server.client(t)), \
                patch("asyncio.sleep", side_effect=fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(qw.queuewarden_notifications_worker(None))
        return delays

    def test_404_5xx_and_network_errors_do_not_crash_loop(self) -> None:
        server = _FakeQW([
            httpx.Response(404),
            httpx.Response(500),
            httpx.ConnectError("boom"),
            httpx.ReadTimeout("slow"),
            httpx.Response(200, content=b"not json"),
            httpx.Response(200, json={"items": [_item(1)]}),
            httpx.Response(503),
        ])
        delays = self._run_worker(server, sleeps_before_stop=6)
        # 404 → 60; 500/сеть/таймаут/битый JSON → backoff 5,10,20,40;
        # успех сбрасывает backoff → следующий 503 снова ждёт 5.
        self.assertEqual(delays, [60.0, 5.0, 10.0, 20.0, 40.0, 5.0])
        self.assertEqual(len(self.triggers()), 1)
        self.assertEqual(server.acked, [[1]])

    def test_backoff_capped(self) -> None:
        server = _FakeQW([httpx.Response(500)] * 8)
        delays = self._run_worker(server, sleeps_before_stop=7)
        self.assertEqual(delays, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0])


class WorkerDisabledTest(unittest.TestCase):
    def _run(self, env: dict) -> _FakeQW:
        server = _FakeQW([])
        full = {k: v for k, v in {**ENV, **env}.items() if v is not None}
        with patch.dict(os.environ, full), \
                patch.object(qw, "_make_client", side_effect=lambda t: server.client(t)):
            for key, value in env.items():
                if value is None:
                    os.environ.pop(key, None)
            asyncio.run(asyncio.wait_for(qw.queuewarden_notifications_worker(None), 2))
        return server

    def test_no_token_exits(self) -> None:
        self.assertEqual(self._run({"QUEUEWARDEN_MCP_TOKEN": None}).gets, 0)

    def test_switched_off_exits(self) -> None:
        for value in ("0", "off"):
            with self.subTest(value=value):
                self.assertEqual(self._run({"JARVIS_QW_NOTIFICATIONS": value}).gets, 0)

    def test_no_topic_exits(self) -> None:
        server = self._run({
            "JARVIS_TEAMLEAD_CHAT_ID": None, "JARVIS_TEAMLEAD_THREAD_ID": None, "JARVIS_SECRETARY_CHAT_ID": None, "JARVIS_SECRETARY_THREAD_ID": None,
            "JARVIS_MANAGER_CHAT_ID": None, "JARVIS_MANAGER_THREAD_ID": None,
            "JARVIS_QW_NOTICE_CHAT_ID": None, "JARVIS_QW_NOTICE_THREAD_ID": None,
        })
        self.assertEqual(server.gets, 0)


if __name__ == "__main__":
    unittest.main()
