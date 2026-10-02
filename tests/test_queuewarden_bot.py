"""Канал QueueWarden «Бот»: long-poll → agent_triggers Тимлиду, дедуп, ack.

HTTP мокается через httpx.MockTransport, БД — временный файл: живую
bot_state.db тесты не трогают.
"""

from __future__ import annotations

import asyncio
import base64
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
INST = qw.Installation("default", BASE, "tok", "queuewarden")
ENV = {
    "QUEUEWARDEN_MCP_TOKEN": "tok",
    "QUEUEWARDEN_URL": BASE,
    "JARVIS_TEAMLEAD_CHAT_ID": str(TOPIC[0]),
    "JARVIS_TEAMLEAD_THREAD_ID": str(TOPIC[1]),
    "JARVIS_QW_NOTIFICATIONS": "1",
}

# Боевой .env (его грузит импорт бота) задаёт список установок — тесты одной
# установки ушли бы в режим нескольких и крутились бы бесконечно.
# Тесты нескольких установок ставят список сами через patch.dict.
os.environ.pop("QUEUEWARDEN_INSTALLATIONS", None)


def _item(n: int, notification_id: int | None = None) -> dict:
    return {
        "id": n,
        "notificationId": notification_id if notification_id is not None else 1000 + n,
        "type": "task.moved",
        "title": f"QW-{n}: Починить корзину",
        "body": "Задача переведена в «В работе»",
        "taskId": 500 + n,
        "projectId": 7,
        "url": f"https://qw.example/task/{500 + n}",
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
        with bot_db.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT chat_id, thread_id, text, source, status, role "
                "FROM agent_triggers ORDER BY id"
            ).fetchall()

    def poll(self, server: _FakeQW) -> float | None:
        async def run() -> float | None:
            async with server.client() as client:
                return await qw.poll_once(client, INST, TOPIC, qw._QuietLog())
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

    def test_trigger_text_carries_notification(self) -> None:
        text = qw.build_trigger_text(_item(1), INST)
        for part in ("task.moved", "QW-1: Починить корзину", "«В работе»",
                     "https://qw.example/task/501", "taskId: 501",
                     "projectId: 7"):
            self.assertIn(part, text)

    def test_batch_prompt_numbers_events_and_allows_silence(self) -> None:
        prompt = qw.build_batch_prompt(["событие А", "событие Б"])
        self.assertIn("2 шт.", prompt)
        self.assertLess(prompt.index("--- 1 ---\nсобытие А"),
                        prompt.index("--- 2 ---\nсобытие Б"))
        for part in ("queuewarden_task_get", "не подтверждай gate", "ask_user",
                     "AGENTS.md твоего рабочего каталога", "`**[<установка>] <номер задачи>",
                     "6) новая задача (task.created) — докладывай ВСЕГДА",
                     "участие уже проверено", "<тег>"):
            self.assertIn(part, prompt)
        self.assertTrue(prompt.endswith("ответь ровно [[SILENT]]"))

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
                    return await qw.poll_once(c, INST, TOPIC, qw._QuietLog())
            asyncio.run(run())
        # Упавшее не подтверждено (QW доставит снова), записанное — да,
        # и к моменту ack триггер уже в базе.
        self.assertEqual(server.acked, [[1]])
        self.assertEqual(seen_at_ack, [1])

    def test_failed_write_rolls_back_seen_mark(self) -> None:
        # Отметка и триггер — одна транзакция: упал INSERT триггера → отметки
        # нет, и повторная доставка поставит триггер.
        with bot_db.connect(self.db_path) as conn:
            conn.execute("ALTER TABLE agent_triggers RENAME TO agent_triggers_off")
        with self.assertRaises(sqlite3.OperationalError):
            qw.enqueue_notification(_item(1), TOPIC, INST)
        with bot_db.connect(self.db_path) as conn:
            conn.execute("ALTER TABLE agent_triggers_off RENAME TO agent_triggers")
            seen = conn.execute("SELECT COUNT(*) FROM integration_seen_items").fetchone()[0]
        self.assertEqual(seen, 0)
        self.assertIsNotNone(qw.enqueue_notification(_item(1), TOPIC, INST))

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


MULTI_ENV = {
    "QUEUEWARDEN_INSTALLATIONS": "alpha, Beta,alpha",
    "QUEUEWARDEN_ALPHA_URL": "https://alpha.test/",
    "QUEUEWARDEN_ALPHA_TOKEN": "tok-a",
    "QUEUEWARDEN_BETA_URL": "https://beta.test",
    "QUEUEWARDEN_BETA_TOKEN": "tok-t",
}


class InstallationsTest(unittest.TestCase):
    def test_legacy_single_installation(self) -> None:
        with patch.dict(os.environ, ENV):
            os.environ.pop("QUEUEWARDEN_INSTALLATIONS", None)
            self.assertEqual(qw.load_installations(), [INST])
            os.environ.pop("QUEUEWARDEN_MCP_TOKEN")
            self.assertEqual(qw.load_installations(), [])

    def test_list_of_installations(self) -> None:
        with patch.dict(os.environ, {**ENV, **MULTI_ENV}):
            self.assertEqual(qw.load_installations(), [
                qw.Installation("alpha", "https://alpha.test", "tok-a",
                                "queuewarden_alpha"),
                qw.Installation("beta", "https://beta.test", "tok-t", "queuewarden_beta"),
            ])

    def test_installation_without_token_or_bad_slug_skipped(self) -> None:
        env = {**ENV, **MULTI_ENV, "QUEUEWARDEN_INSTALLATIONS": "alpha,beta,bad.slug",
               "QUEUEWARDEN_BETA_TOKEN": ""}
        with patch.dict(os.environ, env):
            self.assertEqual([i.slug for i in qw.load_installations()], ["alpha"])

    def test_trigger_text_names_installation_mcp(self) -> None:
        inst = qw.Installation("beta", "https://beta.test", "t", "queuewarden_beta")
        text = qw.build_trigger_text(_item(1), inst)
        self.assertIn("Установка: beta (https://beta.test), MCP-сервер: queuewarden_beta", text)

    def test_hashtag_from_task_key_in_title(self) -> None:
        beta = qw.Installation("beta", "https://beta.test", "t", "queuewarden_beta")
        item = {**_item(1), "title": "2609-2: Починить корзину"}
        self.assertEqual(qw.task_hashtag(item, beta), "#qwbeta2609_2")
        self.assertIn("Тег: #qwbeta2609_2", qw.build_trigger_text(item, beta))
        self.assertEqual(qw.task_hashtag(item, INST), "#qw2609_2")

    def test_no_hashtag_without_task_key(self) -> None:
        beta = qw.Installation("beta", "https://beta.test", "t", "queuewarden_beta")
        self.assertEqual(qw.task_hashtag(_item(1), beta), "")
        self.assertNotIn("Тег:", qw.build_trigger_text(_item(1), beta))


class MultiInstallationTest(_DbCase):
    def test_same_notification_id_from_two_installations_not_deduped(self) -> None:
        a = qw.Installation("alpha", "https://a.test", "t", "queuewarden_alpha")
        t = qw.Installation("beta", "https://t.test", "t", "queuewarden_beta")
        self.assertIsNotNone(qw.enqueue_notification(_item(1), TOPIC, a))
        self.assertIsNotNone(qw.enqueue_notification(_item(1), TOPIC, t))
        self.assertIsNone(qw.enqueue_notification(_item(1), TOPIC, t))
        rows = self.triggers()
        self.assertEqual(len(rows), 2)
        self.assertIn("queuewarden_alpha", rows[0][2])
        self.assertIn("queuewarden_beta", rows[1][2])

    def test_worker_polls_every_installation_with_its_token(self) -> None:
        # После уведомления — 404, чтобы каждая установка дошла до sleep и
        # цикл остановился (пустой ответ даёт паузу 0 и sleep не зовёт).
        servers = {
            tok: _FakeQW([httpx.Response(200, json={"items": [_item(1)]}),
                          httpx.Response(404)])
            for tok in ("tok-a", "tok-t")
        }

        slept: list[float] = []

        async def fake_sleep(delay: float) -> None:
            # Первая дошедшая до паузы установка ждёт вторую — иначе отмена
            # могла бы прийти раньше, чем вторая сделала свой GET.
            slept.append(delay)
            if len(slept) < 2:
                await asyncio.get_running_loop().create_future()
            raise asyncio.CancelledError

        with patch.dict(os.environ, {**ENV, **MULTI_ENV}), \
                patch.object(qw, "_make_client",
                             side_effect=lambda tok: servers[tok].client(tok)), \
                patch("asyncio.sleep", side_effect=fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(qw.queuewarden_notifications_worker(None))
        for tok, server in servers.items():
            self.assertEqual(server.acked, [[1]])
            self.assertEqual(server.auth[0], f"Bearer {tok}")
        texts = sorted(r[2] for r in self.triggers())
        self.assertEqual(len(texts), 2)
        self.assertIn("queuewarden_alpha", texts[0])
        self.assertIn("queuewarden_beta", texts[1])


CREATOR, ME = "u-tikhon", "u-me"
# Задача 2609-5: создатель и оператор — Тихон, ревизор и исполнитель — агенты
# владельца канала.
TASK_2609_5 = {
    "id": 505, "task_key": "2609-5", "project_id": 7, "created_by": CREATOR,
    "reviewer_id": "a-rev", "assignee_id": "a-exec", "operator_id": CREATOR,
}
PEOPLE = {
    "a-rev": {"id": "a-rev", "kind": "agent", "ownerId": ME},
    "a-exec": {"id": "a-exec", "kind": "agent", "ownerId": ME},
}


class _FakeMCP:
    """Мок MCP установки: tools/call → structuredContent по имени инструмента."""

    def __init__(self, artifacts: list[dict]) -> None:
        self.artifacts = artifacts
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        params = json.loads(request.content)["params"]
        name, args = params["name"], params["arguments"]
        self.calls.append((name, args))
        if name == "queuewarden_task_get":
            data = {"task": TASK_2609_5, "artifacts": self.artifacts}
        elif name == "queuewarden_department_users":
            data = {"rows": list(PEOPLE.values())}
        elif name == "queuewarden_artifact_get":
            data = {"content": base64.b64encode(b"PNG-" + args["artifactId"].encode()).decode()}
        else:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
                                             "error": {"message": "unknown tool"}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
                                         "result": {"structuredContent": data}})


class CreatedCardTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        p = patch.object(qw, "ATTACH_DIR", self._tmp.name)
        p.start()
        self.addCleanup(p.stop)

    def test_roles_for_2609_5_are_agent_owner(self) -> None:
        self.assertEqual(qw.operator_roles(TASK_2609_5, PEOPLE),
                         ["ревизор (🤖)", "исполнитель (🤖)"])

    def test_roles_human_in_slot_and_creators_agent_skipped(self) -> None:
        task = {"created_by": CREATOR, "reviewer_id": ME, "assignee_id": "a-x",
                "operator_id": ME}
        people = {ME: {"kind": "human"},
                  "a-x": {"kind": "agent", "ownerId": CREATOR}}
        self.assertEqual(qw.operator_roles(task, people), ["ревизор", "оператор"])

    def test_enrich_downloads_attachments_and_skips_big_and_run_files(self) -> None:
        mcp = _FakeMCP([
            {"id": "aaaaaaaa-1", "kind": "attachment", "filename": "скрин 1.png",
             "size_bytes": 10, "run_id": None},
            {"id": "bbbbbbbb-2", "kind": "attachment", "filename": "big.mp4",
             "size_bytes": qw.ARTIFACT_LIMIT_BYTES + 1, "run_id": None},
            {"id": "cccccccc-3", "kind": "attachment", "filename": "run.log",
             "size_bytes": 10, "run_id": "r1"},
        ])

        async def run() -> dict:
            async with httpx.AsyncClient(transport=httpx.MockTransport(mcp)) as client:
                return await qw.enrich_created(client, INST, {"taskId": 505})
        extra = asyncio.run(run())

        self.assertEqual(extra["roles"],
                         ["ревизор (🤖)", "исполнитель (🤖)"])
        path = os.path.join(self._tmp.name, "default", "2609-5", "aaaaaaaa-скрин 1.png")
        self.assertEqual(extra["files"], [path])
        with open(path, "rb") as fh:
            self.assertEqual(fh.read(), b"PNG-aaaaaaaa-1")
        self.assertEqual(extra["skipped"], ["big.mp4"])
        roles = [a["role"] for n, a in mcp.calls if n == "queuewarden_department_users"]
        self.assertEqual(roles, ["reviewer", "assignee"])

    def test_trigger_text_carries_role_and_file_markers(self) -> None:
        text = qw.build_trigger_text(
            _item(1), INST,
            {"roles": ["ревизор (🤖)", "исполнитель (🤖)"], "files": ["/m/a.png"],
             "skipped": ["big.mp4"]})
        self.assertIn("Роль оператора: ревизор (🤖) / исполнитель (🤖)", text)
        self.assertIn("\n[[FILE: /m/a.png]]", text)
        self.assertIn("big.mp4", text)

    def test_prune_removes_only_stale_task_dirs(self) -> None:
        old = os.path.join(self._tmp.name, "beta", "2609-1")
        new = os.path.join(self._tmp.name, "beta", "2609-2")
        os.makedirs(old)
        os.makedirs(new)
        os.utime(old, (0, 0))
        self.assertEqual(qw.prune_attachments(30), 1)
        self.assertFalse(os.path.exists(old))
        self.assertTrue(os.path.exists(new))


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
