from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engines import topic_mcp
from engines.claude_engine import _mcp_config_flags as claude_mcp_flags
from engines.codex_engine import (
    _mcp_config_overrides as codex_mcp_overrides,
    _split_codex_global_flags,
)
from engines.opencode_engine import _opencode_mcp_config
from engines.persistent_codex import _mcp_config_overrides as persistent_codex_mcp_overrides


CONFIG = {
    "servers": [
        {
            "name": "mxboard",
            "url": "https://example.test/mcp.php",
            "roles": {
                "manager": {"headers": {"Authorization": "Bearer manager-token"}},
                "agent": {"headers": {"Authorization": "Bearer agent-token"}},
            },
        }
    ]
}


class TopicMcpTestBase(unittest.TestCase):
    def setUp(self) -> None:
        # Модуль кэширует разобранный конфиг между вызовами — иначе бот
        # перечитывал бы файл на каждый ход. В тестах кэш обязан быть чистым.
        topic_mcp._CACHE["stat"] = None
        topic_mcp._CACHE["servers"] = None

    def _write(self, tmp: str, data, name: str = "topic-mcp.json") -> Path:
        path = Path(tmp) / name
        path.write_text(
            data if isinstance(data, str) else json.dumps(data), encoding="utf-8"
        )
        return path

    def _env(self, path: Path | str):
        return patch.dict("os.environ", {"JARVIS_TOPIC_MCP_CONFIG": str(path)})


class MissingConfigTest(TopicMcpTestBase):
    """Отсутствие конфига — не ошибка.

    До 2026-07-25 нехватка личного файла автора роняла RuntimeError на КАЖДОМ
    сообщении: свежий клон был неработоспособен. Эти тесты держат инвариант
    «нет интеграции → бот просто работает без её тулов».
    """

    def test_no_env_var_returns_no_servers(self) -> None:
        with patch.dict("os.environ", {}, clear=False) as _:
            import os

            os.environ.pop("JARVIS_TOPIC_MCP_CONFIG", None)
            self.assertEqual(topic_mcp.servers_for_role("agent"), [])
            self.assertIsNone(topic_mcp.config_path())

    def test_missing_file_warns_and_returns_no_servers(self) -> None:
        with self._env("/nonexistent/topic-mcp.json"):
            with self.assertLogs("engines.topic_mcp", level="WARNING") as logs:
                self.assertEqual(topic_mcp.servers_for_role("agent"), [])
        self.assertIn("unreadable", "\n".join(logs.output))

    def test_broken_json_warns_and_returns_no_servers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, "{not json")
            with self._env(path):
                with self.assertLogs("engines.topic_mcp", level="WARNING") as logs:
                    self.assertEqual(topic_mcp.servers_for_role("manager"), [])
        self.assertIn("cannot parse", "\n".join(logs.output))

    def test_engines_build_no_flags_without_config(self) -> None:
        with self._env("/nonexistent/topic-mcp.json"):
            self.assertEqual(claude_mcp_flags(False, "agent"), [])
            flags, cleanup = codex_mcp_overrides(False, "agent")
            self.assertEqual((flags, cleanup), ([], []))
            self.assertIsNone(_opencode_mcp_config(False, "agent"))
            self.assertEqual(persistent_codex_mcp_overrides(False, "agent"), ([], []))

    def test_global_kill_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path), patch.dict("os.environ", {"JARVIS_TOPIC_MCP": "0"}):
                self.assertEqual(topic_mcp.servers_for_role("manager"), [])


class RoleResolutionTest(TopicMcpTestBase):
    def test_roles_get_different_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path):
                manager = topic_mcp.servers_for_role("manager")
                agent = topic_mcp.servers_for_role("agent")

        self.assertEqual(manager[0]["headers"]["Authorization"], "Bearer manager-token")
        self.assertEqual(agent[0]["headers"]["Authorization"], "Bearer agent-token")
        self.assertEqual(manager[0]["url"], "https://example.test/mcp.php")

    def test_service_roles_fall_back_to_legacy_manager_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path):
                secretary = topic_mcp.servers_for_role("secretary")
                teamlead = topic_mcp.servers_for_role("teamlead")

        self.assertEqual(secretary[0]["headers"]["Authorization"], "Bearer manager-token")
        self.assertEqual(teamlead[0]["headers"]["Authorization"], "Bearer manager-token")

    def test_service_roles_may_have_explicit_credentials(self) -> None:
        data = {"servers": [{
            "name": "mxboard",
            "url": "https://example.test/mcp.php",
            "roles": {
                "manager": {"headers": {"Authorization": "Bearer manager-token"}},
                "secretary": {"headers": {"Authorization": "Bearer secretary-token"}},
                "teamlead": {"headers": {"Authorization": "Bearer teamlead-token"}},
            },
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                secretary = topic_mcp.servers_for_role("secretary")
                teamlead = topic_mcp.servers_for_role("teamlead")

        self.assertEqual(secretary[0]["headers"]["Authorization"], "Bearer secretary-token")
        self.assertEqual(teamlead[0]["headers"]["Authorization"], "Bearer teamlead-token")

    def test_server_without_roles_applies_to_every_role(self) -> None:
        data = {"servers": [{
            "name": "shared",
            "url": "https://example.test/shared",
            "headers": {"Authorization": "Bearer one-token"},
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                for role in ("manager", "agent"):
                    specs = topic_mcp.servers_for_role(role)
                    self.assertEqual(len(specs), 1)
                    self.assertEqual(specs[0]["headers"]["Authorization"], "Bearer one-token")

    def test_role_absent_from_roles_map_excludes_server(self) -> None:
        data = {"servers": [{
            "name": "manageronly",
            "url": "https://example.test/m",
            "roles": {"manager": {"headers": {"Authorization": "Bearer m"}}},
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                self.assertEqual(len(topic_mcp.servers_for_role("manager")), 1)
                self.assertEqual(topic_mcp.servers_for_role("agent"), [])

    def test_role_entry_may_override_url(self) -> None:
        data = {"servers": [{
            "name": "split",
            "url": "https://example.test/default",
            "roles": {"agent": {"url": "https://example.test/agent"}},
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                specs = topic_mcp.servers_for_role("agent")
        self.assertEqual(specs[0]["url"], "https://example.test/agent")

    def test_role_headers_merge_over_common_headers(self) -> None:
        data = {"servers": [{
            "name": "merged",
            "url": "https://example.test/m",
            "headers": {"X-Client": "jarvis", "Authorization": "Bearer common"},
            "roles": {"agent": {"headers": {"Authorization": "Bearer agent"}}},
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                headers = topic_mcp.servers_for_role("agent")[0]["headers"]
        self.assertEqual(headers, {"X-Client": "jarvis", "Authorization": "Bearer agent"})

    def test_unknown_role_attaches_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path):
                with self.assertLogs("engines.topic_mcp", level="WARNING"):
                    self.assertEqual(topic_mcp.servers_for_role("nobody"), [])

    def test_multiple_servers_are_all_attached(self) -> None:
        data = {"servers": [
            {"name": "one", "url": "https://example.test/1"},
            {"name": "two", "url": "https://example.test/2"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                names = [s["name"] for s in topic_mcp.servers_for_role("agent")]
        self.assertEqual(names, ["one", "two"])

    def test_edited_config_is_picked_up_without_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, {"servers": [
                {"name": "before", "url": "https://example.test/1"},
            ]})
            with self._env(path):
                first = topic_mcp.servers_for_role("agent")
                self._write(tmp, {"servers": [
                    {"name": "after", "url": "https://example.test/2"},
                    {"name": "extra", "url": "https://example.test/3"},
                ]})
                second = topic_mcp.servers_for_role("agent")

        self.assertEqual([s["name"] for s in first], ["before"])
        self.assertEqual([s["name"] for s in second], ["after", "extra"])


class BadEntryTest(TopicMcpTestBase):
    """Одна плохая запись не должна уносить остальные."""

    def _one_bad_one_good(self, bad: dict) -> list[str]:
        data = {"servers": [bad, {"name": "good", "url": "https://example.test/ok"}]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                with self.assertLogs("engines.topic_mcp", level="WARNING"):
                    specs = topic_mcp.servers_for_role("agent")
        return [s["name"] for s in specs]

    def test_invalid_name_skipped(self) -> None:
        self.assertEqual(
            self._one_bad_one_good({"name": "bad name!", "url": "https://x.test"}),
            ["good"],
        )

    def test_missing_url_skipped(self) -> None:
        self.assertEqual(self._one_bad_one_good({"name": "nourl"}), ["good"])

    def test_unsupported_type_skipped(self) -> None:
        self.assertEqual(
            self._one_bad_one_good(
                {"name": "local", "type": "stdio", "command": "/bin/true"}
            ),
            ["good"],
        )

    def test_non_object_entry_skipped(self) -> None:
        self.assertEqual(self._one_bad_one_good("just a string"), ["good"])

    def test_duplicate_name_ignored(self) -> None:
        self.assertEqual(
            self._one_bad_one_good({"name": "good", "url": "https://example.test/dup"}),
            ["good"],
        )

    def test_enabled_false_skipped_silently(self) -> None:
        data = {"servers": [
            {"name": "off", "url": "https://example.test/1", "enabled": False},
            {"name": "on", "url": "https://example.test/2"},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            with self._env(path):
                names = [s["name"] for s in topic_mcp.servers_for_role("agent")]
        self.assertEqual(names, ["on"])


class EngineRenderingTest(TopicMcpTestBase):
    def test_claude_inline_config_carries_role_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path):
                flags = claude_mcp_flags(False, "agent")

        self.assertEqual(flags[0], "--mcp-config")
        servers = json.loads(flags[1])["mcpServers"]
        self.assertEqual(servers["mxboard"]["type"], "http")
        self.assertEqual(servers["mxboard"]["headers"]["Authorization"], "Bearer agent-token")

    def test_codex_uses_profile_and_keeps_token_out_of_argv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            codex_home = Path(tmp) / "codex-home"
            with self._env(path), patch.dict("os.environ", {"CODEX_HOME": str(codex_home)}):
                flags, cleanup_paths = codex_mcp_overrides(False, "manager")
                profile_text = cleanup_paths[0].read_text(encoding="utf-8")
                mode = cleanup_paths[0].stat().st_mode & 0o777

            for cleanup_path in cleanup_paths:
                cleanup_path.unlink(missing_ok=True)

        self.assertEqual(flags[0], "--profile")
        self.assertNotIn("manager-token", "\n".join(flags))
        self.assertEqual(mode, 0o600)
        self.assertIn("[mcp_servers.mxboard]", profile_text)
        self.assertIn("manager-token", profile_text)
        self.assertNotIn("agent-token", profile_text)

    def test_codex_profile_holds_every_server(self) -> None:
        data = {"servers": [
            {"name": "one", "url": "https://example.test/1",
             "headers": {"Authorization": "Bearer t1"}},
            {"name": "two", "url": "https://example.test/2",
             "headers": {"Authorization": "Bearer t2"}},
        ]}
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, data)
            codex_home = Path(tmp) / "codex-home"
            with self._env(path), patch.dict("os.environ", {"CODEX_HOME": str(codex_home)}):
                flags, cleanup_paths = codex_mcp_overrides(False, "agent")
                profile_text = cleanup_paths[0].read_text(encoding="utf-8")
            for cleanup_path in cleanup_paths:
                cleanup_path.unlink(missing_ok=True)

        self.assertEqual(flags.count("--profile"), 1)
        self.assertIn("[mcp_servers.one]", profile_text)
        self.assertIn("[mcp_servers.two]", profile_text)

    def test_opencode_temp_config_merges_into_base(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            base = Path(tmp) / "opencode.json"
            base.write_text(json.dumps({"mcp": {"jarvis": {"type": "local"}}}), encoding="utf-8")
            with self._env(path), patch("engines.playwright_mcp.OPENCODE_CONFIG", base):
                temp_path = _opencode_mcp_config(False, "agent")

            try:
                data = json.loads(Path(temp_path).read_text(encoding="utf-8"))
            finally:
                Path(temp_path).unlink(missing_ok=True)

        self.assertEqual(data["mcp"]["jarvis"]["type"], "local")
        self.assertEqual(data["mcp"]["mxboard"]["type"], "remote")
        self.assertEqual(data["mcp"]["mxboard"]["url"], "https://example.test/mcp.php")
        self.assertEqual(
            data["mcp"]["mxboard"]["headers"]["Authorization"], "Bearer agent-token",
        )

    def test_persistent_codex_uses_inline_config_not_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, CONFIG)
            with self._env(path):
                flags, cleanup_paths = persistent_codex_mcp_overrides(False, "manager")

        joined = "\n".join(flags)
        self.assertNotIn("--profile", flags)
        self.assertEqual(cleanup_paths, [])
        self.assertIn("mcp_servers.mxboard.url", joined)
        self.assertIn("mcp_servers.mxboard.http_headers", joined)
        self.assertIn("manager-token", joined)

    def test_codex_global_flags_move_before_subcommand(self) -> None:
        global_flags, command_flags = _split_codex_global_flags([
            "-c",
            "mcp_servers.playwright.enabled=true",
            "--profile",
            "jarvis-topic-mcp-test",
        ])

        self.assertEqual(global_flags, ["--profile", "jarvis-topic-mcp-test"])
        self.assertEqual(command_flags, ["-c", "mcp_servers.playwright.enabled=true"])


if __name__ == "__main__":
    unittest.main()
