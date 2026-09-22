from __future__ import annotations

import unittest

from engines.claude_engine import _accumulate_assistant_event, _tool_step

CWD = "/home/u/projects/jarvis"


class ClaudeJournalStepsTest(unittest.TestCase):
    def test_thinking_is_skipped(self) -> None:
        buf: list[str] = []
        _accumulate_assistant_event({"message": {"content": [
            {"type": "thinking", "thinking": ""},
            {"type": "text", "text": "Смотрю код"},
        ]}}, buf, CWD)
        self.assertEqual(buf, ["Смотрю код"])

    def test_bash_prefers_description(self) -> None:
        self.assertEqual(
            _tool_step("Bash", {"command": "cd x && sed -n 1,2p f", "description": "Read file head"}),
            "💻 Read file head",
        )
        self.assertEqual(_tool_step("Bash", {"command": "ls -la"}), "💻 ls -la")

    def test_file_paths_are_relative_to_cwd(self) -> None:
        self.assertEqual(
            _tool_step("Read", {"file_path": f"{CWD}/bot/delivery.py"}, CWD),
            "📖 bot/delivery.py",
        )
        self.assertEqual(
            _tool_step("Edit", {"file_path": "/etc/hosts"}, CWD), "✏️ /etc/hosts",
        )

    def test_mcp_tools(self) -> None:
        self.assertEqual(
            _tool_step("mcp__jarvis__manager_inbox", {"chat_id": -1, "thread_id": 2498}),
            "📨 jarvis · manager_inbox (thread 2498)",
        )
        self.assertEqual(
            _tool_step("mcp__jarvis__ask_user", {"question": "Какой\nвывод?", "thread_id": 1}),
            "❓ Спрашиваю: «Какой вывод?»",
        )
        self.assertEqual(
            _tool_step("mcp__codegraph__codegraph_explore", {"query": "ProgressJournal"}),
            "🔎 codegraph · codegraph_explore: ProgressJournal",
        )
        self.assertEqual(_tool_step("mcp__mxboard__board_list", {}), "🔌 mxboard · board_list")

    def test_unknown_tool_keeps_name(self) -> None:
        self.assertEqual(_tool_step("ToolSearch", {"query": "x"}), "🔧 ToolSearch")
        self.assertEqual(_tool_step("Foo", {"path": "a"}), "🔧 Foo a")


if __name__ == "__main__":
    unittest.main()
