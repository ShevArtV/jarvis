import os
import subprocess
import unittest
from unittest.mock import mock_open, patch

from engines import cli_scope

SERVICE_CG = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/jarvis-bot.service\n"


class CliScopeTest(unittest.TestCase):
    def setUp(self):
        cli_scope.scope_prefix.cache_clear()
        self.addCleanup(cli_scope.scope_prefix.cache_clear)
        env = patch.dict(os.environ, {"JARVIS_CLI_SCOPE": "1"})
        env.start()
        self.addCleanup(env.stop)

    def patched(self, cgroup=SERVICE_CG, rc=0, which="/usr/bin/systemd-run", platform="linux"):
        probe = subprocess.CompletedProcess([], rc, b"", b"boom")
        return (patch("builtins.open", mock_open(read_data=cgroup)),
                patch.object(cli_scope.shutil, "which", return_value=which),
                patch.object(cli_scope.subprocess, "run", return_value=probe),
                patch.object(cli_scope.sys, "platform", platform))

    def wrap(self, **kw):
        a, b, c, d = self.patched(**kw)
        with a, b, c as run, d:
            return cli_scope.wrap(["/bin/claude", "-p", "$HOME"]), run

    def test_wraps_in_scope_part_of_service(self):
        argv, run = self.wrap()
        self.assertEqual(argv[:6], ["/usr/bin/systemd-run", "--user", "--scope", "--quiet", "--collect",
                                    "--expand-environment=no"])
        self.assertIn("--property=PartOf=jarvis-bot.service", argv)
        self.assertIn("--property=MemoryMax=4G", argv)
        self.assertTrue(any(a.startswith("--unit=jarvis-cli-") for a in argv))
        self.assertEqual(argv[-3:], ["/bin/claude", "-p", "$HOME"])
        self.assertEqual(run.call_args.args[0][-1], "true")

    def test_plain_when_unavailable(self):
        for kw in ({"cgroup": "0::/user.slice/user-1000.slice/session-2.scope\n"}, {"which": None},
                   {"rc": 1}, {"platform": "darwin"}):
            cli_scope.scope_prefix.cache_clear()
            argv, _ = self.wrap(**kw)
            self.assertEqual(argv, ["/bin/claude", "-p", "$HOME"], kw)

    def test_env_switch_off(self):
        with patch.dict(os.environ, {"JARVIS_CLI_SCOPE": "0"}):
            argv, run = self.wrap()
        self.assertEqual(argv, ["/bin/claude", "-p", "$HOME"])
        run.assert_not_called()

    def test_probe_once(self):
        a, b, c, d = self.patched()
        with a, b, c as run, d:
            cli_scope.wrap(["x"])
            cli_scope.wrap(["y"])
        self.assertEqual(run.call_count, 1)
