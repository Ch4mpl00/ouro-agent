"""Deployment safety regressions; runs without Docker, SSH or production data."""
import argparse
import importlib.util
import io
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

import deploy

spec = importlib.util.spec_from_file_location("probe", Path(__file__).resolve().parent.parent / "deploy/probe.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Selection(unittest.TestCase):
    def test_default_command_and_remote_apply_accept_no_service_arguments(self):
        self.assertEqual(deploy.arguments([]).services, [])
        self.assertEqual(deploy.arguments(["--apply", "/tmp/payload"]).apply, Path("/tmp/payload"))

    def test_tests_docs_and_specs_never_deploy(self):
        paths = ["README.md", "CLAUDE.md", ".claude/tasks/native-deploy.md", "scripts/test.py",
                 "crates/mcp/tests/http_transport.rs", "crates/agent/tests/supervisor.rs",
                 "crates/mcp/specs/signal_queue.feature"]
        result = deploy.plan(paths)
        self.assertEqual(result["binaries"], [])
        self.assertEqual(result["services"], [])
        self.assertFalse(result["infrastructure"])

    def test_agent_change_does_not_restart_mcp_or_build_codex(self):
        result = deploy.plan(["crates/agent/src/agent.rs"])
        self.assertEqual(result["binaries"], ["agent-supervisor"])
        self.assertEqual(result["services"], ["agent"])

    def test_mcp_binary_switches_both_instances_and_revalidates_agent(self):
        result = deploy.plan(["crates/mcp/src/server.rs"])
        self.assertEqual(result["binaries"], ["mcp-tools"])
        self.assertEqual(set(result["services"]), deploy.NATIVE)

    def test_live_configuration_and_default_skills_do_not_compile(self):
        gateway = deploy.plan(["crates/mcp/gateway.config.json"])
        skills = deploy.plan(["skills.default/telegram.md"])
        self.assertEqual(gateway["binaries"], [])
        self.assertEqual(set(gateway["services"]), {"mcp", "agent"})
        self.assertEqual(skills["binaries"], [])
        self.assertEqual(set(skills["services"]), deploy.NATIVE)

    def test_bootstrap_has_all_executables_even_without_a_diff(self):
        result = deploy.plan([], bootstrap=True)
        self.assertEqual(set(result["binaries"]), set(deploy.BINARIES.values()))
        self.assertTrue(result["infrastructure"])

    def test_manual_mcp_deploy_never_leaves_old_tunnel_binary_running(self):
        result = deploy.plan([], requested=["mcp"])
        self.assertEqual(set(result["services"]), deploy.NATIVE)

    def test_noop_does_not_build_upload_or_restart(self):
        root = Path(__file__).resolve().parent.parent
        def git_output(command, **kwargs):
            if command[1:3] == ["rev-parse", "--show-toplevel"]:
                return str(root)
            if command[1:3] == ["rev-parse", "HEAD"]:
                return "new"
            if command[1:3] == ["branch", "-r"]:
                return "origin/main"
            if command[1:3] == ["diff", "--name-only"]:
                return "README.md\ncrates/mcp/tests/http_transport.rs"
            self.fail(str(command))
        args = argparse.Namespace(host="unused", repository="unused", services=[], runtime_image=False)
        with patch.object(deploy, "output", side_effect=git_output), \
             patch.object(deploy, "remote_info", return_value={"revision": "old", "hashes": {}}), \
             patch.object(deploy, "run") as commands, patch.object(deploy, "ssh") as remote:
            deploy.local(args)
            self.assertTrue(all(command.args[0][0] == "git" for command in commands.call_args_list))
            remote.assert_not_called()


class Configuration(unittest.TestCase):
    def test_payload_extracts_through_a_symlinked_temporary_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            actual = root / "actual"
            actual.mkdir()
            alias = root / "alias"
            alias.symlink_to(actual, target_is_directory=True)
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                entry = tarfile.TarInfo("crates/mcp/gateway.config.json")
                entry.size = 2
                archive.addfile(entry, io.BytesIO(b"{}"))
            deploy.extract_archive(buffer.getvalue(), alias)
            self.assertEqual((actual / "crates/mcp/gateway.config.json").read_text(), "{}")

    def test_credentials_are_taken_from_effective_container_env_without_shell_evaluation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env.agent").write_text('API_KEY="source-value"\nMODEL=example\n# comment\n')
            actual = {"Config": {"Env": ["API_KEY=actual=$`value", "MODEL=deployed", "PATH=/bin"]}}
            result = deploy.credential_environment(root, ".env.agent", actual)
            self.assertEqual(result, {"API_KEY": "actual=$`value", "MODEL": "deployed"})

    def test_environment_file_protects_credentials_and_escapes_quotes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "agent.env"
            deploy.write_env(path, {"KEY": 'x"y\\z$literal'})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_text(), 'KEY="x\\"y\\\\z$literal"\n')

    def test_probe_requires_mcp_protocol_rejection_without_creating_a_session(self):
        import urllib.error
        for status, expected in [(422, True), (400, False), (403, False), (404, False), (500, False)]:
            with self.subTest(status=status), patch.object(probe.urllib.request, "urlopen",
                    side_effect=urllib.error.HTTPError("url", status, "", {}, None)) as request:
                self.assertEqual(probe.ready("mcp"), expected)
                self.assertEqual(request.call_args.args[0].method, "POST")
                self.assertEqual(request.call_args.args[0].data, b'{"jsonrpc":"2.0","id":0,"method":"ping"}')
                self.assertEqual(request.call_args.args[0].get_header("Content-type"), "application/json")


if __name__ == "__main__":
    unittest.main()
