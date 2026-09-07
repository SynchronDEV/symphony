"""Offline tests: no real credentials, API calls, installs or agent processes."""
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import install
import preflight


class StudioOperationsTest(unittest.TestCase):
    def test_install_is_idempotent_backs_up_and_pins_explicit_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "operator"
            launcher = home / ".local/bin/spektra-symphony"
            workflow = home / ".config/symphony/spektra-workflow.md"
            launcher.parent.mkdir(parents=True)
            workflow.parent.mkdir(parents=True)
            launcher.write_text("old launcher\n")
            workflow.write_text("old workflow\n")
            binary, escript, codex = [root / name for name in ("symphony", "escript", "codex")]
            for path in (binary, escript, codex):
                path.write_text("test executable\n")
                path.chmod(0o755)
            args = argparse.Namespace(destination_home=str(home), runtime_binary=str(binary),
                escript=str(escript), codex=str(codex), source_repo=str(root / "source"))
            safe_report = json.dumps({"preflight": "ok", "runtime": {"recorded_cleanup": True, "turn_interrupt": True}})
            with patch.object(install, "run", return_value=safe_report) as calls, patch.object(install, "schema_check"), contextlib.redirect_stdout(io.StringIO()) as output:
                install.install(args)
                first = json.loads(output.getvalue())
                output.seek(0)
                output.truncate(0)
                install.install(args)
                second = json.loads(output.getvalue())
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual(first["runtime"], second["runtime"])
            backup = Path(first["backup"])
            self.assertEqual((backup / launcher.name).read_text(), "old launcher\n")
            self.assertEqual((backup / workflow.name).read_text(), "old workflow\n")
            self.assertIn(str(Path(first["runtime"]) / "launch.py"), launcher.read_text())
            self.assertNotIn("exec symphony", launcher.read_text())
            self.assertTrue(all("--preflight" in call.args[0] for call in calls.call_args_list))
            self.assertFalse(first["agents_started"])

    def test_invalid_binary_check_leaves_existing_entrypoints_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "missing"
            args = argparse.Namespace(destination_home=directory, runtime_binary=str(binary), escript="/bin/sh", codex="/bin/sh", source_repo=directory)
            with self.assertRaises(preflight.CheckError):
                install.install(args)
            self.assertFalse((root / ".local").exists())

    def test_explicit_credentials_override_inherited_values_without_shell_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".config/symphony/studio.env"
            env_file.parent.mkdir(parents=True)
            env_file.write_text("LINEAR_API_KEY='file-secret'\nGH_TOKEN='$(touch unsafe)'\n")
            with patch.dict(os.environ, {"LINEAR_API_KEY": "inherited-secret"}, clear=True):
                values = preflight.credentials(directory)
            self.assertEqual(values["LINEAR_API_KEY"], "file-secret")
            self.assertEqual(values["GH_TOKEN"], "$(touch unsafe)")
            self.assertFalse(Path("unsafe").exists())

    def test_repository_default_main_can_target_existing_staging(self):
        repository = json.dumps({"full_name": "spektra-org/spektra", "default_branch": "main", "permissions": {"push": True}})
        with patch.object(preflight, "run", side_effect=[repository, "a" * 40]) as calls:
            metadata, commit = preflight.github_check({})
        self.assertEqual(metadata["default_branch"], "main")
        self.assertEqual(commit, "a" * 40)
        self.assertIn("repos/spektra-org/spektra/branches/staging", calls.call_args_list[1].args[0])
        with patch.object(preflight, "run", side_effect=[repository, preflight.CheckError("Missing staging")]), self.assertRaises(preflight.CheckError):
            preflight.github_check({})

    def test_inherited_credentials_work_without_an_explicit_file(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"LINEAR_API_KEY": "inherited-secret"}, clear=True):
            self.assertEqual(preflight.credentials(directory)["LINEAR_API_KEY"], "inherited-secret")

    def test_failed_command_output_is_never_exposed(self):
        with self.assertRaises(preflight.CheckError) as error:
            preflight.run(["/bin/sh", "-c", "echo very-secret-token >&2; exit 1"])
        self.assertNotIn("very-secret-token", str(error.exception))

    def test_schema_checks_require_active_turn_ids(self):
        def fake_generation(command, **_kwargs):
            directory = Path(command[-1]) / "v2"
            directory.mkdir()
            (directory / "TurnInterruptParams.json").write_text(json.dumps({"required": ["threadId"]}))
            return ""
        with patch.object(preflight, "run", side_effect=fake_generation), self.assertRaises(preflight.CheckError):
            preflight.schema_check("codex", {})

    def test_workflow_preserves_staging_limits_and_existing_statuses(self):
        text = (Path(__file__).resolve().parents[2] / "workflows/studio.md").read_text()
        config, prompt = text.split("---", 2)[1:]
        for expected in ("project_slug: '89a635a4f38d'", "required_labels: ['symphony-studio']", "max_concurrent_agents: 1", "max_turns: 8", "max_tokens_per_issue: 250000", "max_dispatch_attempts: 3", "max_rework_cycles: 2", "interval_ms: 60000", "bun install --frozen-lockfile", "refs/remotes/origin/staging"):
            self.assertIn(expected, config)
        self.assertNotIn("origin main", config)
        self.assertNotIn("Ready for Agent", config)
        self.assertIn("Do not approve, merge", prompt.replace("\n", " "))


if __name__ == "__main__":
    unittest.main()
