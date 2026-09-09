"""Offline tests: no real credentials, API calls, installs or agent processes."""
import argparse
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import install
import preflight


def studio_codex_settings():
    return {
        "approval_policy": {"granular": {key: False for key in
            ("sandbox_approval", "rules", "mcp_elicitations", "request_permissions", "skill_approval")}},
        "thread_sandbox": "workspace-write",
        "turn_sandbox_policy": None,
        "permission_profile": preflight.PERMISSION_PROFILE,
        "command_sha256": preflight.hashlib.sha256(preflight.CANONICAL_COMMAND.encode()).hexdigest(),
    }


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
            safe_report = json.dumps({"preflight": "ok", "runtime": {"recorded_cleanup": True, "turn_interrupt": True}, "codex": studio_codex_settings()})
            with patch.object(install, "run", return_value=safe_report) as calls, patch.object(install, "schema_check"), patch.object(install, "sandbox_check"), contextlib.redirect_stdout(io.StringIO()) as output:
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

    def test_generated_schema_rejects_legacy_reject_and_accepts_granular_false(self):
        schema = json.loads((Path(__file__).parent / "fixtures/codex-0.153.4-policies.json").read_text())
        config = studio_codex_settings()
        preflight.validate_configured_policies(config, schema["ThreadStartParams"], schema["TurnStartParams"])
        legacy = copy.deepcopy(config)
        legacy["approval_policy"] = {"reject": {"sandbox_approval": True, "rules": True, "mcp_elicitations": True}}
        with self.assertRaisesRegex(preflight.CheckError, "approval_policy.*incompatible"):
            preflight.validate_configured_policies(legacy, schema["ThreadStartParams"], schema["TurnStartParams"])

    def test_generated_schema_rejects_bad_sandboxes_flags_and_model_overrides(self):
        schema = json.loads((Path(__file__).parent / "fixtures/codex-0.153.4-policies.json").read_text())
        invalid = []
        for key, value in [("thread_sandbox", "future-write"), ("turn_sandbox_policy", {"type": "workspaceWrite", "networkAccess": "true"}),
                           ("turn_sandbox_policy", {"type": "dangerFullAccess"}),
                           ("turn_sandbox_policy", {"type": "workspaceWrite", "writableRoots": ["/"]}),
                           ("permission_profile", "unreviewed_profile")]:
            config = studio_codex_settings()
            config[key] = value
            invalid.append(config)
        enabled = studio_codex_settings()
        enabled["approval_policy"]["granular"]["request_permissions"] = True
        invalid.append(enabled)
        numeric = studio_codex_settings()
        numeric["approval_policy"]["granular"]["rules"] = 0
        invalid.append(numeric)
        for config in invalid:
            with self.subTest(config=config), self.assertRaises(preflight.CheckError):
                preflight.validate_configured_policies(config, schema["ThreadStartParams"], schema["TurnStartParams"])

    def test_exact_command_contract_rejects_competing_or_unknown_flags(self):
        schema = json.loads((Path(__file__).parent / "fixtures/codex-0.153.4-policies.json").read_text())
        commands = [preflight.CANONICAL_COMMAND.replace('model_reasoning_effort="medium"', 'model_reasoning_effort="low"'),
                    preflight.CANONICAL_COMMAND.replace(" app-server", " --model other app-server"),
                    preflight.CANONICAL_COMMAND.replace(" app-server", " -c model='\"other\"' app-server"),
                    preflight.CANONICAL_COMMAND.replace(" app-server", " --profile unexpected app-server"),
                    preflight.CANONICAL_COMMAND + "; echo unexpected"]
        for command in commands:
            configured = studio_codex_settings()
            configured["command_sha256"] = preflight.hashlib.sha256(command.encode()).hexdigest()
            with self.subTest(command=command), self.assertRaisesRegex(preflight.CheckError, "exact reviewed"):
                preflight.validate_configured_policies(configured, schema["ThreadStartParams"], schema["TurnStartParams"])

    def test_sandbox_probe_requires_git_writes_and_protected_path_denials(self):
        expected = {"workspace": True, "git_index": True, "git_branch": True, "bun_install": True, "bun_environment": True,
                    ".codex_read": True, ".agents_read": True,
                    "codex_write": False, "agents_write": False, "outside_write": False}
        with patch.object(preflight, "run", side_effect=["", "", json.dumps(expected)]) as calls:
            preflight.sandbox_check("codex", {})
        probe_command = calls.call_args_list[2].args[0]
        self.assertEqual(probe_command[:4], ["codex", "sandbox", "-P", "symphony_studio"])
        self.assertIn(preflight.PERMISSION_CONFIG, probe_command)
        self.assertTrue(any(arg.startswith("shell_environment_policy.set=") for arg in probe_command))
        self.assertIn('"--frozen-lockfile"', probe_command[-3])
        self.assertIn('.git/symphony-runtime/bun-cache', probe_command[-3])
        self.assertNotIn("app-server", probe_command)
        self.assertEqual(Path(probe_command[5]).parent, Path.home())
        self.assertEqual(Path(probe_command[-2]).parent, Path.home())
        for key in ("bun_environment", "bun_install", "git_branch", "codex_write", "agents_write", "outside_write"):
            incorrect = dict(expected, **{key: not expected[key]})
            with self.subTest(key=key), patch.object(preflight, "run", side_effect=["", "", json.dumps(incorrect)]), self.assertRaisesRegex(preflight.CheckError, "sandbox probe failed"):
                preflight.sandbox_check("codex", {})

    def test_malformed_schema_objects_fail_with_a_clear_error(self):
        for schema in (True, False, None, []):
            with self.subTest(schema=schema), self.assertRaisesRegex(preflight.CheckError, "schema must be an object"):
                preflight.schema_valid(None, schema, {})

    def test_policy_validation_fails_closed_on_missing_runtime_report(self):
        with self.assertRaisesRegex(preflight.CheckError, "lacks Codex policy details"):
            preflight.validate_configured_policies(None, {}, {})

    def test_workflow_allows_only_disposable_database_migration_proofs(self):
        text = (Path(__file__).resolve().parents[2] / "workflows/studio.md").read_text()
        prompt = " ".join(text.split("---", 2)[2].split())
        self.assertIn("run migrations against hosted or shared databases", prompt)
        self.assertIn("disposable, isolated test database with no existing data", prompt)
        self.assertIn("run migrations and database proof tests against it", prompt)
        self.assertIn("worker-owned test resources and synthetic fixtures", prompt)
        self.assertIn("never use hosted/shared databases, copied user data or hosted credentials", prompt)
        self.assertIn("Tear down only the disposable resources created for that proof", prompt)
        self.assertNotIn("run database migrations,", prompt)

    def test_workflow_preserves_staging_limits_and_existing_statuses(self):
        text = (Path(__file__).resolve().parents[2] / "workflows/studio.md").read_text()
        config, prompt = text.split("---", 2)[1:]
        for expected in ("project_slug: '89a635a4f38d'", "required_labels: ['symphony-studio']", "max_concurrent_agents: 1", "max_turns: 8", "max_tokens_per_issue: 250000", "max_dispatch_attempts: 3", "max_rework_cycles: 2", "interval_ms: 60000", "bun install --frozen-lockfile", "refs/remotes/origin/staging"):
            self.assertIn(expected, config)
        command = config.split("  command: >-\n    ", 1)[1].split("\n", 1)[0]
        self.assertEqual(command, preflight.CANONICAL_COMMAND)
        self.assertIn('model_reasoning_effort="medium"', command)
        self.assertIn('model="gpt-6-astra"', command)
        self.assertIn("permission_profile: symphony_studio", config)
        self.assertNotIn("turn_sandbox_policy:", config)
        self.assertIn(preflight.ENVIRONMENT_COMMAND, config)
        self.assertIn('$PWD/.git/symphony-runtime/bun-cache', preflight.ENVIRONMENT_COMMAND)
        self.assertIn('$PWD/.git/symphony-runtime/tmp', preflight.ENVIRONMENT_COMMAND)
        self.assertNotIn("origin main", config)
        self.assertNotIn("Ready for Agent", config)
        self.assertIn("Do not approve, merge", prompt.replace("\n", " "))


if __name__ == "__main__":
    unittest.main()
