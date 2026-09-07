#!/usr/bin/env python3
"""Read-only Studio checks. No Symphony supervisor, hooks or Codex turns start here."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request


PERMISSION_PROFILE = "symphony_studio"
PERMISSION_CONFIG = 'permissions={symphony_studio={extends=":workspace",filesystem={":workspace_roots"={".git"="write",".codex"="read",".agents"="read"}},network={enabled=true}}}'
COMMAND_CONFIG = ['default_permissions="symphony_studio"', PERMISSION_CONFIG,
                  'model="gpt-6-astra"', 'model_reasoning_effort="low"']
ENVIRONMENT_COMMAND = r'-c "shell_environment_policy.set={BUN_INSTALL_CACHE_DIR=\"$PWD/.git/symphony-runtime/bun-cache\",TMPDIR=\"$PWD/.git/symphony-runtime/tmp\"}"'
CANONICAL_COMMAND = '"$SYMPHONY_STUDIO_CODEX_BIN" ' + " ".join("-c " + shlex.quote(value) for value in COMMAND_CONFIG) + " " + ENVIRONMENT_COMMAND + " app-server"


class CheckError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise CheckError(message)


def run(command, *, env=None, cwd=None, timeout=30):
    try:
        result = subprocess.run(command, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        raise CheckError(f"Could not complete {Path(command[0]).name} check") from None
    require(result.returncode == 0, f"{Path(command[0]).name} check failed; output suppressed to protect credentials")
    return result.stdout.strip()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def credentials(home):
    env = os.environ.copy()
    env_file = Path(home) / ".config/symphony/studio.env"
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            key, separator, value = line.partition("=")
            require(separator and key in {"LINEAR_API_KEY", "GH_TOKEN", "GITHUB_TOKEN", "OPENAI_API_KEY"},
                    "studio.env accepts only LINEAR_API_KEY, GH_TOKEN, GITHUB_TOKEN, OPENAI_API_KEY assignments")
            try:
                parsed = shlex.split(value, comments=True)
            except ValueError:
                raise CheckError("Invalid studio.env assignment") from None
            require(len(parsed) == 1, "Invalid studio.env value")
            env[key] = parsed[0]
    return env


def linear_query(query, variables, env):
    token = env.get("LINEAR_API_KEY", "")
    require(bool(token), "LINEAR_API_KEY is missing; inherited credentials or studio.env are supported")
    request = urllib.request.Request("https://api.linear.app/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Authorization": token, "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
    except (urllib.error.URLError, ValueError):
        raise CheckError("Linear read-only credential/query check failed; response suppressed") from None
    require(not payload.get("errors") and isinstance(payload.get("data"), dict), "Linear query rejected; no mutation was attempted")
    return payload["data"]


def tracker_check(settings, env):
    metadata = linear_query('''query StudioPreflight($slug: String!) {
      organization { urlKey }
      projects(filter: {slugId: {eq: $slug}}, first: 2) { nodes { id name slugId } }
      teams(filter: {key: {eq: "SPK"}}, first: 2) { nodes { key states { nodes { name } } } }
    }''', {"slug": settings["tracker"]["project_slug"]}, env)
    require(metadata["organization"]["urlKey"] == "ante-digital", "Wrong Linear organization")
    projects = metadata["projects"]["nodes"]
    require(len(projects) == 1 and projects[0]["id"] == "1e5eddc5-255a-4d9d-b238-02ab99c2ccd9", "Wrong or inaccessible Studio Linear project")
    teams = metadata["teams"]["nodes"]
    require(len(teams) == 1, "SPEKTRA team is inaccessible")
    states = {state["name"] for state in teams[0]["states"]["nodes"]}
    require({"Todo", "In Progress", "In Review", "Done", "Canceled", "Duplicate", "Backlog"} <= states,
            "Live SPEKTRA statuses do not match the pilot workflow")
    candidates, cursor = [], None
    while True:
        data = linear_query('''query StudioCandidates($slug: String!, $states: [String!]!, $after: String) {
          issues(filter: {project: {slugId: {eq: $slug}}, state: {name: {in: $states}}}, first: 100, after: $after) {
            nodes { identifier state { name } labels { nodes { name } }
              inverseRelations(first: 50) { nodes { type issue { state { name } } } pageInfo { hasNextPage } }
            }
            pageInfo { hasNextPage endCursor }
          }
        }''', {"slug": settings["tracker"]["project_slug"], "states": settings["tracker"]["active_states"], "after": cursor}, env)
        connection = data["issues"]
        for issue in connection["nodes"]:
            labels = {label["name"].strip().lower() for label in issue["labels"]["nodes"]}
            if "symphony-studio" not in labels or labels & {"symphony-hold", "symphony-stuck"}:
                continue
            relations = issue["inverseRelations"]
            blocked = relations["pageInfo"]["hasNextPage"] or any(
                relation["type"] == "blocks" and relation["issue"]["state"]["name"] not in settings["tracker"]["terminal_states"]
                for relation in relations["nodes"])
            candidates.append({"identifier": issue["identifier"], "state": issue["state"]["name"], "blocked": blocked})
        if not connection["pageInfo"]["hasNextPage"]:
            break
        next_cursor = connection["pageInfo"]["endCursor"]
        require(next_cursor and next_cursor != cursor, "Linear pagination did not advance")
        cursor = next_cursor
    return candidates


def schema_valid(value, schema, document, depth=0):
    """Validate the generated policy-schema subset; unknown constraints fail closed."""
    require(isinstance(schema, dict) and isinstance(document, dict), "Codex policy schema must be an object")
    require(depth < 64, "Codex policy schema recursion limit exceeded")
    supported = {"$ref", "title", "description", "default", "type", "enum", "const", "oneOf", "anyOf", "allOf",
                 "properties", "required", "additionalProperties", "items", "pattern", "minLength", "maxLength"}
    require(not set(schema) - supported, "Codex policy schema contains an unsupported validation constraint")
    if "$ref" in schema:
        reference = schema["$ref"]
        require(reference.startswith("#/definitions/"), "Codex policy schema contains an unsupported reference")
        target = document["definitions"][reference.split("/")[-1]]
        if not schema_valid(value, target, document, depth + 1):
            return False
    for keyword in ("oneOf", "anyOf", "allOf"):
        if keyword in schema:
            matches = [schema_valid(value, option, document, depth + 1) for option in schema[keyword]]
            if (keyword == "oneOf" and sum(matches) != 1) or (keyword == "anyOf" and not any(matches)) or (keyword == "allOf" and not all(matches)):
                return False
    kind = schema.get("type")
    kinds = kind if isinstance(kind, list) else [kind]
    types = {"null": value is None, "object": isinstance(value, dict), "array": isinstance(value, list),
             "string": isinstance(value, str), "boolean": type(value) is bool,
             "integer": type(value) is int, "number": type(value) in (int, float)}
    if kind is not None and not any(types.get(candidate, False) for candidate in kinds):
        return False
    if "enum" in schema and value not in schema["enum"]:
        return False
    if "const" in schema and value != schema["const"]:
        return False
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        if not set(schema.get("required", [])) <= value.keys():
            return False
        unknown = value.keys() - properties.keys()
        additional = schema.get("additionalProperties", True)
        if unknown and additional is False:
            return False
        if isinstance(additional, dict) and not all(schema_valid(value[key], additional, document, depth + 1) for key in unknown):
            return False
        if not all(schema_valid(value[key], properties[key], document, depth + 1) for key in value.keys() & properties.keys()):
            return False
    if isinstance(value, list) and "items" in schema:
        return all(schema_valid(item, schema["items"], document, depth + 1) for item in value)
    if isinstance(value, str):
        if "pattern" in schema and not re.search(schema["pattern"], value):
            return False
        if len(value) < schema.get("minLength", 0) or len(value) > schema.get("maxLength", float("inf")):
            return False
    return True


def validate_configured_policies(configured, thread_schema, turn_schema):
    require(isinstance(configured, dict), "Runtime preflight lacks Codex policy details; rebuild and reinstall")
    # Profile mode deliberately omits the two legacy sandbox fields on the wire.
    checks = [(thread_schema, "approvalPolicy", "approval_policy"), (turn_schema, "approvalPolicy", "approval_policy")]
    for document, property_name, setting in checks:
        require(setting in configured and schema_valid(configured[setting], document["properties"][property_name], document),
                f"Configured codex.{setting} is incompatible with the installed Codex schema")
    categories = {"sandbox_approval", "rules", "mcp_elicitations", "request_permissions", "skill_approval"}
    require(configured["approval_policy"] == {"granular": dict.fromkeys(categories, False)},
            "Studio requires explicit granular approval flags set to false for all five categories")
    require(configured.get("permission_profile") == PERMISSION_PROFILE,
            "Studio requires its reviewed named permission profile")
    # These are Config's legacy defaults, not wire overrides. Reject any explicit
    # broadened turn policy, even though profile mode should ignore it.
    require(configured.get("thread_sandbox") == "workspace-write" and configured.get("turn_sandbox_policy") is None,
            "Studio profile mode forbids legacy sandbox overrides and additional writable roots")
    expected = hashlib.sha256(CANONICAL_COMMAND.encode()).hexdigest()
    require(configured.get("command_sha256") == expected,
            "Studio command differs from the exact reviewed profile/model command; competing or unknown flags are forbidden")


def sandbox_check(codex, env):
    """Exercise filesystem permissions in disposable paths, without a model turn."""
    probe = r'''import json, os, pathlib, subprocess, sys
root = pathlib.Path.cwd(); outside = pathlib.Path(sys.argv[1]); result = {}
for name, path in [("workspace", root / "write-check"), ("git_index", root / ".git/index.lock"),
                   ("codex_write", root / ".codex/forbidden"), ("agents_write", root / ".agents/forbidden"),
                   ("outside_write", outside / "forbidden")]:
    try:
        path.write_text("probe"); path.unlink(); result[name] = True
    except OSError:
        result[name] = False
for name in (".codex", ".agents"):
    try:
        result[name + "_read"] = (root / name / "read-check").read_text() == "readable"
    except OSError:
        result[name + "_read"] = False
result["git_branch"] = subprocess.run(["git", "switch", "-c", "codex/permission-probe"], capture_output=True).returncode == 0
result["bun_environment"] = (os.environ.get("TMPDIR") == str(root / ".git/symphony-runtime/tmp") and
    os.environ.get("BUN_INSTALL_CACHE_DIR") == str(root / ".git/symphony-runtime/bun-cache"))
installed = subprocess.run([sys.argv[2], "install", "--frozen-lockfile", "--ignore-scripts"], capture_output=True)
result["bun_install"] = installed.returncode == 0 and (root / "node_modules/probe-local/index.js").read_text() == "export default 42;\n"
print(json.dumps(result))
'''
    with tempfile.TemporaryDirectory(prefix=".symphony-studio-sandbox-", dir=Path.home()) as workspace, \
         tempfile.TemporaryDirectory(prefix=".symphony-studio-outside-", dir=Path.home()) as outside:
        bun = shutil.which("bun", path=env.get("PATH"))
        require(bun, "bun is missing")
        root = Path(workspace)
        run(["git", "init", "-q", workspace], env=env)
        for name in (".git/symphony-runtime/tmp", ".git/symphony-runtime/bun-cache"):
            (root / name).mkdir(parents=True)
        package = {"name": "probe-local", "version": "1.0.0"}
        with tarfile.open(root / "probe-local.tgz", "w:gz") as archive:
            for name, data in [("package/package.json", json.dumps(package).encode()),
                               ("package/index.js", b"export default 42;\n")]:
                entry = tarfile.TarInfo(name); entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
        (root / "package.json").write_text(json.dumps({"name": "studio-permission-probe", "private": True,
            "dependencies": {"probe-local": "file:./probe-local.tgz"}}))
        bun_env = dict(env, TMPDIR=str(root / ".git/symphony-runtime/tmp"), BUN_INSTALL_CACHE_DIR=str(root / ".git/symphony-runtime/bun-cache"))
        run([bun, "install", "--lockfile-only", "--ignore-scripts"], env=bun_env, cwd=workspace)
        shutil.rmtree(root / ".git/symphony-runtime/bun-cache")
        (root / ".git/symphony-runtime/bun-cache").mkdir()
        for name in (".codex", ".agents"):
            path = Path(workspace) / name
            path.mkdir()
            (path / "read-check").write_text("readable")
        tool_environment = 'shell_environment_policy.set={BUN_INSTALL_CACHE_DIR=' + json.dumps(str(root / ".git/symphony-runtime/bun-cache")) + ',TMPDIR=' + json.dumps(str(root / ".git/symphony-runtime/tmp")) + '}'
        output = run([codex, "sandbox", "-P", PERMISSION_PROFILE, "-C", workspace, "-c", PERMISSION_CONFIG, "-c", tool_environment,
                      "--", sys.executable, "-c", probe, outside, bun], env=env, timeout=20)
        expected = {"workspace": True, "git_index": True, "git_branch": True, "bun_install": True, "bun_environment": True,
                    ".codex_read": True, ".agents_read": True,
                    "codex_write": False, "agents_write": False, "outside_write": False}
        require(json.loads(output) == expected, "Studio sandbox probe failed: Bun installation, Git writes or protected-path boundaries differ from the reviewed profile")


def schema_check(codex, env, configured=None):
    with tempfile.TemporaryDirectory(prefix="symphony-studio-schema-") as directory:
        run([codex, "app-server", "generate-json-schema", "--experimental", "--out", directory], env=env)
        root = Path(directory) / "v2"
        interrupt = json.loads((root / "TurnInterruptParams.json").read_text())
        require({"threadId", "turnId"} <= set(interrupt["required"]), "Codex interrupt schema incompatible")
        errors = json.loads((root / "ErrorNotification.json").read_text())
        require("willRetry" in errors["properties"], "Codex retryable error schema incompatible")
        completion = json.loads((root / "TurnCompletedNotification.json").read_text())
        require({"threadId", "turn"} <= set(completion["required"]), "Codex completion schema incompatible")
        turn = completion["definitions"]["Turn"]
        require({"id", "status", "error"} <= set(turn["properties"]), "Codex terminal status schema incompatible")
        thread_schema = json.loads((root / "ThreadStartParams.json").read_text())
        turn_schema = json.loads((root / "TurnStartParams.json").read_text())
        validate_configured_policies(configured, thread_schema, turn_schema)
        response = json.loads((root / "ThreadStartResponse.json").read_text())
        require("activePermissionProfile" in response["properties"], "Codex cannot report the selected permission profile")


def github_check(env):
    github = json.loads(run(["gh", "api", "repos/spektra-org/spektra", "--jq", "{full_name, default_branch, permissions}"], env=env))
    require(github["full_name"] == "spektra-org/spektra", "GitHub repository identity mismatch")
    require(github.get("permissions", {}).get("push") is True, "GitHub account lacks permission to push issue branches")
    staging_sha = run(["gh", "api", "repos/spektra-org/spektra/branches/staging", "--jq", ".commit.sha"], env=env)
    require(bool(re.fullmatch(r"[0-9a-f]{40}", staging_sha)), "Staging branch returned an invalid commit")
    return github, staging_sha


def check(runtime, *, full=False, start=False):
    runtime = Path(runtime).resolve()
    manifest = json.loads((runtime / "manifest.json").read_text())
    env = credentials(manifest["home"])
    env["SYMPHONY_STUDIO_CODEX_BIN"] = manifest["codex"]
    for relative, expected in manifest["files"].items():
        require(digest(runtime / relative) == expected, f"Installed runtime changed: {relative}")
    workflow = Path(manifest["workflow"])
    require(digest(workflow) == manifest["workflow_sha256"], "Installed workflow differs from its reviewed runtime; reinstall the intended version")
    report = json.loads(run([manifest["escript"], str(runtime / "symphony"), "--preflight", str(workflow)], env=env, cwd=runtime))
    require(report.get("preflight") == "ok", "Installed runtime preflight failed")
    require(all(report["runtime"][key] for key in ("recorded_cleanup", "turn_interrupt")), "Installed runtime lacks readiness fixes")
    require(report["tracker"]["project_slug"] == "89a635a4f38d" and report["tracker"]["required_labels"] == ["symphony-studio"], "Pilot opt-in routing changed")
    require(report["workspace_root"] == str(Path(manifest["home"]) / "code/spektra-symphony-workspaces"), "Unexpected workspace root")
    require(report["cleanup_base_ref"] == "refs/remotes/origin/staging", "Unexpected cleanup merge base")
    expected = {"max_concurrent_agents": 1, "max_turns": 8, "max_tokens_per_issue": 250000, "max_dispatch_attempts": 3, "max_rework_cycles": 2}
    require(all(report["agent"].get(key) == value for key, value in expected.items()), "Pilot resource limits changed")
    require(report["polling_interval_ms"] == 60000, "Unexpected polling interval")
    require(tuple(map(int, report["runtime"]["elixir"].split(".")[:2])) >= (1, 19) and int(report["runtime"]["otp"]) >= 28, "Elixir 1.19 / OTP 28 or newer is required")
    require(set(report["tracker"]["active_states"]) == {"Todo", "In Progress", "In Review"}, "Pilot role routing changed")
    require({"symphony-hold", "symphony-stuck"} <= set(report["agent"]["stop_continue_labels"]), "Pilot stop labels changed")
    version_text = run([manifest["codex"], "--version"], env=env)
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_text)
    require(match and tuple(map(int, match.groups())) >= (0, 153, 4), "Codex 0.153.4 or newer is required")
    version = match.group(0)
    # Validate the actual selected policies every time, including when the binary
    # hash matches. Version/field-presence checks alone cannot detect bad variants.
    schema_check(manifest["codex"], env, report.get("codex"))
    sandbox_check(manifest["codex"], env)
    run([manifest["codex"], "login", "status"], env=env)
    for executable in ("git", "bun", "gh"):
        require(shutil.which(executable, path=env.get("PATH")), f"{executable} is missing")
    repo = Path(manifest["source_repo"])
    origin = run(["git", "-C", str(repo), "remote", "get-url", "origin"], env=env)
    require(origin in {"https://github.com/spektra-org/spektra.git", "git@github.com:spektra-org/spektra.git"}, "Source checkout has unexpected origin")
    github, staging_sha = github_check(env)
    source_branch = run(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"], env=env)
    dirty_entries = len(run(["git", "-C", str(repo), "status", "--porcelain"], env=env).splitlines())
    candidates = tracker_check(report, env)
    lock = Path(report["ledger_path"] + ".lock")
    require(not lock.exists(), "Studio ledger is locked; check the recorded owner before start or offline recovery")
    if start:
        require(len(candidates) == 1 and not candidates[0]["blocked"], "Pilot start requires exactly one opted-in, unblocked issue")
    result = {"result": "ready", "runtime": str(runtime), "source_repo": str(repo), "workflow": str(workflow),
              "workspace_root": report["workspace_root"], "ledger_path": report["ledger_path"], "base_branch": "staging",
              "codex": version, "model": "gpt-6-astra", "reasoning_effort": "low", "permission_profile": PERMISSION_PROFILE, "limits": expected, "candidates": candidates,
              "source_branch": source_branch, "source_dirty_entries": dirty_entries, "remote_staging_sha": staging_sha,
              "github_default_branch": github["default_branch"], "agents_started": False}
    return result, manifest, env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--full", action="store_true", help="Compatibility alias; every preflight validates the generated policy schemas")
    args = parser.parse_args()
    try:
        report, _, _ = check(args.runtime, full=args.full)
        print(json.dumps(report, indent=2))
    except (CheckError, OSError, ValueError, KeyError):
        # CheckError messages are deliberately credential-free; parser/OS details are not.
        error = sys.exc_info()[1]
        print(str(error) if isinstance(error, CheckError) else "Preflight could not validate the runtime metadata", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
