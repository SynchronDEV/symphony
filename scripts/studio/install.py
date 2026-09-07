#!/usr/bin/env python3
"""Install a versioned Studio runtime and backed-up entrypoints; never start it."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
from preflight import CheckError, credentials, digest, require, run, sandbox_check, schema_check


def atomic_copy(source, destination, mode):
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=".studio-install-", dir=destination.parent)
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(Path(source).read_bytes())
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def resolve_escript(explicit, repository):
    if explicit:
        return Path(explicit).resolve()
    found = shutil.which("escript")
    if not found:
        found = run(["mise", "which", "escript"], cwd=repository / "elixir")
    return Path(found).resolve()


def install(args):
    repository = Path(__file__).resolve().parents[2]
    home = Path(args.destination_home).expanduser().resolve()
    binary = Path(args.runtime_binary).expanduser().resolve()
    escript = resolve_escript(args.escript, repository)
    codex_path = args.codex or shutil.which("codex")
    require(bool(codex_path), "Codex executable is missing")
    codex = Path(codex_path).expanduser().resolve()
    source_repo = Path(args.source_repo).expanduser().resolve()
    require(binary.is_file() and binary.stat().st_size > 0, "Explicit built runtime binary is missing")
    require(escript.is_file() and os.access(escript, os.X_OK), "Escript interpreter is missing")
    require(codex.is_file() and os.access(codex, os.X_OK), "Codex executable is missing")
    workflow_source = repository / "workflows/studio.md"
    env = credentials(home)
    env["SYMPHONY_STUDIO_CODEX_BIN"] = str(codex)
    report = json.loads(run([str(escript), str(binary), "--preflight", str(workflow_source)], env=env, cwd=repository))
    require(report.get("preflight") == "ok" and all(report["runtime"].values()), "Runtime does not support safe preflight")
    schema_check(str(codex), env, report.get("codex"))
    sandbox_check(str(codex), env)

    sources = {"symphony": binary, "studio.md": workflow_source,
               "preflight.py": repository / "scripts/studio/preflight.py", "launch.py": repository / "scripts/studio/launch.py"}
    hashes = {name: digest(path) for name, path in sources.items()}
    identity = json.dumps({"files": hashes, "escript": str(escript), "python": sys.executable,
                           "codex": str(codex), "codex_sha256": digest(codex), "source_repo": str(source_repo), "home": str(home)}, sort_keys=True)
    version = hashlib.sha256(identity.encode()).hexdigest()[:20]
    runtime_root = home / ".local/share/symphony-studio/runtimes"
    runtime = runtime_root / version
    workflow = home / ".config/symphony/spektra-workflow.md"
    launcher = home / ".local/bin/spektra-symphony"
    manifest = {"version": version, "home": str(home), "source_repo": str(source_repo), "workflow": str(workflow),
                "workflow_sha256": hashes["studio.md"], "files": hashes, "escript": str(escript),
                "python": sys.executable, "codex": str(codex), "codex_sha256": digest(codex)}
    runtime_root.mkdir(parents=True, exist_ok=True)
    if runtime.exists():
        require(json.loads((runtime / "manifest.json").read_text()) == manifest, "Versioned runtime metadata conflict")
        require(all(digest(runtime / name) == expected for name, expected in hashes.items()), "Versioned runtime was modified")
    else:
        with tempfile.TemporaryDirectory(prefix=".install-", dir=runtime_root) as staging:
            stage = Path(staging) / "runtime"
            stage.mkdir()
            for name, source in sources.items():
                shutil.copy2(source, stage / name)
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            os.rename(stage, runtime)

    launcher_body = ("#!/usr/bin/env sh\nset -eu\nexec " + shlex.quote(sys.executable) + " " +
                     shlex.quote(str(runtime / "launch.py")) + ' "$@"\n')
    changed = not launcher.is_file() or launcher.read_text() != launcher_body or not workflow.is_file() or digest(workflow) != hashes["studio.md"]
    backup = None
    if changed:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        backup = home / ".local/share/symphony-studio/backups" / stamp
        backup.mkdir(parents=True)
        for current in (launcher, workflow):
            if current.exists() or current.is_symlink():
                shutil.copy2(current, backup / current.name, follow_symlinks=False)
        with tempfile.TemporaryDirectory(prefix="studio-launcher-") as temporary:
            staged_launcher = Path(temporary) / "spektra-symphony"
            staged_launcher.write_text(launcher_body)
            # Publish workflow before launcher. Any interrupted update fails the
            # old launcher's content check rather than silently starting drifted config.
            atomic_copy(workflow_source, workflow, 0o600)
            atomic_copy(staged_launcher, launcher, 0o755)
    print(json.dumps({"installed": True, "changed": changed, "runtime": str(runtime), "launcher": str(launcher),
                      "workflow": str(workflow), "backup": str(backup) if backup else None, "agents_started": False}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-binary", required=True, help="Explicit freshly built elixir/bin/symphony")
    parser.add_argument("--source-repo", default="/Users/chrdav/dev/spektra/studio")
    parser.add_argument("--destination-home", default=str(Path.home()), help="Use a temporary home for installation tests")
    parser.add_argument("--escript", help="Pin an absolute Erlang escript executable; otherwise resolve via PATH/mise")
    parser.add_argument("--codex", help="Pin an absolute Codex executable")
    args = parser.parse_args()
    try:
        install(args)
    except (CheckError, OSError, ValueError, KeyError, subprocess.SubprocessError):
        error = sys.exc_info()[1]
        print(str(error) if isinstance(error, CheckError) else "Studio installation failed; existing backups and runtimes were preserved", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
