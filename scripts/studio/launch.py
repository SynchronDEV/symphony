#!/usr/bin/env python3
"""Pinned Studio entrypoint. Starting work requires an explicit --start."""
import argparse
import json
import os
from pathlib import Path
import sys
from preflight import CheckError, check


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--start", action="store_true")
    parser.add_argument("--full", action="store_true", help="Also regenerate the Codex protocol schema")
    args = parser.parse_args()
    runtime = Path(__file__).resolve().parent
    try:
        report, manifest, env = check(runtime, full=args.full, start=args.start)
        print(json.dumps(report, indent=2), flush=True)
        if args.start:
            logs = Path(manifest["home"]) / ".local/state/symphony-studio/logs"
            logs.mkdir(parents=True, exist_ok=True)
            os.chdir(runtime)
            os.execve(manifest["escript"], [manifest["escript"], str(runtime / "symphony"),
                "--i-understand-that-this-will-be-running-without-the-usual-guardrails",
                "--logs-root", str(logs), "--port", "4767", manifest["workflow"]], env)
    except (CheckError, OSError, ValueError, KeyError):
        error = sys.exc_info()[1]
        print(str(error) if isinstance(error, CheckError) else "Studio launch failed before dispatch", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
