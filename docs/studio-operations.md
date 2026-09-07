# Studio Symphony pilot operations

Last verified: 2026-09-07. The protocol checks use the installed
[Codex app-server schema](https://developers.openai.com/codex/app-server/).
Tracker identities were read from Linear; preflight checks them again before a run.

The canonical workflow is [workflows/studio.md](../workflows/studio.md).
Installation copies a reviewed workflow and an explicitly selected built executable
into an isolated Studio runtime. Installation never starts Symphony or Codex work.
The generic `symphony` launcher and Salesight configuration are not modified.

## Runtime contract

| Setting | Studio value |
| --- | --- |
| Source checkout, inspected only | `/Users/chrdav/dev/spektra/studio` |
| GitHub repository and PR base | `spektra-org/spektra`, `staging` |
| Linear organization | `ante-digital` |
| Linear project | Spektra - Remotion Editor, slugId `89a635a4f38d` |
| Opt-in label | `symphony-studio` |
| Stop labels | `symphony-hold`, `symphony-stuck` |
| Workspace root | `~/code/spektra-symphony-workspaces` |
| Cleanup merge evidence | `refs/remotes/origin/staging` |
| Model | `gpt-6-astra`, reasoning effort `medium` |
| Approval policy | `granular`, all five approval categories explicitly `false` (reject requests) |
| Filesystem profile | `symphony_studio`, extends `:workspace`; `.git` writable, `.codex` and `.agents` read-only; network enabled |
| Limits | One worker; eight turns per invocation; 250,000 effective tokens per issue; three dispatches; two rework cycles |
| Poll interval | 60 seconds |
| Dashboard | `http://127.0.0.1:4767` |
| Logs | `~/.local/state/symphony-studio/logs` |

Three dispatch attempts are deliberately stricter than two complete rework cycles:
initial implementation, review and implementation can exhaust the dispatch cap
before a second review. A ceiling is not a promise to finish that many cycles.
Token accounting and caps persist in the workflow-specific ledger. A new process
or binary version does not grant a new budget for an existing issue.

No agent merges PRs, enables auto-merge, pushes staging/main, deploys, or runs
external migrations. Human review and merge remain mandatory. Local preview proof
for a render change does not establish that deployed exports work. Missing external
access or required deployment verification is disclosed and held for the human.

## Existing statuses and handoffs

| State and labels | Meaning |
| --- | --- |
| Backlog | Human triage; never dispatched by the pilot |
| Todo + symphony-studio | Implementation queue |
| In Progress + symphony-studio | Implementation or recovery of the existing branch |
| In Review + symphony-studio | Independent agent review |
| In Review + symphony-hold, without symphony-studio | Agent review passed; waiting for human review and merge |
| Todo after an In Review failure | Counted rework; use the existing PR branch |
| Any state + symphony-hold or symphony-stuck | No dispatch or continuation until an operator resolves the reason |
| Done / Canceled / Duplicate | Terminal; no new work |

No custom Ready for Agent, Human Review or Rework states are required. Review pass
adds the stop label before removing the opt-in label so there is no redispatch gap.
Review failure includes concrete evidence and returns to Todo only for actionable
implementation work. Missing human decisions or credentials receive a hold instead.
The human marks Done after merge. Merely opening a PR never marks an issue Done.

## Build and install

From this Symphony repository root, with the intended changes reviewed and gates
complete, build the executable once:

```sh
(cd elixir && mise exec -- mix build)
python3 scripts/studio/install.py --runtime-binary "$PWD/elixir/bin/symphony"
```

The installer requires an explicit executable path. It resolves and pins an
absolute Erlang `escript` interpreter and Codex executable, validates the binary's
read-only `--preflight`, and checks the Codex protocol schema. It does not reuse a
generic Symphony executable from PATH. `--escript /absolute/path/to/escript` and
`--codex /absolute/path/to/codex` select explicit executables when needed.

Installation writes only these Studio locations:

- `~/.local/share/symphony-studio/runtimes/<content-id>/`: executable, pinned
  launch/preflight scripts, immutable workflow copy and content hashes.
- `~/.config/symphony/spektra-workflow.md`: the stable runtime workflow path.
- `~/.local/bin/spektra-symphony`: launcher pinned to one runtime directory.
- `~/.local/share/symphony-studio/backups/<timestamp>/`: previous launcher and
  workflow, saved before replacement.

The content ID covers the executable, workflow, scripts, selected tool paths and
source location. Repeating an identical installation is a no-op for the entrypoints
and creates no duplicate backup. A changed installation creates a new runtime;
old runtime directories remain available. If interrupted between publishing the
workflow and launcher, the old launcher's hash check fails closed.

The stable workflow path preserves ledger identity across runtime upgrades.
Do not move or rename it to obtain a fresh ledger. Existing shared legacy ledgers
are never automatically imported, overwritten or combined with Studio's new ledger.
Any historical counter migration requires an explicitly scoped, reviewed operation.

## Credentials and preflight

Inherited `LINEAR_API_KEY` is supported directly. The installer and launcher also
read optional `~/.config/symphony/studio.env`, allowing only simple assignments for
`LINEAR_API_KEY`, `GH_TOKEN`, `GITHUB_TOKEN` and `OPENAI_API_KEY`. Explicit file assignments override inherited values; omit the file to use the inherited environment.
The file is parsed as data; shell commands and variable expansion are not executed.
Keep it private and outside Git. Neither script prints credential values or failed
API response bodies. The Studio checkout's `.env` is not implicitly sourced.

Authenticate Codex and GitHub separately before preflight. Existing ChatGPT Codex
login is supported; an OpenAI API key is not mandatory for that login mode.
Missing secrets required by the actual Studio task still cause an explicit hold.

```sh
~/.local/bin/spektra-symphony --preflight --full
```

Full preflight checks:

1. Runtime/workflow hashes; the actual installed executable's Config, Schema and
   prompt rendering for every active role, without starting Symphony's supervisor.
2. Elixir/OTP compatibility, readiness APIs, all configured pilot limits and roots.
3. Codex 0.153.4 or newer, current login, and generated interrupt/completion/error
   schema fields. Every preflight validates the actual configured approval policy,
   approval-policy fields against the installed ThreadStart/TurnStart schemas,
   even when the binary hash has not changed. Legacy `reject` variants fail before
   dispatch. The named profile is selected by the exact reviewed command; runtime
   startup must confirm its returned ID. Legacy sandbox wire overrides are omitted
   because they clear the named profile. Explicit turn-policy overrides, including
   extra writable roots, are rejected by Studio preflight. Studio requires `granular` with `sandbox_approval`,
   `rules`, `mcp_elicitations`, `request_permissions` and `skill_approval` all false;
   false means those requests are rejected. The CLI reports a full command hash,
   never the raw command. It must match the exact reviewed Astra/medium/profile command;
   extra or alternate flags and shell suffixes fail instead of being partially parsed.
   Schema checks do not prove model/account availability. `--full` remains accepted
   as a compatibility alias; policy checks are no longer optional.
4. A no-model `codex sandbox` probe creates a disposable Git workspace under the home directory
   (outside inherited writable system-temp paths) and a separate home-directory sentinel. It creates a local package archive and frozen lockfile,
   clears the package cache, then requires a real offline Bun install to extract
   the expected module. Bun temporary/cache paths stay under the disposable
   workspace's `.git/symphony-runtime/`. The probe checks Codex's configured tool
   environment and runs bare Bun without an inline environment workaround. Git lock/branch writes and protected-file reads must
   succeed; `.codex`, `.agents` and outside-home writes must fail. The temporary paths
   are removed afterward. This detects permissions that parse but cannot perform
   the task; no thread or turn is started.
5. Source checkout origin, GitHub repository identity, staging existence and
   the current GitHub account's issue-branch push permission.
6. Linear organization/project/statuses and all opted-in active issue identifiers,
   with blockers and stop labels taken into account.
7. The workflow-specific ledger path and any existing writer lock.

The report includes source branch/dirty-entry count and remote staging commit, so
local uncommitted Studio changes are not confused with the branch cloned for work.
Preflight does not certify Studio app health, run editor tests, create issue
workspaces, execute bootstrap hooks, modify Linear, migrate counters or spend
Codex tokens. Its disposable sandbox probe only checks local filesystem permissions.
The binary also supports a purely local check:

```sh
/absolute/path/to/escript /absolute/path/to/symphony --preflight /absolute/path/to/workflow.md
```

That local command validates configuration and templates only; it does not perform
live credential, project, schema or candidate checks. Use the installed launcher
for the full pilot readiness check.

## Start one bounded issue

First inspect the issue acceptance criteria and attached workpad/PR. Keep all other
issues outside the opt-in active set. Add `symphony-studio` to the selected issue and
move it to Todo only when it is ready; remove a hold only after its documented
reason is resolved. For recovery, preserve its existing branch and local edits.

```sh
~/.local/bin/spektra-symphony --preflight
~/.local/bin/spektra-symphony --start
```

There is no implicit start when invoking the launcher without arguments.
`--start` reruns checks and requires exactly one opted-in, unblocked active issue.
Newly opted-in issues added later are still part of the live queue: do not add more
labels during the bounded pilot. The underlying single-writer ledger lock guards
against a second Studio process even if two launch checks race.

Watch the named issue, logs, dashboard and GitHub PR. Confirm implementation moves
to In Review, the independent reviewer assesses that exact commit, and pass leaves
the issue held for the human. Verify workpad evidence, required CI and local proof
before human merge. Inspect effective spend and retry/rework counts after restart;
source tests alone do not prove the external end-to-end lifecycle.

To stop, terminate only the recorded Studio process, using its foreground Ctrl-C
or exact PID. Wait for confirmed exit. Never use a broad `pkill beam`, generic
Symphony kill command, or Salesight launcher. Review an unusually long/expensive
run and each configured completed batch with `codex-run-reviewer` before widening
the pilot.

## Workspaces, retention and recovery

New workspaces clone staging. Every run fetches the staging remote-tracking ref and
runs `bun install --frozen-lockfile`; agents repeat installation after an actual
branch switch. Hooks and agent Bun shells set `TMPDIR` and
`BUN_INSTALL_CACHE_DIR` under `.git/symphony-runtime/` inside the assigned workspace.
Hook exports do not persist into agent shells. The canonical Codex command sets
`shell_environment_policy.set` using shell-expanded issue-workspace `$PWD`, so
bare Bun commands inherit these paths deterministically. The workflow also
repeats the setup explicitly as operator guidance. This avoids requiring global package-cache permissions. The offline
preflight fixture checks local archive extraction, not every registry dependency
or postinstall script used by Studio. No additional global writable roots are granted.
Failed new bootstrap is retried from a removed partial directory.
Established issue directories and edits are reused.

Automated cleanup uses recorded paths and lifecycle state after worker termination.
It preserves unregistered, active, claimed, queued, retrying, blocked, dirty or
unmerged work. Clean Git status alone is insufficient: cleanup checks merge
ancestry, other local branches, stashes, symlinks and a current ownership callback.
Missing proof preserves data. Retention uses recorded completion time, not directory
mtime. Remote automatic cleanup is preserved until remote safety proof is available.

The ledger lock fails closed, including after an unclean stop. It is never removed
automatically based only on age. For offline recovery:

1. Read the exact ledger path reported by preflight and its `.lock` owner PID.
2. Verify that PID's command and confirm no Studio process or worker can still use
   the same workflow/ledger. A reused PID or missing process alone is insufficient;
   check the runtime process tree and recent logs.
3. Back up both that ledger and that lock together before any recovery action.
4. Only after confirming the owner is dead and the deployment is offline, remove
   that specific stale `.lock`. Do not remove any Salesight or unrelated lock.
5. Rerun preflight; inspect preserved workspaces and counters before resuming the
   held issue. Do not reset spending or retry caps merely to get past a blocker.

To roll back an installation, stop Studio first, then restore the matching launcher
and workflow pair from one backup directory. Keep the ledger and workspaces in place.
Run full preflight before starting the restored runtime. Do not silently substitute
the generic Symphony executable if a pinned runtime or interpreter is unavailable.

## Offline script tests

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/studio -p 'test_*.py' -v
```

These tests use temporary destinations and fake executables; they do not install
into the operator's home, contact APIs or start agents. CLI preflight tests are in
`elixir/test/symphony_elixir/cli_preflight_test.exs` and should run with the normal
serialized Elixir suite.
