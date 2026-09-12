# Symphony Elixir

This directory contains the current Elixir/OTP implementation of Symphony, based on
[`SPEC.md`](../SPEC.md) at the repository root.

> [!WARNING]
> Symphony Elixir is prototype software intended for evaluation only and is presented as-is.
> We recommend implementing your own hardened version based on `SPEC.md`.

## Screenshot

![Symphony Elixir screenshot](../.github/media/elixir-screenshot.png)

## How it works

1. Polls Linear for candidate work
2. Creates a workspace per issue
3. Launches Codex in [App Server mode](https://developers.openai.com/codex/app-server/) inside the
   workspace
4. Sends a workflow prompt to Codex
5. Keeps Codex working on the issue until the work is done

During app-server sessions, Symphony also serves a client-side `linear_graphql` tool so that repo
skills can make raw Linear GraphQL calls.

If a claimed issue moves to a terminal state (`Done`, `Closed`, `Cancelled`, or `Duplicate`),
Symphony confirms the active agent has stopped before considering cleanup. Automatic cleanup requires
a recorded workspace/root, a configured `workspace.cleanup_base_ref` under `refs/remotes/`, clean Git
state, no stashes, and proof that HEAD and every local branch are merged into that ref. Missing proof
preserves the workspace. Remote automatic cleanup is disabled until equivalent checks exist.

If Codex reports that operator input, approval, or MCP elicitation is required, Symphony keeps the
issue claimed and exposes it as blocked in the runtime state, JSON API, and dashboard. Blocked
entries are in memory only; restarting the orchestrator clears that blocked map, so any still-active
Linear issue can become a dispatch candidate again after restart.

Claimed issues also get a Symphony claim lease marker through the tracker comment API. Comments
are point-in-time snapshots published on the initial claim and material ownership, retry, or blocked
transitions. Routine heartbeats refresh the internal lease without adding tracker comments; the
dashboard and JSON API expose the current last-seen worker, workspace, attempt, heartbeat, and expiry.
Failed marker publications retain retry backoff and are superseded by newer material transitions.
If a non-live claim lease expires, Symphony logs the recovery and requeues the issue without starting
a duplicate worker for a still-running claim.

Workspace creation, the before-run hook, and Codex startup have separate preparation clocks. Hook
and startup timeout settings remain the authority for those phases; preparation does not consume
one aggregate Codex inactivity window. Codex activity takes over the inactivity clock after startup.

Stall recovery stops and quarantines the worker while its counter is committed by a bounded
background operation. Retry is allowed only after the durable write is acknowledged. An operation
deadline or write failure leaves the claim blocked for operator reconciliation, because a timed-out
write may still commit; the counter and quarantine are persisted atomically and the increment is
never replayed automatically. A late commit therefore remains blocked across service restarts.
Only an acknowledged recovery clears that durable quarantine before scheduling a retry.
Snapshots and retention scans read an owned ETS cache of the last durably committed ledger during
an in-flight write; authorization and budget gates continue to use authoritative ledger reads.
Other synchronous ledger operations still depend on filesystem responsiveness; this recovery path
does not make the whole service immune to storage stalls.

## How to use it

1. Make sure your codebase is set up to work well with agents: see
   [Harness engineering](https://openai.com/index/harness-engineering/).
2. Get a new personal token in Linear via Settings → Security & access → Personal API keys, and
   set it as the `LINEAR_API_KEY` environment variable.
3. Copy this directory's `WORKFLOW.md` to your repo.
4. Optionally copy the `commit`, `push`, `pull`, `land`, and `linear` skills to your repo.
   - The `linear` skill expects Symphony's `linear_graphql` app-server tool for raw Linear GraphQL
     operations such as comment editing or upload flows.
5. Customize the copied `WORKFLOW.md` file for your project.
   - To get your project's slug, right-click the project and copy its URL. The slug is part of the
     URL.
   - When creating a workflow based on this repo, note that it depends on non-standard Linear
     issue statuses: "Rework", "Human Review", and "Merging". You can customize them in
     Team Settings → Workflow in Linear.
6. Follow the instructions below to install the required runtime dependencies and start the service.

## Prerequisites

We recommend using [mise](https://mise.jdx.dev/) to manage Elixir/Erlang versions.

```bash
mise install
mise exec -- elixir --version
```

## Run

```bash
git clone https://github.com/openai/symphony
cd symphony/elixir
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build
mise exec -- ./bin/symphony ./WORKFLOW.md
```

## Configuration

Pass a custom workflow file path to `./bin/symphony` when starting the service:

```bash
./bin/symphony /path/to/custom/WORKFLOW.md
```

If no path is passed, Symphony defaults to `./WORKFLOW.md`.

Optional flags:

- `--logs-root` tells Symphony to write logs under a different directory (default: `./log`)
- `--port` also starts the Phoenix observability service (default: disabled)

Symphony also writes durable Codex token usage observations to `token_usage.jsonl` next to the
configured log file. With the default log path, this is `./log/token_usage.jsonl`; with
`--logs-root`, it follows the same log root. The ledger stores cumulative high-water token totals
per issue/session so completed tickets can still be inspected after the in-memory dashboard state
has moved on.

The `WORKFLOW.md` file uses YAML front matter for configuration, plus a Markdown body used as the
Codex session prompt.

Minimal example:

```md
---
tracker:
  kind: linear
  project_slug: "..."
workspace:
  root: ~/code/workspaces
hooks:
  after_create: |
    git clone git@github.com:your-org/your-repo.git .
agent:
  max_concurrent_agents: 10
  max_turns: 20
  max_turns_by_state:
    "In Review": 2
codex:
  command: codex app-server
---

You are working on a Linear issue {{ issue.identifier }}.

Title: {{ issue.title }} Body: {{ issue.description }}
```

Notes:

- If a value is missing, defaults are used.
- `tracker.required_labels` is optional. When set, an issue must have every
  configured label to dispatch or continue running. Label matching ignores
  case and surrounding whitespace. A blank configured label matches no issue.
- Safer Codex defaults are used when policy fields are omitted:
  - `codex.approval_policy` defaults to `{"granular":{"sandbox_approval":false,"rules":false,"mcp_elicitations":false,"request_permissions":false,"skill_approval":false}}`
  - `codex.thread_sandbox` defaults to `workspace-write`
  - `codex.turn_sandbox_policy` defaults to a `workspaceWrite` policy rooted at the current issue workspace
- Last verified: 2026-09-07 against Codex CLI 0.153.4's generated
  [app-server protocol](https://developers.openai.com/codex/app-server). Its approval values are
  `untrusted`, `on-request`, `never`, and object-form `granular`. Legacy `reject` and `on-failure`
  are unsupported by this installed version. The default disables all five approval-prompt
  categories; it does not grant additional permissions or automatically approve requests.
- Explicit approval policies are forwarded unchanged, including policies intended for another
  Codex version. Symphony does not silently migrate existing workflow permissions. Inspect the
  installed contract with `codex app-server generate-json-schema --out <dir>` and update an
  incompatible workflow deliberately before dispatch.
- `codex.permission_profile` optionally selects the named permission profile expected from the
  Codex command's `default_permissions` configuration. In this mode Symphony omits both
  `thread/start.sandbox` and `turn/start.sandboxPolicy`, because these legacy overrides disable
  named profiles. Startup fails before a turn if Codex's returned `activePermissionProfile.id`
  does not exactly match. Configure and verify the profile in the launch command; Symphony does
  not grant permissions by defining a profile name alone. With this field omitted, legacy sandbox
  fields retain their existing behavior.
- Supported `codex.thread_sandbox` values: `read-only`, `workspace-write`, `danger-full-access`.
- When `codex.turn_sandbox_policy` is set explicitly, Symphony forwards the configured map to
  Codex, but for `workspaceWrite` policies it ensures the current issue workspace stays in
  `writableRoots` at runtime. This allows adding extra writable paths without granting access to
  sibling workspaces by default. Compatibility for the remaining fields still depends on the
  targeted Codex app-server version rather than local Symphony validation.
- Workflows that run package managers or other commands that resolve external hosts should set
  `networkAccess: true` in `codex.turn_sandbox_policy`; otherwise DNS/network access may be denied
  by the Codex turn sandbox.
- `agent.max_turns` caps how many back-to-back Codex turns Symphony will run in a single agent
  invocation when a turn completes normally but the issue is still in an active state. Default: `20`.
- `agent.max_turns_by_state` overrides that cap for specific active tracker states. State names
  are normalized for lookup, so `"In Review"` and `"in review"` refer to the same state.
- If the Markdown body is blank, Symphony uses a default prompt template that includes the issue
  identifier, title, and body.
- Use `hooks.after_create` to bootstrap a fresh workspace. For a Git-backed repo, you can run
  `git clone ... .` there, along with any other setup commands you need.
- If a hook needs `mise exec` inside a freshly cloned workspace, trust the repo config and fetch
  the project dependencies in `hooks.after_create` before invoking `mise` later from other hooks.
- `tracker.api_key` reads from `LINEAR_API_KEY` when unset or when value is `$LINEAR_API_KEY`.
- For path values, `~` is expanded to the home directory.
- Relative local workspace roots are anchored to the selected workflow's directory, including
  retention and Codex sandbox checks. Existing work keeps the root captured at dispatch after reload.
- A failed fresh-workspace bootstrap removes its partial directory so the next attempt reruns setup.
- Retention considers only recorded completed workspaces and rechecks live ownership before removal.
  It never scans arbitrary directories into deletion candidates; disk limits cannot override safety.
- For env-backed path values, use `$VAR`. `workspace.root` resolves `$VAR` before path handling,
  while `codex.command` stays a shell command string and any `$VAR` expansion there happens in the
  launched shell.

```yaml
tracker:
  api_key: $LINEAR_API_KEY
workspace:
  root: $SYMPHONY_WORKSPACE_ROOT
hooks:
  after_create: |
    git clone --depth 1 "$SOURCE_REPO_URL" .
codex:
  command: "$CODEX_BIN --config 'model=\"gpt-5.5\"' app-server"
```

- If `WORKFLOW.md` is missing or has invalid YAML at startup, Symphony does not boot.
- If a later reload fails, Symphony keeps running with the last known good workflow and logs the
  reload error until the file is fixed.
- `server.port` or CLI `--port` enables the optional Phoenix LiveView dashboard and JSON API at
  `/`, `/api/v1/state`, `/api/v1/<issue_identifier>`, and `/api/v1/refresh`.

## Reliability and accounting

Tracker polling, final dispatch refreshes, rate-limit backoff, and workspace cleanup run outside the
orchestrator mailbox. Reservations count toward capacity while refreshes and worker stops are in
flight. Results apply only to the operation and worker identity that requested them.

Codex notifications must match the active thread and turn. Failed, interrupted, malformed, or
unexpected terminal states cannot be treated as successful completion. A timed-out turn must receive
an interrupt acknowledgement and terminal notification before another turn starts. Retryable Codex
errors continue waiting; unsupported input requests block visibly.

Each canonical workflow filename gets a separate `.symphony/<name>-<path-hash>/ledger.json` and
`metrics.jsonl`. A writer lock prevents two processes from using the same ledger. Shared legacy files
are not imported automatically: migration must supply a reviewed, scoped source while the target is
offline. Invalid counters or JSON fail startup without overwriting evidence.

Token deltas update memory immediately and flush at most once per 250 ms; zero deltas do not write.
Lifecycle updates, explicit flush, and graceful termination flush pending values atomically using
temporary-file sync and rename. An abrupt host/process failure may lose the last 250 ms of token
deltas. Effective-token caps subtract cached input; they are not monetary spend limits.

`agent.min_tokens_before_dispatch` reserves effective-token headroom for each fresh worker,
including independent review. It defaults to zero for compatibility and is applied only when
`max_tokens_per_issue` is configured. Exhausted issues never dispatch, even with zero reserve;
remaining tokens equal to the reserve are sufficient. A reserve rejection preserves counters
and applies an operator hold rather than starting a worker.

Failed `before_run` hooks are readiness blockers, not automatic retry loops. Correct the
environment and explicitly release the hold before another attempt. Confirmed worker stops
persist `stopped`, scheduled continuations persist `retrying`, and existing blocked/completed
records retain their stronger outcome. These execution states do not mark a Linear issue Done.

A crash lock deliberately fails closed. Stop all processes using that deployment, verify the recorded
owner is gone, back up the ledger and lock, then remove the stale lock before restart. Never remove
a live lock. Any supervised service failure restarts workers and orchestration together, preventing
orphaned agents from surviving lost dispatch bookkeeping. This favors bounded execution over availability.

The [Studio operations guide](../docs/studio-operations.md) describes the isolated launcher and
conservative pilot configuration. Existing deployments retain their own launcher and binary.

## Web dashboard

The observability UI now runs on a minimal Phoenix stack:

- LiveView for the dashboard at `/`
- JSON API for operational debugging under `/api/v1/*`
- Active, retrying, blocked, and expired claim lease visibility
- Bandit as the HTTP server
- Phoenix dependency static assets for the LiveView client bootstrap
- Tracker issue identifiers link to the tracker-provided URL when it uses `http` or `https`
- Token cards and running rows report effective spend separately from raw provider totals:
  cached input is tracked and subtracted from the headline effective token count.

The JSON API includes durable token summaries from `token_usage.jsonl`:

- `/api/v1/state` includes `token_usage` totals plus issue/session counts.
- `/api/v1/<issue_identifier>` can return `status: "inactive"` with `token_usage` for a completed
  or otherwise inactive issue that is no longer present in the live running/retry state.

## Project Layout

- `lib/`: application code and Mix tasks
- `test/`: ExUnit coverage for runtime behavior
- `WORKFLOW.md`: in-repo workflow contract used by local runs
- `../.codex/`: repository-local Codex skills and setup helpers

## Testing

```bash
make all
```

Run the real external end-to-end test only when you want Symphony to create disposable Linear
resources and launch a real `codex app-server` session:

```bash
cd elixir
export LINEAR_API_KEY=...
make e2e
```

Optional environment variables:

- `SYMPHONY_LIVE_LINEAR_TEAM_KEY` defaults to `SYME2E`
- `SYMPHONY_LIVE_SSH_WORKER_HOSTS` uses those SSH hosts when set, as a comma-separated list

`make e2e` runs two live scenarios:
- one with a local worker
- one with SSH workers

If `SYMPHONY_LIVE_SSH_WORKER_HOSTS` is unset, the SSH scenario uses `docker compose` to start two
disposable SSH workers on `localhost:<port>`. The live test generates a temporary SSH keypair,
mounts the host `~/.codex/auth.json` into each worker, verifies that Symphony can talk to them
over real SSH, then runs the same orchestration flow against those worker addresses. This keeps
the transport representative without depending on long-lived external machines.

Set `SYMPHONY_LIVE_SSH_WORKER_HOSTS` if you want `make e2e` to target real SSH hosts instead.

The live test creates a temporary Linear project and issue, writes a temporary `WORKFLOW.md`, runs
a real agent turn, verifies the workspace side effect, requires Codex to comment on and close the
Linear issue, then marks the project completed so the run remains visible in Linear.

## FAQ

### Why Elixir?

Elixir is built on Erlang/BEAM/OTP, which is great for supervising long-running processes. It has an
active ecosystem of tools and libraries. It also supports hot code reloading without stopping
actively running subagents, which is very useful during development.

### What's the easiest way to set this up for my own codebase?

Launch `codex` in your repo, give it the URL to the Symphony repo, and ask it to set things up for
you.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
