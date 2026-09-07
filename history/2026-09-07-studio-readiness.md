# Symphony Studio readiness verification

Last verified: 2026-09-07. Upstream release reviewed:
[OpenAI Symphony v0.0.2](https://github.com/openai/symphony/releases/tag/v0.0.2).
The implementation also incorporates fork `origin/main` at `d261a33`.
Tracking: [SPK-1020](https://linear.app/ante-digital/issue/SPK-1020).

Work was performed in an isolated checkout. Existing Studio and Symphony working
changes were preserved. Astra agents at low reasoning effort implemented bounded
areas; separate cross-reviews and parent-run tests checked their results.

## Changes and evidence

| Risk | Change | Verification |
| --- | --- | --- |
| Failed bootstrap leaves unusable reused directory | Remove only the newly created partial workspace and retry setup | Workspace readiness regressions |
| Retention can delete active, dirty, or unmerged work | Require completed ledger records, captured physical paths, stopped ownership, clean Git, merged HEAD/all local branches and no stash; recheck after hooks | Live-worker, symlink, dirty/unmerged, hook mutation and root-reload regressions |
| Workflow root changes redirect cleanup | Capture root and merge ref at dispatch; use recorded paths | Actual GenServer stop-before-delete and root-change test |
| Retry final refresh strands claims | Explicit release/reschedule outcomes with operation identities and capacity reservations | Missing/ineligible/error refresh and queue tests |
| Slow tracker calls freeze state handling | Move polling, refresh, history, cleanup and coalesced lease markers into bounded monitored operations | Delayed real poll, event, snapshot, worker-exit and stale-result tests |
| Failed or interrupted Codex turn is reported as success | Match active thread/turn and explicit terminal status; interrupt with acknowledgement plus termination before continuation | Protocol regression suite, early notifications, retryable errors and input blocking |
| Workflow deployments overwrite each other's counters | Filename/path-derived ledger identity, exclusive writer lock, atomic snapshots, explicit offline legacy migration | Two-workflow isolation, duplicate writer, malformed data and persistence-failure tests |
| Redundant whole-ledger writes | Zero deltas are no-ops; token updates batch for 250 ms; lifecycle and graceful shutdown flush | Ledger regression suite and reproducible benchmark |
| Orchestrator crashes leave paid workers alive | Restart workers and dispatch bookkeeping as one supervised failure domain | Actual orchestrator crash with shutdown barrier and durable counter recovery |
| Stale Studio launcher and unsafe defaults | Pinned binary/scripts, backed-up installation, live preflight, one worker, explicit labels, staging PRs, human merges | Eight offline installer/preflight tests and runtime CLI tests |
| Narrow dashboard overflows | Constrain grid track; retain horizontal scrolling inside tables; fix touch targets and hover contrast | 18 live breakpoint/state screens plus interaction checks: PASS |

The exact remote baseline passes 277 tests with two live tests skipped, but its
coverage gate fails at 91.30% against the existing 100% requirement. The threshold
has not been reduced. This baseline finding is separate from functional regressions.

## Final local validation

- Full functional suite: **354 tests, zero failures, two skipped live integration tests**,
  seed `309148`, 31.4 seconds. Command: `LINEAR_API_KEY= mise exec -- mix test --cover --seed 309148`.
- Build, formatting, public specs and Credo: passed through `mise exec -- make all`.
- Dialyzer: passed with zero errors (`mise exec -- mix dialyzer --format short`).
- Coverage: 91.20%; the unchanged 100% gate remains red. Exact remote baseline is
  91.30%; the small difference depends partly on credential/rate-limit test paths.
  `make all` therefore does not have an overall green result.
- Installer/preflight scripts: eight offline tests passed.
- Live UI review: all 18 viewport/data-state combinations plus interactions passed.
  A minor pre-existing event-metadata truncation without tooltip remains.

Live preflight verified Codex 0.153.4, Astra/low, the current `spektra-org/spektra`
repository, staging commit `30b2c56cfce4888249dcc3717dc2318609aa34da`, the correct
Linear project, opt-in routing, bounded settings and the isolated ledger path.
Installed runtime: `~/.local/share/symphony-studio/runtimes/24161765e561f75d1ad4`.
Original launcher/workflow backup: `~/.local/share/symphony-studio/backups/20260907T200136.549897Z`.
The Salesight session remained running. No production deployment or merge occurred.

## Ledger measurement

`mise exec -- mix run --no-start scripts/ledger_benchmark.exs` uses 1,000 persisted
issue records and 1,000 updates in an isolated temporary ledger:

- Zero-token updates: 1.334 ms, zero writes (audited implementation: about 2,608 ms).
- Positive updates: 1.230 ms before flush; one final write; expected 2,000 tokens persisted.
- Snapshot size: 57,894 bytes. This is a synthetic local measurement, not a production SLA.

## Operational boundaries

The stable workflow path preserves budget identity across binary upgrades.
Abrupt process/host failure can lose the last 250 ms of buffered token deltas;
lifecycle changes are synchronously persisted. Stale locks require verified offline
recovery and are never removed automatically. Remote automatic cleanup remains
disabled pending equivalent safety proof. Token caps exclude cached input and are
token metrics, not currency estimates.

The controlled Studio pilot is [SPK-1022](https://linear.app/ante-digital/issue/SPK-1022),
which fixes existing operator commands that target nonexistent Linear statuses.
Its scope is limited to the operator script, tests, guide and required changeset.
It opens a staging PR, passes through independent review, and leaves human merge
and deployment pending. See [operator instructions](../docs/studio-operations.md).
