# Symphony Studio readiness verification

Last verified: 2026-09-07. Upstream release reviewed:
[OpenAI Symphony v0.0.2](https://github.com/openai/symphony/releases/tag/v0.0.2).
The implementation also incorporates fork `origin/main` at `d261a33`.
Tracking: [SPK-1020](https://linear.app/ante-digital/issue/SPK-1020).
Draft implementation: [Symphony PR #5](https://github.com/SynchronDEV/symphony/pull/5).
Studio operator fix: [Studio PR #951](https://github.com/spektra-org/spektra/pull/951),
commit `1713fe7855f040993f02fabdbe2a592554671d3a`, targeting `staging`.

The code and isolated runtime are implemented and independently reviewed. Studio
is stopped with no eligible issues. This establishes readiness for another
supervised run; a complete unattended implementation-to-review cycle has not been
demonstrated. The existing coverage gate remains red. Both PRs await human review
and merge.

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
| Stale Studio launcher and unsafe defaults | Pinned binary/scripts, backed-up installation, live preflight, one worker, explicit labels, staging PRs, human merges | Fourteen offline installer/preflight tests and runtime CLI tests |
| Narrow dashboard overflows | Constrain grid track; retain horizontal scrolling inside tables; fix touch targets and hover contrast | 18 live breakpoint/state screens plus interaction checks: PASS |
| Installed Codex rejects the legacy approval policy before starting work | Use granular approval flags, all false, and validate configured policies against generated installed schemas | Real positive and negative initialize/thread-start handshakes; schema regressions |
| Legacy sandbox blocks Git branch creation | Opt-in named profile permits assigned-workspace Git writes; retain protected agent configuration paths; omit conflicting legacy overrides | Real sandbox probe, profile mismatch rejection, initial and continuation wire tests |
| Codex filters inherited Bun cache settings and dependency installation fails | Set per-tool temporary/cache paths explicitly under the assigned workspace's `.git/symphony-runtime/` | Actual Studio frozen install and postinstall passed; every preflight exercises bare Bun with the same configured environment |
| Usage telemetry appends identical counters on unrelated notifications | Compare prior serialized observation; retain changes and lifecycle/final evidence | End-to-end notification regression and real-log replay: 95% fewer records, identical totals |

The exact remote baseline passes 277 tests with two live tests skipped, but its
coverage gate fails at 91.30% against the existing 100% requirement. The threshold
has not been reduced. This baseline finding is separate from functional regressions.

## Final local validation

- Full functional suite: **362 tests, zero failures, two skipped live integration tests**,
  seed `309148`, 32.1 seconds. Command: `LINEAR_API_KEY= mise exec -- mix test --seed 309148`.
  The final `LINEAR_API_KEY= mise exec -- make all` repeated all 362 tests with zero
  failures in 27.4 seconds while measuring coverage.
- Build, formatting, public specs and Credo: passed through `mise exec -- make all`.
- Dialyzer: passed with zero errors (`mise exec -- mix dialyzer --format short`).
- Coverage: 91.19%; the unchanged 100% gate remains red. Exact remote baseline is
  91.30%; this branch therefore remains 0.11 percentage points below that baseline.
  `make all` therefore does not have an overall green result.
- Installer/preflight scripts: fourteen offline tests passed in the parent run.
- Real installed-Codex schema and filesystem probe: passed, without a model turn.
- Studio operator fix: 17 focused tests passed in both worker and independent
  parent runs. Biome, typecheck, `verify:affected`, full `verify` and push checks
  passed. The full gate selected no related source tests; the 17 script tests
  provide the behavioral coverage. A separate Astra reviewer read the complete
  four-file diff and call paths and found no actionable regression. Current
  Linear input-schema introspection confirmed additive/removal label fields;
  behavior tests used mocks, without test mutations to live issues.
- Live UI review: all 18 viewport/data-state combinations plus interactions passed.
  A minor pre-existing event-metadata truncation without tooltip remains.

Live preflight verified Codex 0.153.4, Astra/low, the current `spektra-org/spektra`
repository, staging commit `1b1e308fc9ffa1b480cb72849da36993933d376a`, the correct
Linear project, opt-in routing, bounded settings and the isolated ledger path.
Installed runtime: `~/.local/share/symphony-studio/runtimes/250385559f2928770e21`.
Original launcher/workflow backup: `~/.local/share/symphony-studio/backups/20260907T200136.549897Z`.
Final launcher preflight returned `ready`, no candidates, and `agents_started: false`.
No Studio runtime process or writer lock remains. The Salesight session remained
running. No production deployment or merge occurred. The source Studio checkout
continued changing independently and was not used for this implementation.

## Ledger measurement

`mise exec -- mix run --no-start scripts/ledger_benchmark.exs` uses 1,000 persisted
issue records and 1,000 updates in an isolated temporary ledger:

- Zero-token updates: 1.334 ms, zero writes (audited implementation: about 2,608 ms).
- Positive updates: 1.230 ms before flush; one final write; expected 2,000 tokens persisted.
- Snapshot size: 57,894 bytes. This is a synthetic local measurement, not a production SLA.

The real pilot's separate usage log initially contained 431 observations but only
11 distinct usage/final tuples. Replaying the later 440-record log through the
deduplication path produced 22 records (195,294 bytes to 9,752 bytes), retaining
identical summary totals. Replay took 26.069 ms. Summary endpoints still read that
JSONL file; long-duration log growth has not been load-tested.

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
Its operator fix now has a staging PR and independent review; human merge and
deployment remain pending. See [operator instructions](../docs/studio-operations.md).

## Real pilot startup finding

The first pilot reached a fresh staging checkout and completed dependency setup.
Codex 0.153.4 rejected the legacy `reject` approval-policy variant at `thread/start`.
The three-dispatch cap held the issue as designed; all attempts consumed zero model
tokens and zero turns. Studio was stopped cleanly, the lock released, and the issue
held while correcting the root cause. Salesight continued running.

An independent real app-server probe reproduced that exact error. With `granular`
and all five flags false, initialize and thread/start succeeded in 0.929 seconds,
returning `gpt-6-astra`, `low`, and the expected approval/sandbox settings. The probe
sent no `turn/start`. Both default configuration and canonical Studio workflow now
use the supported policy; explicit existing workflow policies are preserved.
Preflight now checks actual approval and sandbox values against the installed
generated schema on every launch, rather than only checking field presence.

The next two dispatches exposed an account-limit interruption and the legacy
workspace sandbox's read-only Git metadata. The agent correctly held the issue
after branch creation was denied. Their 121,494 effective tokens and two dispatches
were preserved. Only the earlier zero-token startup attempts were recovered, with
the original ledger backup and recovery metadata retained.

The final runtime uses a named profile extending `:workspace`, explicitly allowing
`.git` writes while retaining `.codex` and `.agents` as read-only. A disposable
probe verifies normal file/Git branch writes, protected-path reads, and denied
protected/outside-home writes on every install and launch. The exact reviewed
command hash prevents competing model/profile flags; runtime startup verifies the
selected profile. See [Codex permissions](https://developers.openai.com/codex/permissions).
Legacy thread/turn sandbox fields are omitted in this opt-in mode because Codex
otherwise replaces the named profile. The default behavior for other workflows
remains available.

The third paid dispatch successfully created the issue branch but failed during
Bun installation. Reproduction in the actual paused Studio workspace established
that Codex filters `BUN_INSTALL_CACHE_DIR` inherited from the launcher. Hook exports
therefore did not solve agent-shell installs. With explicit per-tool
`shell_environment_policy.set`, bare `bun install --frozen-lockfile` and the real
Studio postinstall passed in 0.853 seconds; tracked files were unchanged. An
explicit inner-shell environment also passed in 1.295 seconds. No broad filesystem
grant was needed. The installed runtime now applies that setting for every tool
shell and validates it using an offline archive-extraction fixture under home,
outside system-temp permissions.

The paid pilot consumed **193,105 effective tokens across three dispatches**.
Those counters and the earlier recovery audit remain intact; no paid allowance
was reset or raised. After those environment failures, a supervising Astra/low
implementation agent completed the scoped operator fix, a separate Astra/low
agent reviewed it, and the parent independently reran its tests. This was a
supervised completion, not a successful autonomous Symphony review dispatch.
SPK-1022 is held for human review and must not be restarted to bypass its cap.
The stopped ledger retains its last observed runtime snapshot; that snapshot is
not evidence of a live process or the later supervised commit.

Run-reviewer checks covered the interrupted runs and this long supervising task.
The task exceeded the duration/token review thresholds; repeated environment
reproduction accounted for substantial avoidable cost. Future batches should run
the real schema, Git and full dependency preflight before the first model turn.
Per-action attribution is inferred, and unavailable tool telemetry is not treated
as zero cost.
