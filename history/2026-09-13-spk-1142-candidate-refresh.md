# SPK-1142 candidate prerequisite refresh proof

Last verified: 2026-09-13. Base: [Symphony main e9bf767](https://github.com/SynchronDEV/symphony/commit/e9bf767f0d49a363a27520b22f065dc7e4624e54).
Tracking: [SPK-1142](https://linear.app/ante-digital/issue/SPK-1142/refresh-cached-symphony-blockers-when-a-prerequisite-completes).

## Behavior

Linear can complete a prerequisite without changing the child issue's update timestamp.
The incremental poll retained the child's old `blocked_by` snapshot and skipped the final
dispatch refresh, leaving newly eligible work idle until a full refresh.

Delta polls now read the unique unresolved prerequisite IDs from retained children absent
from that delta, using the existing paginated tracker boundary. Only blocker observations
are updated in the current cache. Fresh child records win, and a child removed during the
observation cannot be recreated. Unknown or missing parent states remain blocking. Read
errors use the existing candidate-poll error/backoff path without advancing the cutoff.
Full/manual refreshes replace the cache without the additional prerequisite lookup.

The existing final child refresh still enforces current dependencies and routing. Slot
reservations, attempts, tokens, retries, leases, and worker-readiness policy are unchanged.

## Regression evidence

- Original main: the initial eight-case fixture failed three cases: unchanged child
  eligibility, shared prerequisite refresh, and fail-closed admission when a queried
  prerequisite's cached snapshots disagree. Five controls passed. Exit 2.
- Patched source: the initial eight cases passed. Eleven final cases plus the existing
  asynchronous lifecycle suite passed: 38 tests, zero failures, exit 0.
- Full gate on final source: 418 tests, zero failures, two existing live-test skips;
  formatting, spec checks, strict Credo, configured coverage (100%), and Dialyzer passed.
  Exit 0. The full test stage took 33.9 seconds, seed 377006.
- The first full-gate attempt stopped at Credo before full tests. Helper nesting and two
  test lines were corrected; focused lint passed before the successful final gate.
- `git diff --check` passed. `mix.lock`, `mix.exs`, and `WORKFLOW.md` are unchanged.

Focused command, from `elixir/`:

```sh
TMPDIR=/tmp LINEAR_API_KEY='' mise exec -- mix test \
  test/symphony_elixir/candidate_dependency_refresh_test.exs \
  test/symphony_elixir/orchestrator_async_regression_test.exs
```

Normal final gate, from `elixir/`:

```sh
TMPDIR=/tmp LINEAR_API_KEY='' mise exec -- make all
```

The eleven new cases cover unchanged child/parent completion; all active child states;
missing, unknown and failed observations; shared prerequisites fetched once; one available
slot reserved once; renewed blocking rejected by the final child read; unchanged ledger
and token totals; fresh child state/routing and relation precedence; manual/coalesced
refresh; in-flight cache removal; and ordinary children requiring no prerequisite query.

## Source identity and receipts

Final SHA-256:

- `elixir/lib/symphony_elixir/orchestrator.ex`:
  `e8bfd1e1dbede359480d7f8405bade42fc20d7c7ee635d61730e98e330ece84c`
- `elixir/test/symphony_elixir/candidate_dependency_refresh_test.exs`:
  `93ecf0364db4041e18a41e5f2fb13238ed8aae844080c8ab4e38d1c867d62198`

Durable operator receipts are retained outside the source tree:

- `/tmp/spk1142-focused-red.log` and `.exit`
- `/tmp/spk1142-focused-green.log` and `.exit`
- `/tmp/spk1142-focused-final.log` and `.exit`
- `/tmp/spk1142-core-make-all.log` and `.exit` (initial Credo failure)
- `/tmp/spk1142-style-final.log` and `.exit`
- `/tmp/spk1142-core-make-all-final.log` and `.exit` (final passing gate)

No UI changed; visual proof is not applicable. These are isolated source/test results,
not live Linear dispatch or rollout proof. No installed runtime/workflow, running workers,
ledger, counters, or budgets were changed. Independent review and a new immutable candidate
with both preflights and an idle rollout are required before activation.
