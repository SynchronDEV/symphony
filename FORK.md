# SynchronDEV/symphony — fork of openai/symphony

Maintained fork of OpenAI's experimental Symphony orchestrator, driven by
production use dispatching Codex agents against the Spektra repo (Linear team
Synchron). Upstream is an unmaintained reference implementation; this fork
fixes the failure modes we hit running it continuously.

## Already landed (diverges from upstream)

- `stop_continue_labels` — issues carrying a configured label are excluded from
  dispatch, retry, AND have their running agent terminated even while the issue
  sits in an active state. Fixes the merged-but-state-bounced zombie class
  (observed: one issue burned ~47M tokens re-reviewing already-merged code).
- Auto-decline MCP elicitation requests — headless agents wedged forever on
  `mcpServer/elicitation/request`; we answer `decline` and emit an event.
- Hard backoff (≥5 min) for tracker rate-limit retry failures — Linear reports
  `RATELIMITED` with HTTP 400 and a shared 2500 req/hr window; the stock
  10s·2ⁿ retry cadence kept the budget pinned at zero (observed: ~44 failed
  agent runs per issue). NOTE: current detection is a string-match stopgap that
  treats all 400s as rate limits; superseded by Phase 1 item 2 below.

## Roadmap

### Phase 0 — rebase onto upstream #88–#90

`main` is currently based 3 commits behind upstream tip. Upstream #88
("Require opt-in labels for dispatch", `tracker.required_labels`) is the
positive mirror of our `stop_continue_labels` and ships its own label
normalization on the Issue struct plus gating across dispatch / retry /
reconciliation / continuation. Rebasing conflicts in 4 files
(agent_runner.ex, linear/issue.ex, core_test.exs,
workspace_and_config_test.exs). The right resolution is to re-express
`stop_continue_labels` on top of #88's label plumbing — likely shrinking our
patch considerably. Do this before any Phase 1 work.

### Phase 1 — stop losing work and money

1. **Tracker-read failures must not kill agent runs** (`agent_runner.ex`).
   Retry the refresh in place; if still failing, pause the run keeping the
   Codex thread + workspace alive. A bookkeeping read failing is not a reason
   to discard an agent's accumulated context.
2. **Structured rate-limit handling in `linear/client.ex`** (currently has zero
   retry logic). Parse `X-RateLimit-Requests-Remaining`/`-Reset` into a shared
   budget gauge; delay low-priority calls when the budget is low; on
   `RATELIMITED` sleep until reset and retry the request itself. Return
   structured errors (`{:error, {:rate_limited, reset_at}}`).
3. **Delete the string-match heuristic** (`rate_limited_error?/1`) once item 2
   lands — it currently misclassifies every HTTP 400 as a rate limit, so
   genuine bad requests back off 5 min and retry forever instead of failing fast.
4. **Per-issue token budget** (`agent.max_tokens_per_issue`). Accumulation from
   `thread/tokenUsage/updated` already exists (orchestrator.ex ~1670–1706) and
   only feeds the dashboard. Enforce before each continuation; on breach:
   interrupt, comment on the issue with the final count, apply
   `symphony-budget-exceeded` (reuses stop_continue machinery).
5. **Per-issue dispatch/rework caps** (`agent.max_dispatch_attempts`,
   `agent.max_rework_cycles`). On breach: `symphony-stuck` label + comment.
6. **Persist orchestrator state** (new `ledger` module; SQLite/DETS/JSONL keyed
   by issue id: dispatch_count, rework_count, cumulative_tokens,
   blocked_reason, last_thread_id). The memory-only blocked map currently
   forgets wedges on restart and re-dispatches into them; also provides the
   counters for items 4–5.

### Phase 2 — stop paying the re-exploration tax

7. **Same-thread stall recovery**: on stall/turn-timeout send
   `thread/interrupt` and continue on the same thread instead of killing the
   process and starting a cold thread that re-reads the whole repo.
8. **Smarter stall detection**: reset the stall clock on any item activity
   (running `commandExecution`, `outputDelta`), not just message traffic — a
   long silent test run is progress, not a stall.
9. **Previous-attempt context injection**: template gets
   `previous_attempt: {last_agent_message, dirty_files, commits_ahead,
   turns_used, token_total}` on retries/rework, so a restart resumes from
   notes instead of starting over. (Distinct from upstream's reverted #84/#85
   cross-restart Linear-comment resume — this is orchestrator-internal.)
10. **Per-state prompt templates** (`prompt_template_by_state`) so implementer
    and reviewer agents each carry only their half of the protocol.

### Phase 3 — polling and infrastructure efficiency

11. **Delta polling**: steady-state poll filters `updatedAt > lastPollAt`;
    full re-query only on startup and explicit refresh. (~2,880 full-project
    GraphQL queries/day at idle today.)
12. **Batch per-turn issue-state refreshes** through the existing
    `fetch_issue_states_by_ids/1` instead of one call per agent per turn.
13. **Retry jitter (±25%) + FIFO slot queue** (replace the "No available
    slots… retrying again" timer spin).
14. **Workspace creation via local `--reference` mirror** + `workspace.env`
    config for shared caches (`REMOTION_BROWSER_EXECUTABLE`, turbo/vite cache
    dirs). Today each workspace is ~2.1 GB incl. a private headless-Chrome
    download.
15. **Async workspace cleanup + retention policy** (supervised Task on
    terminal state; `workspace.max_total_gb` / keep-last-N) instead of
    synchronous cleanup at startup that blocks the poll loop.

### Phase 4 — observability

16. **Per-issue metrics ledger**: on terminal state emit
    `{issue, pr, tokens, turns, rework_cycles, retries, wall_time, merged_at}`
    (formalizes the manual `metrics.jsonl` convention).
17. **Status API**: expose blocked reasons, per-issue token totals, retry
    counts, stall events via the observability controller; keep logging + port
    active in TUI mode (monitoring currently scrapes `tmux capture-pane`).
18. **Configurable elicitation policy** (`codex.elicitation_policy:
    decline | block`) + record declined elicitations in the ledger.

### Workflow-side companions (not in this repo)

- Repo map for agents: move the component→location table into `AGENTS.md` (or
  a generated `REPO_MAP.md`) — Codex never reads `CLAUDE.md`.
- Cache-friendly prompt ordering in `WORKFLOW.md`: static protocol first,
  `{{ issue.* }}` interpolation last, so concurrent agents share an OpenAI
  prompt-cache prefix.

## Conventions

- `main` tracks `upstream/main` + our patches, rebased when upstream moves.
- One branch + PR per roadmap item; all changes unit-tested against the
  in-memory tracker (`tracker/memory.ex`) and the app-server seam.
- Run `mix test` in `elixir/` before pushing (one test compares
  `System.tmp_dir!()` and fails under a sandboxed `TMPDIR`; that failure is
  environmental).
