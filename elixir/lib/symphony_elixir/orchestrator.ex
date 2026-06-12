defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls Linear and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{AgentRunner, Config, Ledger, StatusDashboard, TokenUsageLedger, Tracker, Workspace}
  alias SymphonyElixir.Linear.Issue

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  # Dedicated backoff for tracker (Linear) rate-limit failures. The normal
  # 10s*2^n backoff is far too aggressive for Linear's 1-hour rolling 2500-req
  # window: many agents each retrying every 10-40s keep the budget pinned at 0,
  # so it never recovers (self-perpetuating thrash). A 429/issue_state_refresh
  # failure means "stop hitting the API", so we wait minutes, not seconds.
  @rate_limit_retry_ms 300_000
  @minimum_claim_lease_ttl_ms 60_000
  @claim_lease_ttl_poll_multiplier 3
  @claim_lease_marker_interval_ms 60_000
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    @type t :: %__MODULE__{}

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :tick_timer_ref,
      :tick_token,
      candidate_cache: %{},
      last_candidate_poll_at: nil,
      force_full_poll?: true,
      slot_queue: [],
      running: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      blocked: %{},
      retry_attempts: %{},
      claim_leases: %{},
      expired_claims: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    now_ms = System.monotonic_time(:millisecond)
    config = Config.settings!()

    state = %State{
      poll_interval_ms: config.polling.interval_ms,
      max_concurrent_agents: config.agent.max_concurrent_agents,
      next_poll_due_at_ms: now_ms,
      poll_check_in_progress: false,
      tick_timer_ref: nil,
      tick_token: nil,
      candidate_cache: %{},
      last_candidate_poll_at: nil,
      force_full_poll?: true,
      slot_queue: [],
      codex_totals: @empty_codex_totals,
      codex_rate_limits: nil
    }

    run_terminal_workspace_cleanup()
    state = schedule_tick(state, 0)

    {:ok, state}
  end

  @impl true
  def handle_info({:tick, tick_token}, %{tick_token: tick_token} = state)
      when is_reference(tick_token) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = maybe_dispatch(state)
    state = schedule_tick(state, state.poll_interval_ms)
    state = %{state | poll_check_in_progress: false}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        {:noreply, state}

      issue_id ->
        {running_entry, state} = pop_running_entry(state, issue_id)
        state = record_session_completion_totals(state, running_entry)
        session_id = running_entry_session_id(running_entry)

        state =
          reason
          |> handle_agent_down(state, issue_id, running_entry, session_id)
          |> drain_slot_queue()

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, %{running: running} = state)
      when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        updated_running_entry =
          running_entry
          |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
          |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])

        state =
          state
          |> Map.put(:running, Map.put(running, issue_id, updated_running_entry))
          |> refresh_claim_lease_from_running(issue_id, updated_running_entry)

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)
        :ok = append_token_usage_observation(issue_id, updated_running_entry, update, false)

        Ledger.add_tokens(issue_id, token_delta)
        maybe_record_codex_ledger_event(issue_id, update)

        state =
          state
          |> apply_codex_token_delta(token_delta)
          |> apply_codex_rate_limits(update)
          |> Map.put(:running, Map.put(running, issue_id, updated_running_entry))
          |> refresh_claim_lease_from_running(issue_id, updated_running_entry)
          |> enforce_issue_token_budget(issue_id, updated_running_entry)

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:retry_issue, issue_id, retry_token}, state) do
    result =
      case pop_retry_attempt_state(state, issue_id, retry_token) do
        {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
        :missing -> {:noreply, state}
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp handle_agent_down(:normal, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, :normal)
    else
      Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

      state
      |> complete_issue(issue_id)
      |> schedule_issue_retry(issue_id, 1, %{
        identifier: running_entry.identifier,
        issue_url: running_entry.issue.url,
        delay_type: :continuation,
        worker_host: Map.get(running_entry, :worker_host),
        workspace_path: Map.get(running_entry, :workspace_path),
        previous_attempt: previous_attempt_from_running(running_entry),
        worker_id: lease_worker_id(running_entry)
      })
    end
  end

  defp handle_agent_down(reason, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, reason)
    else
      retry_agent_down(state, issue_id, running_entry, session_id, reason)
    end
  end

  defp block_input_required_agent_down(state, issue_id, running_entry, session_id, reason) do
    error = blocker_error(running_entry, "agent exited: #{inspect(reason)}")

    Logger.warning("Agent task blocked for issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} session_id=#{session_id}: #{error}")

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp maybe_record_codex_ledger_event(issue_id, %{event: :elicitation_auto_declined}) when is_binary(issue_id) do
    Ledger.increment(issue_id, :declined_elicitations)
  end

  defp maybe_record_codex_ledger_event(_issue_id, _update), do: :ok

  defp record_terminal_issue(%Issue{id: issue_id} = issue, metadata) when is_binary(issue_id) do
    attrs =
      %{
        identifier: issue.identifier,
        state: issue.state
      }
      |> maybe_put_terminal_metadata(metadata)

    Ledger.record_terminal(issue_id, attrs)
  end

  defp record_terminal_issue(_issue, _metadata), do: :ok

  defp maybe_put_terminal_metadata(attrs, metadata) when is_map(metadata) do
    attrs
    |> Map.put(:turns_used, Map.get(metadata, :turn_count, Map.get(metadata, :turns_used, 0)))
    |> Map.put(:wall_time, terminal_wall_time(metadata))
  end

  defp maybe_put_terminal_metadata(attrs, _metadata), do: attrs

  defp terminal_wall_time(%{started_at: %DateTime{} = started_at}) do
    DateTime.diff(DateTime.utc_now(), started_at, :second)
  end

  defp terminal_wall_time(_metadata), do: nil

  defp retry_agent_down(state, issue_id, running_entry, session_id, reason) do
    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

    next_attempt = next_retry_attempt_from_running(running_entry)

    schedule_issue_retry(state, issue_id, next_attempt, %{
      identifier: running_entry.identifier,
      issue_url: running_entry.issue.url,
      error: "agent exited: #{inspect(reason)}",
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      previous_attempt: previous_attempt_from_running(running_entry),
      worker_id: lease_worker_id(running_entry)
    })
  end

  defp maybe_dispatch(%State{} = state) do
    state =
      state
      |> reconcile_running_issues()
      |> reconcile_blocked_issues()
      |> refresh_running_claim_leases()

    with :ok <- Config.validate!(),
         :ok <- ensure_workspace_mirror(),
         {:ok, issues} <- fetch_candidate_issues_for_dispatch(state) do
      state
      |> recover_expired_claim_leases(issues)
      |> update_candidate_cache(issues)
      |> drain_slot_queue()
      |> dispatch_cached_issues()
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Linear API token missing in WORKFLOW.md")
        state

      {:error, :missing_linear_project_slug} ->
        Logger.error("Linear project slug missing in WORKFLOW.md")
        state

      {:error, :missing_tracker_kind} ->
        Logger.error("Tracker kind missing in WORKFLOW.md")

        state

      {:error, {:unsupported_tracker_kind, kind}} ->
        Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

        state

      {:error, {:invalid_workflow_config, message}} ->
        Logger.error("Invalid WORKFLOW.md config: #{message}")
        state

      {:error, {:missing_workflow_file, path, reason}} ->
        Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
        state

      {:error, :workflow_front_matter_not_a_map} ->
        Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
        state

      {:error, {:workflow_parse_error, reason}} ->
        Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, reason} ->
        Logger.error("Failed to fetch from Linear: #{inspect(reason)}")
        state
    end
  end

  defp dispatch_cached_issues(%State{} = state) do
    if available_slots(state) > 0 do
      choose_issues_from_cache(state)
    else
      state
    end
  end

  defp ensure_workspace_mirror do
    case Workspace.ensure_mirror() do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Continuing without refreshed workspace mirror: #{inspect(reason)}")
        :ok
    end
  end

  defp fetch_candidate_issues_for_dispatch(%State{} = state) do
    cutoff =
      if Config.settings!().tracker.delta_polling == true and state.force_full_poll? != true do
        state.last_candidate_poll_at
      end

    Tracker.fetch_candidate_issues(cutoff)
  end

  defp update_candidate_cache(%State{} = state, issues) when is_list(issues) do
    cache =
      if state.force_full_poll? == true or is_nil(state.last_candidate_poll_at) do
        %{}
      else
        state.candidate_cache || %{}
      end

    cache =
      Enum.reduce(issues, cache, fn
        %Issue{id: issue_id} = issue, acc when is_binary(issue_id) -> Map.put(acc, issue_id, issue)
        _issue, acc -> acc
      end)

    %{state | candidate_cache: cache, last_candidate_poll_at: DateTime.utc_now(), force_full_poll?: false}
  end

  defp choose_issues_from_cache(%State{} = state) do
    state.candidate_cache
    |> Map.values()
    |> choose_issues(state)
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issue_states_by_ids(running_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_running_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_running_issue_ids(running_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  defp reconcile_blocked_issues(%State{} = state) do
    blocked_ids = Map.keys(state.blocked)

    if blocked_ids == [] do
      state
    else
      case Tracker.fetch_issue_states_by_ids(blocked_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_blocked_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_blocked_issue_ids(blocked_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh blocked issue states: #{inspect(reason)}; keeping blocked issues")

          state
      end
    end
  end

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  @doc false
  @spec dispatch_issue_for_test(Issue.t(), term()) :: term()
  def dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    dispatch_issue(state, issue)
  end

  @doc false
  @spec retry_delay_for_test(pos_integer(), map()) :: non_neg_integer()
  def retry_delay_for_test(attempt, metadata) when is_integer(attempt) and is_map(metadata) do
    retry_delay(attempt, metadata)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil) :: String.t() | nil | :no_worker_capacity
  def select_worker_host_for_test(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host)
  end

  @doc false
  @spec start_claim_lease_for_test(State.t(), Issue.t(), map(), integer() | nil) :: State.t()
  def start_claim_lease_for_test(%State{} = state, %Issue{} = issue, running_entry, attempt \\ nil)
      when is_map(running_entry) do
    start_claim_lease(state, issue, running_entry, attempt)
  end

  @doc false
  @spec refresh_claim_lease_from_running_for_test(State.t(), String.t(), map()) :: State.t()
  def refresh_claim_lease_from_running_for_test(%State{} = state, issue_id, running_entry)
      when is_binary(issue_id) and is_map(running_entry) do
    refresh_claim_lease_from_running(state, issue_id, running_entry)
  end

  @doc false
  @spec recover_expired_claim_leases_for_test(State.t(), [Issue.t()]) :: State.t()
  def recover_expired_claim_leases_for_test(%State{} = state, issues) when is_list(issues) do
    recover_expired_claim_leases(state, issues)
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        record_terminal_issue(issue, Map.get(state.running, issue.id))
        terminate_running_issue(state, issue.id, true)

      !issue_routable?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        terminate_running_issue(state, issue.id, false)

      stop_continue_labeled?(issue) ->
        Logger.info("Issue carries a stop-continue label: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false)

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false)
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_blocked_issue_states(
      rest,
      reconcile_blocked_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_blocked_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Blocked issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        record_terminal_issue(issue, Map.get(state.blocked, issue.id))
        cleanup_issue_workspace(issue.identifier, blocked_issue_worker_host(state, issue.id))
        release_issue_claim(state, issue.id)

      !issue_routable?(issue) ->
        Logger.info("Blocked issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; releasing block")
        release_issue_claim(state, issue.id)

      active_issue_state?(issue.state, active_states) ->
        refresh_blocked_issue_state(state, issue)

      true ->
        Logger.info("Blocked issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        release_issue_claim(state, issue.id)
    end
  end

  defp reconcile_blocked_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_missing_running_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        log_missing_running_issue(state_acc, issue_id)
        terminate_running_issue(state_acc, issue_id, false)
      end
    end)
  end

  defp reconcile_missing_running_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp reconcile_missing_blocked_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        Logger.info("Blocked issue no longer visible during state refresh: issue_id=#{issue_id}; releasing block")
        release_issue_claim(state_acc, issue_id)
      end
    end)
  end

  defp reconcile_missing_blocked_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp log_missing_running_issue(%State{} = state, issue_id) when is_binary(issue_id) do
    case Map.get(state.running, issue_id) do
      %{identifier: identifier} ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id} issue_identifier=#{identifier}; stopping active agent")

      _ ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id}; stopping active agent")
    end
  end

  defp log_missing_running_issue(_state, _issue_id), do: :ok

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp refresh_blocked_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.blocked, issue.id) do
      %{issue: _} = blocked_entry ->
        %{state | blocked: Map.put(state.blocked, issue.id, %{blocked_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp refresh_running_claim_leases(%State{} = state) do
    Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
      refresh_claim_lease_from_running(state_acc, issue_id, running_entry)
    end)
  end

  defp start_claim_lease(%State{} = state, %Issue{} = issue, running_entry, attempt)
       when is_map(running_entry) do
    times = claim_lease_times()

    lease =
      state.claim_leases
      |> Map.get(issue.id, %{})
      |> Map.merge(%{
        issue_id: issue.id,
        identifier: issue.identifier || issue.id,
        state: :active,
        worker_id: lease_worker_id(running_entry),
        worker_host: Map.get(running_entry, :worker_host),
        workspace_path: Map.get(running_entry, :workspace_path),
        attempt: claim_attempt_number(attempt),
        last_seen_at: times.now,
        lease_started_at: times.now,
        lease_expires_at: times.expires_at,
        lease_expires_at_ms: times.expires_at_ms,
        heartbeat_count: 1,
        retry_due_at: nil,
        retry_due_at_ms: nil,
        retry_backoff_ms: nil,
        error: nil
      })

    put_claim_lease(state, issue.id, lease, times.now_ms, force: true)
  end

  defp refresh_claim_lease_from_running(%State{} = state, issue_id, running_entry)
       when is_binary(issue_id) and is_map(running_entry) do
    case Map.get(state.claim_leases, issue_id) do
      nil ->
        state

      lease ->
        times = claim_lease_times()

        refreshed_lease =
          lease
          |> Map.merge(%{
            state: :active,
            worker_id: lease_worker_id(running_entry),
            worker_host: Map.get(running_entry, :worker_host),
            workspace_path: Map.get(running_entry, :workspace_path),
            attempt: Map.get(lease, :attempt, 1),
            last_seen_at: times.now,
            lease_expires_at: times.expires_at,
            lease_expires_at_ms: times.expires_at_ms,
            heartbeat_count: Map.get(lease, :heartbeat_count, 0) + 1,
            retry_due_at: nil,
            retry_due_at_ms: nil,
            retry_backoff_ms: nil,
            error: nil
          })

        put_claim_lease(state, issue_id, refreshed_lease, times.now_ms)
    end
  end

  defp mark_retry_claim_lease(%State{} = state, issue_id, retry_entry) when is_binary(issue_id) do
    case Map.get(state.claim_leases, issue_id) do
      nil ->
        state

      lease ->
        times = claim_lease_times()
        retry_due_at_ms = Map.get(retry_entry, :due_at_ms)
        retry_expires_at_ms = retry_lease_expires_at_ms(times, retry_due_at_ms)

        retry_lease =
          lease
          |> Map.merge(%{
            state: :retrying,
            identifier: retry_entry_value(retry_entry, lease, :identifier, issue_id),
            worker_id: retry_entry_value(retry_entry, lease, :worker_id),
            worker_host: Map.get(retry_entry, :worker_host),
            workspace_path: Map.get(retry_entry, :workspace_path),
            attempt: retry_entry_value(retry_entry, lease, :attempt, 1),
            lease_expires_at: DateTime.add(times.now, retry_expires_at_ms - times.now_ms, :millisecond),
            lease_expires_at_ms: retry_expires_at_ms,
            retry_due_at: retry_due_at(times, retry_due_at_ms),
            retry_due_at_ms: retry_due_at_ms,
            retry_backoff_ms: retry_backoff_ms(times, retry_due_at_ms),
            error: Map.get(retry_entry, :error)
          })

        put_claim_lease(state, issue_id, retry_lease, times.now_ms, force: true)
    end
  end

  defp retry_entry_value(retry_entry, lease, key, fallback \\ nil) do
    Map.get(retry_entry, key) || Map.get(lease, key) || fallback
  end

  defp retry_lease_expires_at_ms(times, retry_due_at_ms) do
    max(times.expires_at_ms, (retry_due_at_ms || times.now_ms) + claim_lease_ttl_ms())
  end

  defp retry_due_at(_times, nil), do: nil

  defp retry_due_at(times, retry_due_at_ms) when is_integer(retry_due_at_ms) do
    DateTime.add(times.now, retry_due_at_ms - times.now_ms, :millisecond)
  end

  defp retry_backoff_ms(_times, nil), do: nil

  defp retry_backoff_ms(times, retry_due_at_ms) when is_integer(retry_due_at_ms) do
    max(0, retry_due_at_ms - times.now_ms)
  end

  defp mark_blocked_claim_lease(%State{} = state, issue_id, blocked_entry) when is_binary(issue_id) do
    case Map.get(state.claim_leases, issue_id) do
      nil ->
        state

      lease ->
        times = claim_lease_times()

        blocked_lease =
          lease
          |> Map.merge(%{
            state: :blocked,
            identifier: Map.get(blocked_entry, :identifier) || Map.get(lease, :identifier) || issue_id,
            worker_id: Map.get(lease, :worker_id),
            worker_host: Map.get(blocked_entry, :worker_host),
            workspace_path: Map.get(blocked_entry, :workspace_path),
            session_id: Map.get(blocked_entry, :session_id),
            lease_expires_at: times.expires_at,
            lease_expires_at_ms: times.expires_at_ms,
            error: Map.get(blocked_entry, :error)
          })

        put_claim_lease(state, issue_id, blocked_lease, times.now_ms, force: true)
    end
  end

  defp recover_expired_claim_leases(%State{} = state, issues) when is_list(issues) do
    now_ms = System.monotonic_time(:millisecond)
    issues_by_id = Map.new(issues, &{&1.id, &1})

    state.claim_leases
    |> Enum.filter(fn {issue_id, lease} ->
      claim_lease_expired?(lease, now_ms) and not live_claim?(state, issue_id)
    end)
    |> Enum.reduce(state, fn {issue_id, lease}, state_acc ->
      recover_expired_claim_lease(state_acc, issue_id, lease, Map.get(issues_by_id, issue_id), now_ms)
    end)
  end

  defp recover_expired_claim_leases(state, _issues), do: state

  defp recover_expired_claim_lease(%State{} = state, issue_id, lease, %Issue{} = issue, _now_ms) do
    if retry_candidate_issue?(issue, terminal_state_set()) do
      expired_at = DateTime.utc_now()
      error = "claim lease expired at #{iso8601(expired_at)}; requeueing"
      attempt = expired_retry_attempt(state, issue_id, lease)

      Logger.warning("Claim lease expired; requeueing issue_id=#{issue_id} issue_identifier=#{issue.identifier} attempt=#{attempt}")

      state
      |> put_expired_claim(issue_id, lease, expired_at, error)
      |> schedule_issue_retry(issue_id, attempt, %{
        identifier: issue.identifier,
        error: error,
        worker_host: Map.get(lease, :worker_host),
        workspace_path: Map.get(lease, :workspace_path),
        worker_id: Map.get(lease, :worker_id),
        delay_ms: 0
      })
    else
      Logger.warning("Claim lease expired for non-candidate issue_id=#{issue_id}; releasing claim")
      release_issue_claim(state, issue_id)
    end
  end

  defp recover_expired_claim_lease(%State{} = state, issue_id, _lease, _issue, _now_ms) do
    Logger.warning("Claim lease expired for invisible issue_id=#{issue_id}; releasing claim")
    release_issue_claim(state, issue_id)
  end

  defp put_expired_claim(%State{} = state, issue_id, lease, expired_at, error) do
    expired_claim =
      lease
      |> Map.merge(%{
        state: :expired,
        expired_at: expired_at,
        requeued_at: DateTime.utc_now(),
        error: error
      })

    %{state | expired_claims: Map.put(state.expired_claims, issue_id, expired_claim)}
  end

  defp put_claim_lease(%State{} = state, issue_id, lease, now_ms, opts \\ []) do
    previous = Map.get(state.claim_leases, issue_id)
    lease = maybe_publish_claim_lease_marker(issue_id, previous, lease, now_ms, opts)

    %{state | claim_leases: Map.put(state.claim_leases, issue_id, lease)}
  end

  defp maybe_publish_claim_lease_marker(issue_id, previous, lease, now_ms, opts) do
    if claim_lease_marker_due?(previous, lease, now_ms, opts) do
      body = claim_lease_marker_body(lease)

      case safe_create_tracker_comment(issue_id, body) do
        :ok -> Map.put(lease, :last_marker_at_ms, now_ms)
        {:error, _reason} -> inherit_last_marker_at(lease, previous)
      end
    else
      inherit_last_marker_at(lease, previous)
    end
  end

  defp claim_lease_marker_due?(nil, _lease, _now_ms, _opts), do: true

  defp claim_lease_marker_due?(previous, lease, now_ms, opts) do
    Keyword.get(opts, :force, false) or
      claim_lease_material_change?(previous, lease) or
      claim_lease_marker_interval_due?(previous, now_ms)
  end

  defp claim_lease_material_change?(previous, lease) do
    fields = [:state, :worker_id, :worker_host, :workspace_path, :attempt, :retry_due_at_ms, :error]

    Enum.any?(fields, fn field ->
      Map.get(previous, field) != Map.get(lease, field)
    end)
  end

  defp claim_lease_marker_interval_due?(previous, now_ms) do
    case Map.get(previous, :last_marker_at_ms) do
      marker_at_ms when is_integer(marker_at_ms) -> now_ms - marker_at_ms >= @claim_lease_marker_interval_ms
      _ -> true
    end
  end

  defp inherit_last_marker_at(lease, nil), do: lease

  defp inherit_last_marker_at(lease, previous) do
    Map.put(lease, :last_marker_at_ms, Map.get(previous, :last_marker_at_ms))
  end

  defp safe_create_tracker_comment(issue_id, body) do
    Tracker.create_comment(issue_id, body)
  rescue
    error ->
      Logger.warning("Failed to write claim lease marker for issue_id=#{issue_id}: #{Exception.message(error)}")
      {:error, error}
  catch
    kind, reason ->
      Logger.warning("Failed to write claim lease marker for issue_id=#{issue_id}: #{inspect({kind, reason})}")
      {:error, reason}
  end

  defp claim_lease_marker_body(lease) do
    ["## Symphony Claim Lease", "" | claim_lease_marker_lines(lease)]
    |> Enum.join("\n")
  end

  defp claim_lease_marker_lines(lease) do
    [
      {:state, Map.get(lease, :state), "n/a"},
      {:worker_id, Map.get(lease, :worker_id), "n/a"},
      {:worker_host, Map.get(lease, :worker_host), "local"},
      {:workspace_path, Map.get(lease, :workspace_path), "pending"},
      {:attempt, Map.get(lease, :attempt), 1},
      {:last_seen_at, iso8601(Map.get(lease, :last_seen_at)), "n/a"},
      {:lease_expires_at, iso8601(Map.get(lease, :lease_expires_at)), "n/a"},
      {:retry_due_at, iso8601(Map.get(lease, :retry_due_at)), "n/a"},
      {:retry_backoff_ms, Map.get(lease, :retry_backoff_ms), "n/a"},
      {:error, Map.get(lease, :error), "n/a"}
    ]
    |> Enum.map(fn {key, value, fallback} -> "- #{key}: #{value || fallback}" end)
  end

  defp claim_lease_expired?(lease, now_ms) when is_map(lease) and is_integer(now_ms) do
    case Map.get(lease, :lease_expires_at_ms) do
      expires_at_ms when is_integer(expires_at_ms) -> expires_at_ms <= now_ms
      _ -> false
    end
  end

  defp live_claim?(%State{} = state, issue_id) do
    Map.has_key?(state.running, issue_id) or Map.has_key?(state.blocked, issue_id)
  end

  defp expired_retry_attempt(%State{} = state, issue_id, lease) do
    lease_attempt = Map.get(lease, :attempt, 1)
    retry_attempt = state.retry_attempts |> Map.get(issue_id, %{}) |> Map.get(:attempt, 0)

    max(lease_attempt, retry_attempt) + 1
  end

  defp claim_lease_times do
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)
    ttl_ms = claim_lease_ttl_ms()

    %{
      now: now,
      now_ms: now_ms,
      expires_at: DateTime.add(now, ttl_ms, :millisecond),
      expires_at_ms: now_ms + ttl_ms
    }
  end

  defp claim_lease_ttl_ms do
    Config.settings!().polling.interval_ms
    |> Kernel.*(@claim_lease_ttl_poll_multiplier)
    |> max(@minimum_claim_lease_ttl_ms)
  end

  defp claim_attempt_number(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp claim_attempt_number(_attempt), do: 1

  defp lease_worker_id(running_entry) when is_map(running_entry) do
    case Map.get(running_entry, :worker_id) do
      worker_id when is_binary(worker_id) and worker_id != "" ->
        worker_id

      _ ->
        host = Map.get(running_entry, :worker_host) || "local"
        pid = Map.get(running_entry, :pid)
        "#{host}:#{if(is_pid(pid), do: inspect(pid), else: "unknown")}"
    end
  end

  defp restore_claim_lease(state, _issue_id, nil), do: state

  defp restore_claim_lease(%State{} = state, issue_id, lease) when is_binary(issue_id) and is_map(lease) do
    %{state | claim_leases: Map.put(state.claim_leases, issue_id, lease)}
  end

  defp claim_lease_snapshot_entry(lease, now_ms) do
    %{
      issue_id: Map.get(lease, :issue_id),
      identifier: Map.get(lease, :identifier),
      state: lease_state_string(Map.get(lease, :state)),
      worker_id: Map.get(lease, :worker_id),
      worker_host: Map.get(lease, :worker_host),
      workspace_path: Map.get(lease, :workspace_path),
      attempt: Map.get(lease, :attempt),
      last_seen_at: Map.get(lease, :last_seen_at),
      lease_expires_at: Map.get(lease, :lease_expires_at),
      lease_expires_in_ms: lease_expires_in_ms(lease, now_ms),
      retry_due_at: Map.get(lease, :retry_due_at),
      retry_due_in_ms: retry_due_in_ms(lease, now_ms),
      retry_backoff_ms: Map.get(lease, :retry_backoff_ms),
      error: Map.get(lease, :error)
    }
  end

  defp expired_claim_snapshot_entry(lease, now_ms) do
    lease
    |> claim_lease_snapshot_entry(now_ms)
    |> Map.merge(%{
      state: "expired",
      expired_at: Map.get(lease, :expired_at),
      requeued_at: Map.get(lease, :requeued_at)
    })
  end

  defp lease_expires_in_ms(lease, now_ms) do
    case Map.get(lease, :lease_expires_at_ms) do
      expires_at_ms when is_integer(expires_at_ms) -> expires_at_ms - now_ms
      _ -> nil
    end
  end

  defp retry_due_in_ms(lease, now_ms) do
    case Map.get(lease, :retry_due_at_ms) do
      due_at_ms when is_integer(due_at_ms) -> max(0, due_at_ms - now_ms)
      _ -> nil
    end
  end

  defp lease_state_string(state) when is_atom(state), do: Atom.to_string(state)
  defp lease_state_string(state) when is_binary(state), do: state
  defp lease_state_string(_state), do: nil

  defp iso8601(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp iso8601(_datetime), do: nil

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        state = record_session_completion_totals(state, running_entry)
        worker_host = Map.get(running_entry, :worker_host)

        if cleanup_workspace do
          cleanup_issue_workspace(identifier, worker_host)
        end

        stop_running_task(pid, ref)

        %{
          state
          | running: Map.delete(state.running, issue_id),
            claimed: MapSet.delete(state.claimed, issue_id),
            blocked: Map.delete(state.blocked, issue_id),
            retry_attempts: Map.delete(state.retry_attempts, issue_id),
            claim_leases: Map.delete(state.claim_leases, issue_id),
            expired_claims: Map.delete(state.expired_claims, issue_id)
        }

      _ ->
        release_issue_claim(state, issue_id)
    end
  end

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.settings!().codex.stall_timeout_ms

    cond do
      timeout_ms <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          maybe_restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp maybe_restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    if Map.has_key?(state.blocked, issue_id) do
      state
    else
      restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if is_integer(elapsed_ms) and elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      if input_required_blocker?(running_entry) do
        error = blocker_error(running_entry, "stalled for #{elapsed_ms}ms after Codex requested operator input")
        Ledger.increment(issue_id, :stall_events)

        Logger.warning("Issue blocked: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; #{error}")

        state
        |> record_session_completion_totals(running_entry)
        |> stop_and_block_issue(issue_id, running_entry, error)
      else
        Ledger.increment(issue_id, :stall_events)
        Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

        next_attempt = next_retry_attempt_from_running(running_entry)
        previous_lease = Map.get(state.claim_leases, issue_id)

        state
        |> terminate_running_issue(issue_id, false)
        |> restore_claim_lease(issue_id, previous_lease)
        |> schedule_issue_retry(issue_id, next_attempt, %{
          identifier: identifier,
          issue_url: running_entry.issue.url,
          error: "stalled for #{elapsed_ms}ms without codex activity",
          worker_host: Map.get(running_entry, :worker_host),
          workspace_path: Map.get(running_entry, :workspace_path),
          previous_attempt: previous_attempt_from_running(running_entry),
          worker_id: lease_worker_id(running_entry)
        })
      end
    else
      state
    end
  end

  defp stall_elapsed_ms(running_entry, now) do
    running_entry
    |> last_activity_timestamp()
    |> case do
      %DateTime{} = timestamp ->
        max(0, DateTime.diff(now, timestamp, :millisecond))

      _ ->
        nil
    end
  end

  defp last_activity_timestamp(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_timestamp) || Map.get(running_entry, :started_at)
  end

  defp last_activity_timestamp(_running_entry), do: nil

  defp input_required_blocker?(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_event) in [:turn_input_required, :approval_required] or
      not is_nil(input_required_completion_outcome(Map.get(running_entry, :completion))) or
      codex_message_method(Map.get(running_entry, :last_codex_message)) ==
        "mcpServer/elicitation/request"
  end

  defp input_required_blocker?(_running_entry), do: false

  defp input_required_completion_outcome(completion) when is_map(completion) do
    outcome = Map.get(completion, :outcome) || Map.get(completion, "outcome")
    normalize_input_required_outcome(outcome)
  end

  defp input_required_completion_outcome(_completion), do: nil

  defp normalize_input_required_outcome(outcome)
       when outcome in [:input_required, :needs_input, :approval_required],
       do: outcome

  defp normalize_input_required_outcome(outcome) when is_binary(outcome) do
    case outcome do
      "input_required" -> :input_required
      "needs_input" -> :needs_input
      "approval_required" -> :approval_required
      _ -> nil
    end
  end

  defp normalize_input_required_outcome(_outcome), do: nil

  defp blocker_error(running_entry, fallback) when is_map(running_entry) do
    codex_event_blocker_error(Map.get(running_entry, :last_codex_event)) ||
      completion_blocker_error(Map.get(running_entry, :completion)) ||
      codex_message_blocker_error(Map.get(running_entry, :last_codex_message)) ||
      fallback
  end

  defp blocker_error(_running_entry, fallback), do: fallback

  defp codex_event_blocker_error(:turn_input_required), do: "codex turn requires operator input"
  defp codex_event_blocker_error(:approval_required), do: "codex turn requires approval"
  defp codex_event_blocker_error(_event), do: nil

  defp completion_blocker_error(completion) do
    case input_required_completion_outcome(completion) do
      outcome when outcome in [:input_required, :needs_input] -> "codex turn requires operator input"
      :approval_required -> "codex turn requires approval"
      nil -> nil
    end
  end

  defp codex_message_blocker_error(message) do
    if codex_message_method(message) == "mcpServer/elicitation/request" do
      "codex MCP elicitation requires operator input"
    end
  end

  defp codex_message_method(%{message: %{"method" => method}}) when is_binary(method), do: method
  defp codex_message_method(%{message: %{method: method}}) when is_binary(method), do: method
  defp codex_message_method(%{"method" => method}) when is_binary(method), do: method
  defp codex_message_method(%{method: method}) when is_binary(method), do: method
  defp codex_message_method(_message), do: nil

  defp terminate_task(pid) when is_pid(pid) do
    case Task.Supervisor.terminate_child(SymphonyElixir.TaskSupervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid), do: :ok

  defp stop_running_task(pid, ref) do
    if is_pid(pid) do
      terminate_task(pid)
    end

    if is_reference(ref) do
      Process.demonitor(ref, [:flush])
    end

    :ok
  end

  defp stop_and_block_issue(%State{} = state, issue_id, running_entry, error) do
    stop_running_task(Map.get(running_entry, :pid), Map.get(running_entry, :ref))
    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp block_issue_from_entry(%State{} = state, issue_id, running_entry, error) do
    Ledger.put(issue_id, %{
      blocked_reason: error,
      last_thread_id: running_entry_session_id(running_entry)
    })

    blocked_entry = %{
      issue_id: issue_id,
      identifier: Map.get(running_entry, :identifier, issue_id),
      issue: Map.get(running_entry, :issue),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      session_id: running_entry_session_id(running_entry),
      error: error,
      blocked_at: DateTime.utc_now(),
      last_codex_message: Map.get(running_entry, :last_codex_message),
      last_codex_event: Map.get(running_entry, :last_codex_event),
      last_codex_timestamp: Map.get(running_entry, :last_codex_timestamp)
    }

    state = %{
      state
      | running: Map.delete(state.running, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        claimed: MapSet.put(state.claimed, issue_id),
        blocked: Map.put(state.blocked, issue_id, blocked_entry)
    }

    mark_blocked_claim_lease(state, issue_id, blocked_entry)
  end

  defp block_issue_without_running(%State{} = state, %Issue{} = issue, reason) do
    Logger.warning("Blocking #{issue_context(issue)}: #{reason}")
    apply_block_label(issue, block_label_for_reason(reason))
    post_blocked_comment(issue, reason)

    Ledger.put(issue.id, %{
      blocked_reason: reason,
      identifier: issue.identifier,
      state: issue.state
    })

    blocked_entry = %{
      issue_id: issue.id,
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: nil,
      session_id: nil,
      error: reason,
      blocked_at: DateTime.utc_now(),
      last_codex_message: nil,
      last_codex_event: nil,
      last_codex_timestamp: nil
    }

    %{
      state
      | retry_attempts: Map.delete(state.retry_attempts, issue.id),
        claimed: MapSet.put(state.claimed, issue.id),
        blocked: Map.put(state.blocked, issue.id, blocked_entry)
    }
  end

  defp post_blocked_comment(%Issue{id: issue_id}, reason) when is_binary(issue_id) do
    body = """
    Symphony paused this issue.

    Reason: #{reason}
    """

    case Tracker.create_comment(issue_id, String.trim(body)) do
      :ok -> :ok
      {:error, comment_reason} -> Logger.warning("Failed to post Symphony block comment for issue_id=#{issue_id}: #{inspect(comment_reason)}")
    end
  end

  defp apply_block_label(_issue, nil), do: :ok

  defp apply_block_label(%Issue{id: issue_id}, label_name) when is_binary(issue_id) and is_binary(label_name) do
    case Tracker.apply_label(issue_id, label_name) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("Failed to apply Symphony block label #{label_name} for issue_id=#{issue_id}: #{inspect(reason)}")
    end
  end

  defp apply_block_label(_issue, _label_name), do: :ok

  defp block_label_for_reason(reason) when is_binary(reason) do
    cond do
      String.starts_with?(reason, "symphony-budget-exceeded") -> "symphony-budget-exceeded"
      String.starts_with?(reason, "symphony-stuck") -> "symphony-stuck"
      true -> nil
    end
  end

  defp block_label_for_reason(_reason), do: nil

  defp dispatch_cap_status(%Issue{id: issue_id} = issue) when is_binary(issue_id) do
    settings = Config.settings!().agent
    ledger_entry = Ledger.get(issue_id)

    cond do
      is_binary(Map.get(ledger_entry, :blocked_reason)) ->
        {:block, Map.get(ledger_entry, :blocked_reason)}

      cap_reached?(Map.get(ledger_entry, :dispatch_count, 0), settings.max_dispatch_attempts) ->
        {:block, "symphony-stuck: max_dispatch_attempts=#{settings.max_dispatch_attempts} reached after #{Map.get(ledger_entry, :dispatch_count, 0)} dispatches"}

      rework_dispatch?(issue) and cap_reached?(Map.get(ledger_entry, :rework_count, 0), settings.max_rework_cycles) ->
        {:block, "symphony-stuck: max_rework_cycles=#{settings.max_rework_cycles} reached after #{Map.get(ledger_entry, :rework_count, 0)} rework cycles"}

      true ->
        :ok
    end
  end

  defp dispatch_cap_status(_issue), do: :ok

  defp cap_reached?(_count, nil), do: false
  defp cap_reached?(count, cap) when is_integer(count) and is_integer(cap), do: count >= cap
  defp cap_reached?(_count, _cap), do: false

  defp record_dispatch_in_ledger(%Issue{id: issue_id} = issue, worker_host) when is_binary(issue_id) do
    # Rework counting lives in Ledger.observe_state so the agent runner's
    # between-turn refreshes count in-run state bounces too (a full
    # review->rework cycle can happen inside one agent run, invisible to
    # dispatch-time counting).
    Ledger.observe_state(issue_id, issue.state)

    Ledger.update(issue_id, fn entry ->
      entry
      |> Map.update(:dispatch_count, 1, &increment_integer/1)
      |> Map.merge(%{
        identifier: issue.identifier,
        state: issue.state,
        worker_host: worker_host
      })
    end)
  end

  defp record_dispatch_in_ledger(_issue, _worker_host), do: %{}

  defp increment_integer(value) when is_integer(value), do: value + 1
  defp increment_integer(_value), do: 1

  defp rework_dispatch?(%Issue{state: state}) when is_binary(state) do
    normalize_issue_state(state) == "rework"
  end

  defp rework_dispatch?(_issue), do: false

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch_issue(state_acc, issue)
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, claimed: claimed, blocked: blocked} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      !stop_continue_labeled?(issue) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(blocked, issue.id) and
      available_slots(state) > 0 and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp stop_continue_labeled?(%Issue{} = issue) do
    Issue.stop_continue_labeled?(issue, Config.settings!().agent.stop_continue_labels)
  end

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    issue_routable?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp todo_issue_blocked_by_non_terminal?(
         %Issue{state: issue_state, blocked_by: blockers},
         terminal_states
       )
       when is_binary(issue_state) and is_list(blockers) do
    normalize_issue_state(issue_state) == "todo" and
      Enum.any?(blockers, fn
        %{state: blocker_state} when is_binary(blocker_state) ->
          !terminal_issue_state?(blocker_state, terminal_states)

        _ ->
          true
      end)
  end

  defp todo_issue_blocked_by_non_terminal?(_issue, _terminal_states), do: false

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    Config.settings!().tracker.terminal_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.settings!().tracker.active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(%State{} = state, issue, attempt \\ nil, preferred_worker_host \\ nil, metadata \\ %{}) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issue_states_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        case dispatch_cap_status(refreshed_issue) do
          :ok ->
            do_dispatch_issue(state, refreshed_issue, attempt, preferred_worker_host, metadata)

          {:block, reason} ->
            block_issue_without_running(state, refreshed_issue, reason)
        end

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        state

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        state

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp do_dispatch_issue(%State{} = state, issue, attempt, preferred_worker_host, metadata) do
    recipient = self()

    case select_worker_host(state, preferred_worker_host) do
      :no_worker_capacity ->
        Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
        state

      worker_host ->
        spawn_issue_on_worker_host(state, issue, attempt, recipient, worker_host, metadata)
    end
  end

  defp spawn_issue_on_worker_host(%State{} = state, issue, attempt, recipient, worker_host, metadata) do
    previous_attempt = metadata[:previous_attempt] || previous_attempt_from_ledger(issue)

    case Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fn ->
           AgentRunner.run(issue, recipient,
             attempt: attempt,
             worker_host: worker_host,
             previous_attempt: previous_attempt
           )
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")
        ledger_entry = record_dispatch_in_ledger(issue, worker_host)

        running_entry = %{
          pid: pid,
          ref: ref,
          identifier: issue.identifier,
          issue: issue,
          worker_host: worker_host,
          workspace_path: nil,
          session_id: nil,
          last_codex_message: nil,
          last_codex_timestamp: nil,
          last_codex_event: nil,
          codex_app_server_pid: nil,
          codex_input_tokens: 0,
          codex_output_tokens: 0,
          codex_total_tokens: 0,
          codex_last_reported_input_tokens: 0,
          codex_last_reported_output_tokens: 0,
          codex_last_reported_total_tokens: 0,
          turn_count: 0,
          retry_attempt: normalize_retry_attempt(attempt),
          ledger_entry: ledger_entry,
          previous_attempt: previous_attempt,
          started_at: DateTime.utc_now()
        }

        running = Map.put(state.running, issue.id, running_entry)

        state = %{
          state
          | running: running,
            claimed: MapSet.put(state.claimed, issue.id),
            retry_attempts: Map.delete(state.retry_attempts, issue.id),
            expired_claims: Map.delete(state.expired_claims, issue.id)
        }

        start_claim_lease(state, issue, running_entry, attempt)

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

        schedule_issue_retry(state, issue.id, next_attempt, %{
          identifier: issue.identifier,
          issue_url: issue.url,
          error: "failed to spawn agent: #{inspect(reason)}",
          worker_host: worker_host
        })
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    retry_token = make_ref()
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    issue_url = pick_retry_issue_url(previous_retry, metadata)
    error = pick_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)
    previous_attempt = pick_retry_previous_attempt(previous_retry, metadata)
    worker_id = pick_retry_worker_id(previous_retry, metadata)

    Ledger.put(issue_id, %{
      retries: next_attempt,
      identifier: identifier
    })

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms)

    error_suffix = if is_nil(error), do: "", else: " error=#{inspect(error)}"

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    retry_entry = %{
      attempt: next_attempt,
      timer_ref: timer_ref,
      retry_token: retry_token,
      due_at_ms: due_at_ms,
      identifier: identifier,
      issue_url: issue_url,
      error: error,
      worker_host: worker_host,
      workspace_path: workspace_path,
      previous_attempt: previous_attempt,
      worker_id: worker_id
    }

    state = %{
      state
      | claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.put(state.retry_attempts, issue_id, retry_entry)
    }

    mark_retry_claim_lease(state, issue_id, retry_entry)
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          issue_url: Map.get(retry_entry, :issue_url),
          error: Map.get(retry_entry, :error),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path),
          previous_attempt: Map.get(retry_entry, :previous_attempt),
          worker_id: Map.get(retry_entry, :worker_id)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_candidate_issues() do
      {:ok, issues} ->
        issues
        |> find_issue_by_id(issue_id)
        |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

      {:error, reason} ->
        Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt + 1,
           Map.merge(metadata, %{error: reason})
         )}
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        record_terminal_issue(issue, nil)
        cleanup_issue_workspace(issue.identifier, metadata[:worker_host])
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    start_cleanup_task(fn ->
      Workspace.remove_issue_workspaces(identifier, worker_host)
      Workspace.enforce_retention()
    end)
  end

  defp cleanup_issue_workspace(_identifier, _worker_host), do: :ok

  # credo:disable-for-lines:18 Credo.Check.Refactor.Nesting
  defp start_cleanup_task(fun) when is_function(fun, 0) do
    if test_env?() do
      fun.()
    else
      case Process.whereis(SymphonyElixir.TaskSupervisor) do
        pid when is_pid(pid) ->
          case Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fun) do
            {:ok, _pid} -> :ok
            {:error, reason} -> Logger.warning("Unable to start workspace cleanup task: #{inspect(reason)}")
          end

        _ ->
          fun.()
      end
    end

    :ok
  end

  defp test_env? do
    Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test
  end

  defp blocked_issue_worker_host(%State{} = state, issue_id) do
    state.blocked
    |> Map.get(issue_id, %{})
    |> Map.get(:worker_host)
  end

  # credo:disable-for-lines:22 Credo.Check.Refactor.Nesting
  defp run_terminal_workspace_cleanup do
    start_cleanup_task(fn ->
      case Tracker.fetch_issues_by_states(Config.settings!().tracker.terminal_states) do
        {:ok, issues} ->
          issues
          |> Enum.each(fn
            %Issue{identifier: identifier} when is_binary(identifier) ->
              Workspace.remove_issue_workspaces(identifier)

            _ ->
              :ok
          end)

          Workspace.enforce_retention()

        {:error, reason} ->
          Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
      end
    end)
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host]) do
      {:noreply, dispatch_issue(state, issue, attempt, metadata[:worker_host], metadata)}
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; queueing retry")
      {:noreply, enqueue_slot_retry(state, issue, attempt, metadata)}
    end
  end

  defp enqueue_slot_retry(%State{} = state, %Issue{} = issue, attempt, metadata) do
    entry = %{
      issue_id: issue.id,
      issue: issue,
      attempt: attempt,
      metadata:
        Map.merge(metadata, %{
          identifier: issue.identifier,
          error: "waiting for orchestrator slot"
        })
    }

    queue =
      state.slot_queue
      |> reject_slot_queue_issue(issue.id)
      |> Kernel.++([entry])

    %{state | slot_queue: queue}
  end

  defp drain_slot_queue(%State{slot_queue: []} = state), do: state

  defp drain_slot_queue(%State{} = state) do
    do_drain_slot_queue(%{state | slot_queue: []}, state.slot_queue)
  end

  defp do_drain_slot_queue(%State{} = state, []), do: state

  defp do_drain_slot_queue(%State{} = state, [entry | rest]) do
    issue = entry.issue
    metadata = entry.metadata

    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host]) do
      state
      |> dispatch_issue(issue, entry.attempt, metadata[:worker_host], metadata)
      |> do_drain_slot_queue(rest)
    else
      %{state | slot_queue: [entry | rest]}
    end
  end

  defp reject_slot_queue_issue(queue, issue_id) do
    Enum.reject(queue || [], &(&1.issue_id == issue_id))
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{
      state
      | claimed: MapSet.delete(state.claimed, issue_id),
        blocked: Map.delete(state.blocked, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        slot_queue: reject_slot_queue_issue(state.slot_queue, issue_id),
        claim_leases: Map.delete(state.claim_leases, issue_id),
        expired_claims: Map.delete(state.expired_claims, issue_id)
    }
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    cond do
      is_integer(metadata[:delay_ms]) and metadata[:delay_ms] >= 0 ->
        metadata[:delay_ms]

      rate_limited_error?(metadata[:error]) ->
        max(rate_limit_retry_delay(metadata[:error]), failure_retry_delay(attempt))

      metadata[:delay_type] == :continuation and attempt == 1 ->
        @continuation_retry_delay_ms

      true ->
        jitter_delay(failure_retry_delay(attempt), metadata)
    end
  end

  defp rate_limited_error?({:rate_limited, _reset_at}), do: true
  defp rate_limited_error?({:linear_api_status, 429}), do: true
  defp rate_limited_error?({:linear_api_request, {:rate_limited, _reset_at}}), do: true
  defp rate_limited_error?(_error), do: false

  defp rate_limit_retry_delay({:rate_limited, %DateTime{} = reset_at}), do: delay_until(reset_at)
  defp rate_limit_retry_delay({:linear_api_request, {:rate_limited, %DateTime{} = reset_at}}), do: delay_until(reset_at)
  defp rate_limit_retry_delay(_error), do: @rate_limit_retry_ms

  defp delay_until(%DateTime{} = reset_at) do
    reset_at
    |> DateTime.diff(DateTime.utc_now(), :millisecond)
    |> max(@rate_limit_retry_ms)
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.settings!().agent.max_retry_backoff_ms)
  end

  defp jitter_delay(delay_ms, metadata) when is_integer(delay_ms) and delay_ms > 0 do
    if metadata[:jitter?] == false or test_env?() do
      delay_ms
    else
      min = trunc(delay_ms * 0.75)
      max = trunc(delay_ms * 1.25)
      min + :rand.uniform(max - min + 1) - 1
    end
  end

  defp jitter_delay(delay_ms, _metadata), do: delay_ms

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp previous_attempt_from_running(running_entry) when is_map(running_entry) do
    workspace_path = Map.get(running_entry, :workspace_path)

    %{
      last_agent_message: summarize_previous_agent_message(Map.get(running_entry, :last_codex_message)),
      dirty_files: dirty_files(workspace_path, Map.get(running_entry, :worker_host)),
      commits_ahead: commits_ahead(workspace_path, Map.get(running_entry, :worker_host)),
      turns_used: Map.get(running_entry, :turn_count, 0),
      token_total: Map.get(running_entry, :codex_total_tokens, 0)
    }
  end

  defp previous_attempt_from_ledger(%Issue{id: issue_id}) when is_binary(issue_id) do
    entry = Ledger.get(issue_id)

    %{
      last_agent_message: Map.get(entry, :last_agent_message),
      dirty_files: Map.get(entry, :dirty_files, []),
      commits_ahead: Map.get(entry, :commits_ahead),
      turns_used: Map.get(entry, :turns_used, 0),
      token_total: Map.get(entry, :cumulative_tokens, 0)
    }
  end

  defp previous_attempt_from_ledger(_issue), do: %{}

  defp summarize_previous_agent_message(nil), do: nil
  defp summarize_previous_agent_message(message), do: StatusDashboard.humanize_codex_message(message)

  defp dirty_files(nil, _worker_host), do: []
  defp dirty_files(_workspace_path, worker_host) when is_binary(worker_host), do: []

  defp dirty_files(workspace_path, nil) when is_binary(workspace_path) do
    if File.dir?(Path.join(workspace_path, ".git")) do
      case System.cmd("git", ["-C", workspace_path, "status", "--short"], stderr_to_stdout: true) do
        {output, 0} ->
          output
          |> String.split("\n", trim: true)
          |> Enum.map(&String.trim/1)
          |> Enum.take(50)

        _ ->
          []
      end
    else
      []
    end
  rescue
    _ -> []
  end

  defp commits_ahead(nil, _worker_host), do: nil
  defp commits_ahead(_workspace_path, worker_host) when is_binary(worker_host), do: nil

  # credo:disable-for-lines:18 Credo.Check.Refactor.Nesting
  defp commits_ahead(workspace_path, nil) when is_binary(workspace_path) do
    if File.dir?(Path.join(workspace_path, ".git")) do
      case System.cmd("git", ["-C", workspace_path, "rev-list", "--count", "@{upstream}..HEAD"], stderr_to_stdout: true) do
        {output, 0} ->
          output
          |> String.trim()
          |> Integer.parse()
          |> case do
            {count, ""} -> count
            _ -> nil
          end

        _ ->
          nil
      end
    end
  rescue
    _ -> nil
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_issue_url(previous_retry, metadata) do
    metadata[:issue_url] || Map.get(previous_retry, :issue_url)
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp pick_retry_worker_host(previous_retry, metadata) do
    metadata[:worker_host] || Map.get(previous_retry, :worker_host)
  end

  defp pick_retry_workspace_path(previous_retry, metadata) do
    metadata[:workspace_path] || Map.get(previous_retry, :workspace_path)
  end

  defp pick_retry_previous_attempt(previous_retry, metadata) do
    metadata[:previous_attempt] || Map.get(previous_retry, :previous_attempt)
  end

  defp pick_retry_worker_id(previous_retry, metadata) do
    metadata[:worker_id] || Map.get(previous_retry, :worker_id)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp select_worker_host(%State{} = state, preferred_worker_host) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        nil

      hosts ->
        available_hosts = Enum.filter(hosts, &worker_host_slots_available?(state, &1))

        cond do
          available_hosts == [] ->
            :no_worker_capacity

          preferred_worker_host_available?(preferred_worker_host, available_hosts) ->
            preferred_worker_host

          true ->
            least_loaded_worker_host(state, available_hosts)
        end
    end
  end

  defp preferred_worker_host_available?(preferred_worker_host, hosts)
       when is_binary(preferred_worker_host) and is_list(hosts) do
    preferred_worker_host != "" and preferred_worker_host in hosts
  end

  defp preferred_worker_host_available?(_preferred_worker_host, _hosts), do: false

  defp least_loaded_worker_host(%State{} = state, hosts) when is_list(hosts) do
    hosts
    |> Enum.with_index()
    |> Enum.min_by(fn {host, index} ->
      {running_worker_host_count(state.running, host), index}
    end)
    |> elem(0)
  end

  defp running_worker_host_count(running, worker_host) when is_map(running) and is_binary(worker_host) do
    Enum.count(running, fn
      {_issue_id, %{worker_host: ^worker_host}} -> true
      _ -> false
    end)
  end

  defp worker_slots_available?(%State{} = state) do
    select_worker_host(state, nil) != :no_worker_capacity
  end

  defp worker_slots_available?(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host) != :no_worker_capacity
  end

  defp worker_host_slots_available?(%State{} = state, worker_host) when is_binary(worker_host) do
    case Config.settings!().worker.max_concurrent_agents_per_host do
      limit when is_integer(limit) and limit > 0 ->
        running_worker_host_count(state.running, worker_host) < limit

      _ ->
        true
    end
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    max(
      (state.max_concurrent_agents || Config.settings!().agent.max_concurrent_agents) -
        map_size(state.running),
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          issue_url: metadata.issue.url,
          state: metadata.issue.state,
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          issue_url: Map.get(retry, :issue_url),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path)
        }
      end)

    blocked =
      state.blocked
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: Map.get(metadata, :identifier),
          issue_url: blocked_issue_url(metadata),
          state: blocked_issue_state(metadata),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: Map.get(metadata, :session_id),
          error: Map.get(metadata, :error),
          blocked_at: Map.get(metadata, :blocked_at),
          last_codex_timestamp: Map.get(metadata, :last_codex_timestamp),
          last_codex_message: Map.get(metadata, :last_codex_message),
          last_codex_event: Map.get(metadata, :last_codex_event)
        }
      end)

    claim_leases =
      state.claim_leases
      |> Enum.map(fn {_issue_id, lease} -> claim_lease_snapshot_entry(lease, now_ms) end)

    expired =
      state.expired_claims
      |> Enum.map(fn {_issue_id, lease} -> expired_claim_snapshot_entry(lease, now_ms) end)

    {:reply,
     %{
       running: running,
       retrying: retrying,
       blocked: blocked,
       claim_leases: claim_leases,
       expired: expired,
       codex_totals: state.codex_totals,
       ledger: Ledger.all(),
       rate_limits: Map.get(state, :codex_rate_limits),
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?
    state = %{state | force_full_poll?: true}
    state = if coalesced, do: state, else: schedule_tick(state, 0)

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  defp blocked_issue_state(%{issue: %Issue{state: state}}), do: state
  defp blocked_issue_state(_metadata), do: nil

  defp blocked_issue_url(%{issue: %Issue{url: url}}), do: url
  defp blocked_issue_url(_metadata), do: nil

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        codex_last_reported_cached_input_tokens:
          max(
            Map.get(running_entry, :codex_last_reported_cached_input_tokens, 0),
            token_delta.cached_input_reported
          ),
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      }),
      token_delta
    }
  end

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    %{
      event: update[:event],
      message: update[:payload] || update[:raw],
      timestamp: update[:timestamp]
    }
  end

  defp schedule_tick(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.tick_timer_ref) do
      Process.cancel_timer(state.tick_timer_ref)
    end

    tick_token = make_ref()
    timer_ref = Process.send_after(self(), {:tick, tick_token}, delay_ms)

    %{
      state
      | tick_timer_ref: timer_ref,
        tick_token: tick_token,
        next_poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    {Map.get(state.running, issue_id), %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    :ok = append_token_usage_observation(running_entry_issue_id(running_entry), running_entry, nil, true)
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())
    maybe_record_previous_attempt(running_entry)

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp maybe_record_previous_attempt(%{issue: %Issue{id: issue_id}} = running_entry) when is_binary(issue_id) do
    previous_attempt = previous_attempt_from_running(running_entry)

    Ledger.put(issue_id, %{
      last_agent_message: previous_attempt.last_agent_message,
      dirty_files: previous_attempt.dirty_files,
      commits_ahead: previous_attempt.commits_ahead,
      turns_used: previous_attempt.turns_used,
      cumulative_tokens: max(Map.get(Ledger.get(issue_id), :cumulative_tokens, 0), previous_attempt.token_total || 0)
    })
  end

  defp maybe_record_previous_attempt(_running_entry), do: :ok

  defp append_token_usage_observation(issue_id, running_entry, update, final?) when is_map(running_entry) do
    if token_usage_observation?(running_entry) do
      TokenUsageLedger.append_observation(%{
        observed_at: DateTime.utc_now(),
        final: final?,
        issue_id: issue_id,
        issue_identifier: Map.get(running_entry, :identifier),
        session_id: Map.get(running_entry, :session_id),
        worker_host: Map.get(running_entry, :worker_host),
        workspace_path: Map.get(running_entry, :workspace_path),
        turn_count: Map.get(running_entry, :turn_count, 0),
        input_tokens: Map.get(running_entry, :codex_input_tokens, 0),
        output_tokens: Map.get(running_entry, :codex_output_tokens, 0),
        total_tokens: Map.get(running_entry, :codex_total_tokens, 0),
        source_event: token_usage_source_event(update, final?)
      })
    end

    :ok
  end

  defp token_usage_observation?(running_entry) do
    is_binary(Map.get(running_entry, :session_id)) and
      Enum.any?(
        [
          Map.get(running_entry, :codex_input_tokens, 0),
          Map.get(running_entry, :codex_output_tokens, 0),
          Map.get(running_entry, :codex_total_tokens, 0)
        ],
        &(&1 > 0)
      )
  end

  defp token_usage_source_event(_update, true), do: :session_final

  defp token_usage_source_event(%{event: event}, _final?), do: event

  defp token_usage_source_event(_update, _final?), do: nil

  defp running_entry_issue_id(%{issue: %Issue{id: issue_id}}) when is_binary(issue_id), do: issue_id

  defp running_entry_issue_id(%{issue_id: issue_id}) when is_binary(issue_id), do: issue_id

  defp running_entry_issue_id(_running_entry), do: nil

  defp refresh_runtime_config(%State{} = state) do
    config = Config.settings!()

    %{
      state
      | poll_interval_ms: config.polling.interval_ms,
        max_concurrent_agents: config.agent.max_concurrent_agents
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states) and
      !stop_continue_labeled?(issue) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp enforce_issue_token_budget(%State{} = state, issue_id, running_entry) when is_binary(issue_id) and is_map(running_entry) do
    case Config.settings!().agent.max_tokens_per_issue do
      max_tokens when is_integer(max_tokens) and max_tokens > 0 ->
        ledger_entry = Ledger.get(issue_id)
        total_tokens = Map.get(ledger_entry, :cumulative_tokens, 0)

        if total_tokens >= max_tokens do
          issue = Map.get(running_entry, :issue) || %Issue{id: issue_id, identifier: Map.get(running_entry, :identifier)}
          reason = "symphony-budget-exceeded: max_tokens_per_issue=#{max_tokens} reached with total_tokens=#{total_tokens}"

          apply_block_label(issue, "symphony-budget-exceeded")
          post_budget_comment(issue, running_entry, total_tokens, max_tokens)

          state
          |> stop_and_block_issue(issue_id, running_entry, reason)
        else
          state
        end

      _ ->
        state
    end
  end

  defp enforce_issue_token_budget(state, _issue_id, _running_entry), do: state

  defp post_budget_comment(%Issue{id: issue_id}, running_entry, total_tokens, max_tokens)
       when is_binary(issue_id) do
    body = """
    Symphony paused this issue because it exceeded the configured token budget.

    Total tokens: #{total_tokens}
    Budget: #{max_tokens}
    Workspace: #{Map.get(running_entry, :workspace_path) || "unknown"}
    Thread/session: #{running_entry_session_id(running_entry)}
    """

    case Tracker.create_comment(issue_id, String.trim(body)) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("Failed to post Symphony budget comment for issue_id=#{issue_id}: #{inspect(reason)}")
    end
  end

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_codex_rate_limits(state, _update), do: state

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    running_entry = running_entry || %{}
    usage = extract_token_usage(update)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      ),
      compute_token_delta(
        running_entry,
        :cached_input,
        usage,
        :codex_last_reported_cached_input_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total, cached_input] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        cached_input_tokens: cached_input.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported,
        cached_input_reported: cached_input.reported
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    cond do
      rate_limits_map?(direct) ->
        direct

      rate_limits_map?(payload) ->
        payload

      true ->
        rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limits_map?(payload) when is_map(payload) do
    limit_id =
      Map.get(payload, "limit_id") ||
        Map.get(payload, :limit_id) ||
        Map.get(payload, "limit_name") ||
        Map.get(payload, :limit_name)

    has_buckets =
      Enum.any?(
        ["primary", :primary, "secondary", :secondary, "credits", :credits],
        &Map.has_key?(payload, &1)
      )

    !is_nil(limit_id) and has_buckets
  end

  defp rate_limits_map?(_payload), do: false

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  # Cached prompt-prefix reads. Codex reports these inside the input total;
  # they cost ~0 but dominate the raw numbers (a healthy turn re-reads its
  # full context every model call). The ledger budget subtracts them so
  # max_tokens_per_issue measures real spend. Absent field → delta 0 →
  # budget falls back to counting the raw total (previous behavior).
  defp get_token_usage(usage, :cached_input),
    do:
      payload_get(usage, [
        "cached_input_tokens",
        :cached_input_tokens,
        "cachedInputTokens",
        :cachedInputTokens,
        "cached_input",
        :cached_input,
        "cachedInput",
        :cachedInput,
        "cache_read_input_tokens",
        :cache_read_input_tokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
