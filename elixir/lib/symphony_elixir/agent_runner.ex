defmodule SymphonyElixir.AgentRunner do
  @moduledoc """
  Executes a single Linear issue in its workspace with Codex.
  """

  require Logger
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.{Config, IssueStateBatcher, Ledger, Linear.Issue, PromptBuilder, Workspace}

  @issue_refresh_attempts 5
  @issue_refresh_retry_ms 1_000

  @type worker_host :: String.t() | nil

  @spec run(map(), pid() | nil, keyword()) :: :ok | no_return()
  def run(issue, codex_update_recipient \\ nil, opts \\ []) do
    # The orchestrator owns host retries so one worker lifetime never hops machines.
    worker_host = selected_worker_host(Keyword.get(opts, :worker_host), Config.settings!().worker.ssh_hosts)

    Logger.info("Starting agent run for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    case run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Agent run failed for #{issue_context(issue)}: #{inspect(reason)}")
        raise RuntimeError, "Agent run failed for #{issue_context(issue)}: #{inspect(reason)}"
    end
  end

  @doc false
  @spec continue_with_issue_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:continue, Issue.t()} | {:done, Issue.t()} | {:pause, Issue.t(), term()} | {:error, term()}
  def continue_with_issue_for_test(%Issue{} = issue, issue_state_fetcher)
      when is_function(issue_state_fetcher, 1) do
    continue_with_issue?(issue, issue_state_fetcher)
  end

  defp run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
    Logger.info("Starting worker attempt for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")
    send_preparation_phase(codex_update_recipient, issue, :workspace)

    case Workspace.create_for_issue(issue, worker_host) do
      {:ok, workspace} ->
        send_worker_runtime_info(codex_update_recipient, issue, worker_host, workspace)
        send_preparation_phase(codex_update_recipient, issue, :before_run)

        try do
          case Workspace.run_before_run_hook(workspace, issue, worker_host) do
            :ok ->
              run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host)

            {:error, _reason} ->
              send_codex_update(codex_update_recipient, issue, %{
                event: :worker_preflight_failed,
                timestamp: DateTime.utc_now()
              })

              {:error, :worker_preflight_failed}
          end
        after
          Workspace.run_after_run_hook(workspace, issue, worker_host)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp codex_message_handler(recipient, issue) do
    fn message ->
      send_codex_update(recipient, issue, message)
    end
  end

  defp send_codex_update(recipient, %Issue{id: issue_id}, message)
       when is_binary(issue_id) and is_pid(recipient) do
    send(recipient, {:codex_worker_update, issue_id, message})
    :ok
  end

  defp send_codex_update(_recipient, _issue, _message), do: :ok

  defp send_preparation_phase(recipient, %Issue{id: issue_id}, phase) when is_pid(recipient) do
    send(recipient, {:worker_preparation_phase, issue_id, phase, DateTime.utc_now()})
  end

  defp send_preparation_phase(_recipient, _issue, _phase), do: :ok

  defp send_worker_runtime_info(recipient, %Issue{id: issue_id}, worker_host, workspace)
       when is_binary(issue_id) and is_pid(recipient) and is_binary(workspace) do
    send(
      recipient,
      {:worker_runtime_info, issue_id,
       %{
         worker_host: worker_host,
         workspace_path: workspace
       }}
    )

    :ok
  end

  defp send_worker_runtime_info(_recipient, _issue, _worker_host, _workspace), do: :ok

  defp run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host) do
    send_preparation_phase(codex_update_recipient, issue, :codex_startup)
    max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
    issue_state_fetcher = Keyword.get(opts, :issue_state_fetcher, &IssueStateBatcher.fetch_issue_states_by_ids/1)

    with {:ok, session} <- AppServer.start_session(workspace, worker_host: worker_host) do
      send_preparation_phase(codex_update_recipient, issue, :ready)

      try do
        do_run_codex_turns(session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, 1, max_turns)
      after
        AppServer.stop_session(session)
      end
    end
  end

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp do_run_codex_turns(app_session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, turn_number, max_turns) do
    prompt = build_turn_prompt(issue, opts, turn_number, max_turns_for_issue_state(issue, max_turns))

    case AppServer.run_turn(
           app_session,
           prompt,
           issue,
           on_message: codex_message_handler(codex_update_recipient, issue)
         ) do
      {:ok, turn_session} ->
        Logger.info("Completed agent run for #{issue_context(issue)} session_id=#{turn_session[:session_id]} workspace=#{workspace} turn=#{turn_number}/#{max_turns}")

        case continue_with_issue?(issue, issue_state_fetcher) do
          {:continue, refreshed_issue} ->
            continue_or_stop_at_state_cap(
              app_session,
              workspace,
              refreshed_issue,
              codex_update_recipient,
              opts,
              issue_state_fetcher,
              turn_number,
              max_turns,
              :normal_completion
            )

          {:done, _refreshed_issue} ->
            :ok

          {:pause, paused_issue, reason} ->
            resume_after_issue_refresh_pause(
              app_session,
              workspace,
              paused_issue,
              codex_update_recipient,
              opts,
              issue_state_fetcher,
              turn_number,
              max_turns,
              reason
            )

          {:error, reason} ->
            {:error, reason}
        end

      {:error, {timeout_reason, turn_id}} when timeout_reason in [:turn_timeout, :stall_timeout] ->
        max_turns_for_state = max_turns_for_issue_state(issue, max_turns)

        if turn_number < max_turns_for_state do
          Logger.warning(
            "Codex turn #{timeout_reason_for_log(timeout_reason)} for #{issue_context(issue)}; interrupting active turn and continuing on same session turn=#{turn_number}/#{max_turns_for_state}"
          )

          turn = %{turn_id: turn_id, turn_number: turn_number, max_turns: max_turns}
          recipient = codex_update_recipient
          continue_interrupted_turn(app_session, workspace, issue, recipient, opts, issue_state_fetcher, turn)
        else
          {:error, timeout_reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp continue_interrupted_turn(app_session, workspace, issue, recipient, opts, issue_state_fetcher, turn) do
    with :ok <- AppServer.interrupt_turn(app_session, turn.turn_id) do
      do_run_codex_turns(
        app_session,
        workspace,
        issue,
        recipient,
        put_timeout_previous_attempt(opts, turn.turn_number),
        issue_state_fetcher,
        turn.turn_number + 1,
        turn.max_turns
      )
    end
  end

  defp build_turn_prompt(issue, opts, 1, _max_turns), do: PromptBuilder.build_prompt(issue, opts)

  defp build_turn_prompt(_issue, _opts, turn_number, max_turns) do
    """
    Continuation guidance:

    - The previous Codex turn has ended, and the Linear issue is still in an active state.
    - This is continuation turn ##{turn_number} of #{max_turns} for the current agent run.
    - Resume from the current workspace and workpad state instead of restarting from scratch.
    - The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.
    - Focus on the remaining ticket work and do not end the turn while the issue stays active unless you are truly blocked.
    """
  end

  defp put_timeout_previous_attempt(opts, turn_number) do
    Keyword.put(opts, :previous_attempt, %{
      "last_agent_message" => "Previous turn timed out and was interrupted by Symphony.",
      "dirty_files" => [],
      "commits_ahead" => nil,
      "turns_used" => turn_number,
      "token_total" => nil
    })
  end

  defp timeout_reason_for_log(:stall_timeout), do: "stalled"
  defp timeout_reason_for_log(:turn_timeout), do: "timed out"

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  defp resume_after_issue_refresh_pause(
         app_session,
         workspace,
         issue,
         codex_update_recipient,
         opts,
         issue_state_fetcher,
         turn_number,
         max_turns,
         reason
       ) do
    emit_issue_refresh_paused(codex_update_recipient, issue, reason)
    sleep_for_issue_pause(reason)

    case continue_with_issue?(issue, issue_state_fetcher) do
      {:continue, refreshed_issue} ->
        continue_or_stop_at_state_cap(
          app_session,
          workspace,
          refreshed_issue,
          codex_update_recipient,
          opts,
          issue_state_fetcher,
          turn_number,
          max_turns,
          :paused_refresh
        )

      {:done, _refreshed_issue} ->
        :ok

      {:pause, paused_issue, pause_reason} ->
        resume_after_issue_refresh_pause(
          app_session,
          workspace,
          paused_issue,
          codex_update_recipient,
          opts,
          issue_state_fetcher,
          turn_number,
          max_turns,
          pause_reason
        )

      {:error, refresh_reason} ->
        {:error, refresh_reason}
    end
  end

  defp emit_issue_refresh_paused(recipient, %Issue{} = issue, reason) do
    send_codex_update(recipient, issue, %{
      event: :issue_state_refresh_paused,
      timestamp: DateTime.utc_now(),
      message: %{reason: reason}
    })
  end

  defp sleep_for_issue_pause({:rate_limited, %DateTime{}} = reason), do: sleep_for_issue_refresh(reason)

  defp sleep_for_issue_pause(_reason) do
    Config.settings!().polling.interval_ms
    |> max(@issue_refresh_retry_ms)
    |> Process.sleep()
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  defp continue_or_stop_at_state_cap(
         app_session,
         workspace,
         refreshed_issue,
         codex_update_recipient,
         opts,
         issue_state_fetcher,
         turn_number,
         max_turns,
         continuation_context
       ) do
    max_turns_for_state = max_turns_for_issue_state(refreshed_issue, max_turns)

    if turn_number < max_turns_for_state do
      Logger.info(continuation_log_message(refreshed_issue, turn_number, max_turns_for_state, continuation_context))

      do_run_codex_turns(
        app_session,
        workspace,
        refreshed_issue,
        codex_update_recipient,
        opts,
        issue_state_fetcher,
        turn_number + 1,
        max_turns
      )
    else
      Logger.info(max_turns_reached_log_message(refreshed_issue, turn_number, max_turns_for_state, continuation_context))

      :ok
    end
  end

  defp continuation_log_message(issue, turn_number, max_turns_for_state, :normal_completion) do
    "Continuing agent run for #{issue_context(issue)} after normal turn completion turn=#{turn_number}/#{max_turns_for_state}"
  end

  defp continuation_log_message(issue, turn_number, max_turns_for_state, :paused_refresh) do
    "Resuming paused agent run for #{issue_context(issue)} on same Codex session turn=#{turn_number}/#{max_turns_for_state}"
  end

  defp max_turns_reached_log_message(issue, turn_number, max_turns_for_state, :normal_completion) do
    "Reached agent max turns for #{issue_context(issue)} state=#{inspect(issue.state)} turn=#{turn_number}/#{max_turns_for_state}; returning control to orchestrator"
  end

  defp max_turns_reached_log_message(issue, turn_number, max_turns_for_state, :paused_refresh) do
    "Reached agent max turns for #{issue_context(issue)} state=#{inspect(issue.state)} after paused issue refresh turn=#{turn_number}/#{max_turns_for_state}; returning control to orchestrator"
  end

  defp continue_with_issue?(%Issue{id: issue_id} = issue, issue_state_fetcher) when is_binary(issue_id) do
    case fetch_issue_for_continuation(issue, issue_state_fetcher) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        # Count rework transitions here too: a full review->rework cycle can
        # happen INSIDE one continuous run with zero dispatches, so
        # dispatch-time counting alone lets issues thrash past
        # max_rework_cycles (observed: 4 cycles in one session).
        Ledger.observe_state(issue_id, refreshed_issue.state)

        case continuation_stop_reason(issue, refreshed_issue, include_rework_cap?: true) do
          nil ->
            {:continue, refreshed_issue}

          reason ->
            log_continuation_stop(reason, issue, refreshed_issue)
            {:done, refreshed_issue}
        end

      {:ok, []} ->
        {:done, issue}

      {:error, reason} ->
        retry_issue_refresh(issue, issue_state_fetcher, 2, reason)
    end
  end

  defp continue_with_issue?(issue, _issue_state_fetcher), do: {:done, issue}

  # The cap is exhausted once the just-observed rework cycle exceeds it
  # (observe_state has already counted the current cycle). The run ends and
  # the orchestrator's dispatch_cap_status blocks the re-dispatch with the
  # symphony-stuck label.
  defp rework_cap_exhausted?(%Issue{id: issue_id, state: state}) do
    cap = Config.settings!().agent.max_rework_cycles

    is_integer(cap) and Ledger.rework_state?(state) and
      Map.get(Ledger.get(issue_id), :rework_count, 0) > cap
  end

  defp fetch_issue_for_continuation(%Issue{id: issue_id}, issue_state_fetcher) when is_binary(issue_id) do
    issue_state_fetcher.([issue_id])
  end

  defp retry_issue_refresh(issue, issue_state_fetcher, attempt, last_reason)
       when attempt <= @issue_refresh_attempts do
    sleep_for_issue_refresh(last_reason)

    case fetch_issue_for_continuation(issue, issue_state_fetcher) do
      {:error, reason} ->
        retry_issue_refresh(issue, issue_state_fetcher, attempt + 1, reason)

      result ->
        continue_with_refreshed_issue(issue, result)
    end
  end

  defp retry_issue_refresh(issue, _issue_state_fetcher, _attempt, last_reason) do
    Logger.warning("Pausing #{issue_context(issue)} in-place after issue-state refresh failed repeatedly: #{inspect(last_reason)}")
    {:pause, issue, last_reason}
  end

  defp sleep_for_issue_refresh({:rate_limited, %DateTime{} = reset_at}) do
    reset_at
    |> DateTime.diff(DateTime.utc_now(), :millisecond)
    |> max(@issue_refresh_retry_ms)
    |> Process.sleep()
  end

  defp sleep_for_issue_refresh(_reason), do: Process.sleep(@issue_refresh_retry_ms)

  defp continue_with_refreshed_issue(issue, {:ok, [%Issue{} = refreshed_issue | _]}) do
    case continuation_stop_reason(issue, refreshed_issue, include_rework_cap?: false) do
      nil ->
        {:continue, refreshed_issue}

      reason ->
        log_continuation_stop(reason, issue, refreshed_issue)
        {:done, refreshed_issue}
    end
  end

  defp continue_with_refreshed_issue(issue, {:ok, []}), do: {:done, issue}
  defp continue_with_refreshed_issue(_issue, {:error, reason}), do: {:error, {:issue_state_refresh_failed, reason}}

  defp stop_continue_label?(%Issue{} = issue) do
    Issue.stop_continue_labeled?(issue, Config.settings!().agent.stop_continue_labels)
  end

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp continuation_stop_reason(issue, refreshed_issue, opts) do
    cond do
      not active_issue_state?(refreshed_issue.state) ->
        :inactive

      stop_continue_label?(refreshed_issue) ->
        :stop_continue_label

      !issue_routable?(refreshed_issue) ->
        :unroutable

      active_role_changed?(issue, refreshed_issue) ->
        :active_role_changed

      Keyword.get(opts, :include_rework_cap?, false) and rework_cap_exhausted?(refreshed_issue) ->
        :rework_cap_exhausted

      true ->
        nil
    end
  end

  defp log_continuation_stop(:inactive, _issue, _refreshed_issue), do: :ok

  defp log_continuation_stop(:stop_continue_label, _issue, refreshed_issue) do
    Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue carries a stop-continue label while still in an active state; returning control to orchestrator")
  end

  defp log_continuation_stop(:unroutable, _issue, refreshed_issue) do
    Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue is no longer routed to this worker")
  end

  defp log_continuation_stop(:active_role_changed, issue, refreshed_issue) do
    Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue moved from #{inspect(issue.state)} to #{inspect(refreshed_issue.state)} active role; returning control to orchestrator")
  end

  defp log_continuation_stop(:rework_cap_exhausted, _issue, refreshed_issue) do
    Logger.info("Not continuing #{issue_context(refreshed_issue)}: max_rework_cycles reached; returning control to orchestrator")
  end

  defp active_issue_state?(state_name) when is_binary(state_name) do
    normalized_state = normalize_issue_state(state_name)

    Config.settings!().tracker.active_states
    |> Enum.any?(fn active_state -> normalize_issue_state(active_state) == normalized_state end)
  end

  defp active_issue_state?(_state_name), do: false

  defp active_role_changed?(%Issue{} = issue, %Issue{} = refreshed_issue) do
    active_role(issue.state) != active_role(refreshed_issue.state)
  end

  defp active_role(state_name) when is_binary(state_name) do
    case normalize_issue_state(state_name) do
      "in review" -> :review
      _state_name -> :implementation
    end
  end

  defp active_role(_state_name), do: :implementation

  defp max_turns_for_issue_state(%Issue{state: state_name}, default_max_turns) do
    Config.max_turns_for_state(state_name, default_max_turns)
  end

  defp max_turns_for_issue_state(_issue, default_max_turns), do: default_max_turns

  defp selected_worker_host(nil, []), do: nil

  defp selected_worker_host(preferred_host, configured_hosts) when is_list(configured_hosts) do
    hosts =
      configured_hosts
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    case preferred_host do
      host when is_binary(host) and host != "" -> host
      _ when hosts == [] -> nil
      _ -> List.first(hosts)
    end
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    state_name
    |> String.trim()
    |> String.downcase()
  end

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end
end
