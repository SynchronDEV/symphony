defmodule SymphonyElixir.AgentRunner do
  @moduledoc """
  Executes a single Linear issue in its workspace with Codex.
  """

  require Logger
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.{Config, IssueStateBatcher, Linear.Issue, PromptBuilder, Workspace}

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

    case Workspace.create_for_issue(issue, worker_host) do
      {:ok, workspace} ->
        send_worker_runtime_info(codex_update_recipient, issue, worker_host, workspace)

        try do
          with :ok <- Workspace.run_before_run_hook(workspace, issue, worker_host) do
            run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host)
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
    max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
    issue_state_fetcher = Keyword.get(opts, :issue_state_fetcher, &IssueStateBatcher.fetch_issue_states_by_ids/1)

    with {:ok, session} <- AppServer.start_session(workspace, worker_host: worker_host) do
      try do
        do_run_codex_turns(session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, 1, max_turns)
      after
        AppServer.stop_session(session)
      end
    end
  end

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp do_run_codex_turns(app_session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, turn_number, max_turns) do
    prompt = build_turn_prompt(issue, opts, turn_number, max_turns)

    case AppServer.run_turn(
           app_session,
           prompt,
           issue,
           on_message: codex_message_handler(codex_update_recipient, issue)
         ) do
      {:ok, turn_session} ->
        Logger.info("Completed agent run for #{issue_context(issue)} session_id=#{turn_session[:session_id]} workspace=#{workspace} turn=#{turn_number}/#{max_turns}")

        case continue_with_issue?(issue, issue_state_fetcher) do
          {:continue, refreshed_issue} when turn_number < max_turns ->
            Logger.info("Continuing agent run for #{issue_context(refreshed_issue)} after normal turn completion turn=#{turn_number}/#{max_turns}")

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

          {:continue, refreshed_issue} ->
            Logger.info("Reached agent.max_turns for #{issue_context(refreshed_issue)} with issue still active; returning control to orchestrator")

            :ok

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

      {:error, timeout_reason} when timeout_reason in [:turn_timeout, :stall_timeout] and turn_number < max_turns ->
        Logger.warning("Codex turn #{timeout_reason_for_log(timeout_reason)} for #{issue_context(issue)}; interrupting thread and continuing on same session turn=#{turn_number}/#{max_turns}")
        _ = AppServer.interrupt_thread(app_session)

        do_run_codex_turns(
          app_session,
          workspace,
          issue,
          codex_update_recipient,
          put_timeout_previous_attempt(opts, turn_number),
          issue_state_fetcher,
          turn_number + 1,
          max_turns
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_turn_prompt(issue, opts, 1, _max_turns), do: PromptBuilder.build_prompt(issue, opts)

  defp build_turn_prompt(_issue, _opts, turn_number, max_turns) do
    """
    Continuation guidance:

    - The previous Codex turn completed normally, but the Linear issue is still in an active state.
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
      {:continue, refreshed_issue} when turn_number < max_turns ->
        Logger.info("Resuming paused agent run for #{issue_context(refreshed_issue)} on same Codex session turn=#{turn_number}/#{max_turns}")

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

      {:continue, refreshed_issue} ->
        Logger.info("Reached agent.max_turns for #{issue_context(refreshed_issue)} after paused issue refresh; returning control to orchestrator")
        :ok

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

  defp continue_with_issue?(%Issue{id: issue_id} = issue, issue_state_fetcher) when is_binary(issue_id) do
    case fetch_issue_for_continuation(issue, issue_state_fetcher) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        cond do
          not active_issue_state?(refreshed_issue.state) ->
            {:done, refreshed_issue}

          stop_continue_label?(refreshed_issue) ->
            Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue carries a stop-continue label while still in an active state; returning control to orchestrator")

            {:done, refreshed_issue}

          !issue_routable?(refreshed_issue) ->
            Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue is no longer routed to this worker")

            {:done, refreshed_issue}

          true ->
            {:continue, refreshed_issue}
        end

      {:ok, []} ->
        {:done, issue}

      {:error, reason} ->
        retry_issue_refresh(issue, issue_state_fetcher, 2, reason)
    end
  end

  defp continue_with_issue?(issue, _issue_state_fetcher), do: {:done, issue}

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

  defp continue_with_refreshed_issue(_issue, {:ok, [%Issue{} = refreshed_issue | _]}) do
    cond do
      not active_issue_state?(refreshed_issue.state) ->
        {:done, refreshed_issue}

      stop_continue_label?(refreshed_issue) ->
        Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue carries a stop-continue label while still in an active state; returning control to orchestrator")

        {:done, refreshed_issue}

      !issue_routable?(refreshed_issue) ->
        Logger.info("Not continuing #{issue_context(refreshed_issue)}: issue is no longer routed to this worker")

        {:done, refreshed_issue}

      true ->
        {:continue, refreshed_issue}
    end
  end

  defp continue_with_refreshed_issue(issue, {:ok, []}), do: {:done, issue}
  defp continue_with_refreshed_issue(_issue, {:error, reason}), do: {:error, {:issue_state_refresh_failed, reason}}

  defp stop_continue_label?(%Issue{} = issue) do
    Issue.stop_continue_labeled?(issue, Config.settings!().agent.stop_continue_labels)
  end

  defp stop_continue_label?(_issue), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp issue_routable?(_issue), do: false

  defp active_issue_state?(state_name) when is_binary(state_name) do
    normalized_state = normalize_issue_state(state_name)

    Config.settings!().tracker.active_states
    |> Enum.any?(fn active_state -> normalize_issue_state(active_state) == normalized_state end)
  end

  defp active_issue_state?(_state_name), do: false

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
