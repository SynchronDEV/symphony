defmodule SymphonyElixir.CandidateDependencyRefreshTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Ledger
  alias SymphonyElixir.Orchestrator.State

  setup do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_required_labels: ["opted-in"],
      tracker_active_states: ["Todo", "In Progress", "In Review"],
      max_concurrent_agents: 1
    )

    :ok
  end

  test "an unchanged child becomes eligible once when its prerequisite completes" do
    child = child("child")
    state = poll(state([child], {:ok, [parent("In Progress")]}))
    assert state.candidate_cache[child.id] == child
    assert state.issue_operations == %{}
    assert state.claimed == MapSet.new()
    refute_receive {:states, _}

    cutoff = state.last_candidate_poll_at
    ledger = Ledger.get(child.id)
    next = poll(delta(state, {:ok, [parent(" Done ")]}))
    assert_receive {:candidates, ^cutoff}
    assert_receive {:states, ["parent"]}
    assert_receive {:final_refresh, final_pid, ["child"]}
    assert Map.keys(next.issue_operations) == [child.id]
    assert next.claimed == MapSet.new([child.id])
    assert next.running == %{}
    assert next.codex_totals == state.codex_totals
    assert Ledger.get(child.id) == ledger
    refute Orchestrator.should_dispatch_issue_for_test(%{child | id: "other", blocked_by: []}, next)

    # A subsequent delta must not reserve or refresh this child a second time.
    again = poll(next)
    assert again.issue_operations[child.id].task.ref == next.issue_operations[child.id].task.ref
    refute_receive {:final_refresh, _, _}
    assert Ledger.get(child.id) == ledger

    # Parent observations do not bypass the final child refresh if it is blocked again.
    send(final_pid, {:finish, {:ok, [child]}})
    ref = again.issue_operations[child.id].task.ref
    assert_receive {^ref, result}
    assert {:noreply, finished} = Orchestrator.handle_info({ref, result}, again)
    assert finished.running == %{}
    assert finished.claimed == MapSet.new()
    assert finished.issue_operations == %{}
    assert Ledger.get(child.id) == ledger
  end

  test "unchanged, missing, unknown and failed prerequisite observations remain blocked in every active state" do
    for child_state <- ["Todo", "In Progress", "In Review"],
        observation <- [{:ok, [parent("In Progress")]}, {:ok, []}, {:ok, [parent(nil)]}, {:error, :unavailable}] do
      child = %{child("blocked-#{child_state}") | state: child_state}
      state = state([child], observation) |> poll() |> delta(observation) |> poll()
      assert state.issue_operations == %{}
      assert state.claimed == MapSet.new()
      assert state.running == %{}
      refute Orchestrator.should_dispatch_issue_for_test(state.candidate_cache[child.id], state)
      assert Ledger.get(child.id) == %{}
    end

    refute_receive {:final_refresh, _, _}
  end

  test "a prerequisite fetch failure cannot admit an already-terminal cached snapshot" do
    blocked = child("blocked")
    terminal = %{child("terminal") | blocked_by: [%{id: "parent", state: "Done"}]}
    state = %{state([], {:error, {:rate_limited, nil}}) | candidate_cache: %{blocked.id => blocked, terminal.id => terminal}, last_candidate_poll_at: DateTime.utc_now(), force_full_poll?: false}
    result = poll(state)
    assert_receive {:states, ["parent"]}
    assert result.issue_operations == %{}
    assert result.claimed == MapSet.new()
    assert result.last_candidate_poll_at == state.last_candidate_poll_at
    assert result.next_poll_due_at_ms - System.monotonic_time(:millisecond) > 250_000
    refute_receive {:final_refresh, _, _}
  end

  test "shared prerequisites are fetched once and only one child can reserve the available slot" do
    children = [child("first"), child("second")]
    state = state(children, {:ok, []}) |> poll() |> delta({:ok, [parent("Done")]}) |> poll()
    assert_receive {:states, ["parent"]}
    refute_receive {:states, _}
    assert map_size(state.issue_operations) == 1
    assert MapSet.size(state.claimed) == 1
    assert state.running == %{}
    assert Enum.all?(state.candidate_cache, fn {_id, issue} -> hd(issue.blocked_by).state == "Done" end)
  end

  test "fresh delta children win and retain their ordinary state and routing updates" do
    child = child("updated")
    state = poll(state([child], {:ok, []}))
    fresh = %{child | state: "In Review", labels: [], blocked_by: [%{id: "replacement", state: "In Progress"}]}
    state = state |> delta({:ok, [parent("Done")]}, [fresh]) |> poll()
    assert state.candidate_cache[child.id] == fresh
    assert state.issue_operations == %{}
    refute_receive {:states, _}
    refute_receive {:final_refresh, _, _}
  end

  test "a fresh child wins even when another cached child refreshes their shared prerequisite" do
    child = child("fresh")
    other = %{child("other") | labels: []}
    state = poll(state([child, other], {:ok, []}))
    fresh = %{child | state: "In Review", blocked_by: [%{id: "parent", state: "In Progress"}]}
    state = state |> delta({:ok, [parent("Done")]}, [fresh]) |> poll()
    assert_receive {:states, ["parent"]}
    assert state.candidate_cache[child.id] == fresh
    assert state.candidate_cache[other.id].blocked_by == [%{id: "parent", state: "Done"}]
    assert state.issue_operations == %{}
    refute_receive {:final_refresh, _, _}
  end

  test "missing and unknown observations invalidate a queried prerequisite's old terminal snapshot" do
    for observation <- [{:ok, []}, {:ok, [parent(nil)]}] do
      blocked = child("blocked")
      terminal = %{child("terminal") | blocked_by: [%{id: "parent", state: "Done"}]}
      state = %{state([], observation) | candidate_cache: %{blocked.id => blocked, terminal.id => terminal}, last_candidate_poll_at: DateTime.utc_now(), force_full_poll?: false}
      result = poll(state)
      assert result.candidate_cache[terminal.id].blocked_by == [%{id: "parent", state: nil}]
      assert result.issue_operations == %{}
      assert result.claimed == MapSet.new()
    end

    refute_receive {:final_refresh, _, _}
  end

  test "manual refresh replaces the cache without prerequisite reads" do
    child = child("removed")
    state = poll(state([child], {:ok, []}))
    assert_receive {:candidates, nil}
    state = delta(state, {:error, :must_not_fetch})
    {:reply, %{queued: true}, requested} = Orchestrator.handle_call(:request_refresh, nil, state)
    Process.cancel_timer(requested.tick_timer_ref)
    result = poll(requested)
    assert_receive {:candidates, nil}
    assert result.candidate_cache == %{}
    refute result.force_full_poll?
    refute_receive {:states, _}
  end

  test "a manual refresh requested during a delta observation remains queued for a full poll" do
    child = child("manual-during-delta")
    state = state([child], {:ok, []}) |> poll() |> delta({:ok, [parent("In Progress")]})
    {:noreply, pending} = Orchestrator.handle_info(:run_poll_cycle, state)
    {:reply, %{coalesced: true}, requested} = Orchestrator.handle_call(:request_refresh, nil, pending)
    ref = pending.poll_task.task.ref
    assert_receive {^ref, result}, 1_000
    {:noreply, finished} = Orchestrator.handle_info({ref, result}, requested)
    Process.cancel_timer(finished.tick_timer_ref)
    assert finished.force_full_poll?
    assert_receive {:candidates, nil}
    final = poll(finished)
    assert_receive {:candidates, nil}
    assert final.candidate_cache == %{}
    refute final.force_full_poll?
  end

  test "a dependency observation cannot resurrect a child removed while the poll was in flight" do
    child = child("removed-in-flight")
    state = state([child], {:ok, []}) |> poll() |> delta({:ok, [parent("Done")]})
    {:noreply, pending} = Orchestrator.handle_info(:run_poll_cycle, state)
    ref = pending.poll_task.task.ref
    assert_receive {^ref, result}, 1_000
    {:noreply, finished} = Orchestrator.handle_info({ref, result}, %{pending | candidate_cache: %{}})
    Process.cancel_timer(finished.tick_timer_ref)
    assert finished.candidate_cache == %{}
    assert finished.issue_operations == %{}
    refute_receive {:final_refresh, _, _}
  end

  test "children without dependencies keep delta polling without extra tracker reads" do
    child = %{child("ordinary") | blocked_by: [], labels: []}
    state = state([child], {:error, :must_not_fetch}) |> poll() |> delta({:error, :must_not_fetch}) |> poll()
    assert state.candidate_cache[child.id] == child
    refute_receive {:states, _}
  end

  defp state(children, observation) do
    %State{
      candidate_fetcher: candidate_fetcher(children),
      issue_fetcher: issue_fetcher(observation),
      max_concurrent_agents: 1,
      poll_interval_ms: 30_000,
      codex_totals: %{
        input_tokens: 3,
        output_tokens: 2,
        total_tokens: 5,
        cached_input_tokens: 0,
        effective_total_tokens: 5,
        seconds_running: 0
      }
    }
  end

  defp delta(state, observation, children \\ []) do
    %{state | candidate_fetcher: candidate_fetcher(children), issue_fetcher: issue_fetcher(observation)}
  end

  defp candidate_fetcher(children) do
    recipient = self()

    fn cutoff ->
      send(recipient, {:candidates, cutoff})
      {:ok, children}
    end
  end

  defp issue_fetcher(observation) do
    recipient = self()

    fn ids ->
      if ids == ["parent"] do
        send(recipient, {:states, ids})
        observation
      else
        send(recipient, {:final_refresh, self(), ids})

        receive do
          {:finish, result} -> result
        end
      end
    end
  end

  defp poll(state) do
    {:noreply, pending} = Orchestrator.handle_info(:run_poll_cycle, state)
    task = pending.poll_task.task
    on_exit(fn -> if Process.alive?(task.pid), do: Process.exit(task.pid, :kill) end)
    assert_receive {ref, result} when ref == task.ref, 1_000
    {:noreply, finished} = Orchestrator.handle_info({ref, result}, pending)
    Process.cancel_timer(finished.tick_timer_ref)

    for {_id, operation} <- finished.issue_operations do
      on_exit(fn -> stop_observation(operation) end)
    end

    finished
  end

  defp stop_observation(operation) do
    Process.cancel_timer(operation.timer)
    if Process.alive?(operation.task.pid), do: Process.exit(operation.task.pid, :kill)
  end

  defp child(id), do: %Issue{id: id, identifier: "SPK-#{id}", title: id, state: "Todo", priority: 1, labels: ["opted-in"], blocked_by: [%{id: "parent", state: "In Progress"}]}
  defp parent(state), do: %Issue{id: "parent", identifier: "SPK-parent", title: "parent", state: state}
end
