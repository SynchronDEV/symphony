defmodule SymphonyElixir.DependencyAdmissionTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Ledger
  alias SymphonyElixir.Orchestrator.State

  @active_states ["Todo", "In Progress", "In Review", "Rework", "Ready for Agent"]
  @usage %{dispatch_count: 1, cumulative_tokens: 43_161, retries: 1, turns_used: 1}

  setup do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: @active_states,
      max_concurrent_agents: 1
    )

    :ok
  end

  test "every configured active state rejects unfinished or unknown blockers" do
    for state <- @active_states,
        blocker <- [%{state: "In Review"}, %{state: "Unknown"}, %{state: nil}, %{}] do
      issue = issue(state, [blocker])
      refute Orchestrator.should_dispatch_issue_for_test(issue, empty_state()), inspect({state, blocker})
    end
  end

  test "every configured active state accepts no blockers or normalized terminal blockers" do
    for state <- @active_states,
        blockers <- [[], [%{state: " dOnE "}, %{state: " CLOSED "}, %{state: "canceled"}]] do
      assert Orchestrator.should_dispatch_issue_for_test(issue(state, blockers), empty_state())
    end
  end

  test "Todo to In Progress refresh cannot bypass the same unfinished dependency" do
    stale = issue("Todo", [%{state: "In Review"}])
    refreshed = %{stale | state: "In Progress"}

    assert {:skip, ^refreshed} =
             Orchestrator.revalidate_issue_for_dispatch_for_test(stale, fn [_id] -> {:ok, [refreshed]} end)
  end

  test "final asynchronous admission releases blocked issues without starting workers or consuming budgets" do
    for active_state <- @active_states do
      stale = issue("Todo", [])
      refreshed = issue(active_state, [%{state: "In Review"}])
      Ledger.put(stale.id, Map.merge(@usage, %{identifier: stale.identifier, status: "retrying"}))

      state = claimed_state(stale, fn [_id] -> {:ok, [refreshed]} end)
      pending = Orchestrator.dispatch_issue_for_test(stale, state)
      {ref, observation} = receive_observation(pending, stale.id)

      # Assert before applying the observation: a regression must never start a real worker.
      assert observation == {:skip, refreshed}
      assert {:noreply, finished} = Orchestrator.handle_info({ref, observation}, pending)
      assert_released_without_usage(finished, stale)
    end
  end

  test "retry refresh releases blocked active issues before a new dispatch or budget increment" do
    for active_state <- @active_states do
      blocked = issue(active_state, [%{state: nil}])
      Ledger.put(blocked.id, Map.merge(@usage, %{identifier: blocked.identifier, status: "retrying"}))
      retry_token = make_ref()

      state = %{
        claimed_state(blocked, fn [_id] -> {:ok, [blocked]} end)
        | retry_attempts: %{
            blocked.id => %{attempt: 2, retry_token: retry_token, identifier: blocked.identifier}
          }
      }

      assert {:noreply, pending} = Orchestrator.handle_info({:retry_issue, blocked.id, retry_token}, state)
      {ref, observation} = receive_observation(pending, blocked.id)
      assert {:noreply, finished} = Orchestrator.handle_info({ref, observation}, pending)
      assert_released_without_usage(finished, blocked)
    end
  end

  defp assert_released_without_usage(state, issue) do
    assert state.running == %{}
    assert state.issue_operations == %{}
    assert state.retry_attempts == %{}
    assert state.slot_queue == []
    refute MapSet.member?(state.claimed, issue.id)
    assert Map.take(Ledger.get(issue.id), Map.keys(@usage)) == @usage
  end

  defp receive_observation(state, issue_id) do
    ref = state.issue_operations[issue_id].task.ref

    receive do
      {^ref, result} -> {ref, result}
    after
      1_000 -> flunk("dependency observation did not finish")
    end
  end

  defp claimed_state(issue, fetcher) do
    %{empty_state() | claimed: MapSet.new([issue.id]), issue_fetcher: fetcher}
  end

  defp empty_state do
    %State{max_concurrent_agents: 1, poll_interval_ms: 30_000}
  end

  defp issue(state, blockers) do
    %Issue{
      id: "dependency-admission",
      identifier: "SPK-1141",
      title: "Dependency admission",
      state: state,
      priority: 1,
      blocked_by: blockers,
      labels: []
    }
  end
end
