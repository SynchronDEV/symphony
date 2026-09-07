defmodule SymphonyElixir.IssueStateBatcherBoundaryTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.IssueStateBatcher

  test "default start preserves the existing batcher and empty default fetch needs no tracker" do
    pid = Process.whereis(IssueStateBatcher)
    assert {:error, {:already_started, ^pid}} = IssueStateBatcher.start_link()
    assert {:ok, []} = IssueStateBatcher.fetch_issue_states_by_ids([])
  end

  test "an absent batcher falls back to the configured memory tracker" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")
    issue = %Issue{id: "issue-1", state: "In Progress"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])

    assert {:ok, [^issue]} =
             IssueStateBatcher.fetch_issue_states_by_ids(["issue-1", "issue-1", "missing"],
               server: :absent_boundary_issue_state_batcher
             )
  end

  test "a flush during an active batch preserves queued requests and clears the consumed timer" do
    pending = %{make_ref() => %{from: {self(), make_ref()}, ids: ["next"]}}
    flight = %{task: %{ref: make_ref()}, pending: %{}}

    state = %IssueStateBatcher{
      pending: pending,
      pending_ids: MapSet.new(["next"]),
      in_flight: flight,
      timer_ref: make_ref()
    }

    assert {:noreply, next} = IssueStateBatcher.handle_info(:flush, state)
    assert next == %{state | timer_ref: nil}
    assert {:noreply, ^next} = IssueStateBatcher.handle_info({:batch_timeout, make_ref()}, next)
    assert {:noreply, ^next} = IssueStateBatcher.handle_info(:unrelated, next)
  end
end
