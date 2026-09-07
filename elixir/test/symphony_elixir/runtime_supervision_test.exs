defmodule SymphonyElixir.RuntimeSupervisionTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Ledger

  test "orchestrator crash stops existing workers before restarting empty bookkeeping and reloads saved counters" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", server_port: nil)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    supervisor = Process.whereis(SymphonyElixir.Supervisor)
    old_orchestrator = Process.whereis(Orchestrator)
    old_task_supervisor = Process.whereis(SymphonyElixir.TaskSupervisor)
    old_ledger = Process.whereis(Ledger)
    assert is_pid(old_orchestrator)

    issue_id = "runtime-supervision-#{System.unique_integer([:positive])}"
    Ledger.put(issue_id, %{dispatch_count: 3, cumulative_tokens: 450, rework_count: 1})
    assert :ok = Ledger.flush()
    ledger_path = Ledger.info().path
    saved_entries = Jason.decode!(File.read!(ledger_path))

    owner = self()

    {:ok, worker} =
      Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fn ->
        Process.flag(:trap_exit, true)
        send(owner, {:worker_started, self()})

        receive do
          {:EXIT, _supervisor, :shutdown} ->
            send(owner, {:worker_stopping, self()})

            receive do
              :allow_stop -> :ok
            end
        end
      end)

    assert_receive {:worker_started, ^worker}, 1_000
    worker_ref = Process.monitor(worker)
    orchestrator_ref = Process.monitor(old_orchestrator)

    try do
      capture_log(fn ->
        Process.exit(old_orchestrator, :kill)
        assert_receive {:DOWN, ^orchestrator_ref, :process, ^old_orchestrator, :killed}, 2_000
        assert_receive {:worker_stopping, ^worker}, 2_000

        # Hold the old worker at shutdown: replacement bookkeeping must not exist yet.
        assert Process.alive?(worker)
        assert Process.whereis(Orchestrator) == nil

        send(worker, :allow_stop)
        assert_receive {:DOWN, ^worker_ref, :process, ^worker, :normal}, 2_000

        # The supervisor serializes this call after completing its restart sequence.
        children = Supervisor.which_children(supervisor)
        assert Process.whereis(SymphonyElixir.Supervisor) == supervisor
        new_orchestrator = child_pid(children, Orchestrator)
        new_task_supervisor = child_pid(children, SymphonyElixir.TaskSupervisor)
        new_ledger = child_pid(children, Ledger)
        refute new_orchestrator == old_orchestrator
        refute new_task_supervisor == old_task_supervisor
        refute new_ledger == old_ledger
        assert is_pid(new_orchestrator)
        refute Process.alive?(worker)
        refute worker in Task.Supervisor.children(new_task_supervisor)
        assert :sys.get_state(new_orchestrator).running == %{}
        assert Ledger.info().path == ledger_path
        assert Ledger.get(issue_id) == %{dispatch_count: 3, cumulative_tokens: 450, rework_count: 1}
        assert Jason.decode!(File.read!(ledger_path)) == saved_entries
      end)
    after
      send(worker, :allow_stop)
      # Also serves as a barrier if an assertion failed while shutdown was held.
      Supervisor.which_children(supervisor)
      stop_default_http_server()
    end
  end

  defp child_pid(children, id) do
    Enum.find_value(children, fn
      {^id, pid, _type, _modules} -> pid
      _ -> nil
    end)
  end
end
