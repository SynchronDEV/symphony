defmodule SymphonyElixir.OrchestratorAsyncRegressionTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.IssueStateBatcher
  alias SymphonyElixir.Ledger
  alias SymphonyElixir.Orchestrator.State

  setup do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", max_concurrent_agents: 1)
    :ok
  end

  test "a delayed poll leaves snapshots, token events and worker exits responsive and never installs its old state" do
    parent = self()
    name = unique_name()

    fetcher = fn _cutoff ->
      send(parent, {:poll_waiting, self()})

      receive do
        {:finish_poll, result} -> result
      end
    end

    server = start_supervised!({Orchestrator, name: name, candidate_fetcher: fetcher})
    assert_receive {:poll_waiting, poll_pid}, 1_000

    worker =
      spawn(fn ->
        receive do
          :finish -> :ok
        end
      end)

    issue = issue("responsive")

    :sys.replace_state(server, fn state ->
      %{state | running: %{issue.id => running_entry(issue, worker)}, claimed: MapSet.new([issue.id])}
    end)

    usage = %{"tokenUsage" => %{"total" => %{"inputTokens" => 11, "outputTokens" => 3, "totalTokens" => 14}}}
    update = %{event: :token_usage_updated, timestamp: DateTime.utc_now(), usage: usage}
    send(server, {:codex_worker_update, issue.id, update})

    snapshot = Orchestrator.snapshot(name, 200)
    assert snapshot.codex_totals.total_tokens == 14
    assert snapshot.polling.checking?

    send(server, :run_poll_cycle)
    send(server, :tick)
    refute_receive {:poll_waiting, _}, 30
    send(worker, :finish)
    eventually(fn -> Map.has_key?(:sys.get_state(server).retry_attempts, issue.id) end)
    assert Orchestrator.snapshot(name, 200).running == []

    send(poll_pid, {:finish_poll, {:ok, []}})
    eventually(fn -> is_nil(:sys.get_state(server).poll_task) end)
    state = :sys.get_state(server)
    assert state.codex_totals.total_tokens == 14
    assert state.running == %{}
    assert Map.has_key?(state.retry_attempts, issue.id)
    assert MapSet.member?(state.claimed, issue.id)
  end

  test "a stale reconciliation cannot stop a replacement worker or overwrite its token totals" do
    issue = issue("generation")
    old_ref = make_ref()
    new_ref = make_ref()
    poll_ref = make_ref()
    timer = Process.send_after(self(), :unused, 60_000)
    entry = %{running_entry(issue, nil) | ref: new_ref, codex_total_tokens: 42}
    poll = %{task: %{ref: poll_ref}, timer: timer, running_refs: %{issue.id => old_ref}, blocked_refs: %{}, retention_ids: [], started_at: DateTime.utc_now(), full?: true, force_full?: true}
    state = %State{running: %{issue.id => entry}, claimed: MapSet.new([issue.id]), poll_task: poll, poll_interval_ms: 30_000, max_concurrent_agents: 1, codex_totals: totals(42)}

    {:noreply, result} =
      Orchestrator.handle_info(
        {poll_ref, %{running: {:ok, [%{issue | state: "Done"}]}, blocked: {:ok, []}, candidates: {:ok, []}}},
        state
      )

    assert result.running[issue.id].ref == new_ref
    assert result.running[issue.id].codex_total_tokens == 42
    assert result.codex_totals.total_tokens == 42
    Process.cancel_timer(result.tick_timer_ref)
  end

  test "missing and ineligible final retry refreshes release their claim" do
    for result <- [{:ok, []}, {:ok, [%{issue("retry") | state: "Human Review"}]}] do
      issue = issue("retry")
      state = dispatch_state(issue, fn _ -> result end)
      pending = Orchestrator.dispatch_issue_for_test(issue, state)
      assert MapSet.member?(pending.claimed, issue.id)
      assert map_size(pending.issue_operations) == 1
      {ref, observation} = receive_observation(pending, issue.id)
      {:noreply, finished} = Orchestrator.handle_info({ref, observation}, pending)
      refute MapSet.member?(finished.claimed, issue.id)
      assert finished.retry_attempts == %{}
      assert finished.issue_operations == %{}
    end
  end

  test "transient final refresh errors reschedule instead of stranding retry claims" do
    issue = issue("retry-error")
    state = dispatch_state(issue, fn _ -> {:error, {:rate_limited, nil}} end)
    pending = Orchestrator.dispatch_issue_for_test(issue, state)
    {ref, observation} = receive_observation(pending, issue.id)
    {:noreply, finished} = Orchestrator.handle_info({ref, observation}, pending)
    assert MapSet.member?(finished.claimed, issue.id)
    assert finished.retry_attempts[issue.id].error == {:rate_limited, nil}
    assert finished.retry_attempts[issue.id].due_at_ms - System.monotonic_time(:millisecond) > 250_000
    Process.cancel_timer(finished.retry_attempts[issue.id].timer_ref)
  end

  test "dispatch reserves capacity before its final tracker read finishes" do
    parent = self()
    first = issue("first")

    fetcher = fn _ ->
      send(parent, {:dispatch_waiting, self()})

      receive do
        :finish -> {:ok, []}
      end
    end

    pending = Orchestrator.dispatch_issue_for_test(first, dispatch_state(first, fetcher))
    assert_receive {:dispatch_waiting, pid}
    refute Orchestrator.should_dispatch_issue_for_test(issue("second"), pending)
    assert pending.running == %{}
    assert MapSet.member?(pending.claimed, first.id)
    send(pid, :finish)
    {ref, result} = receive_observation(pending, first.id)
    assert {:noreply, _} = Orchestrator.handle_info({ref, result}, pending)
  end

  test "slot queue dispatch releases a stale claim and continues to the next queued entry" do
    first = issue("queue-first")
    second = issue("queue-second")
    queue = Enum.map([first, second], fn issue -> %{issue_id: issue.id, issue: issue, attempt: 2, metadata: %{identifier: issue.identifier}} end)

    state = %State{
      slot_queue: queue,
      claimed: MapSet.new([first.id, second.id]),
      max_concurrent_agents: 1,
      poll_interval_ms: 30_000,
      issue_fetcher: fn _ -> {:ok, []} end,
      candidate_fetcher: fn _ -> {:ok, []} end
    }

    {:noreply, pending} = Orchestrator.handle_info(:run_poll_cycle, state)
    {ref, result} = receive_observation(pending, first.id)
    {:noreply, next} = Orchestrator.handle_info({ref, result}, pending)
    refute MapSet.member?(next.claimed, first.id)
    assert Map.has_key?(next.issue_operations, second.id)
    {second_ref, result} = receive_observation(next, second.id)
    {:noreply, done} = Orchestrator.handle_info({second_ref, result}, next)
    refute MapSet.member?(done.claimed, second.id)
    assert done.slot_queue == []
    Process.exit(done.poll_task.task.pid, :kill)
    Process.cancel_timer(done.poll_task.timer)
  end

  test "batcher accepts new callers during a delayed fetch and keeps only one batch in flight" do
    parent = self()
    name = unique_name()

    fetcher = fn ids ->
      send(parent, {:batch_waiting, self(), ids})

      receive do
        :finish -> {:ok, Enum.map(ids, &%{id: &1})}
      end
    end

    batcher = start_supervised!({IssueStateBatcher, name: name, fetcher: fetcher, batch_delay_ms: 1})
    first = Task.async(fn -> GenServer.call(batcher, {:fetch, ["one"]}) end)
    assert_receive {:batch_waiting, first_pid, ["one"]}
    second = Task.async(fn -> GenServer.call(batcher, {:fetch, ["two"]}) end)
    eventually(fn -> map_size(:sys.get_state(batcher).pending) == 1 end)
    assert :sys.get_state(batcher, 200).in_flight.task.pid == first_pid
    refute_receive {:batch_waiting, _, ["two"]}, 30
    send(first_pid, :finish)
    assert {:ok, [%{id: "one"}]} = Task.await(first)
    assert_receive {:batch_waiting, second_pid, ["two"]}
    send(second_pid, :finish)
    assert {:ok, [%{id: "two"}]} = Task.await(second)
  end

  test "Linear quota denial returns promptly without sleeping or issuing a request" do
    alias SymphonyElixir.Linear.RateLimitBudget
    reset_at = DateTime.add(DateTime.utc_now(), 600)
    RateLimitBudget.update_from_headers(%{"x-ratelimit-requests-remaining" => "0", "x-ratelimit-requests-reset" => DateTime.to_iso8601(reset_at)})
    parent = self()
    on_exit(fn -> RateLimitBudget.update_from_headers(%{"x-ratelimit-requests-remaining" => "1000"}) end)

    task =
      Task.async(fn ->
        Client.graphql("query { viewer { id } }", %{},
          request_fun: fn _, _ -> send(parent, :unexpected_request) end,
          sleep_fun: fn _ -> send(parent, :unexpected_sleep) end
        )
      end)

    assert {:error, {:rate_limited, ^reset_at}} = Task.await(task, 200)
    refute_receive :unexpected_request
    refute_receive :unexpected_sleep
  end

  test "terminal cleanup waits for worker exit and uses its captured path after a root change" do
    parent = self()
    root = Path.join(System.tmp_dir!(), "symphony-cleanup-#{System.unique_integer([:positive])}")
    old_root = Path.join(root, "old")
    new_root = Path.join(root, "new")
    issue = issue("cleanup")
    workspace = Path.join(old_root, issue.identifier)
    other_workspace = Path.join(new_root, issue.identifier)
    alive_marker = Path.join(root, "worker-alive")
    unsafe_marker = Path.join(root, "cleanup-before-worker-exit")
    File.mkdir_p!(workspace)
    File.mkdir_p!(other_workspace)
    File.write!(Path.join(other_workspace, "precious"), "preserve")
    File.write!(alive_marker, "alive")

    for args <- [
          ["init", "-b", "main"],
          ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "base"],
          ["update-ref", "refs/remotes/origin/staging", "HEAD"]
        ] do
      assert {_output, 0} = System.cmd("git", ["-C", workspace | args], stderr_to_stdout: true)
    end

    on_exit(fn -> File.rm_rf(root) end)
    hook = "if [ -e '#{alive_marker}' ]; then touch '#{unsafe_marker}'; fi"
    workflow_opts = [tracker_kind: "memory", workspace_root: new_root, hook_before_remove: hook]
    write_workflow_file!(Workflow.workflow_file_path(), workflow_opts)

    worker =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, {:worker_ready, self()})

        receive do
          {:EXIT, _sender, :shutdown} ->
            send(parent, :worker_stopping)

            receive do
              :allow_exit -> File.rm!(alive_marker)
            end
        end
      end)

    on_exit(fn -> Process.exit(worker, :kill) end)
    assert_receive {:worker_ready, ^worker}
    name = unique_name()
    issue_fetcher = fn _ -> {:ok, [%{issue | state: "Done"}]} end
    opts = [name: name, candidate_fetcher: fn _ -> {:ok, []} end, issue_fetcher: issue_fetcher]
    server = start_supervised!({Orchestrator, opts})

    :sys.replace_state(server, fn state ->
      entry = Map.merge(running_entry(issue, worker), %{workspace_path: workspace, workspace_root: old_root, cleanup_base_ref: "refs/remotes/origin/staging"})
      %{state | running: %{issue.id => entry}, claimed: MapSet.new([issue.id])}
    end)

    send(server, :run_poll_cycle)
    assert_receive :worker_stopping, 1_000
    assert File.dir?(workspace)
    assert MapSet.member?(:sys.get_state(server).claimed, issue.id)
    assert Orchestrator.snapshot(name, 200).running == []
    send(worker, :allow_exit)
    eventually(fn -> not MapSet.member?(:sys.get_state(server).claimed, issue.id) end)
    refute Process.alive?(worker)
    refute File.exists?(workspace)
    refute File.exists?(unsafe_marker)
    assert File.read!(Path.join(other_workspace, "precious")) == "preserve"
  end

  test "an exhausted budget without reset headers eventually allows one recovery probe" do
    alias SymphonyElixir.Linear.RateLimitBudget
    RateLimitBudget.update_from_headers(%{"x-ratelimit-requests-remaining" => "0"})
    on_exit(fn -> RateLimitBudget.update_from_headers(%{"x-ratelimit-requests-remaining" => "1000"}) end)
    assert {:error, {:rate_limited, nil}} = RateLimitBudget.acquire_read()
    Agent.update(RateLimitBudget, &%{&1 | updated_at: DateTime.add(DateTime.utc_now(), -61, :second)})
    assert :ok = RateLimitBudget.acquire_read()
    assert {:error, {:rate_limited, %DateTime{}}} = RateLimitBudget.acquire_read()
  end

  test "a malformed batch result fails callers without crashing the batcher" do
    name = unique_name()
    fetcher = fn _ -> {:ok, [:malformed]} end
    batcher = start_supervised!({IssueStateBatcher, name: name, fetcher: fetcher, batch_delay_ms: 1})
    assert {:error, :invalid_issue_state_result} = GenServer.call(batcher, {:fetch, ["issue"]})
    assert Process.alive?(batcher)
  end

  test "batch task failure replies to waiters and permits the next batch" do
    parent = self()

    fetcher = fn ids ->
      send(parent, {:fetch_started, self(), ids})

      receive do
        :crash -> exit(:controlled_failure)
        :finish -> {:ok, Enum.map(ids, &%{id: &1})}
      end
    end

    batcher = start_supervised!({IssueStateBatcher, name: unique_name(), fetcher: fetcher, batch_delay_ms: 1})
    first = Task.async(fn -> GenServer.call(batcher, {:fetch, ["first"]}) end)
    assert_receive {:fetch_started, fetch_pid, ["first"]}
    second = Task.async(fn -> GenServer.call(batcher, {:fetch, ["second"]}) end)
    eventually(fn -> map_size(:sys.get_state(batcher).pending) == 1 end)
    send(fetch_pid, :crash)
    assert {:error, {:issue_state_batch_failed, :controlled_failure}} = Task.await(first)
    assert_receive {:fetch_started, next_pid, ["second"]}
    send(next_pid, :finish)
    assert {:ok, [%{id: "second"}]} = Task.await(second)
    assert Process.alive?(batcher)
  end

  test "batch timeout terminates its fetch and answers the waiting caller" do
    parent = self()

    fetcher = fn _ ->
      send(parent, {:fetch_started, self()})

      receive do
        :finish -> {:ok, []}
      end
    end

    batcher = start_supervised!({IssueStateBatcher, name: unique_name(), fetcher: fetcher, batch_delay_ms: 1})
    caller = Task.async(fn -> GenServer.call(batcher, {:fetch, ["waiting"]}) end)
    assert_receive {:fetch_started, fetch_pid}
    monitor = Process.monitor(fetch_pid)
    ref = :sys.get_state(batcher).in_flight.task.ref
    send(batcher, {:batch_timeout, ref})
    assert {:error, :issue_state_batch_timeout} = Task.await(caller)
    assert_receive {:DOWN, ^monitor, :process, ^fetch_pid, :killed}
    assert :sys.get_state(batcher).in_flight == nil
    assert Process.alive?(batcher)
  end

  test "non-list batch results are structured errors and empty IDs never fetch" do
    parent = self()
    name = unique_name()

    fetcher = fn ids ->
      send(parent, {:fetched, ids})
      {:ok, :malformed}
    end

    batcher = start_supervised!({IssueStateBatcher, name: name, fetcher: fetcher, batch_delay_ms: 1})
    assert {:ok, []} = IssueStateBatcher.fetch_issue_states_by_ids([], server: name)
    assert :sys.get_state(batcher).pending == %{}
    refute_receive {:fetched, _}, 10
    assert {:error, {:invalid_issue_state_result, {:ok, :malformed}}} = GenServer.call(batcher, {:fetch, ["bad"]})
    assert_receive {:fetched, ["bad"]}
    assert Process.alive?(batcher)
  end

  test "Studio review failures routed back to Todo enforce the rework cap" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", max_rework_cycles: 1)
    issue = %{issue("todo-rework") | state: "Todo"}
    SymphonyElixir.Ledger.observe_state(issue.id, "In Review")
    assert {:ok, _} = Orchestrator.prepare_issue_for_dispatch_for_test(issue)
    SymphonyElixir.Ledger.observe_state(issue.id, "In Review")

    assert {:block, "symphony-stuck: max_rework_cycles=1" <> _} =
             Orchestrator.prepare_issue_for_dispatch_for_test(issue)

    assert SymphonyElixir.Ledger.get(issue.id).rework_count == 2
  end

  test "stopping workers reserve capacity until termination is confirmed" do
    issue = issue("stopping")

    worker =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> Process.exit(worker, :kill) end)
    entry = running_entry(issue, worker)
    operation = %{kind: :cleanup, entry: entry}
    state = %State{max_concurrent_agents: 1, issue_operations: %{issue.id => operation}}
    refute Orchestrator.should_dispatch_issue_for_test(issue("new"), state)
    blocked = %State{max_concurrent_agents: 1, blocked: %{issue.id => entry}}
    refute Orchestrator.should_dispatch_issue_for_test(issue("new"), blocked)
  end

  test "missing blocked issues retain ownership until a previously unconfirmed worker stops" do
    parent = self()

    worker =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, :blocked_worker_ready)

        receive do
          {:EXIT, _sender, :shutdown} ->
            send(parent, :blocked_worker_stopping)

            receive do
              :finish -> :ok
            end
        end
      end)

    on_exit(fn -> Process.exit(worker, :kill) end)
    assert_receive :blocked_worker_ready
    issue = issue("blocked-stop")
    entry = Map.merge(running_entry(issue, worker), %{blocked_at: DateTime.utc_now(), error: "stop unconfirmed"})
    poll_ref = make_ref()
    timer = Process.send_after(self(), :unused, 60_000)
    poll = %{task: %{ref: poll_ref}, timer: timer, running_refs: %{}, blocked_refs: %{issue.id => entry.blocked_at}, retention_ids: [], started_at: DateTime.utc_now(), full?: true, force_full?: true}
    state = %State{blocked: %{issue.id => entry}, claimed: MapSet.new([issue.id]), poll_task: poll, poll_interval_ms: 30_000, max_concurrent_agents: 1, codex_totals: totals(0)}

    {:noreply, stopping} =
      Orchestrator.handle_info(
        {poll_ref, %{running: {:ok, []}, blocked: {:ok, []}, candidates: {:ok, []}}},
        state
      )

    assert_receive :blocked_worker_stopping
    assert MapSet.member?(stopping.claimed, issue.id)
    refute Orchestrator.should_dispatch_issue_for_test(issue("new"), stopping)
    send(worker, :finish)
    {ref, result} = receive_observation(stopping, issue.id)
    {:noreply, stopped} = Orchestrator.handle_info({ref, result}, stopping)
    refute MapSet.member?(stopped.claimed, issue.id)
    refute Process.alive?(worker)
    Process.cancel_timer(stopped.tick_timer_ref)
  end

  test "fresh implementation and review dispatch preserve the remaining token reserve" do
    opts = [tracker_kind: "memory", max_tokens_per_issue: 250_000, min_tokens_before_dispatch: 80_000]
    write_workflow_file!(Workflow.workflow_file_path(), opts)

    for role <- ["Todo", "In Review"] do
      issue = %{issue("reserve-#{role}") | state: role}
      SymphonyElixir.Ledger.put(issue.id, %{cumulative_tokens: 170_001, dispatch_count: 1})
      assert {:block, "symphony-budget-reserve: " <> _} = Orchestrator.prepare_issue_for_dispatch_for_test(issue)
      assert SymphonyElixir.Ledger.get(issue.id).cumulative_tokens == 170_001
      assert SymphonyElixir.Ledger.get(issue.id).dispatch_count == 1
      SymphonyElixir.Ledger.put(issue.id, %{cumulative_tokens: 170_000})
      assert {:ok, _} = Orchestrator.prepare_issue_for_dispatch_for_test(issue)
    end
  end

  test "an exhausted token cap blocks dispatch even with the default zero reserve" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", max_tokens_per_issue: 250_000)
    issue = issue("exhausted")
    SymphonyElixir.Ledger.put(issue.id, %{cumulative_tokens: 250_000})
    assert {:block, "symphony-budget-exceeded: " <> _} = Orchestrator.prepare_issue_for_dispatch_for_test(issue)
  end

  test "routing removal persists stopped status after confirmed worker exit without erasing budgets" do
    issue = issue("removed-routing")
    worker = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> if Process.alive?(worker), do: Process.exit(worker, :kill) end)
    SymphonyElixir.Ledger.put(issue.id, %{status: :running, cumulative_tokens: 1234, dispatch_count: 2})
    state = %State{running: %{issue.id => running_entry(issue, worker)}, claimed: MapSet.new([issue.id]), codex_totals: totals(0)}
    result = Orchestrator.reconcile_issue_states_for_test([%{issue | state: "Backlog"}], state)
    eventually(fn -> not Process.alive?(worker) end)
    {ref, observation} = receive_observation(result, issue.id)
    {:noreply, result} = Orchestrator.handle_info({ref, observation}, result)
    refute Map.has_key?(result.running, issue.id)
    assert SymphonyElixir.Ledger.get(issue.id).status == :stopped
    assert SymphonyElixir.Ledger.get(issue.id).cumulative_tokens == 1234
    assert SymphonyElixir.Ledger.get(issue.id).dispatch_count == 2
  end

  test "a readiness failure blocks instead of scheduling another paid attempt" do
    issue = issue("readiness")
    ref = make_ref()
    entry = Map.merge(running_entry(issue, nil), %{ref: ref, last_codex_event: :worker_preflight_failed})
    state = %State{running: %{issue.id => entry}, claimed: MapSet.new([issue.id]), codex_totals: totals(0)}
    {:noreply, result} = Orchestrator.handle_info({:DOWN, ref, :process, self(), :failed}, state)
    assert result.retry_attempts == %{}
    assert result.blocked[issue.id].error == "worker readiness hook failed; operator correction required before retry"
    assert SymphonyElixir.Ledger.get(issue.id).status == :blocked
  end

  test "a failing before-run hook reports readiness failure without launching Codex" do
    root = Path.join(System.tmp_dir!(), "symphony-preflight-#{System.unique_integer([:positive])}")
    marker = Path.join(root, "codex-started")
    on_exit(fn -> File.rm_rf(root) end)
    opts = [tracker_kind: "memory", workspace_root: root, hook_before_run: "exit 42", codex_command: "touch #{marker}"]
    write_workflow_file!(Workflow.workflow_file_path(), opts)
    issue = issue("hook-failure")
    capture_log(fn -> assert_raise RuntimeError, fn -> AgentRunner.run(issue, self()) end end)
    assert_receive {:worker_preparation_phase, _, :workspace, %DateTime{}}
    assert_receive {:worker_preparation_phase, _, :before_run, %DateTime{}}
    assert_receive {:codex_worker_update, _, %{event: :worker_preflight_failed}}
    refute_receive {:worker_preparation_phase, _, :codex_startup, _}
    refute File.exists?(marker)
  end

  test "preparation stages have separate bounded clocks before Codex activity begins" do
    opts = [tracker_kind: "memory", codex_stall_timeout_ms: 1_000, hook_timeout_ms: 60_000]
    write_workflow_file!(Workflow.workflow_file_path(), opts)
    {server, name, issue, worker} = stalled_server("preparation")
    stage_at = DateTime.add(DateTime.utc_now(), -10, :second)

    for stage <- [:workspace, :before_run, :codex_startup] do
      send(server, {:worker_preparation_phase, issue.id, stage, stage_at})
      send(server, :tick)
      assert [%{issue_id: id}] = Orchestrator.snapshot(name, 200).running
      assert id == issue.id
      assert Process.alive?(worker)
      assert :sys.get_state(server).running[issue.id].preparation_started_at == stage_at
    end

    send(server, {:worker_preparation_phase, issue.id, :ready, DateTime.utc_now()})
    send(server, :tick)
    assert [_] = Orchestrator.snapshot(name, 200).running

    send(server, {:codex_worker_update, issue.id, %{event: :notification, timestamp: stage_at}})
    send(server, :tick)
    eventually(fn -> Map.has_key?(:sys.get_state(server).retry_attempts, issue.id) end)
    refute Process.alive?(worker)
    assert Ledger.get(issue.id).stall_events == 1
  end

  test "a preparation phase that exceeds its own hook deadline is still recovered" do
    opts = [tracker_kind: "memory", codex_stall_timeout_ms: 1_000, hook_timeout_ms: 2_000]
    write_workflow_file!(Workflow.workflow_file_path(), opts)
    {server, _name, issue, worker} = stalled_server("hung-preparation")
    send(server, {:worker_preparation_phase, issue.id, :before_run, DateTime.add(DateTime.utc_now(), -5, :second)})
    send(server, :tick)
    eventually(fn -> Map.has_key?(:sys.get_state(server).retry_attempts, issue.id) end)
    refute Process.alive?(worker)
    assert Ledger.get(issue.id).stall_events == 1
  end

  test "slow durable stall accounting leaves snapshots responsive and retries only after acknowledgement" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", codex_stall_timeout_ms: 1_000)
    {server, name, issue, worker} = stalled_server("slow-stall-accounting")
    ledger = hold_next_ledger_sync()
    send(server, :tick)
    assert_receive {:ledger_sync_waiting, ^ledger}, 1_000
    refute Process.alive?(worker)
    assert Orchestrator.snapshot(name, 200).running == []
    assert :sys.get_state(server).retry_attempts == %{}
    refute Map.has_key?(Ledger.committed_snapshot(), issue.id)

    # Cross the former synchronous GenServer.call deadline while the real fsync
    # is held. The dispatcher must remain alive and must not replay the increment.
    refute_receive {:ledger_sync_finished, ^ledger}, 5_100
    assert is_map(Orchestrator.snapshot(name, 200))
    assert :sys.get_state(server).retry_attempts == %{}
    send(ledger, :allow_ledger_sync)
    assert_receive {:ledger_sync_finished, ^ledger}, 1_000
    eventually(fn -> Map.has_key?(:sys.get_state(server).retry_attempts, issue.id) end)
    assert Ledger.get(issue.id).stall_events == 1
    assert Ledger.committed_snapshot()[issue.id].stall_events == 1
    assert Ledger.get(issue.id).blocked_reason == nil
  end

  test "an ambiguous stall accounting deadline stays blocked after a late durable write" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", codex_stall_timeout_ms: 1_000)
    {server, name, issue, worker} = stalled_server("unknown-stall-accounting")
    ledger = hold_next_ledger_sync()
    send(server, :tick)
    assert_receive {:ledger_sync_waiting, ^ledger}, 1_000
    op = :sys.get_state(server).issue_operations[issue.id]
    send(server, {:observation_timeout, op.task.ref})
    assert is_map(Orchestrator.snapshot(name, 200))
    refute Process.alive?(worker)
    assert :sys.get_state(server).blocked[issue.id].error =~ "accounting unconfirmed"
    send(ledger, :allow_ledger_sync)
    assert_receive {:ledger_sync_finished, ^ledger}, 1_000
    assert Ledger.get(issue.id).stall_events == 1

    send(server, {op.task.ref, :ok})
    send(server, :tick)
    assert Orchestrator.snapshot(name, 200).running == []
    state = :sys.get_state(server)
    assert MapSet.member?(state.claimed, issue.id)
    assert state.retry_attempts == %{}
    assert state.blocked[issue.id].error =~ "accounting unconfirmed"
    assert Ledger.get(issue.id).stall_events == 1

    stop_supervised!(Orchestrator)
    opts = [name: name, candidate_fetcher: fn _ -> {:ok, [issue]} end, issue_fetcher: fn _ -> {:ok, [issue]} end]
    restarted = start_supervised!({Orchestrator, opts})
    eventually(fn -> Map.has_key?(:sys.get_state(restarted).blocked, issue.id) end)
    assert Orchestrator.snapshot(name, 200).running == []
    assert :sys.get_state(restarted).retry_attempts == %{}
    assert Ledger.get(issue.id).blocked_reason =~ "accounting unconfirmed"
    assert Ledger.get(issue.id).stall_events == 1
    persisted = Ledger.info().path |> File.read!() |> Jason.decode!()
    assert persisted[issue.id]["blocked_reason"] =~ "accounting unconfirmed"
    assert persisted[issue.id]["stall_events"] == 1
  end

  test "durable snapshots are isolated by ledger owner and synchronous counter APIs remain compatible" do
    assert Ledger.increment("compatibility", :stall_events) == %{stall_events: 1}
    assert Ledger.all()["compatibility"].stall_events == 1
    assert Ledger.committed_snapshot()["compatibility"].stall_events == 1
    Ledger.put("compatibility", %{cumulative_tokens: 1234, dispatch_count: 2})
    quarantined = Ledger.quarantine_stall("compatibility", "repeated stall requires review")
    assert quarantined.stall_events == 2
    assert quarantined.cumulative_tokens == 1234
    assert quarantined.dispatch_count == 2
    assert quarantined.blocked_reason == "repeated stall requires review"

    name = unique_name()
    path = Path.join(System.tmp_dir!(), "symphony-snapshot-#{name}.json")
    on_exit(fn -> File.rm(path) end)
    named = start_supervised!({Ledger, name: name, path: path})
    assert Ledger.committed_snapshot(name) == %{}
    assert %{stall_events: 2} = GenServer.call(named, {:update, "other", fn _ -> %{stall_events: 2} end})
    assert Ledger.committed_snapshot(name) == %{"other" => %{stall_events: 2}}
    refute Map.has_key?(Ledger.committed_snapshot(), "other")
    stop_supervised!(Ledger)
    assert Ledger.committed_snapshot(name) == %{}
  end

  defp stalled_server(id) do
    issue = issue(id)
    worker = spawn(fn -> receive do: (:finish -> :ok) end)
    on_exit(fn -> if Process.alive?(worker), do: Process.exit(worker, :kill) end)
    name = unique_name()
    opts = [name: name, candidate_fetcher: fn _ -> {:ok, []} end, issue_fetcher: fn _ -> {:ok, [issue]} end]
    server = start_supervised!({Orchestrator, opts})
    eventually(fn -> is_nil(:sys.get_state(server).poll_task) end)

    :sys.replace_state(server, fn state ->
      entry = %{running_entry(issue, worker) | started_at: DateTime.add(DateTime.utc_now(), -10, :second)}
      %{state | running: %{issue.id => entry}, claimed: MapSet.new([issue.id])}
    end)

    {server, name, issue, worker}
  end

  defp hold_next_ledger_sync do
    parent = self()
    ledger = Process.whereis(Ledger)
    once = make_ref()

    sync = fn file ->
      if Process.get(once) do
        :file.sync(file)
      else
        Process.put(once, :waiting)
        send(parent, {:ledger_sync_waiting, self()})
        receive do: (:allow_ledger_sync -> :ok)
        result = :file.sync(file)
        Process.put(once, :done)
        send(parent, {:ledger_sync_finished, self()})
        result
      end
    end

    :sys.replace_state(ledger, &%{&1 | file_sync: sync})

    on_exit(fn ->
      {:dictionary, dictionary} = Process.info(ledger, :dictionary)
      if List.keyfind(dictionary, once, 0) == {once, :waiting}, do: send(ledger, :allow_ledger_sync)
      :sys.replace_state(ledger, fn state -> %{state | file_sync: &:file.sync/1} end)
    end)

    ledger
  end

  defp receive_observation(state, id) do
    ref = state.issue_operations[id].task.ref

    receive do
      {^ref, result} -> {ref, result}
    after
      1_000 -> flunk("operation did not finish")
    end
  end

  defp dispatch_state(issue, fetcher) do
    %State{claimed: MapSet.new([issue.id]), issue_fetcher: fetcher, max_concurrent_agents: 1, poll_interval_ms: 30_000, codex_totals: totals(0)}
  end

  defp running_entry(issue, worker) do
    %{
      pid: worker,
      ref: if(is_pid(worker), do: Process.monitor(worker), else: make_ref()),
      issue: issue,
      identifier: issue.identifier,
      worker_host: nil,
      workspace_path: nil,
      workspace_root: nil,
      session_id: nil,
      started_at: DateTime.utc_now(),
      retry_attempt: 0,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      last_codex_message: nil,
      codex_app_server_pid: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      turn_count: 0
    }
  end

  defp totals(total), do: %{input_tokens: total, output_tokens: 0, cached_input_tokens: 0, total_tokens: total, effective_total_tokens: total, seconds_running: 0}
  defp issue(id), do: %Issue{id: id, identifier: "SPK-#{id}", title: id, state: "Todo", priority: 1, blocked_by: [], labels: []}
  defp unique_name, do: String.to_atom("async_regression_#{System.unique_integer([:positive])}")
  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: flunk("condition did not become true")

  defp eventually(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (receive do
         after
           5 -> eventually(fun, attempts - 1)
         end)
  end
end
