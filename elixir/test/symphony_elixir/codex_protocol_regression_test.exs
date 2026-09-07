defmodule SymphonyElixir.CodexProtocolRegressionTest do
  use SymphonyElixir.TestSupport

  for status <- ["failed", "interrupted", "inProgress", "unknown"] do
    test "terminal status #{status} cannot complete successfully" do
      {session, issue, _trace} = server(unquote(status))
      assert {:error, {reason, turn}} = AppServer.run_turn(session, "work", issue)
      assert reason in [:turn_failed, :turn_interrupted, :invalid_terminal_turn]
      assert turn["status"] == unquote(status)
      assert turn["error"] == %{"message" => "terminal detail"}
    end
  end

  test "stale turn and foreign thread completions and errors cannot end the active turn" do
    {session, issue, _trace} = server("stale")
    assert {:ok, %{turn_id: "turn-1"}} = AppServer.run_turn(session, "work", issue)
    assert {:error, {:turn_failed, %{"id" => "turn-2"}}} = AppServer.run_turn(session, "continue", issue)
  end

  for ordering <- ["ack_first", "terminal_first"] do
    test "interrupt #{ordering} waits for both acknowledgement and terminal before continuation" do
      {session, issue, trace} = server(unquote(ordering))
      assert {:error, {:stall_timeout, "turn-1"}} = AppServer.run_turn(session, "work", issue)
      assert :ok = AppServer.interrupt_turn(session, "turn-1")
      assert {:ok, %{turn_id: "turn-2"}} = AppServer.run_turn(session, "continue", issue)
      requests = trace |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
      interrupt = Enum.find(requests, &(&1["method"] == "turn/interrupt"))
      assert interrupt["params"] == %{"threadId" => "thread-1", "turnId" => "turn-1"}
      ids = for request <- requests, Map.has_key?(request, "id"), do: request["id"]
      assert ids == Enum.uniq(ids)
    end
  end

  for scenario <- ["refused", "missing_ack", "missing_terminal", "stale_terminal"] do
    test "interrupt #{scenario} is an error" do
      {session, issue, _trace} = server(unquote(scenario))
      assert {:error, {:stall_timeout, "turn-1"}} = AppServer.run_turn(session, "work", issue)
      assert {:error, reason} = AppServer.interrupt_turn(session, "turn-1")
      assert reason == :interrupt_timeout or match?({:interrupt_refused, _}, reason)
    end
  end

  for scenario <- ["freeform", "generic_approval_labels", "mixed_questions"] do
    test "#{scenario} blocks without inventing an operator answer" do
      {session, issue, trace} = server(unquote(scenario))
      assert {:error, {:turn_input_required, _}} = AppServer.run_turn(session, "work", issue)
      refute File.read!(trace) =~ "answers"
    end
  end

  test "operator input remains the final event so orchestrator blocks instead of retrying" do
    {session, issue, _trace} = server("freeform")
    on_message = fn message -> send(self(), {:protocol_event, message.event}) end
    assert {:error, {:turn_input_required, _}} = AppServer.run_turn(session, "work", issue, on_message: on_message)
    assert_received {:protocol_event, :session_started}
    assert_received {:protocol_event, :turn_input_required}
    assert_received {:protocol_event, :turn_input_required}
    refute_received {:protocol_event, :turn_ended_with_error}
  end

  test "recognized MCP approval question uses the deliberate approval policy" do
    {session, issue, trace} = server("recognized_approval")
    assert {:ok, _} = AppServer.run_turn(session, "work", issue)
    assert File.read!(trace) =~ "Approve this Session"
  end

  test "retrying errors await terminal status, while final active errors fail" do
    {session, issue, _trace} = server("retrying_error")
    assert {:ok, _} = AppServer.run_turn(session, "work", issue)
    assert {:error, {:turn_failed, %{"willRetry" => false}}} = AppServer.run_turn(session, "continue", issue)
  end

  test "completion preceding turn start response is retained and correlated" do
    {session, issue, _trace} = server("early_completion")
    assert {:ok, %{turn_id: "turn-1"}} = AppServer.run_turn(session, "work", issue)
  end

  test "active updates reset the silence interval instead of capping runtime" do
    {session, issue, _trace} = server("active")
    assert {:ok, _} = AppServer.run_turn(session, "work", issue)
  end

  defp server(scenario) do
    root = Path.join(System.tmp_dir!(), "symphony-protocol-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/issue")
    File.mkdir_p!(workspace)
    script = Path.join(root, "server.py")
    trace = Path.join(root, "requests.jsonl")

    File.write!(script, """
    import json, sys, time
    scenario = #{Jason.encode!(scenario)}
    trace = open(#{Jason.encode!(trace)}, 'w', buffering=1)
    turn = 0
    def emit(value):
        print(json.dumps(value), flush=True)
    def completed(status='completed', tid=None, thread='thread-1'):
        emit({'method':'turn/completed', 'params':{'threadId':thread, 'turn':{'id':tid or 'turn-'+str(turn), 'status':status, 'error':None if status == 'completed' else {'message':'terminal detail'}}}})
    for line in sys.stdin:
        trace.write(line)
        request = json.loads(line)
        method = request.get('method')
        rid = request.get('id')
        if method == 'initialize':
            emit({'id':rid, 'result':{}})
        elif method == 'thread/start':
            emit({'id':rid, 'result':{'thread':{'id':'thread-1'}}})
        elif method == 'turn/start':
            turn += 1
            if turn > 1:
                emit({'id':3, 'result':{'turn':{'id':'stale-response'}}})
            if scenario == 'early_completion':
                completed()
            emit({'id':rid, 'result':{'turn':{'id':'turn-'+str(turn)}}})
            if scenario in ['failed','interrupted','inProgress','unknown']:
                completed(scenario)
            elif scenario == 'stale':
                if turn == 2:
                    completed(tid='turn-1')
                    completed(thread='foreign')
                    emit({'method':'error', 'params':{'threadId':'thread-1','turnId':'turn-1','error':{'message':'old'}}})
                    completed('failed')
                else:
                    completed()
            elif scenario == 'retrying_error':
                emit({'method':'error','params':{'threadId':'thread-1','turnId':'turn-'+str(turn),'willRetry':turn == 1,'error':{'message':'retry status'}}})
                if turn == 1:
                    completed()
            elif scenario == 'active':
                for n in range(8):
                    emit({'method':'item/agentMessage/delta','params':{'threadId':'thread-1','turnId':'turn-1','delta':'working'}})
                    time.sleep(0.025)
                completed()
            elif scenario in ['freeform','generic_approval_labels','mixed_questions','recognized_approval']:
                qid = 'mcp_tool_call_approval_call-1' if scenario in ['recognized_approval','mixed_questions'] else 'generic-1'
                questions = [{'id':qid,'question':'Choose','options':None if scenario == 'freeform' else [{'label':'Approve this Session'}]}]
                if scenario == 'mixed_questions':
                    questions.append({'id':'generic-2','question':'Which project?','options':None})
                emit({'id':100,'method':'item/tool/requestUserInput','params':{'threadId':'thread-1','turnId':'turn-1','questions':questions}})
            elif turn > 1:
                completed()
        elif method == 'turn/interrupt':
            assert request['params'] == {'threadId':'thread-1','turnId':'turn-1'}
            if scenario == 'refused':
                emit({'id':rid,'error':{'code':-1,'message':'refused'}})
            elif scenario == 'missing_ack':
                completed('interrupted')
            elif scenario == 'missing_terminal':
                emit({'id':rid,'result':{}})
            elif scenario == 'stale_terminal':
                emit({'id':rid,'result':{}})
                completed('interrupted', tid='old-turn')
            elif scenario == 'terminal_first':
                completed('interrupted')
                time.sleep(0.03)
                emit({'id':rid,'result':{}})
            else:
                emit({'id':rid,'result':{}})
                time.sleep(0.03)
                completed('interrupted')
        elif rid == 100:
            completed()
    """)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: Path.join(root, "workspaces"),
      codex_command: "#{System.find_executable("python3")} -u #{script}",
      codex_approval_policy: "never",
      codex_read_timeout_ms: 150,
      codex_turn_timeout_ms: 100,
      codex_stall_timeout_ms: 75
    )

    assert {:ok, session} = AppServer.start_session(workspace)

    on_exit(fn ->
      AppServer.stop_session(session)
      File.rm_rf!(root)
    end)

    issue = %Issue{id: "protocol", identifier: "SPK-1020", title: "Protocol", state: "In Progress"}
    {session, issue, trace}
  end
end
