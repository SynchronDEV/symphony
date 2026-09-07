defmodule SymphonyElixir.PermissionProfileProtocolTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Config.Schema

  test "named profile omits legacy sandbox overrides for thread and continuation turns" do
    {workspace, trace} = configure_server("symphony_studio", "symphony_studio")
    assert {:ok, runtime} = Config.codex_runtime_settings(workspace)
    assert runtime.permission_profile == "symphony_studio"
    assert {:ok, session} = AppServer.start_session(workspace)

    try do
      issue = %Issue{id: "profile", identifier: "SPK-1020", title: "Profile policy"}
      assert {:ok, _} = AppServer.run_turn(session, "first", issue)
      assert {:ok, _} = AppServer.run_turn(session, "continue", issue)
      requests = read_requests(trace)
      thread = Enum.find(requests, &(&1["method"] == "thread/start"))
      refute Map.has_key?(thread["params"], "sandbox")
      turns = Enum.filter(requests, &(&1["method"] == "turn/start"))
      assert length(turns) == 2
      assert Enum.all?(turns, &(not Map.has_key?(&1["params"], "sandboxPolicy")))
      assert Enum.all?(turns, &(&1["params"]["approvalPolicy"] == runtime.approval_policy))
    after
      AppServer.stop_session(session)
    end
  end

  for actual <- ["wrong_profile", nil] do
    test "mismatched active profile #{inspect(actual)} fails before any turn" do
      {workspace, trace} = configure_server("symphony_studio", unquote(actual))

      assert {:error, {:permission_profile_mismatch, "symphony_studio", unquote(actual)}} =
               AppServer.start_session(workspace)

      refute Enum.any?(read_requests(trace), &(&1["method"] == "turn/start"))
    end
  end

  test "legacy mode continues sending its sandbox fields without requiring a named profile" do
    {workspace, trace} = configure_server(nil, nil)
    assert {:ok, session} = AppServer.start_session(workspace)

    try do
      issue = %Issue{id: "legacy", identifier: "SPK-1020", title: "Legacy policy"}
      assert {:ok, _} = AppServer.run_turn(session, "work", issue)
      requests = read_requests(trace)
      thread = Enum.find(requests, &(&1["method"] == "thread/start"))
      turn = Enum.find(requests, &(&1["method"] == "turn/start"))
      assert thread["params"]["sandbox"] == "workspace-write"
      assert turn["params"]["sandboxPolicy"]["type"] == "workspaceWrite"
    after
      AppServer.stop_session(session)
    end
  end

  test "blank permission profile cannot silently fall back to legacy mode" do
    assert {:error, _} = Schema.parse(%{codex: %{permission_profile: ""}})
    assert {:error, _} = Schema.parse(%{codex: %{permission_profile: "  "}})
  end

  defp configure_server(expected, actual) do
    root = Path.join(System.tmp_dir!(), "symphony-profile-protocol-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces/issue")
    File.mkdir_p!(workspace)
    trace = Path.join(root, "requests.jsonl")
    script = Path.join(root, "server.py")

    File.write!(script, """
    import json,sys
    actual=json.loads(#{Jason.encode!(Jason.encode!(actual))})
    trace=open(#{Jason.encode!(trace)},'w',buffering=1)
    turn=0
    def emit(value):
        print(json.dumps(value),flush=True)
    for line in sys.stdin:
        trace.write(line)
        request=json.loads(line)
        method=request.get('method')
        rid=request.get('id')
        if method=='initialize':
            emit({'id':rid,'result':{}})
        elif method=='thread/start':
            result={'thread':{'id':'thread-profile'}}
            if actual is not None:
                result['activePermissionProfile']={'id':actual,'extends':':workspace'}
            emit({'id':rid,'result':result})
        elif method=='turn/start':
            turn+=1
            tid='turn-'+str(turn)
            emit({'id':rid,'result':{'turn':{'id':tid}}})
            completed={'id':tid,'status':'completed','error':None}
            emit({'method':'turn/completed','params':{'threadId':'thread-profile','turn':completed}})
    """)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: Path.join(root, "workspaces"),
      codex_command: "#{System.find_executable("python3")} -u #{script}",
      codex_permission_profile: expected
    )

    on_exit(fn -> File.rm_rf!(root) end)
    {workspace, trace}
  end

  defp read_requests(path), do: path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
end
