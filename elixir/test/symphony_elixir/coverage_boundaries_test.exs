defmodule SymphonyElixir.CoverageBoundariesTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.{Ledger, TokenUsageLedger}
  alias SymphonyElixir.Tracker.Memory

  test "state templates normalize names and discard non-template values" do
    assert Schema.normalize_state_templates(nil) == %{}

    assert Schema.normalize_state_templates(%{"In Review" => "review", :Todo => "work", "Done" => nil}) ==
             %{"in review" => "review", "todo" => "work"}

    write_workflow_file!(Workflow.workflow_file_path(), prompt_template_by_state: %{"In Review" => "Review {{ issue.identifier }}"})
    assert PromptBuilder.build_prompt(%Issue{identifier: "SPK-1", state: "IN REVIEW"}) =~ "Review SPK-1"
    assert PromptBuilder.build_prompt(%Issue{state: nil}) =~ "You are an agent for this repository."
  end

  test "server configuration accepts ephemeral ports and rejects negative ports" do
    assert Schema.Server.changeset(%Schema.Server{}, %{"port" => 0}).valid?
    refute Schema.Server.changeset(%Schema.Server{}, %{"port" => -1}).valid?
    assert {:ok, settings} = Schema.parse(%{"server" => %{"port" => 4000, "host" => "localhost"}})
    assert settings.server.port == 4000
    assert settings.server.host == "localhost"
  end

  test "blank optional mirror paths resolve to no mirror" do
    assert {:ok, settings} = Schema.parse(%{"workspace" => %{"mirror_path" => ""}})
    assert settings.workspace.mirror_path == nil
    assert {:error, _} = Schema.parse(%{"workspace" => %{"mirror_path" => %{}}})
  end

  test "routing rejects malformed requirements and tolerates missing labels when unrestricted" do
    refute Issue.stop_continue_labeled?(%Issue{labels: nil}, ["stop"])
    assert Issue.routable?(%Issue{labels: nil}, [])
    assert Issue.routable?(%Issue{}, nil)
    refute Issue.routable?(%Issue{}, :invalid)
    refute Issue.routable?(%Issue{labels: [nil]}, ["required"])
  end

  test "delta memory polling retains unknown timestamps but excludes equal and older updates" do
    cursor = ~U[2026-09-07 00:00:00Z]
    old = %Issue{id: "old", updated_at: DateTime.add(cursor, -1)}
    equal = %Issue{id: "equal", updated_at: cursor}
    new = %Issue{id: "new", updated_at: DateTime.add(cursor, 1)}
    unknown = %Issue{id: "unknown"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [old, equal, new, unknown])
    assert Memory.fetch_candidate_issues(cursor) == {:ok, [new, unknown]}
  end

  test "default token observation writes to configured ledger" do
    assert :ok = TokenUsageLedger.append_observation(%{issue_identifier: "SPK-1", session_id: "session", total_tokens: 7})
    assert [%{total_tokens: 7}] = TokenUsageLedger.read_records()
  end

  test "ledger repairs invalid in-memory counters and normalizes unknown state and keys" do
    Ledger.put("SPK-1", %{dispatch_count: nil, rework_count: nil})
    assert Ledger.increment("SPK-1", :dispatch_count, 2).dispatch_count == 2
    assert Ledger.put_rework_count_at_least("SPK-1", 3).rework_count == 3
    assert Ledger.observe_state("SPK-1", nil).last_observed_state == nil
    refute Ledger.rework_state?(nil)
    assert Ledger.put("SPK-2", %{7 => "numeric key"}) == %{7 => "numeric key"}
    assert Ledger.update("SPK-2", fn _ -> nil end) == %{}
  end

  test "metrics failure is reported without losing durable terminal state" do
    old = Application.get_env(:symphony_elixir, :metrics_ledger_path)
    path = Path.join(Path.dirname(Workflow.workflow_file_path()), "metrics-directory")
    File.mkdir!(path)
    Application.put_env(:symphony_elixir, :metrics_ledger_path, path)
    on_exit(fn -> restore_app_env(:metrics_ledger_path, old) end)
    assert capture_log(fn -> assert Ledger.record_terminal("SPK-1", %{}).terminal_at end) =~ "Unable to emit Symphony metrics"
    assert Ledger.get("SPK-1").terminal_at
  end

  test "production defaults isolate ledger and metrics beside the workflow" do
    old_env = Mix.env()
    old_path = Application.get_env(:symphony_elixir, :ledger_path)
    old_metrics = Application.get_env(:symphony_elixir, :metrics_ledger_path)
    Application.delete_env(:symphony_elixir, :ledger_path)
    Application.delete_env(:symphony_elixir, :metrics_ledger_path)

    try do
      Mix.env(:prod)
      assert {:error, {:already_started, _}} = Ledger.start_link()
      {:ok, pid} = Ledger.start_link(name: :coverage_production_ledger)
      assert GenServer.call(pid, :info).path == Ledger.path_for_workflow(Workflow.workflow_file_path())
      GenServer.stop(pid)
      assert Ledger.record_terminal("SPK-1", %{}).terminal_at
      assert File.exists?(Path.join(Path.dirname(Ledger.info().path), "metrics.jsonl"))
    after
      Mix.env(old_env)
      restore_app_env(:ledger_path, old_path)
      restore_app_env(:metrics_ledger_path, old_metrics)
    end
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:symphony_elixir, key)
  defp restore_app_env(key, value), do: Application.put_env(:symphony_elixir, key, value)
end
