defmodule SymphonyElixir.LedgerReadinessTest do
  use ExUnit.Case, async: false
  alias SymphonyElixir.Ledger

  setup do
    Process.flag(:trap_exit, true)
    root = Path.join(System.tmp_dir!(), "ledger-readiness-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    {:ok, root} = SymphonyElixir.PathSafety.canonicalize(root)
    on_exit(fn -> File.rm_rf(root) end)
    {:ok, root: root}
  end

  test "workflows in one directory receive isolated durable state", %{root: root} do
    spektra = Ledger.path_for_workflow(Path.join(root, "spektra-workflow.md"))
    salesight = Ledger.path_for_workflow(Path.join(root, "salesight-workflow.md"))
    refute spektra == salesight
    assert Path.dirname(Path.dirname(spektra)) == Path.join(root, ".symphony")

    first = ledger!(spektra, :readiness_ledger_a)
    second = ledger!(salesight, :readiness_ledger_b)
    update(first, "SPK-1", %{dispatch_count: 1})
    update(second, "ANT-1", %{dispatch_count: 1})
    update(first, "SPK-1", %{dispatch_count: 2})
    assert Map.keys(Jason.decode!(File.read!(spektra))) == ["SPK-1"]
    assert Map.keys(Jason.decode!(File.read!(salesight))) == ["ANT-1"]
  end

  test "a second writer cannot claim the same path", %{root: root} do
    path = Path.join(root, "ledger.json")
    ledger!(path, :readiness_ledger_a)
    assert {:error, {:ledger_locked, lock}} = Ledger.start_link(path: path, name: :readiness_ledger_b)
    assert lock == path <> ".lock"
  end

  test "zero changes do not write, token changes batch, and lifecycle writes flush", %{root: root} do
    path = Path.join(root, "ledger.json")
    pid = ledger!(path, :readiness_ledger_a)
    for _ <- 1..1000, do: GenServer.call(pid, {:tokens, "SPK-1", 0})
    assert GenServer.call(pid, :info).writes == 0
    refute File.exists?(path)
    for _ <- 1..1000, do: GenServer.call(pid, {:tokens, "SPK-1", 2})
    assert GenServer.call(pid, {:get, "SPK-1"}).cumulative_tokens == 2000
    assert GenServer.call(pid, :info).writes == 0
    update(pid, "SPK-1", %{dispatch_count: 1})
    assert GenServer.call(pid, :info).writes == 1
    assert Jason.decode!(File.read!(path))["SPK-1"]["cumulative_tokens"] == 2000
    update(pid, "SPK-1", %{dispatch_count: 1})
    assert GenServer.call(pid, :info).writes == 1
    assert Path.wildcard(path <> ".tmp-*") == []
  end

  test "graceful restart flushes pending tokens and releases ownership", %{root: root} do
    path = Path.join(root, "ledger.json")
    pid = ledger!(path, :readiness_ledger_a)
    GenServer.call(pid, {:tokens, "SPK-1", 25})
    stop_supervised!(:readiness_ledger_a)
    refute File.exists?(path <> ".lock")
    restarted = ledger!(path, :readiness_ledger_a)
    assert GenServer.call(restarted, {:get, "SPK-1"}).cumulative_tokens == 25
  end

  test "invalid state is rejected without overwriting it or retaining the lock", %{root: root} do
    path = Path.join(root, "ledger.json")
    File.write!(path, "truncated{")
    result = Ledger.start_link(path: path, name: :readiness_ledger_a)
    assert {:error, {:invalid_ledger, :invalid_json_or_shape}} = result
    assert File.read!(path) == "truncated{"
    refute File.exists?(path <> ".lock")
  end

  test "legacy file requires an explicit migration source", %{root: root} do
    legacy = Path.join(root, "legacy.json")
    path = Path.join(root, "ledger.json")
    File.write!(legacy, "{\"OTHER-1\":{\"dispatch_count\":9}}")
    assert :ok = Ledger.maybe_migrate_legacy(path)
    refute File.exists?(path)
    pid = ledger!(path, :readiness_ledger_a)
    assert GenServer.call(pid, :all) == %{}
    assert File.read!(legacy) == "{\"OTHER-1\":{\"dispatch_count\":9}}"
  end

  test "malformed entries and counters fail closed", %{root: root} do
    path = Path.join(root, "ledger.json")

    for body <- [~s({"SPK-1":null}), ~s({"SPK-1":{"dispatch_count":"three"}})] do
      File.write!(path, body)
      result = Ledger.start_link(path: path, name: :readiness_ledger_a)
      assert {:error, {:invalid_ledger, :invalid_json_or_shape}} = result
      assert File.read!(path) == body
      refute File.exists?(path <> ".lock")
    end
  end

  test "explicit migration cannot overwrite an active writer", %{root: root} do
    path = Path.join(root, "ledger.json")
    legacy = Path.join(root, "legacy.json")
    File.write!(legacy, ~s({"OTHER-1":{"dispatch_count":9}}))
    pid = ledger!(path, :readiness_ledger_a)
    assert {:error, {:ledger_locked, _}} = Ledger.maybe_migrate_legacy(path, legacy)
    refute File.exists?(path)
    update(pid, "SPK-1", %{dispatch_count: 1})
    assert Map.keys(Jason.decode!(File.read!(path))) == ["SPK-1"]
  end

  test "scheduled flush persists token changes without a lifecycle update", %{root: root} do
    path = Path.join(root, "ledger.json")
    pid = ledger!(path, :readiness_ledger_a)
    GenServer.call(pid, {:tokens, "SPK-1", 12})
    send(pid, :flush)
    assert GenServer.call(pid, :info).writes == 1
    assert Jason.decode!(File.read!(path))["SPK-1"]["cumulative_tokens"] == 12
  end

  test "a stale crash lock fails closed until explicit offline recovery", %{root: root} do
    path = Path.join(root, "ledger.json")
    File.write!(path <> ".lock", "stale-crashed-owner")
    assert {:error, {:ledger_locked, _}} = Ledger.start_link(path: path, name: :readiness_ledger_a)
    assert File.read!(path <> ".lock") == "stale-crashed-owner"
    refute File.exists?(path)
  end

  test "an unreadable ledger releases its startup lock without replacing evidence", %{root: root} do
    path = Path.join(root, "ledger.json")
    File.mkdir!(path)

    assert {:error, {:ledger_read_failed, ^path, :eisdir}} =
             Ledger.start_link(path: path, name: :readiness_ledger_a)

    assert File.dir?(path)
    refute File.exists?(path <> ".lock")
  end

  @tag capture_log: true
  test "a failed durable write is not acknowledged and preserves the last saved counters", %{root: root} do
    path = Path.join(root, "ledger.json")
    backup = Path.join(root, "last-saved.json")
    {:ok, pid} = Ledger.start_link(path: path, name: :readiness_ledger_a)
    update(pid, "SPK-1", %{dispatch_count: 1})
    File.rename!(path, backup)
    File.mkdir!(path)
    monitor = Process.monitor(pid)

    assert catch_exit(update(pid, "SPK-1", %{dispatch_count: 2}))
    assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}
    assert Jason.decode!(File.read!(backup))["SPK-1"]["dispatch_count"] == 1
    refute File.exists?(path <> ".lock")
    assert Path.wildcard(path <> ".tmp-*") == []

    File.rmdir!(path)
    File.rename!(backup, path)
    recovered = ledger!(path, :readiness_ledger_a)
    assert GenServer.call(recovered, {:get, "SPK-1"}).dispatch_count == 1
  end

  defp ledger!(path, name) do
    start_supervised!(%{id: name, start: {Ledger, :start_link, [[path: path, name: name, flush_interval_ms: 60_000]]}})
  end

  defp update(pid, id, attrs), do: GenServer.call(pid, {:update, id, &Map.merge(&1, attrs)})
end
