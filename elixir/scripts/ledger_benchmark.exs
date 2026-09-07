# Run with: mise exec -- mix run --no-start scripts/ledger_benchmark.exs
alias SymphonyElixir.Ledger

root = Path.join(System.tmp_dir!(), "symphony-ledger-benchmark-#{System.unique_integer([:positive])}")
path = Path.join(root, "ledger.json")
File.mkdir_p!(root)

try do
  entries = Map.new(1..1000, &{"issue-#{&1}", %{dispatch_count: 1, cumulative_tokens: 1000}})
  File.write!(path, Jason.encode!(entries))
  {:ok, ledger} = Ledger.start_link(path: path, name: :benchmark_ledger, flush_interval_ms: 60_000)

  {zero_us, _} =
    :timer.tc(fn ->
      for _ <- 1..1000, do: GenServer.call(ledger, {:tokens, "issue-1", 0})
    end)

  zero_writes = GenServer.call(ledger, :info).writes

  {positive_us, _} =
    :timer.tc(fn ->
      for _ <- 1..1000, do: GenServer.call(ledger, {:tokens, "issue-1", 1})
    end)

  before_flush = GenServer.call(ledger, :info).writes
  GenServer.call(ledger, :flush)
  after_flush = GenServer.call(ledger, :info).writes
  persisted = Jason.decode!(File.read!(path))["issue-1"]["cumulative_tokens"]
  GenServer.stop(ledger)

  IO.puts(
    Jason.encode!(%{
      entries: 1000,
      updates: 1000,
      zero_update_ms: zero_us / 1000,
      zero_writes: zero_writes,
      positive_update_ms: positive_us / 1000,
      positive_writes_before_flush: before_flush,
      writes_after_flush: after_flush,
      persisted_tokens: persisted,
      bytes: File.stat!(path).size
    })
  )
after
  File.rm_rf!(root)
end
