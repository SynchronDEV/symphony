defmodule SymphonyElixir.Ledger do
  @moduledoc """
  Small persistent per-issue ledger for orchestrator counters.
  """

  use GenServer
  require Logger

  @known_keys %{
    "blocked_reason" => :blocked_reason,
    "cumulative_tokens" => :cumulative_tokens,
    "dispatch_count" => :dispatch_count,
    "declined_elicitations" => :declined_elicitations,
    "dirty_files" => :dirty_files,
    "commits_ahead" => :commits_ahead,
    "identifier" => :identifier,
    "last_agent_message" => :last_agent_message,
    "last_rework_state" => :last_rework_state,
    "last_thread_id" => :last_thread_id,
    "merged_at" => :merged_at,
    "metrics_emitted_at" => :metrics_emitted_at,
    "pr" => :pr,
    "rework_count" => :rework_count,
    "retries" => :retries,
    "stall_events" => :stall_events,
    "last_observed_state" => :last_observed_state,
    "state" => :state,
    "terminal_at" => :terminal_at,
    "turns_used" => :turns_used,
    "worker_host" => :worker_host,
    "workspace_path" => :workspace_path,
    "workspace_root" => :workspace_root,
    "cleanup_base_ref" => :cleanup_base_ref,
    "eligible" => :eligible,
    "issue_id" => :issue_id,
    "completed_at" => :completed_at,
    "status" => :status,
    "merged_into" => :merged_into,
    "wall_time" => :wall_time
  }

  @type issue_id :: String.t()
  @type issue_entry :: map()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec get(issue_id()) :: issue_entry()
  def get(issue_id) when is_binary(issue_id) do
    GenServer.call(__MODULE__, {:get, issue_id})
  end

  @spec all() :: map()
  def all do
    GenServer.call(__MODULE__, :all)
  end

  @doc "Returns the last durably committed ledger without waiting for an in-flight filesystem write."
  @spec committed_snapshot(GenServer.server()) :: map()
  def committed_snapshot(server \\ __MODULE__) do
    table = :persistent_term.get({__MODULE__, :committed_snapshot, server}, nil)
    [{:entries, entries}] = :ets.lookup(table, :entries)
    entries
  rescue
    ArgumentError -> %{}
  end

  @spec increment(issue_id(), atom(), integer()) :: issue_entry()
  def increment(issue_id, key, amount \\ 1)
      when is_binary(issue_id) and is_atom(key) and is_integer(amount) do
    update(issue_id, fn entry ->
      Map.update(entry, key, amount, &increment_value(&1, amount))
    end)
  end

  @doc "Atomically records a stall and durable quarantine; callers must enforce their own operation deadline."
  @spec quarantine_stall(issue_id(), String.t()) :: issue_entry()
  def quarantine_stall(issue_id, reason) when is_binary(issue_id) and is_binary(reason) do
    update = fn entry ->
      entry
      |> Map.update(:stall_events, 1, &increment_value(&1, 1))
      |> Map.merge(%{status: :blocked, blocked_reason: reason})
    end

    GenServer.call(__MODULE__, {:update, issue_id, update}, :infinity)
  end

  defp increment_value(value, amount) when is_integer(value), do: value + amount
  defp increment_value(_value, amount), do: amount

  defp integer_or_zero(value) when is_integer(value), do: value
  defp integer_or_zero(_value), do: 0

  @spec add_tokens(issue_id(), map()) :: issue_entry()
  def add_tokens(issue_id, token_delta) when is_binary(issue_id) and is_map(token_delta) do
    total = Map.get(token_delta, :total_tokens, 0)
    cached_input = Map.get(token_delta, :cached_input_tokens, 0)

    # Preserve the fork's effective-token accounting. This is a token metric,
    # not a currency calculation: cached tokens are excluded from this cap.
    amount = max(total - cached_input, 0)
    GenServer.call(__MODULE__, {:tokens, issue_id, amount})
  end

  @spec put(issue_id(), map()) :: issue_entry()
  def put(issue_id, attrs) when is_binary(issue_id) and is_map(attrs) do
    update(issue_id, &Map.merge(&1, attrs))
  end

  @spec put_rework_count_at_least(issue_id(), non_neg_integer()) :: issue_entry()
  def put_rework_count_at_least(issue_id, count)
      when is_binary(issue_id) and is_integer(count) and count >= 0 do
    put_rework_count_at_least(issue_id, count, nil)
  end

  @spec put_rework_count_at_least(issue_id(), non_neg_integer(), String.t() | nil) :: issue_entry()
  def put_rework_count_at_least(issue_id, count, observed_state)
      when is_binary(issue_id) and is_integer(count) and count >= 0 do
    update(issue_id, fn entry ->
      current_count = entry |> Map.get(:rework_count, 0) |> integer_or_zero()

      entry
      |> Map.put(:rework_count, max(current_count, count))
      |> maybe_put_rework_observed_state(observed_state)
    end)
  end

  defp maybe_put_rework_observed_state(entry, observed_state) when is_binary(observed_state) do
    Map.put(entry, :last_rework_state, rework_state?(observed_state))
  end

  defp maybe_put_rework_observed_state(entry, _observed_state), do: entry

  @spec update(issue_id(), (issue_entry() -> issue_entry())) :: issue_entry()
  def update(issue_id, fun) when is_binary(issue_id) and is_function(fun, 1) do
    GenServer.call(__MODULE__, {:update, issue_id, fun})
  end

  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset)
  end

  @spec flush() :: :ok
  def flush, do: GenServer.call(__MODULE__, :flush)

  @spec info() :: map()
  def info, do: GenServer.call(__MODULE__, :info)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    ledger_path = path(opts)

    with {:ok, canonical_path} <- SymphonyElixir.PathSafety.canonicalize(ledger_path),
         :ok <- File.mkdir_p(Path.dirname(canonical_path)),
         {:ok, lock} <- acquire_lock(canonical_path, Keyword.get(opts, :lock_writer, &:file.write/2)) do
      case load(canonical_path) do
        {:ok, entries} ->
          snapshot_key = {__MODULE__, :committed_snapshot, Keyword.get(opts, :name, __MODULE__)}
          snapshot_table = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
          :ets.insert(snapshot_table, {:entries, entries})
          :persistent_term.put(snapshot_key, snapshot_table)

          {:ok,
           %{
             path: canonical_path,
             entries: entries,
             lock: lock,
             dirty: false,
             timer: nil,
             flush_interval_ms: Keyword.get(opts, :flush_interval_ms, 250),
             writes: 0,
             snapshot_key: snapshot_key,
             snapshot_table: snapshot_table,
             file_sync: Keyword.get(opts, :file_sync, &:file.sync/1)
           }}

        {:error, reason} ->
          release_lock(lock)
          {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, issue_id}, _from, state), do: {:reply, Map.get(state.entries, issue_id, %{}), state}
  def handle_call(:all, _from, state), do: {:reply, state.entries, state}
  def handle_call(:info, _from, state), do: {:reply, Map.take(state, [:path, :dirty, :writes]), state}

  def handle_call({:tokens, issue_id, 0}, _from, state),
    do: {:reply, Map.get(state.entries, issue_id, %{}), state}

  def handle_call({:tokens, issue_id, amount}, _from, state) do
    entry = Map.update(Map.get(state.entries, issue_id, %{}), :cumulative_tokens, amount, &increment_value(&1, amount))
    state = %{state | entries: Map.put(state.entries, issue_id, entry), dirty: true}
    timer = state.timer || Process.send_after(self(), :flush, state.flush_interval_ms)
    {:reply, entry, %{state | timer: timer}}
  end

  def handle_call({:update, issue_id, fun}, _from, state) do
    entry = state.entries |> Map.get(issue_id, %{}) |> fun.() |> normalize_entry()
    entries = Map.put(state.entries, issue_id, entry)
    state = %{state | entries: entries, dirty: state.dirty or entries != state.entries}
    {:reply, entry, persist_pending!(state)}
  end

  def handle_call(:reset, _from, state),
    do: {:reply, :ok, persist_pending!(%{state | entries: %{}, dirty: true})}

  def handle_call(:flush, _from, state), do: {:reply, :ok, persist_pending!(state)}

  @impl true
  def handle_info(:flush, state), do: {:noreply, persist_pending!(%{state | timer: nil})}

  @impl true
  def terminate(_reason, state) do
    persist_pending!(state)
  after
    release_lock(state.lock)
    :persistent_term.erase(state.snapshot_key)
  end

  # Edge-triggered rework counter shared by EVERY place an issue state is
  # observed (orchestrator dispatch AND the agent-runner's between-turn
  # refresh). Counting only at dispatch undercounts: a whole
  # implement -> review -> rework cycle can happen inside one continuous
  # agent run with zero dispatches (observed live: SYNC-705 ran 4 rework
  # cycles in one session), so the max_rework_cycles cap never fired.
  #
  # Newer workflows route review failures straight back to Ready for Agent
  # instead of the legacy Rework state. Count that In Review -> Ready for Agent
  # transition as the same kind of cycle, while leaving initial Backlog -> Ready
  # promotion uncounted.
  @spec observe_state(issue_id(), String.t() | nil) :: issue_entry()
  def observe_state(issue_id, state) when is_binary(issue_id) do
    normalized_state = normalize_state(state)
    in_rework? = normalized_state == "rework"

    update(issue_id, fn entry ->
      last_observed_state = Map.get(entry, :last_observed_state)
      review_failed_to_ready? = last_observed_state == "in review" and normalized_state in ["ready for agent", "todo"]

      entry =
        if (in_rework? and Map.get(entry, :last_rework_state) != true) or review_failed_to_ready? do
          Map.update(entry, :rework_count, 1, &(&1 + 1))
        else
          entry
        end

      entry
      |> Map.put(:last_rework_state, in_rework?)
      |> Map.put(:last_observed_state, normalized_state)
    end)
  end

  @spec rework_state?(String.t() | nil) :: boolean()
  def rework_state?(state) when is_binary(state) do
    normalize_state(state) == "rework"
  end

  def rework_state?(_state), do: false

  defp normalize_state(state) when is_binary(state) do
    state
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_state(_state), do: nil

  @spec record_terminal(issue_id(), map()) :: issue_entry()
  def record_terminal(issue_id, attrs) when is_binary(issue_id) and is_map(attrs) do
    entry =
      put(
        issue_id,
        Map.merge(attrs, %{
          terminal_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
          metrics_emitted_at: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
        })
      )

    emit_metrics_line(issue_id, entry)
    entry
  end

  defp path(opts) do
    Keyword.get(opts, :path) ||
      Application.get_env(:symphony_elixir, :ledger_path) ||
      default_path()
  end

  defp default_path do
    if Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test do
      Path.join(System.tmp_dir!(), "symphony_elixir_test_ledger-#{System.pid()}.json")
    else
      path_for_workflow(SymphonyElixir.Workflow.workflow_file_path())
    end
  end

  @spec path_for_workflow(Path.t()) :: Path.t()
  def path_for_workflow(workflow) when is_binary(workflow) do
    {:ok, canonical} = SymphonyElixir.PathSafety.canonicalize(workflow)
    stem = canonical |> Path.basename() |> Path.rootname() |> String.replace(~r/[^a-zA-Z0-9._-]/, "_")
    hash = :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower) |> binary_part(0, 12)
    Path.join([Path.dirname(canonical), ".symphony", "#{stem}-#{hash}", "ledger.json"])
  end

  defp load(path) do
    # Legacy shared ledgers need an explicit, scoped migration. Never import
    # another deployment's counters merely because it used this working directory.
    case File.read(path) do
      {:ok, body} -> decode_entries(body)
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, {:ledger_read_failed, path, reason}}
    end
  end

  @doc false
  @spec maybe_migrate_legacy(Path.t(), Path.t() | nil) :: :ok | {:error, term()}
  def maybe_migrate_legacy(path, legacy_path \\ nil) do
    if is_nil(legacy_path), do: :ok, else: migrate_legacy(path, legacy_path)
  end

  defp migrate_legacy(path, legacy) when is_binary(legacy) do
    with {:ok, canonical} <- SymphonyElixir.PathSafety.canonicalize(path),
         :ok <- File.mkdir_p(Path.dirname(canonical)),
         {:ok, lock} <- acquire_lock(canonical) do
      try do
        if File.exists?(canonical) or not File.exists?(legacy) do
          :ok
        else
          with {:ok, body} <- File.read(legacy),
               {:ok, entries} <- decode_entries(body) do
            persist!(canonical, entries)
            :ok
          end
        end
      after
        release_lock(lock)
      end
    end
  end

  defp decode_entries(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) ->
        if Enum.all?(decoded, &valid_entry?/1) do
          {:ok, Map.new(decoded, fn {issue_id, entry} -> {issue_id, normalize_entry(entry)} end)}
        else
          {:error, {:invalid_ledger, :invalid_json_or_shape}}
        end

      _ ->
        {:error, {:invalid_ledger, :invalid_json_or_shape}}
    end
  end

  defp valid_entry?({id, entry}), do: is_binary(id) and is_map(entry) and valid_counters?(entry)

  defp valid_counters?(entry) do
    Enum.all?(["cumulative_tokens", "dispatch_count", "rework_count", "retries", "stall_events", "turns_used"], fn key ->
      case Map.fetch(entry, key) do
        :error -> true
        {:ok, value} -> is_integer(value) and value >= 0
      end
    end)
  end

  defp persist_pending!(%{dirty: false} = state), do: state

  defp persist_pending!(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    persist!(state.path, state.entries, state.file_sync)
    :ets.insert(state.snapshot_table, {:entries, state.entries})
    %{state | dirty: false, timer: nil, writes: state.writes + 1}
  end

  defp persist!(path, entries, file_sync \\ &:file.sync/1) when is_binary(path) and is_map(entries) do
    encoded =
      entries
      |> Map.new(fn {issue_id, entry} -> {issue_id, stringify_entry(entry)} end)
      |> Jason.encode!()

    temporary = path <> ".tmp-#{System.pid()}-#{System.unique_integer([:positive])}"

    try do
      {:ok, file} = File.open(temporary, [:write, :binary, :exclusive])

      try do
        :ok = IO.binwrite(file, encoded)
        :ok = file_sync.(file)
      after
        File.close(file)
      end

      File.rename!(temporary, path)
    after
      File.rm(temporary)
    end
  end

  # The writer seam permits deterministic disk-failure checks after exclusive open.
  # Normal startup and migration both use the filesystem writer.
  defp acquire_lock(path, writer \\ &:file.write/2) do
    lock_path = path <> ".lock"
    owner = "#{System.pid()}:#{System.unique_integer([:positive])}"

    case File.open(lock_path, [:write, :exclusive]) do
      {:ok, file} ->
        case writer.(file, owner) do
          :ok ->
            File.close(file)
            {:ok, {lock_path, owner}}

          {:error, reason} ->
            File.close(file)
            File.rm(lock_path)
            {:error, {:ledger_lock_failed, lock_path, reason}}
        end

      {:error, :eexist} ->
        {:error, {:ledger_locked, lock_path}}

      {:error, reason} ->
        {:error, {:ledger_lock_failed, lock_path, reason}}
    end
  end

  defp release_lock({path, owner}) do
    if File.read(path) == {:ok, owner}, do: File.rm(path)
    :ok
  end

  defp emit_metrics_line(issue_id, entry) do
    payload = %{
      issue: Map.get(entry, :identifier) || issue_id,
      pr: Map.get(entry, :pr),
      tokens: Map.get(entry, :cumulative_tokens, 0),
      turns: Map.get(entry, :turns_used, 0),
      rework_cycles: Map.get(entry, :rework_count, 0),
      retries: Map.get(entry, :retries, 0),
      wall_time: Map.get(entry, :wall_time),
      merged_at: Map.get(entry, :merged_at),
      terminal_at: Map.get(entry, :terminal_at)
    }

    path = metrics_path()

    path
    |> Path.dirname()
    |> File.mkdir_p!()

    File.write!(path, Jason.encode!(payload) <> "\n", [:append])
  rescue
    exception ->
      Logger.warning("Unable to emit Symphony metrics ledger line: #{Exception.message(exception)}")
      :ok
  end

  defp metrics_path do
    Application.get_env(:symphony_elixir, :metrics_ledger_path) ||
      if Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test do
        Path.join(System.tmp_dir!(), "symphony_elixir_test_metrics-#{System.pid()}.jsonl")
      else
        Path.join(Path.dirname(info().path), "metrics.jsonl")
      end
  end

  defp normalize_entry(entry) when is_map(entry) do
    Map.new(entry, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_entry(_entry), do: %{}

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@known_keys, key, key)
  defp normalize_key(key), do: key

  defp stringify_entry(entry) when is_map(entry) do
    Map.new(entry, fn {key, value} -> {to_string(key), value} end)
  end
end
