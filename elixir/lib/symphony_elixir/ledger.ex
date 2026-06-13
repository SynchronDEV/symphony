defmodule SymphonyElixir.Ledger do
  @moduledoc """
  Small persistent per-issue ledger for orchestrator counters.
  """

  use Agent
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
    "state" => :state,
    "terminal_at" => :terminal_at,
    "turns_used" => :turns_used,
    "worker_host" => :worker_host
  }

  @type issue_id :: String.t()
  @type issue_entry :: map()

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> load(path(opts)) end, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec get(issue_id()) :: issue_entry()
  def get(issue_id) when is_binary(issue_id) do
    Agent.get(__MODULE__, &Map.get(&1.entries, issue_id, %{}))
  end

  @spec all() :: map()
  def all do
    Agent.get(__MODULE__, & &1.entries)
  end

  @spec increment(issue_id(), atom(), integer()) :: issue_entry()
  def increment(issue_id, key, amount \\ 1)
      when is_binary(issue_id) and is_atom(key) and is_integer(amount) do
    update(issue_id, fn entry ->
      Map.update(entry, key, amount, &increment_value(&1, amount))
    end)
  end

  defp increment_value(value, amount) when is_integer(value), do: value + amount
  defp increment_value(_value, amount), do: amount

  defp integer_or_zero(value) when is_integer(value), do: value
  defp integer_or_zero(_value), do: 0

  @spec add_tokens(issue_id(), map()) :: issue_entry()
  def add_tokens(issue_id, token_delta) when is_binary(issue_id) and is_map(token_delta) do
    total = Map.get(token_delta, :total_tokens, 0)
    cached_input = Map.get(token_delta, :cached_input_tokens, 0)

    # Budget measures real spend: cached prompt-prefix reads are reported
    # inside the totals but cost ~0, so subtract them. Payloads without a
    # cached field yield cached_input 0 and the previous raw-total behavior.
    increment(issue_id, :cumulative_tokens, max(total - cached_input, 0))
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
    Agent.get_and_update(__MODULE__, fn %{entries: entries, path: path} = state ->
      entry =
        entries
        |> Map.get(issue_id, %{})
        |> fun.()
        |> normalize_entry()

      entries = Map.put(entries, issue_id, entry)
      persist(path, entries)
      {entry, %{state | entries: entries}}
    end)
  end

  @spec reset!() :: :ok
  def reset! do
    Agent.update(__MODULE__, fn %{path: path} = state ->
      persist(path, %{})
      %{state | entries: %{}}
    end)
  end

  # Edge-triggered rework counter shared by EVERY place an issue state is
  # observed (orchestrator dispatch AND the agent-runner's between-turn
  # refresh). Counting only at dispatch undercounts: a whole
  # implement -> review -> rework cycle can happen inside one continuous
  # agent run with zero dispatches (observed live: SYNC-705 ran 4 rework
  # cycles in one session), so the max_rework_cycles cap never fired.
  @spec observe_state(issue_id(), String.t() | nil) :: issue_entry()
  def observe_state(issue_id, state) when is_binary(issue_id) do
    in_rework? = rework_state?(state)

    update(issue_id, fn entry ->
      entry =
        if in_rework? and Map.get(entry, :last_rework_state) != true do
          Map.update(entry, :rework_count, 1, &(&1 + 1))
        else
          entry
        end

      Map.put(entry, :last_rework_state, in_rework?)
    end)
  end

  @spec rework_state?(String.t() | nil) :: boolean()
  def rework_state?(state) when is_binary(state) do
    state |> String.trim() |> String.downcase() == "rework"
  end

  def rework_state?(_state), do: false

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
      Path.join(System.tmp_dir!(), "symphony_elixir_test_ledger.json")
    else
      Path.join(deployment_dir(), ".symphony/ledger.json")
    end
  end

  # The ledger belongs to the deployment it describes — the directory the
  # WORKFLOW.md lives in — NOT the BEAM's cwd. Launchers commonly `cd` to the
  # Symphony install dir before exec (the escript wrapper does), which would
  # put the ledger in the install dir and silently SHARE it between
  # deployments. Same cwd-assumption family as the mirror-source bug.
  defp deployment_dir do
    case SymphonyElixir.Workflow.workflow_file_path() do
      path when is_binary(path) and path != "" ->
        dir = path |> Path.expand() |> Path.dirname()
        if File.dir?(dir), do: dir, else: File.cwd!()

      _ ->
        File.cwd!()
    end
  end

  defp load(path) do
    maybe_migrate_legacy(path)

    entries =
      case File.read(path) do
        {:ok, body} ->
          decode_entries(body)

        {:error, :enoent} ->
          %{}

        {:error, reason} ->
          Logger.warning("Unable to read Symphony ledger #{path}: #{inspect(reason)}")
          %{}
      end

    %{path: path, entries: entries}
  end

  @doc false
  @spec maybe_migrate_legacy(Path.t(), Path.t() | nil) :: :ok
  def maybe_migrate_legacy(path, legacy_path \\ nil) do
    legacy = legacy_path || Path.join(File.cwd!(), ".symphony/ledger.json")

    if legacy != path and not File.exists?(path) and File.exists?(legacy) do
      File.mkdir_p!(Path.dirname(path))

      case File.cp(legacy, path) do
        :ok ->
          Logger.info("Migrated Symphony ledger from legacy cwd location #{legacy} to #{path}")

        {:error, reason} ->
          Logger.warning("Failed to migrate legacy Symphony ledger #{legacy} -> #{path}: #{inspect(reason)}")
      end
    end

    :ok
  end

  defp decode_entries(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) ->
        Map.new(decoded, fn {issue_id, entry} -> {issue_id, normalize_entry(entry)} end)

      _ ->
        %{}
    end
  end

  defp persist(path, entries) when is_binary(path) and is_map(entries) do
    encoded =
      entries
      |> Map.new(fn {issue_id, entry} -> {issue_id, stringify_entry(entry)} end)
      |> Jason.encode!()

    path
    |> Path.dirname()
    |> File.mkdir_p!()

    File.write!(path, encoded)
  rescue
    exception ->
      Logger.warning("Unable to persist Symphony ledger #{path}: #{Exception.message(exception)}")
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
        Path.join(System.tmp_dir!(), "symphony_elixir_test_metrics.jsonl")
      else
        Path.join(File.cwd!(), ".symphony/metrics.jsonl")
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
