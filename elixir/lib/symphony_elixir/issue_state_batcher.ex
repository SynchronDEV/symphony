defmodule SymphonyElixir.IssueStateBatcher do
  @moduledoc """
  Debounces concurrent issue-state refreshes into batched tracker calls.
  """

  use GenServer

  alias SymphonyElixir.Tracker

  @batch_delay_ms 25
  @call_timeout_ms 30_000

  defstruct pending: %{},
            pending_ids: MapSet.new(),
            timer_ref: nil,
            in_flight: nil,
            fetcher: nil,
            batch_delay_ms: @batch_delay_ms

  @type state :: %__MODULE__{}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec fetch_issue_states_by_ids([String.t()], keyword()) :: {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids, opts \\ []) when is_list(issue_ids) and is_list(opts) do
    ids = Enum.uniq(issue_ids)
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, @call_timeout_ms)

    cond do
      ids == [] ->
        {:ok, []}

      Process.whereis(server) ->
        # A hung batched fetch must not block every waiting caller forever
        # (previously :infinity). On timeout the caller gets a structured
        # error and its own retry/pause handling takes over; the batcher's
        # late reply to a timed-out caller is dropped by the runtime.
        try do
          GenServer.call(server, {:fetch, ids}, timeout)
        catch
          :exit, {:timeout, _} -> {:error, :issue_state_batch_timeout}
        end

      true ->
        Tracker.fetch_issue_states_by_ids(ids)
    end
  end

  @impl true
  def init(opts) do
    {:ok,
     %__MODULE__{
       fetcher: Keyword.get(opts, :fetcher, &Tracker.fetch_issue_states_by_ids/1),
       batch_delay_ms: Keyword.get(opts, :batch_delay_ms, @batch_delay_ms)
     }}
  end

  @impl true
  def handle_call({:fetch, ids}, from, %__MODULE__{} = state) do
    request_ref = make_ref()

    pending =
      Map.put(state.pending, request_ref, %{
        from: from,
        ids: ids
      })

    pending_ids = Enum.reduce(ids, state.pending_ids, &MapSet.put(&2, &1))

    state = %{state | pending: pending, pending_ids: pending_ids}
    state = schedule_flush(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, %__MODULE__{in_flight: nil} = state) do
    ids = MapSet.to_list(state.pending_ids)
    fetcher = state.fetcher
    task = Task.Supervisor.async_nolink(SymphonyElixir.TaskSupervisor, fn -> fetcher.(ids) end)
    timer = Process.send_after(self(), {:batch_timeout, task.ref}, @call_timeout_ms)

    {:noreply, %{state | pending: %{}, pending_ids: MapSet.new(), timer_ref: nil, in_flight: %{task: task, pending: state.pending, timer: timer}}}
  end

  def handle_info(:flush, state), do: {:noreply, %{state | timer_ref: nil}}

  def handle_info({ref, result}, %__MODULE__{in_flight: %{task: %{ref: ref}}} = state) do
    Process.demonitor(ref, [:flush])
    finish_batch(state, result)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %__MODULE__{in_flight: %{task: %{ref: ref}}} = state) do
    finish_batch(state, {:error, {:issue_state_batch_failed, reason}})
  end

  def handle_info({:batch_timeout, ref}, %__MODULE__{in_flight: %{task: %{ref: ref, pid: pid}}} = state) do
    Process.exit(pid, :kill)
    Process.demonitor(ref, [:flush])
    finish_batch(state, {:error, :issue_state_batch_timeout})
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp finish_batch(state, result) do
    Process.cancel_timer(state.in_flight.timer)

    result =
      case result do
        {:ok, issues} when is_list(issues) ->
          if Enum.all?(issues, &match?(%{id: id} when is_binary(id), &1)),
            do: {:ok, Map.new(issues, &{&1.id, &1})},
            else: {:error, :invalid_issue_state_result}

        {:error, _} = error ->
          error

        other ->
          {:error, {:invalid_issue_state_result, other}}
      end

    Enum.each(state.in_flight.pending, fn {_ref, %{from: from, ids: ids}} ->
      GenServer.reply(from, reply_for_request(ids, result))
    end)

    {:noreply, schedule_flush(%{state | in_flight: nil})}
  end

  defp schedule_flush(%__MODULE__{in_flight: flight} = state) when not is_nil(flight), do: state
  defp schedule_flush(%__MODULE__{timer_ref: ref} = state) when is_reference(ref), do: state
  defp schedule_flush(%__MODULE__{pending: pending} = state) when map_size(pending) == 0, do: state
  defp schedule_flush(state), do: %{state | timer_ref: Process.send_after(self(), :flush, state.batch_delay_ms)}

  defp reply_for_request(requested_ids, {:ok, issues_by_id}) do
    issues =
      requested_ids
      |> Enum.map(&Map.get(issues_by_id, &1))
      |> Enum.reject(&is_nil/1)

    {:ok, issues}
  end

  defp reply_for_request(_requested_ids, {:error, reason}), do: {:error, reason}
end
