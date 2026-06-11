defmodule SymphonyElixir.IssueStateBatcher do
  @moduledoc """
  Debounces concurrent issue-state refreshes into batched tracker calls.
  """

  use GenServer

  alias SymphonyElixir.Tracker

  @batch_delay_ms 25

  defstruct pending: %{},
            pending_ids: MapSet.new(),
            timer_ref: nil,
            fetcher: nil,
            batch_delay_ms: @batch_delay_ms

  @type state :: %__MODULE__{}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec fetch_issue_states_by_ids([String.t()]) :: {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids) when is_list(issue_ids) do
    ids = Enum.uniq(issue_ids)

    cond do
      ids == [] ->
        {:ok, []}

      Process.whereis(__MODULE__) ->
        GenServer.call(__MODULE__, {:fetch, ids}, :infinity)

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
  def handle_info(:flush, %__MODULE__{} = state) do
    pending = state.pending
    ids = MapSet.to_list(state.pending_ids)

    result =
      case state.fetcher.(ids) do
        {:ok, issues} when is_list(issues) ->
          issues_by_id =
            Map.new(issues, fn
              %{id: issue_id} = issue -> {issue_id, issue}
              issue -> {inspect(issue), issue}
            end)

          {:ok, issues_by_id}

        {:error, reason} ->
          {:error, reason}
      end

    Enum.each(pending, fn {_request_ref, %{from: from, ids: requested_ids}} ->
      GenServer.reply(from, reply_for_request(requested_ids, result))
    end)

    {:noreply, %{state | pending: %{}, pending_ids: MapSet.new(), timer_ref: nil}}
  end

  defp schedule_flush(%__MODULE__{timer_ref: timer_ref} = state) when is_reference(timer_ref), do: state

  defp schedule_flush(%__MODULE__{} = state) do
    %{state | timer_ref: Process.send_after(self(), :flush, state.batch_delay_ms)}
  end

  defp reply_for_request(requested_ids, {:ok, issues_by_id}) do
    issues =
      requested_ids
      |> Enum.map(&Map.get(issues_by_id, &1))
      |> Enum.reject(&is_nil/1)

    {:ok, issues}
  end

  defp reply_for_request(_requested_ids, {:error, reason}), do: {:error, reason}
end
