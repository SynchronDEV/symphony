defmodule SymphonyElixir.Linear.RateLimitBudget do
  @moduledoc """
  Shared Linear rate-limit budget parsed from response headers.
  """

  use Agent

  @threshold 200

  @type budget :: %{remaining: non_neg_integer() | nil, reset_at: DateTime.t() | nil, updated_at: DateTime.t()}

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> nil end, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec update_from_headers(term()) :: budget() | nil
  def update_from_headers(headers) do
    remaining = header_integer(headers, "x-ratelimit-requests-remaining")
    reset_at = header_reset_at(headers, "x-ratelimit-requests-reset")

    if is_integer(remaining) or match?(%DateTime{}, reset_at) do
      budget = %{remaining: remaining, reset_at: reset_at, updated_at: DateTime.utc_now()}

      put_budget(budget)

      budget
    end
  end

  defp put_budget(budget) do
    if Process.whereis(__MODULE__) do
      Agent.update(__MODULE__, fn _ -> budget end)
    end
  end

  @spec current() :: budget() | nil
  def current do
    if Process.whereis(__MODULE__), do: Agent.get(__MODULE__, & &1), else: nil
  end

  @spec low?() :: boolean()
  def low? do
    case current() do
      %{remaining: remaining} when is_integer(remaining) -> remaining < @threshold
      _ -> false
    end
  end

  @spec reset_at() :: DateTime.t() | nil
  def reset_at do
    case current() do
      %{reset_at: %DateTime{} = reset_at} -> reset_at
      _ -> nil
    end
  end

  @spec delay_until_reset((non_neg_integer() -> term()), non_neg_integer()) :: :ok | term()
  def delay_until_reset(sleep_fun \\ &Process.sleep/1, fallback_ms \\ 0)
      when is_function(sleep_fun, 1) and is_integer(fallback_ms) and fallback_ms >= 0 do
    case reset_at() do
      %DateTime{} = reset_at ->
        reset_at
        |> DateTime.diff(DateTime.utc_now(), :millisecond)
        |> max(0)
        |> sleep_fun.()

      # No reset timestamp known (Linear can return RATELIMITED with empty
      # headers). Callers that MUST wait — e.g. an in-request rate-limit
      # retry — pass a fallback so the retry doesn't fire immediately into
      # the same exhausted window. Callers using this as an optional
      # low-budget gate keep the default 0 (no-op).
      _ when fallback_ms > 0 ->
        sleep_fun.(fallback_ms)

      _ ->
        :ok
    end
  end

  @doc false
  @spec header_integer(term(), String.t()) :: integer() | nil
  def header_integer(headers, name) do
    headers
    |> header_value(name)
    |> parse_integer()
  end

  @doc false
  @spec header_reset_at(term(), String.t()) :: DateTime.t() | nil
  def header_reset_at(headers, name) do
    headers
    |> header_value(name)
    |> parse_reset_at()
  end

  defp header_value(headers, name) when is_map(headers) do
    normalized_name = String.downcase(name)

    headers
    |> Enum.find_value(fn {key, value} ->
      if String.downcase(to_string(key)) == normalized_name do
        list_first(value)
      end
    end)
  end

  defp header_value(headers, name) when is_list(headers) do
    normalized_name = String.downcase(name)

    headers
    |> Enum.find_value(fn
      {key, value} ->
        if String.downcase(to_string(key)) == normalized_name, do: list_first(value)

      _ ->
        nil
    end)
  end

  defp header_value(_headers, _name), do: nil

  defp list_first([value | _]), do: value
  defp list_first(value), do: value

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} -> integer
      _ -> nil
    end
  end

  defp parse_integer(value) when is_integer(value), do: value
  defp parse_integer(_value), do: nil

  defp parse_reset_at(value) when is_binary(value) do
    trimmed = String.trim(value)

    with nil <- parse_unix_reset_at(trimmed),
         {:ok, datetime, _offset} <- DateTime.from_iso8601(trimmed) do
      datetime
    else
      %DateTime{} = datetime -> datetime
      _ -> nil
    end
  end

  defp parse_reset_at(value) when is_integer(value), do: parse_unix_reset_at(Integer.to_string(value))
  defp parse_reset_at(_value), do: nil

  defp parse_unix_reset_at(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 1_000_000_000_000 ->
        DateTime.from_unix!(integer, :millisecond)

      {integer, ""} when integer > 1_000_000_000 ->
        DateTime.from_unix!(integer, :second)

      _ ->
        nil
    end
  end
end
