defmodule SymphonyElixir.Linear.RateLimitBudgetBoundaryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Linear.RateLimitBudget

  setup do
    previous = RateLimitBudget.current()
    Agent.update(RateLimitBudget, fn _ -> nil end)
    on_exit(fn -> Agent.update(RateLimitBudget, fn _ -> previous end) end)
    :ok
  end

  test "default start preserves an already registered shared budget" do
    pid = Process.whereis(RateLimitBudget)
    assert {:error, {:already_started, ^pid}} = RateLimitBudget.start_link()
  end

  test "low-budget threshold expires, including the fallback window" do
    refute RateLimitBudget.low?()
    now = DateTime.utc_now()

    for {remaining, reset_at, updated_at, expected} <- [
          {199, DateTime.add(now, 60, :second), now, true},
          {200, DateTime.add(now, 60, :second), now, false},
          {199, DateTime.add(now, -60, :second), now, false},
          {199, nil, now, true},
          {199, nil, DateTime.add(now, -61, :second), false},
          {nil, DateTime.add(now, 60, :second), now, false}
        ] do
      Agent.update(RateLimitBudget, fn _ ->
        %{remaining: remaining, reset_at: reset_at, updated_at: updated_at}
      end)

      assert RateLimitBudget.low?() == expected
    end
  end

  test "delay defaults to no wait and supports fallback and expired windows" do
    assert :ok = RateLimitBudget.delay_until_reset()
    sleeper = fn milliseconds -> {:slept, milliseconds} end
    assert :ok = RateLimitBudget.delay_until_reset(sleeper)
    assert {:slept, 17} = RateLimitBudget.delay_until_reset(sleeper, 17)

    RateLimitBudget.update_from_headers(%{
      "x-ratelimit-requests-reset" => DateTime.to_iso8601(DateTime.add(DateTime.utc_now(), -60, :second))
    })

    assert {:slept, 0} = RateLimitBudget.delay_until_reset(sleeper)
  end

  test "header lists accept case-insensitive keys and ignore malformed entries" do
    headers = [:invalid, {"unrelated", "2"}, {"X-Remaining", [" 123 ", "456"]}]
    assert RateLimitBudget.header_integer(headers, "x-remaining") == 123
    assert RateLimitBudget.header_integer(%{"x-remaining" => 12}, "X-Remaining") == 12

    for value <- ["12x", "invalid", nil, [], 1.5] do
      assert RateLimitBudget.header_integer([{"remaining", value}], "remaining") == nil
    end

    assert RateLimitBudget.header_integer(nil, "remaining") == nil
  end

  test "reset headers support seconds, milliseconds and ISO timestamps" do
    seconds = 1_800_000_000
    milliseconds = 1_800_000_000_123
    assert RateLimitBudget.header_reset_at([{"reset", seconds}], "reset") == DateTime.from_unix!(seconds)

    assert RateLimitBudget.header_reset_at(%{"reset" => milliseconds}, "reset") ==
             DateTime.from_unix!(milliseconds, :millisecond)

    iso = "2027-01-15T08:00:00Z"
    assert RateLimitBudget.header_reset_at(%{"reset" => iso}, "reset") == ~U[2027-01-15 08:00:00Z]

    for value <- ["not a date", "123", nil, []] do
      assert RateLimitBudget.header_reset_at(%{"reset" => value}, "reset") == nil
    end
  end
end
