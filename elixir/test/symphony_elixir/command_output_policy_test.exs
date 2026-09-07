defmodule SymphonyElixir.CommandOutputPolicyTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.CommandOutputPolicy

  test "classifies ci watch commands as noisy" do
    assert CommandOutputPolicy.classify_command("gh run watch 123 --exit-status") == :ci_watch
    assert CommandOutputPolicy.compact_command_status("gh run watch 123 --exit-status") =~ "sparse polling"
  end

  test "classifies tests, builds, and git commands" do
    assert CommandOutputPolicy.classify_command("bun run test:browser") == :test
    assert CommandOutputPolicy.classify_command("bun x playwright test tests/e2e --grep @smoke") == :test
    assert CommandOutputPolicy.classify_command("bun run build:ci") == :build
    assert CommandOutputPolicy.classify_command("git push") == :git
  end

  test "compacts streaming GitHub Actions watch output" do
    output = """
    Refreshing run status every 3 seconds. Press Ctrl+C to quit.

    * branch Build and Analyze org/repo#123
    JOBS
    * Build
    """

    assert CommandOutputPolicy.compact_output_delta(output) ==
             "ci watch output suppressed: GitHub Actions status table is streaming; use sparse polling/deltas"
  end

  test "compacts browser and webserver validation noise while preserving failure-looking text" do
    assert CommandOutputPolicy.compact_output_delta("[browser] src/test/browser/foo.test.tsx\npassed") =~
             "browser test output"

    assert CommandOutputPolicy.compact_output_delta("[WebServer] Export \"CommentThread\" was reexported") =~
             "webserver validation noise"

    refute CommandOutputPolicy.compact_output_delta("[WebServer] fatal build failed")
  end

  test "compacts codex wrapper command and output events" do
    update = %{
      event: :notification,
      payload: %{
        "method" => "codex/event/exec_command_begin",
        "params" => %{"msg" => %{"command" => "gh run watch 456 --exit-status"}}
      },
      raw: "raw"
    }

    compacted = CommandOutputPolicy.compact_codex_update(update)

    assert get_in(compacted, [:payload, "params", "msg", "command"]) =~ "sparse polling"
    assert compacted.raw =~ "sparse polling"
  end

  test "normalizes command inputs and bounds sparse-poll status text" do
    assert CommandOutputPolicy.classify_command(~c"  mix test  ") == :test
    assert CommandOutputPolicy.classify_command(nil) == :unknown
    assert CommandOutputPolicy.classify_command("echo ready") == :unknown
    assert CommandOutputPolicy.compact_command_status("mix test") == nil

    assert CommandOutputPolicy.compact_command_status("gh pr checks\n --json state") ==
             "ci status poll: gh pr checks --json state"

    status = CommandOutputPolicy.compact_command_status("gh run view " <> String.duplicate("x", 300))
    assert String.ends_with?(status, "...")
    assert byte_size(status) < 250
  end

  test "preserves unrecognized updates and payloads, including unencodable data" do
    assert CommandOutputPolicy.compact_codex_update(:ignored) == :ignored

    for payload <- [%{}, %{"method" => "other"}, %{"method" => 123}] do
      result = CommandOutputPolicy.compact_codex_update(%{payload: payload})
      assert result.payload == payload
      assert Jason.decode!(result.raw) == payload
    end

    update = %{payload: %{"opaque" => self()}, raw: "original"}
    assert CommandOutputPolicy.compact_codex_update(update) == update
  end

  test "compacts modern and nested legacy deltas without replacing ordinary output" do
    for delta <- [nil, 42, "", "ordinary output"] do
      payload = %{"method" => "item/commandExecution/outputDelta", "params" => %{"outputDelta" => delta}}
      assert CommandOutputPolicy.compact_codex_update(%{payload: payload}).payload == payload
    end

    payload = %{
      "method" => "item/commandExecution/outputDelta",
      "params" => %{"outputDelta" => "Running 4 tests using 2 workers"}
    }

    result = CommandOutputPolicy.compact_codex_update(%{payload: payload})
    assert get_in(result, [:payload, "params", "outputDelta"]) =~ "playwright smoke output:"

    payload = %{
      "method" => "codex/event/exec_command_output_delta",
      "params" => %{
        "msg" => %{
          "delta" => "NO_COLOR warning",
          "output" => "DSN not configured",
          "payload" => %{"delta" => "export was reexported through module"}
        }
      }
    }

    result = CommandOutputPolicy.compact_codex_update(%{payload: payload})

    for path <- [["delta"], ["output"], ["payload", "delta"]] do
      assert get_in(result.payload, ["params", "msg"] ++ path) =~ "repeated validation warning:"
    end
  end

  test "falls back to parsed commands and leaves non-CI command starts intact" do
    payload = %{
      "method" => "codex/event/exec_command_begin",
      "params" => %{"msg" => %{"parsed_cmd" => "gh pr checks"}}
    }

    result = CommandOutputPolicy.compact_codex_update(%{payload: payload})
    assert get_in(result, [:payload, "params", "msg", "command"]) == "ci status poll: gh pr checks"

    payload = put_in(payload, ["params", "msg", "parsed_cmd"], "mix test")
    assert CommandOutputPolicy.compact_codex_update(%{payload: payload}).payload == payload
  end

  test "keeps the final three summary lines and strips terminal controls" do
    output = "playwright test\nold line\n\e[31mfirst\e[0m\nsecond\nthird\0"
    assert CommandOutputPolicy.compact_output_delta(output) == "playwright smoke output: first | second | third"
    assert CommandOutputPolicy.compact_output_delta("header\nJOBS\nhttps://github.com/org/repo") =~ "ci watch output suppressed"
    assert CommandOutputPolicy.compact_output_delta("[WebServer] exception in startup") == nil
  end
end
