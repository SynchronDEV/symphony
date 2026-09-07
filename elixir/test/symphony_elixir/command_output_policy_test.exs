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
end
