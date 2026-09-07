defmodule SymphonyElixir.CLIPreflightTest do
  use SymphonyElixir.TestSupport
  import ExUnit.CaptureIO

  test "preflight validates and redacts config without calling the startup dependency" do
    key = "preflight-secret-must-not-appear"
    write_workflow_file!(Workflow.workflow_file_path(), tracker_api_token: key)
    path = Workflow.workflow_file_path()
    startup = fn -> flunk("preflight must not start the Symphony supervisor") end

    output =
      capture_io(fn ->
        assert {:ok, :preflight} = CLI.evaluate(["--preflight", path], %{ensure_all_started: startup})
      end)

    refute output =~ key
    assert {:ok, report} = Jason.decode(String.trim(output))
    assert report["preflight"] == "ok"
    assert report["tracker"]["credential_present"]
    assert report["runtime"]["recorded_cleanup"]
    assert report["runtime"]["turn_interrupt"]
    assert report["workflow"] == path
  end

  test "preflight rejects an invalid prompt without printing its content" do
    write_workflow_file!(Workflow.workflow_file_path(), prompt: "{{ missing_secret_variable }}")

    output =
      capture_io(fn ->
        assert {:error, message} = CLI.evaluate(["--preflight", Workflow.workflow_file_path()])
        assert message =~ "Preflight failed"
        refute message =~ "missing_secret_variable"
      end)

    refute output =~ "missing_secret_variable"
  end
end
