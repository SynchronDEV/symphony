defmodule SymphonyElixir.StudioWorkflowPromptTest do
  use SymphonyElixir.TestSupport

  setup do
    [_, _, prompt] =
      __DIR__
      |> Path.join("../../../workflows/studio.md")
      |> File.read!()
      |> String.split("---", parts: 3)

    write_workflow_file!(Workflow.workflow_file_path(), prompt: prompt)

    %{issue: %Issue{identifier: "SPK-1106", title: "Dependency rendering", state: "Todo", labels: []}}
  end

  test "Studio workflow renders an issue without dependencies", %{issue: issue} do
    prompt = PromptBuilder.build_prompt(%{issue | blocked_by: []}, attempt: 1)

    assert prompt =~ "SPK-1106: Dependency rendering"
    assert prompt =~ "Dependencies:"
    refute prompt =~ "- :"
  end

  test "Studio workflow renders completed dependency maps", %{issue: issue} do
    prompt =
      PromptBuilder.build_prompt(
        %{issue | blocked_by: [%{id: "dependency-1", identifier: "SPK-1080", state: "Done"}]},
        attempt: 1
      )

    assert prompt =~ "Dependencies:"
    assert prompt =~ "- SPK-1080: Done"
    refute prompt =~ "dependency-1"
  end
end
