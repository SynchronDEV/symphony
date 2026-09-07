defmodule SymphonyElixir.WorkspaceReadinessTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.PathSafety

  setup do
    root = Path.join(Path.dirname(Workflow.workflow_file_path()), "workspaces")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, workspace_keep_last_n: 0)
    {:ok, root: root}
  end

  test "recorded cleanup survives root reload and preserves replacement", %{root: root} do
    assert {:ok, original} = Workspace.create_for_issue("SPK-RECORDED")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root <> "-new")
    assert {:ok, replacement} = Workspace.create_for_issue("SPK-RECORDED")
    assert {:ok, _} = Workspace.remove_recorded(original, nil, root)
    refute File.exists?(original)
    assert File.dir?(replacement)
  end

  test "recorded cleanup rejects root, relative path, missing provenance and symlink escape", %{root: root} do
    assert {:ok, workspace} = Workspace.create_for_issue("SPK-SYMLINK")
    outside = root <> "-outside"
    File.mkdir_p!(outside)
    File.write!(Path.join(outside, "keep"), "outside")
    File.rm_rf!(workspace)
    File.ln_s!(outside, workspace)
    assert {:error, _, ""} = Workspace.remove_recorded(workspace, nil, root)
    assert {:error, _, ""} = Workspace.remove_recorded(root, nil, root)
    assert {:error, _, ""} = Workspace.remove_recorded("SPK-SYMLINK", nil, root)
    assert {:error, _, ""} = Workspace.remove_recorded(workspace, nil, nil)
    assert File.read!(Path.join(outside, "keep")) == "outside"
  end

  test "recorded cleanup rejects retargeting to a sibling workspace", %{root: root} do
    assert {:ok, workspace} = Workspace.create_for_issue("SPK-LINK")
    assert {:ok, sibling} = Workspace.create_for_issue("SPK-SIBLING")
    File.rm_rf!(workspace)
    File.ln_s!(sibling, workspace)
    assert {:error, {:workspace_symlink_escape, _, _}, ""} = Workspace.remove_recorded(workspace, nil, root)
    assert File.dir?(sibling)
  end

  test "symlink loops return an error", %{root: root} do
    File.mkdir_p!(root)
    link = Path.join(root, "loop")
    File.ln_s!("loop", link)
    assert {:error, {:path_canonicalize_failed, _, :eloop}} = PathSafety.canonicalize(link)
  end

  test "failed new bootstrap is removed and retries setup without touching reused edits", %{root: root} do
    marker = root <> "-attempted"

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: root,
      hook_after_create: "echo partial > partial; if [ ! -f '#{marker}' ]; then touch '#{marker}'; exit 17; fi; echo ready > ready",
      hook_before_remove: "touch '#{root}-wrong-hook'"
    )

    assert {:error, {:workspace_hook_failed, "after_create", 17, _}} = Workspace.create_for_issue("SPK-RETRY")
    refute File.exists?(Path.join(root, "SPK-RETRY"))
    refute File.exists?(root <> "-wrong-hook")
    assert {:ok, workspace} = Workspace.create_for_issue("SPK-RETRY")
    assert File.read!(Path.join(workspace, "ready")) == "ready\n"
    File.write!(Path.join(workspace, "ready"), "operator edits")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, hook_after_create: "exit 18")
    assert {:ok, ^workspace} = Workspace.create_for_issue("SPK-RETRY")
    assert File.read!(Path.join(workspace, "ready")) == "operator edits"
  end

  test "local roots support absolute, workflow-relative and tilde forms", %{root: root} do
    relative = Path.join(Path.dirname(Workflow.workflow_file_path()), "relative/workspaces")

    for {configured, expected} <- [{root, root}, {"relative/workspaces", relative}] do
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: configured)
      assert Config.local_workspace_root() == Path.expand(expected)
      assert {:ok, workspace} = Workspace.create_for_issue("SPK-ROOT")
      assert {:ok, canonical} = PathSafety.canonicalize(Path.join(expected, "SPK-ROOT"))
      assert workspace == canonical
    end

    tilde_root = "~/.symphony-readiness-#{System.unique_integer([:positive])}"
    on_exit(fn -> File.rm_rf(Path.expand(tilde_root)) end)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: tilde_root)
    assert Config.local_workspace_root() == Path.expand(tilde_root)
    assert {:ok, workspace} = Workspace.create_for_issue("SPK-TILDE")
    assert File.dir?(workspace)
  end

  test "retention preserves unregistered workspaces and defaults to no live authorization", %{root: root} do
    assert {:ok, workspace} = Workspace.create_for_issue("SPK-UNREGISTERED")
    assert :ok = Workspace.enforce_retention()
    assert File.exists?(workspace)
    record = completed_record(root, "SPK-NO-CALLBACK")
    assert :ok = Workspace.enforce_retention([record])
    assert File.exists?(record.workspace_path)
  end

  test "running, claimed, queued, retrying and blocked records survive retention", %{root: root} do
    records =
      for status <- [:running, :claimed, :queued, :retrying, :blocked] do
        completed_record(root, "SPK-#{status}") |> Map.put(:status, status)
      end

    assert :ok = Workspace.enforce_retention(records, fn _ -> true end)
    assert Enum.all?(records, &File.dir?(&1.workspace_path))
  end

  test "dirty, unmerged, stashed and hidden branch work survives automated cleanup", %{root: root} do
    dirty = completed_record(root, "SPK-DIRTY")
    File.write!(Path.join(dirty.workspace_path, "untracked"), "edits")
    unmerged = completed_record(root, "SPK-UNMERGED")
    commit_edit(unmerged.workspace_path)
    branch = completed_record(root, "SPK-BRANCH")
    git!(branch.workspace_path, ["checkout", "-b", "unmerged"])
    commit_edit(branch.workspace_path)
    git!(branch.workspace_path, ["checkout", "main"])
    stashed = completed_record(root, "SPK-STASH")
    File.write!(Path.join(stashed.workspace_path, "tracked"), "stashed edits")
    git!(stashed.workspace_path, ["stash", "push"])

    for record <- [dirty, unmerged, branch, stashed] do
      assert {:error, {:workspace_preserved, _}, ""} = Workspace.remove_completed(record, fn -> true end)
      assert File.dir?(record.workspace_path)
    end
  end

  test "only eligible clean merged work is removed; live ownership can veto", %{root: root} do
    protected = completed_record(root, "SPK-LIVE")
    assert {:error, {:workspace_preserved, _}, ""} = Workspace.remove_completed(protected, fn -> false end)
    assert File.dir?(protected.workspace_path)
    completed = completed_record(root, "SPK-COMPLETE")
    assert {:ok, _} = Workspace.remove_completed(completed, fn -> true end)
    refute File.exists?(completed.workspace_path)
  end

  test "retention orders by completion and rechecks live eligibility before deletion", %{root: root} do
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, workspace_keep_last_n: 1)
    older = completed_record(root, "SPK-OLDER") |> Map.put(:completed_at, ~U[2026-01-01 00:00:00Z])
    newer = completed_record(root, "SPK-NEWER") |> Map.put(:completed_at, ~U[2026-02-01 00:00:00Z])
    assert :ok = Workspace.enforce_retention([older, newer], fn _ -> true end)
    refute File.exists?(older.workspace_path)
    assert File.exists?(newer.workspace_path)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, workspace_keep_last_n: 0)
    Process.put(:eligibility_checks, 0)

    assert :ok =
             Workspace.enforce_retention([newer], fn _ ->
               count = Process.get(:eligibility_checks)
               Process.put(:eligibility_checks, count + 1)
               count == 0
             end)

    assert File.exists?(newer.workspace_path)
  end

  test "automated cleanup rechecks dirty state after before_remove hook", %{root: root} do
    record = completed_record(root, "SPK-HOOK")
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: root, hook_before_remove: "echo changed > tracked")
    result = Workspace.remove_completed(record, fn -> true end)
    assert {:error, {:workspace_preserved, :eligibility_changed}, ""} = result
    assert File.read!(Path.join(record.workspace_path, "tracked")) == "changed\n"
  end

  defp completed_record(root, identifier) do
    assert {:ok, workspace} = Workspace.create_for_issue(identifier)
    git!(workspace, ["init", "-b", "main"])
    git!(workspace, ["config", "user.name", "Workspace Test"])
    git!(workspace, ["config", "user.email", "workspace@example.test"])
    File.write!(Path.join(workspace, "tracked"), "initial")
    git!(workspace, ["add", "tracked"])
    git!(workspace, ["commit", "-m", "initial"])
    git!(workspace, ["update-ref", "refs/remotes/origin/staging", "HEAD"])

    %{
      workspace_path: workspace,
      workspace_root: root,
      worker_host: nil,
      status: :completed,
      eligible: true,
      completed_at: DateTime.utc_now(),
      merged_into: "refs/remotes/origin/staging"
    }
  end

  defp commit_edit(workspace) do
    File.write!(Path.join(workspace, "tracked"), "unmerged change")
    git!(workspace, ["commit", "-am", "unmerged"])
  end

  defp git!(workspace, args) do
    assert {output, 0} = System.cmd("git", ["-C", workspace | args], stderr_to_stdout: true)
    output
  end
end
