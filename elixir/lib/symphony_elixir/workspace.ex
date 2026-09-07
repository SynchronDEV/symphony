defmodule SymphonyElixir.Workspace do
  @moduledoc """
  Creates isolated per-issue workspaces for parallel Codex agents.
  """

  require Logger
  alias SymphonyElixir.{Config, PathSafety, SSH}

  @remote_workspace_marker "__SYMPHONY_WORKSPACE__"

  @type worker_host :: String.t() | nil

  @spec ensure_mirror() :: :ok | {:error, term()}
  def ensure_mirror do
    case configured_mirror_path() do
      mirror_path when is_binary(mirror_path) and mirror_path != "" ->
        source = mirror_source_dir()

        cond do
          not File.dir?(Path.join(source, ".git")) ->
            :ok

          File.dir?(mirror_path) ->
            fetch_mirror(mirror_path)

          true ->
            create_mirror(source, mirror_path)
        end

      _ ->
        :ok
    end
  end

  @spec create_for_issue(map() | String.t() | nil, worker_host()) ::
          {:ok, Path.t()} | {:error, term()}
  def create_for_issue(issue_or_identifier, worker_host \\ nil) do
    issue_context = issue_context(issue_or_identifier)

    try do
      safe_id = safe_identifier(issue_context.issue_identifier)

      with {:ok, workspace} <- workspace_path_for_issue(safe_id, worker_host),
           :ok <- validate_workspace_path(workspace, worker_host),
           {:ok, workspace, created?} <- ensure_workspace(workspace, worker_host) do
        case maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
          :ok ->
            {:ok, workspace}

          {:error, _reason} = error ->
            cleanup_failed_new_workspace(workspace, created?, worker_host)
            error
        end
      end
    rescue
      error in [ArgumentError, ErlangError, File.Error] ->
        Logger.error("Workspace creation failed #{issue_log_context(issue_context)} worker_host=#{worker_host_for_log(worker_host)} error=#{Exception.message(error)}")
        {:error, error}
    end
  end

  defp ensure_workspace(workspace, nil) do
    cond do
      File.dir?(workspace) ->
        {:ok, workspace, false}

      File.exists?(workspace) ->
        File.rm_rf!(workspace)
        create_workspace(workspace)

      true ->
        create_workspace(workspace)
    end
  end

  defp ensure_workspace(workspace, worker_host) when is_binary(worker_host) do
    script =
      [
        "set -eu",
        remote_shell_assign("workspace", workspace),
        "if [ -d \"$workspace\" ]; then",
        "  created=0",
        "elif [ -e \"$workspace\" ]; then",
        "  rm -rf \"$workspace\"",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "else",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "fi",
        "cd \"$workspace\"",
        "printf '%s\\t%s\\t%s\\n' '#{@remote_workspace_marker}' \"$created\" \"$(pwd -P)\""
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        parse_remote_workspace_output(output)

      {:ok, {output, status}} ->
        {:error, {:workspace_prepare_failed, worker_host, status, output}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_workspace(workspace) do
    File.rm_rf!(workspace)

    case clone_from_mirror(workspace) do
      :ok ->
        {:ok, workspace, true}

      :skip ->
        File.mkdir_p!(workspace)
        {:ok, workspace, true}

      {:error, reason} ->
        cleanup_failed_new_workspace(workspace, true, nil)
        {:error, reason}
    end
  end

  # The mirror source is the repo the WORKFLOW.md lives in — NOT the BEAM's
  # cwd. Launchers commonly `cd` to the Symphony install dir before exec (the
  # escript wrapper does), so File.cwd!() points at symphony itself and the
  # `.git` check silently skips mirror creation. Fall back to cwd only when
  # the workflow file's directory is not a git repo.
  defp mirror_source_dir do
    workflow_dir =
      case SymphonyElixir.Workflow.workflow_file_path() do
        path when is_binary(path) and path != "" -> path |> Path.expand() |> Path.dirname()
        _ -> nil
      end

    if is_binary(workflow_dir) and File.dir?(Path.join(workflow_dir, ".git")) do
      workflow_dir
    else
      File.cwd!()
    end
  end

  # Config paths like "~/code/spektra-mirror.git" must be expanded before any
  # File.dir?/git use — a raw tilde never matches an existing directory, so the
  # mirror would silently never be created or used.
  defp configured_mirror_path do
    case Config.settings!().workspace.mirror_path do
      mirror_path when is_binary(mirror_path) and mirror_path != "" -> Path.expand(mirror_path)
      other -> other
    end
  end

  # credo:disable-for-lines:18 Credo.Check.Refactor.Nesting
  defp clone_from_mirror(workspace) do
    case configured_mirror_path() do
      mirror_path when is_binary(mirror_path) and mirror_path != "" ->
        source = mirror_source_dir()

        if File.dir?(mirror_path) and File.dir?(Path.join(source, ".git")) do
          parent = Path.dirname(workspace)
          File.mkdir_p!(parent)

          case System.cmd("git", ["clone", "--reference", mirror_path, "--dissociate", source, workspace], stderr_to_stdout: true) do
            {_output, 0} -> :ok
            {output, status} -> {:error, {:workspace_clone_failed, status, output}}
          end
        else
          :skip
        end

      _ ->
        :skip
    end
  end

  defp create_mirror(source, mirror_path) do
    mirror_path
    |> Path.dirname()
    |> File.mkdir_p!()

    case System.cmd("git", ["clone", "--mirror", source, mirror_path], stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Logger.warning("Workspace mirror clone failed status=#{status} output=#{inspect(sanitize_hook_output_for_log(output))}")
        {:error, {:workspace_mirror_clone_failed, status, output}}
    end
  end

  defp fetch_mirror(mirror_path) do
    case System.cmd("git", ["-C", mirror_path, "fetch", "--prune"], stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Logger.warning("Workspace mirror fetch failed status=#{status} output=#{inspect(sanitize_hook_output_for_log(output))}")
        {:error, {:workspace_mirror_fetch_failed, status, output}}
    end
  end

  @spec remove(Path.t()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace), do: remove(workspace, nil)

  @spec remove(Path.t(), worker_host()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace, nil) do
    remove_recorded(workspace, nil, Config.local_workspace_root())
  end

  def remove(workspace, worker_host) when is_binary(worker_host) do
    maybe_run_before_remove_hook(workspace, worker_host)

    script =
      [
        remote_shell_assign("workspace", workspace),
        "rm -rf \"$workspace\""
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {_output, 0}} ->
        {:ok, []}

      {:ok, {output, status}} ->
        {:error, {:workspace_remove_failed, worker_host, status, output}, ""}

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  @doc "Remove an explicitly recorded path using its original root, independent of configuration reloads."
  @spec remove_recorded(Path.t(), worker_host(), Path.t()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove_recorded(workspace, nil, recorded_root) do
    case validate_recorded_workspace_path(workspace, recorded_root) do
      :ok -> remove_after_hook(workspace, recorded_root)
      {:error, reason} -> {:error, reason, ""}
    end
  end

  def remove_recorded(workspace, worker_host, recorded_root)
      when is_binary(workspace) and is_binary(worker_host) and is_binary(recorded_root) do
    # Remote automated deletion is deliberately preserved by remove_completed/2
    # until remote clean/merge evidence is implemented.
    remove(workspace, worker_host)
  end

  def remove_recorded(workspace, _worker_host, _recorded_root),
    do: {:error, {:workspace_path_unreadable, workspace, :invalid}, ""}

  defp remove_after_hook(workspace, root) do
    maybe_run_before_remove_hook(workspace, nil)

    case validate_recorded_workspace_path(workspace, root) do
      :ok -> File.rm_rf(workspace)
      {:error, reason} -> {:error, reason, ""}
    end
  end

  defp validate_recorded_workspace_path(workspace, root) when is_binary(workspace) and is_binary(root) do
    if Path.type(workspace) == :absolute and Path.type(root) == :absolute do
      validate_recorded_child(workspace, root)
    else
      {:error, {:workspace_path_unreadable, workspace, :not_absolute}}
    end
  end

  defp validate_recorded_workspace_path(workspace, _root),
    do: {:error, {:workspace_path_unreadable, workspace, :invalid}}

  defp validate_recorded_child(workspace, root) do
    with :ok <- validate_local_workspace_path(workspace, root),
         {:ok, canonical} <- PathSafety.canonicalize(workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(root) do
      expected = Path.join(canonical_root, Path.basename(Path.expand(workspace)))
      if canonical == expected, do: :ok, else: {:error, {:workspace_symlink_escape, workspace, root}}
    end
  end

  @spec remove_issue_workspaces(term()) :: :ok
  def remove_issue_workspaces(identifier), do: remove_issue_workspaces(identifier, nil)

  @spec remove_issue_workspaces(term(), worker_host()) :: :ok
  def remove_issue_workspaces(identifier, worker_host) when is_binary(identifier) and is_binary(worker_host) do
    safe_id = safe_identifier(identifier)

    case workspace_path_for_issue(safe_id, worker_host) do
      {:ok, workspace} -> remove(workspace, worker_host)
      {:error, _reason} -> :ok
    end

    :ok
  end

  def remove_issue_workspaces(identifier, nil) when is_binary(identifier) do
    safe_id = safe_identifier(identifier)

    case Config.settings!().worker.ssh_hosts do
      [] ->
        case workspace_path_for_issue(safe_id, nil) do
          {:ok, workspace} -> remove(workspace, nil)
          {:error, _reason} -> :ok
        end

      worker_hosts ->
        Enum.each(worker_hosts, &remove_issue_workspaces(identifier, &1))
    end

    :ok
  end

  def remove_issue_workspaces(_identifier, _worker_host) do
    :ok
  end

  @doc """
  Removes only explicitly registered, completed, clean and merged workspaces.

  The caller must supply a live eligibility check which excludes running,
  claimed, queued, retrying and blocked issues. Missing evidence preserves data.
  The limits apply only to eligible records, ordered by completion time.
  """
  @spec enforce_retention([map()], (map() -> boolean())) :: :ok
  def enforce_retention(records \\ [], still_eligible? \\ fn _record -> false end) do
    eligible =
      records
      |> Enum.filter(&(completed_workspace_safe?(&1) and still_eligible?.(&1)))
      |> Enum.sort_by(& &1.completed_at, {:desc, DateTime})

    settings = Config.settings!().workspace
    keep_count = settings.keep_last_n
    max_bytes = if is_number(settings.max_total_gb), do: trunc(settings.max_total_gb * 1024 * 1024 * 1024)

    _retained_bytes = Enum.reduce(Enum.with_index(eligible), 0, &retain_or_remove(&1, &2, keep_count, max_bytes, still_eligible?))

    :ok
  end

  defp retain_or_remove({record, index}, retained_bytes, keep_count, max_bytes, still_eligible?) do
    size = directory_size(record.workspace_path)
    count_exceeded? = is_integer(keep_count) and keep_count >= 0 and index >= keep_count
    size_exceeded? = is_integer(max_bytes) and retained_bytes + size > max_bytes

    if count_exceeded? or size_exceeded? do
      remove_completed(record, fn -> still_eligible?.(record) end)
      retained_bytes
    else
      retained_bytes + size
    end
  end

  @doc "Automated cleanup: preserve unless lifecycle, Git, path and live ownership checks all pass."
  @spec remove_completed(map(), (-> boolean())) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove_completed(record, still_eligible? \\ fn -> false end) do
    if completed_workspace_safe?(record) and still_eligible?.() do
      maybe_run_before_remove_hook(record.workspace_path, nil)

      # Hooks may dirty or replace a workspace. Check again after the hook and
      # immediately before deletion; the caller holds the cleanup claim here.
      if completed_workspace_safe?(record) and still_eligible?.() do
        File.rm_rf(record.workspace_path)
      else
        {:error, {:workspace_preserved, :eligibility_changed}, ""}
      end
    else
      {:error, {:workspace_preserved, :missing_completion_or_merge_proof}, ""}
    end
  end

  defp completed_workspace_safe?(%{
         workspace_path: workspace,
         workspace_root: root,
         worker_host: nil,
         status: :completed,
         eligible: true,
         completed_at: %DateTime{},
         merged_into: merged_into
       })
       when is_binary(workspace) and is_binary(root) and is_binary(merged_into) and merged_into != "" do
    with true <- String.starts_with?(merged_into, "refs/remotes/"),
         :ok <- validate_recorded_workspace_path(workspace, root),
         {"", 0} <- System.cmd("git", ["-C", workspace, "status", "--porcelain=v1", "--untracked-files=all"], stderr_to_stdout: true),
         {_output, 0} <- System.cmd("git", ["-C", workspace, "merge-base", "--is-ancestor", "HEAD", merged_into], stderr_to_stdout: true),
         {"", 0} <- System.cmd("git", ["-C", workspace, "rev-list", "--branches", "--not", merged_into], stderr_to_stdout: true),
         {"", 0} <- System.cmd("git", ["-C", workspace, "for-each-ref", "--format=%(refname)", "refs/stash"], stderr_to_stdout: true) do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp completed_workspace_safe?(_record), do: false

  # credo:disable-for-lines:23 Credo.Check.Refactor.Nesting
  defp directory_size(path) do
    {output, status} = System.cmd("du", ["-sk", path], stderr_to_stdout: true)

    if status == 0 do
      output
      |> String.split()
      |> List.first()
      |> case do
        nil ->
          0

        kb ->
          case Integer.parse(kb) do
            {value, _rest} -> value * 1024
            :error -> 0
          end
      end
    else
      0
    end
  rescue
    _ -> 0
  end

  @spec run_before_run_hook(Path.t(), map() | String.t() | nil, worker_host()) ::
          :ok | {:error, term()}
  def run_before_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.before_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "before_run", worker_host)
    end
  end

  @spec run_after_run_hook(Path.t(), map() | String.t() | nil, worker_host()) :: :ok
  def run_after_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.after_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "after_run", worker_host)
        |> ignore_hook_failure()
    end
  end

  defp workspace_path_for_issue(safe_id, nil) when is_binary(safe_id) do
    Config.local_workspace_root()
    |> Path.join(safe_id)
    |> PathSafety.canonicalize()
  end

  defp workspace_path_for_issue(safe_id, worker_host) when is_binary(safe_id) and is_binary(worker_host) do
    {:ok, Path.join(Config.settings!().workspace.root, safe_id)}
  end

  defp safe_identifier(identifier) do
    String.replace(identifier || "issue", ~r/[^a-zA-Z0-9._-]/, "_")
  end

  defp maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
    hooks = Config.settings!().hooks

    case created? do
      true ->
        case hooks.after_create do
          nil ->
            :ok

          command ->
            run_hook(command, workspace, issue_context, "after_create", worker_host)
        end

      false ->
        :ok
    end
  end

  defp cleanup_failed_new_workspace(_workspace, false, _worker_host), do: :ok

  defp cleanup_failed_new_workspace(workspace, true, nil) do
    # Never invoke before_remove for an incomplete bootstrap. Revalidate against
    # the physical parent captured in the newly created path, not reloaded config.
    with :ok <- validate_recorded_workspace_path(workspace, Path.dirname(workspace)) do
      File.rm_rf(workspace)
    end
  end

  defp cleanup_failed_new_workspace(workspace, true, worker_host) do
    script = [remote_shell_assign("workspace", workspace), "rm -rf \"$workspace\""] |> Enum.join("\n")
    run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms)
  end

  defp maybe_run_before_remove_hook(workspace, nil) do
    hooks = Config.settings!().hooks

    case File.dir?(workspace) do
      true ->
        case hooks.before_remove do
          nil ->
            :ok

          command ->
            run_hook(
              command,
              workspace,
              %{issue_id: nil, issue_identifier: Path.basename(workspace)},
              "before_remove",
              nil
            )
            |> ignore_hook_failure()
        end

      false ->
        :ok
    end
  end

  defp maybe_run_before_remove_hook(workspace, worker_host) when is_binary(worker_host) do
    hooks = Config.settings!().hooks

    case hooks.before_remove do
      nil ->
        :ok

      command ->
        script =
          [
            remote_shell_assign("workspace", workspace),
            "if [ -d \"$workspace\" ]; then",
            "  cd \"$workspace\"",
            "  #{command}",
            "fi"
          ]
          |> Enum.join("\n")

        run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms)
        |> case do
          {:ok, {output, status}} ->
            handle_hook_command_result(
              {output, status},
              workspace,
              %{issue_id: nil, issue_identifier: Path.basename(workspace)},
              "before_remove"
            )

          {:error, {:workspace_hook_timeout, "before_remove", _timeout_ms} = reason} ->
            {:error, reason}

          {:error, reason} ->
            {:error, reason}
        end
        |> ignore_hook_failure()
    end
  end

  defp ignore_hook_failure(:ok), do: :ok
  defp ignore_hook_failure({:error, _reason}), do: :ok

  defp run_hook(command, workspace, issue_context, hook_name, nil) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local")

    task =
      Task.async(fn ->
        System.cmd("sh", ["-lc", command],
          cd: workspace,
          stderr_to_stdout: true,
          env: workspace_env()
        )
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      nil ->
        Task.shutdown(task, :brutal_kill)

        Logger.warning("Workspace hook timed out hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local timeout_ms=#{timeout_ms}")

        {:error, {:workspace_hook_timeout, hook_name, timeout_ms}}
    end
  end

  defp run_hook(command, workspace, issue_context, hook_name, worker_host) when is_binary(worker_host) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=#{worker_host}")

    case run_remote_command(worker_host, remote_env_exports() <> "cd #{shell_escape(workspace)} && #{command}", timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      {:error, {:workspace_hook_timeout, ^hook_name, _timeout_ms} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_hook_command_result({_output, 0}, _workspace, _issue_id, _hook_name) do
    :ok
  end

  defp handle_hook_command_result({output, status}, workspace, issue_context, hook_name) do
    sanitized_output = sanitize_hook_output_for_log(output)

    Logger.warning("Workspace hook failed hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} status=#{status} output=#{inspect(sanitized_output)}")

    {:error, {:workspace_hook_failed, hook_name, status, output}}
  end

  defp sanitize_hook_output_for_log(output, max_bytes \\ 2_048) do
    binary_output = IO.iodata_to_binary(output)

    case byte_size(binary_output) <= max_bytes do
      true ->
        binary_output

      false ->
        binary_part(binary_output, 0, max_bytes) <> "... (truncated)"
    end
  end

  defp validate_workspace_path(workspace, nil) when is_binary(workspace) do
    validate_local_workspace_path(workspace, Config.local_workspace_root())
  end

  defp validate_workspace_path(workspace, worker_host)
       when is_binary(workspace) and is_binary(worker_host) do
    cond do
      String.trim(workspace) == "" ->
        {:error, {:workspace_path_unreadable, workspace, :empty}}

      String.contains?(workspace, ["\n", "\r", <<0>>]) ->
        {:error, {:workspace_path_unreadable, workspace, :invalid_characters}}

      true ->
        :ok
    end
  end

  defp validate_local_workspace_path(workspace, root) do
    expanded_workspace = Path.expand(workspace)
    expanded_root = Path.expand(root)
    expanded_root_prefix = expanded_root <> "/"

    with {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:workspace_equals_root, canonical_workspace, canonical_root}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          :ok

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:workspace_symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:workspace_outside_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:workspace_path_unreadable, path, reason}}
    end
  end

  defp remote_shell_assign(variable_name, raw_path)
       when is_binary(variable_name) and is_binary(raw_path) do
    [
      "#{variable_name}=#{shell_escape(raw_path)}",
      "case \"$#{variable_name}\" in",
      "  '~') #{variable_name}=\"$HOME\" ;;",
      "  '~/'*) " <> variable_name <> "=\"$HOME/${" <> variable_name <> "#~/}\" ;;",
      "esac"
    ]
    |> Enum.join("\n")
  end

  defp parse_remote_workspace_output(output) do
    lines = String.split(IO.iodata_to_binary(output), "\n", trim: true)

    payload =
      Enum.find_value(lines, fn line ->
        case String.split(line, "\t", parts: 3) do
          [@remote_workspace_marker, created, path] when created in ["0", "1"] and path != "" ->
            {created == "1", path}

          _ ->
            nil
        end
      end)

    case payload do
      {created?, workspace} when is_boolean(created?) and is_binary(workspace) ->
        {:ok, workspace, created?}

      _ ->
        {:error, {:workspace_prepare_failed, :invalid_output, output}}
    end
  end

  defp run_remote_command(worker_host, script, timeout_ms)
       when is_binary(worker_host) and is_binary(script) and is_integer(timeout_ms) and timeout_ms > 0 do
    task =
      Task.async(fn ->
        SSH.run(worker_host, script, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:error, {:workspace_hook_timeout, "remote_command", timeout_ms}}
    end
  end

  defp workspace_env do
    Config.settings!().workspace.env
    |> normalize_env_map()
  end

  defp normalize_env_map(env) when is_map(env) do
    Enum.flat_map(env, fn
      {key, value} when is_binary(key) and is_binary(value) -> [{key, value}]
      {key, value} when is_binary(key) -> [{key, to_string(value)}]
      _ -> []
    end)
  end

  defp normalize_env_map(_env), do: []

  defp remote_env_exports do
    case workspace_env() do
      [] ->
        ""

      env ->
        Enum.map_join(env, "\n", fn {key, value} -> "export #{key}=#{shell_escape(value)}" end) <> "\n"
    end
  end

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    %{
      issue_id: issue_id,
      issue_identifier: identifier || "issue"
    }
  end

  defp issue_context(identifier) when is_binary(identifier) do
    %{
      issue_id: nil,
      issue_identifier: identifier
    }
  end

  defp issue_context(_identifier) do
    %{
      issue_id: nil,
      issue_identifier: "issue"
    }
  end

  defp issue_log_context(%{issue_id: issue_id, issue_identifier: issue_identifier}) do
    "issue_id=#{issue_id || "n/a"} issue_identifier=#{issue_identifier || "issue"}"
  end
end
