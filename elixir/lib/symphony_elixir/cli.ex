defmodule SymphonyElixir.CLI do
  @moduledoc """
  Escript entrypoint for running Symphony with an explicit WORKFLOW.md path.
  """

  alias SymphonyElixir.{Config, Ledger, LogFile, PromptBuilder, Workflow}
  alias SymphonyElixir.Linear.Issue

  @acknowledgement_switch :i_understand_that_this_will_be_running_without_the_usual_guardrails
  @switches [{@acknowledgement_switch, :boolean}, logs_root: :string, port: :integer, preflight: :boolean]

  @type ensure_started_result :: {:ok, [atom()]} | {:error, term()}
  @type deps :: %{
          file_regular?: (String.t() -> boolean()),
          set_workflow_file_path: (String.t() -> :ok | {:error, term()}),
          set_logs_root: (String.t() -> :ok | {:error, term()}),
          set_server_port_override: (non_neg_integer() | nil -> :ok | {:error, term()}),
          ensure_all_started: (-> ensure_started_result())
        }

  @spec main([String.t()]) :: no_return()
  def main(args) do
    case evaluate(args) do
      :ok ->
        wait_for_shutdown()

      {:ok, :preflight} ->
        System.halt(0)

      {:error, message} ->
        IO.puts(:stderr, message)
        System.halt(1)
    end
  end

  @spec evaluate([String.t()], deps()) :: :ok | {:ok, :preflight} | {:error, String.t()}
  def evaluate(args, deps \\ runtime_deps()) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        evaluate_workflow(Path.expand("WORKFLOW.md"), opts, deps)

      {opts, [workflow_path], []} ->
        evaluate_workflow(workflow_path, opts, deps)

      _ ->
        {:error, usage_message()}
    end
  end

  defp evaluate_workflow(workflow_path, opts, deps) do
    if Keyword.get(opts, :preflight, false) do
      preflight(workflow_path)
    else
      with :ok <- require_guardrails_acknowledgement(opts),
           :ok <- maybe_set_logs_root(opts, deps),
           :ok <- maybe_set_server_port(opts, deps) do
        run(workflow_path, deps)
      end
    end
  end

  @doc "Validate the installed runtime and workflow without starting Symphony or dispatching work."
  @spec preflight(Path.t()) :: {:ok, :preflight} | {:error, String.t()}
  def preflight(workflow_path) do
    previous_path = Application.get_env(:symphony_elixir, :workflow_file_path)

    try do
      # YAML decoding needs its dependency application, never Symphony's supervisor.
      with {:ok, _} <- Application.ensure_all_started(:yaml_elixir),
           :ok <- Workflow.set_workflow_file_path(Path.expand(workflow_path)),
           :ok <- Config.validate!() do
        settings = Config.settings!()

        for state <- settings.tracker.active_states do
          PromptBuilder.build_prompt(%Issue{id: "preflight", identifier: "SPK-PREFLIGHT", title: "Read-only preflight", state: state})
        end

        IO.puts(
          Jason.encode!(%{
            preflight: "ok",
            workflow: Path.expand(workflow_path),
            workspace_root: Config.local_workspace_root(),
            ledger_path: Ledger.path_for_workflow(workflow_path),
            cleanup_base_ref: Map.get(settings.workspace, :cleanup_base_ref),
            tracker: %{
              kind: settings.tracker.kind,
              endpoint: settings.tracker.endpoint,
              project_slug: settings.tracker.project_slug,
              required_labels: settings.tracker.required_labels,
              active_states: settings.tracker.active_states,
              terminal_states: settings.tracker.terminal_states,
              credential_present: is_binary(settings.tracker.api_key) and settings.tracker.api_key != ""
            },
            agent: %{
              max_concurrent_agents: settings.agent.max_concurrent_agents,
              max_turns: settings.agent.max_turns,
              max_tokens_per_issue: settings.agent.max_tokens_per_issue,
              max_dispatch_attempts: settings.agent.max_dispatch_attempts,
              max_rework_cycles: settings.agent.max_rework_cycles,
              stop_continue_labels: settings.agent.stop_continue_labels
            },
            codex: %{
              approval_policy: settings.codex.approval_policy,
              thread_sandbox: settings.codex.thread_sandbox,
              turn_sandbox_policy: settings.codex.turn_sandbox_policy,
              permission_profile: Map.get(settings.codex, :permission_profile),
              command_sha256: :crypto.hash(:sha256, String.trim(settings.codex.command)) |> Base.encode16(case: :lower)
            },
            polling_interval_ms: settings.polling.interval_ms,
            runtime: %{
              elixir: System.version(),
              otp: System.otp_release(),
              recorded_cleanup: supported?(SymphonyElixir.Workspace, :remove_completed, 2),
              turn_interrupt: supported?(SymphonyElixir.Codex.AppServer, :interrupt_turn, 2)
            }
          })
        )

        {:ok, :preflight}
      else
        _ -> {:error, "Preflight failed: workflow or credentials are invalid. No agents were started."}
      end
    rescue
      _ -> {:error, "Preflight failed: runtime, workflow or prompt validation failed. No agents were started."}
    after
      if previous_path,
        do: Workflow.set_workflow_file_path(previous_path),
        else: Workflow.clear_workflow_file_path()
    end
  end

  defp supported?(module, function, arity),
    do: Code.ensure_loaded?(module) and function_exported?(module, function, arity)

  @spec run(String.t(), deps()) :: :ok | {:error, String.t()}
  def run(workflow_path, deps) do
    expanded_path = Path.expand(workflow_path)

    if deps.file_regular?.(expanded_path) do
      :ok = deps.set_workflow_file_path.(expanded_path)

      case deps.ensure_all_started.() do
        {:ok, _started_apps} ->
          :ok

        {:error, reason} ->
          {:error, "Failed to start Symphony with workflow #{expanded_path}: #{inspect(reason)}"}
      end
    else
      {:error, "Workflow file not found: #{expanded_path}"}
    end
  end

  @spec usage_message() :: String.t()
  defp usage_message do
    "Usage: symphony [--preflight] [--logs-root <path>] [--port <port>] [path-to-WORKFLOW.md]"
  end

  @spec runtime_deps() :: deps()
  defp runtime_deps do
    %{
      file_regular?: &File.regular?/1,
      set_workflow_file_path: &SymphonyElixir.Workflow.set_workflow_file_path/1,
      set_logs_root: &set_logs_root/1,
      set_server_port_override: &set_server_port_override/1,
      ensure_all_started: fn -> Application.ensure_all_started(:symphony_elixir) end
    }
  end

  defp maybe_set_logs_root(opts, deps) do
    case Keyword.get_values(opts, :logs_root) do
      [] ->
        :ok

      values ->
        logs_root = values |> List.last() |> String.trim()

        if logs_root == "" do
          {:error, usage_message()}
        else
          :ok = deps.set_logs_root.(Path.expand(logs_root))
        end
    end
  end

  defp require_guardrails_acknowledgement(opts) do
    if Keyword.get(opts, @acknowledgement_switch, false) do
      :ok
    else
      {:error, acknowledgement_banner()}
    end
  end

  @spec acknowledgement_banner() :: String.t()
  defp acknowledgement_banner do
    lines = [
      "This Symphony implementation is a low key engineering preview.",
      "Codex will run without any guardrails.",
      "SymphonyElixir is not a supported product and is presented as-is.",
      "To proceed, start with `--i-understand-that-this-will-be-running-without-the-usual-guardrails` CLI argument"
    ]

    width = Enum.max(Enum.map(lines, &String.length/1))
    border = String.duplicate("─", width + 2)
    top = "╭" <> border <> "╮"
    bottom = "╰" <> border <> "╯"
    spacer = "│ " <> String.duplicate(" ", width) <> " │"

    content =
      [
        top,
        spacer
        | Enum.map(lines, fn line ->
            "│ " <> String.pad_trailing(line, width) <> " │"
          end)
      ] ++ [spacer, bottom]

    [
      IO.ANSI.red(),
      IO.ANSI.bright(),
      Enum.join(content, "\n"),
      IO.ANSI.reset()
    ]
    |> IO.iodata_to_binary()
  end

  defp set_logs_root(logs_root) do
    Application.put_env(:symphony_elixir, :log_file, LogFile.default_log_file(logs_root))
    :ok
  end

  defp maybe_set_server_port(opts, deps) do
    case Keyword.get_values(opts, :port) do
      [] ->
        :ok

      values ->
        port = List.last(values)

        if is_integer(port) and port >= 0 do
          :ok = deps.set_server_port_override.(port)
        else
          {:error, usage_message()}
        end
    end
  end

  defp set_server_port_override(port) when is_integer(port) and port >= 0 do
    Application.put_env(:symphony_elixir, :server_port_override, port)
    :ok
  end

  @spec wait_for_shutdown() :: no_return()
  defp wait_for_shutdown do
    case Process.whereis(SymphonyElixir.Supervisor) do
      nil ->
        IO.puts(:stderr, "Symphony supervisor is not running")
        System.halt(1)

      pid ->
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, reason} ->
            case reason do
              :normal -> System.halt(0)
              _ -> System.halt(1)
            end
        end
    end
  end
end
