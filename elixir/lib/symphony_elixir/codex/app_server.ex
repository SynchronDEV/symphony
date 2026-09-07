defmodule SymphonyElixir.Codex.AppServer do
  @moduledoc """
  Minimal client for the Codex app-server JSON-RPC 2.0 stream over stdio.
  """

  require Logger
  alias SymphonyElixir.{Codex.DynamicTool, Config, PathSafety, SSH}

  @initialize_id 1
  @thread_start_id 2
  @port_line_bytes 1_048_576
  @max_stream_log_bytes 1_000

  @type session :: %{
          port: port(),
          request_counter: :atomics.atomics_ref(),
          metadata: map(),
          approval_policy: String.t() | map(),
          auto_approve_requests: boolean(),
          thread_sandbox: String.t(),
          turn_sandbox_policy: map(),
          elicitation_policy: String.t(),
          thread_id: String.t(),
          workspace: Path.t(),
          worker_host: String.t() | nil
        }

  @spec run(Path.t(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(workspace, prompt, issue, opts \\ []) do
    with {:ok, session} <- start_session(workspace, opts) do
      try do
        run_turn(session, prompt, issue, opts)
      after
        stop_session(session)
      end
    end
  end

  @spec start_session(Path.t(), keyword()) :: {:ok, session()} | {:error, term()}
  def start_session(workspace, opts \\ []) do
    worker_host = Keyword.get(opts, :worker_host)

    with {:ok, expanded_workspace} <- validate_workspace_cwd(workspace, worker_host),
         {:ok, port} <- start_port(expanded_workspace, worker_host) do
      metadata = port_metadata(port, worker_host)

      with {:ok, session_policies} <- session_policies(expanded_workspace, worker_host),
           {:ok, thread_id} <- do_start_session(port, expanded_workspace, session_policies) do
        {:ok,
         %{
           port: port,
           request_counter: :atomics.new(1, []),
           metadata: metadata,
           approval_policy: session_policies.approval_policy,
           auto_approve_requests: session_policies.approval_policy == "never",
           thread_sandbox: session_policies.thread_sandbox,
           turn_sandbox_policy: session_policies.turn_sandbox_policy,
           elicitation_policy: session_policies.elicitation_policy,
           thread_id: thread_id,
           workspace: expanded_workspace,
           worker_host: worker_host
         }}
      else
        {:error, reason} ->
          stop_port(port)
          {:error, reason}
      end
    end
  end

  @spec run_turn(session(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_turn(
        %{metadata: metadata, thread_id: thread_id} = session,
        prompt,
        issue,
        opts \\ []
      ) do
    on_message = Keyword.get(opts, :on_message, &default_on_message/1)

    tool_executor =
      Keyword.get(opts, :tool_executor, fn tool, arguments ->
        DynamicTool.execute(tool, arguments)
      end)

    case start_turn(session, prompt, issue) do
      {:ok, turn_id} ->
        session_id = "#{thread_id}-#{turn_id}"
        Logger.info("Codex session started for #{issue_context(issue)} session_id=#{session_id}")

        emit_message(
          on_message,
          :session_started,
          %{
            session_id: session_id,
            thread_id: thread_id,
            turn_id: turn_id
          },
          metadata
        )

        case await_turn_completion(session, on_message, tool_executor, turn_id) do
          {:ok, result} ->
            Logger.info("Codex session completed for #{issue_context(issue)} session_id=#{session_id}")

            {:ok,
             %{
               result: result,
               session_id: session_id,
               thread_id: thread_id,
               turn_id: turn_id
             }}

          {:error, reason} ->
            Logger.warning("Codex session ended with error for #{issue_context(issue)} session_id=#{session_id}: #{inspect(reason)}")

            emit_message(
              on_message,
              error_event(reason),
              %{
                session_id: session_id,
                reason: reason
              },
              metadata
            )

            turn_error(reason, turn_id)
        end

      {:error, reason} ->
        Logger.error("Codex session failed for #{issue_context(issue)}: #{inspect(reason)}")
        emit_message(on_message, :startup_failed, %{reason: reason}, metadata)
        {:error, reason}
    end
  end

  defp turn_error(reason, turn_id) when reason in [:turn_timeout, :stall_timeout], do: {:error, {reason, turn_id}}
  defp turn_error(reason, _turn_id), do: {:error, reason}

  defp error_event({:turn_input_required, _}), do: :turn_input_required
  defp error_event({:approval_required, _}), do: :approval_required
  defp error_event(_), do: :turn_ended_with_error

  @spec stop_session(session()) :: :ok
  def stop_session(%{port: port}) when is_port(port) do
    stop_port(port)
  end

  @spec interrupt_turn(session(), String.t()) :: :ok | {:error, term()}
  def interrupt_turn(%{port: port, thread_id: thread_id} = session, turn_id) do
    request_id = next_request_id(session)

    send_message(port, %{
      "method" => "turn/interrupt",
      "id" => request_id,
      "params" => %{"threadId" => thread_id, "turnId" => turn_id}
    })

    deadline = System.monotonic_time(:millisecond) + Config.settings!().codex.read_timeout_ms
    await_interrupt(port, request_id, %{thread_id: thread_id, turn_id: turn_id, deadline: deadline}, false, false, "")
  end

  defp next_request_id(%{request_counter: counter}), do: :atomics.add_get(counter, 1, 1) + 2

  # A terminal event can precede the acknowledgement. Both are required before reuse.
  defp await_interrupt(_port, _request_id, _active, true, true, _pending), do: :ok

  defp await_interrupt(port, request_id, active, acknowledged, terminated, pending) do
    receive do
      {^port, {:data, {:eol, chunk}}} ->
        case Jason.decode(pending <> to_string(chunk)) do
          {:ok, %{"id" => ^request_id, "error" => error}} ->
            {:error, {:interrupt_refused, error}}

          {:ok, %{"id" => ^request_id, "result" => result}} when is_map(result) ->
            await_interrupt(port, request_id, active, true, terminated, "")

          {:ok, %{"method" => "turn/completed"} = payload} ->
            terminal =
              matching_turn?(payload, active) and
                get_in(payload, ["params", "turn", "status"]) in ["completed", "failed", "interrupted"]

            await_interrupt(port, request_id, active, acknowledged, terminated or terminal, "")

          _ ->
            await_interrupt(port, request_id, active, acknowledged, terminated, "")
        end

      {^port, {:data, {:noeol, chunk}}} ->
        await_interrupt(port, request_id, active, acknowledged, terminated, pending <> to_string(chunk))

      {^port, {:exit_status, status}} ->
        {:error, {:port_exit, status}}
    after
      max(active.deadline - System.monotonic_time(:millisecond), 0) -> {:error, :interrupt_timeout}
    end
  end

  defp validate_workspace_cwd(workspace, nil) when is_binary(workspace) do
    expanded_workspace = Path.expand(workspace)
    expanded_root = Config.local_workspace_root()
    expanded_root_prefix = expanded_root <> "/"

    with {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:invalid_workspace_cwd, :workspace_root, canonical_workspace}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          {:ok, canonical_workspace}

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:invalid_workspace_cwd, :symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:invalid_workspace_cwd, :outside_workspace_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:invalid_workspace_cwd, :path_unreadable, path, reason}}
    end
  end

  defp validate_workspace_cwd(workspace, worker_host)
       when is_binary(workspace) and is_binary(worker_host) do
    cond do
      String.trim(workspace) == "" ->
        {:error, {:invalid_workspace_cwd, :empty_remote_workspace, worker_host}}

      String.contains?(workspace, ["\n", "\r", <<0>>]) ->
        {:error, {:invalid_workspace_cwd, :invalid_remote_workspace, worker_host, workspace}}

      true ->
        {:ok, workspace}
    end
  end

  defp start_port(workspace, nil) do
    executable = System.find_executable("bash")

    if is_nil(executable) do
      {:error, :bash_not_found}
    else
      port =
        Port.open(
          {:spawn_executable, String.to_charlist(executable)},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: [~c"-lc", String.to_charlist(Config.settings!().codex.command)],
            cd: String.to_charlist(workspace),
            line: @port_line_bytes
          ]
        )

      {:ok, port}
    end
  end

  defp start_port(workspace, worker_host) when is_binary(worker_host) do
    remote_command = remote_launch_command(workspace)
    SSH.start_port(worker_host, remote_command, line: @port_line_bytes)
  end

  defp remote_launch_command(workspace) when is_binary(workspace) do
    [
      "cd #{shell_escape(workspace)}",
      "exec #{Config.settings!().codex.command}"
    ]
    |> Enum.join(" && ")
  end

  defp port_metadata(port, worker_host) when is_port(port) do
    base_metadata =
      case :erlang.port_info(port, :os_pid) do
        {:os_pid, os_pid} ->
          %{codex_app_server_pid: to_string(os_pid)}

        _ ->
          %{}
      end

    case worker_host do
      host when is_binary(host) -> Map.put(base_metadata, :worker_host, host)
      _ -> base_metadata
    end
  end

  defp send_initialize(port) do
    payload = %{
      "method" => "initialize",
      "id" => @initialize_id,
      "params" => %{
        "capabilities" => %{
          "experimentalApi" => true
        },
        "clientInfo" => %{
          "name" => "symphony-orchestrator",
          "title" => "Symphony Orchestrator",
          "version" => "0.1.0"
        }
      }
    }

    send_message(port, payload)

    with {:ok, _} <- await_startup_response(port, @initialize_id) do
      send_message(port, %{"method" => "initialized", "params" => %{}})
      :ok
    end
  end

  defp session_policies(workspace, nil) do
    Config.codex_runtime_settings(workspace)
  end

  defp session_policies(workspace, worker_host) when is_binary(worker_host) do
    Config.codex_runtime_settings(workspace, remote: true)
  end

  defp do_start_session(port, workspace, session_policies) do
    case send_initialize(port) do
      :ok -> start_thread(port, workspace, session_policies)
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_thread(port, workspace, %{approval_policy: approval_policy, thread_sandbox: thread_sandbox}) do
    send_message(port, %{
      "method" => "thread/start",
      "id" => @thread_start_id,
      "params" => %{
        "approvalPolicy" => approval_policy,
        "sandbox" => thread_sandbox,
        "cwd" => workspace,
        "dynamicTools" => DynamicTool.tool_specs()
      }
    })

    case await_startup_response(port, @thread_start_id) do
      {:ok, %{"thread" => thread_payload}} ->
        case thread_payload do
          %{"id" => thread_id} -> {:ok, thread_id}
          _ -> {:error, {:invalid_thread_payload, thread_payload}}
        end

      other ->
        other
    end
  end

  defp start_turn(session, prompt, issue) do
    request_id = next_request_id(session)
    port = session.port

    send_message(port, %{
      "method" => "turn/start",
      "id" => request_id,
      "params" => %{
        "threadId" => session.thread_id,
        "input" => [
          %{
            "type" => "text",
            "text" => prompt
          }
        ],
        "cwd" => session.workspace,
        "title" => "#{issue.identifier}: #{issue.title}",
        "approvalPolicy" => session.approval_policy,
        "sandboxPolicy" => session.turn_sandbox_policy
      }
    })

    case await_response(port, request_id) do
      {:ok, %{"turn" => %{"id" => turn_id}}} when is_binary(turn_id) -> {:ok, turn_id}
      {:ok, payload} -> {:error, {:invalid_turn_payload, payload}}
      other -> other
    end
  end

  defp await_turn_completion(session, on_message, tool_executor, turn_id) do
    timeout_budget = Map.merge(turn_timeout_budget(), %{thread_id: session.thread_id, turn_id: turn_id})

    receive_loop(
      session.port,
      on_message,
      timeout_budget,
      "",
      tool_executor,
      session.auto_approve_requests,
      session.elicitation_policy
    )
  end

  defp receive_loop(port, on_message, timeout_budget, pending_line, tool_executor, auto_approve_requests, elicitation_policy) do
    receive do
      {^port, {:data, {:eol, chunk}}} ->
        complete_line = pending_line <> to_string(chunk)

        handle_incoming(
          port,
          on_message,
          complete_line,
          timeout_budget,
          tool_executor,
          auto_approve_requests,
          elicitation_policy
        )

      {^port, {:data, {:noeol, chunk}}} ->
        receive_loop(
          port,
          on_message,
          timeout_budget,
          pending_line <> to_string(chunk),
          tool_executor,
          auto_approve_requests,
          elicitation_policy
        )

      {^port, {:exit_status, status}} ->
        {:error, {:port_exit, status}}
    after
      timeout_budget.timeout_ms ->
        {:error, timeout_budget.reason}
    end
  end

  defp handle_incoming(port, on_message, data, timeout_budget, tool_executor, auto_approve_requests, elicitation_policy) do
    stream = %{
      port: port,
      on_message: on_message,
      timeout_budget: timeout_budget,
      tool_executor: tool_executor,
      auto_approve_requests: auto_approve_requests,
      elicitation_policy: elicitation_policy
    }

    payload_string = to_string(data)
    handle_decoded_message(Jason.decode(payload_string), payload_string, stream)
  end

  defp handle_decoded_message({:ok, %{"method" => "error", "params" => %{"willRetry" => true}} = payload}, raw, stream) do
    if matching_turn?(payload, stream.timeout_budget) do
      emit_turn_event(stream.on_message, :notification, payload, raw, stream.port, payload["params"])
    end

    continue_turn_stream(stream)
  end

  defp handle_decoded_message({:ok, %{"method" => method} = payload}, raw, stream)
       when method in ["turn/completed", "turn/failed", "turn/cancelled", "error"] do
    if matching_turn?(payload, stream.timeout_budget) do
      result = terminal_result(method, payload)
      emit_turn_event(stream.on_message, terminal_event(result), payload, raw, stream.port, payload["params"])
      result
    else
      continue_turn_stream(stream)
    end
  end

  defp handle_decoded_message({:ok, %{"method" => method} = payload}, raw, stream) when is_binary(method) do
    if foreign_turn?(payload, stream.timeout_budget) do
      continue_turn_stream(stream)
    else
      handle_turn_method(
        stream.port,
        stream.on_message,
        payload,
        raw,
        method,
        stream.timeout_budget,
        stream.tool_executor,
        stream.auto_approve_requests,
        stream.elicitation_policy
      )
    end
  end

  defp handle_decoded_message({:ok, payload}, raw, stream) do
    metadata = metadata_from_message(stream.port, payload)
    emit_message(stream.on_message, :other_message, %{payload: payload, raw: raw}, metadata)
    continue_turn_stream(stream)
  end

  defp handle_decoded_message({:error, _reason}, raw, stream) do
    log_non_json_stream_line(raw, "turn stream")
    details = %{payload: raw, raw: raw}
    metadata = metadata_from_message(stream.port, %{raw: raw})
    emit_message(stream.on_message, :stream_output, details, metadata)

    if protocol_message_candidate?(raw) do
      emit_message(stream.on_message, :malformed, details, metadata)
    end

    continue_turn_stream(stream)
  end

  defp terminal_event({:ok, _}), do: :turn_completed
  defp terminal_event(_), do: :turn_failed

  defp continue_turn_stream(stream) do
    receive_loop(
      stream.port,
      stream.on_message,
      stream.timeout_budget,
      "",
      stream.tool_executor,
      stream.auto_approve_requests,
      stream.elicitation_policy
    )
  end

  defp foreign_turn?(%{"params" => params}, active) when is_map(params) do
    (is_binary(params["threadId"]) and params["threadId"] != active.thread_id) or
      (is_binary(params["turnId"]) and params["turnId"] != active.turn_id)
  end

  defp foreign_turn?(_, _), do: false

  defp matching_turn?(%{"params" => params}, %{thread_id: thread_id, turn_id: turn_id}) when is_map(params) do
    params["threadId"] == thread_id and
      (params["turnId"] || get_in(params, ["turn", "id"])) == turn_id
  end

  defp matching_turn?(_, _), do: false

  defp terminal_result("turn/completed", %{"params" => %{"turn" => turn}}) do
    case turn do
      %{"status" => "completed", "error" => nil} -> {:ok, :turn_completed}
      %{"status" => "completed", "error" => error} when not is_nil(error) -> {:error, {:turn_failed, turn}}
      %{"status" => "failed"} -> {:error, {:turn_failed, turn}}
      %{"status" => "interrupted"} -> {:error, {:turn_interrupted, turn}}
      _ -> {:error, {:invalid_terminal_turn, turn}}
    end
  end

  defp terminal_result("turn/failed", payload), do: {:error, {:turn_failed, payload["params"]}}
  defp terminal_result("turn/cancelled", payload), do: {:error, {:turn_cancelled, payload["params"]}}
  defp terminal_result("error", payload), do: {:error, {:turn_failed, payload["params"]}}
  defp terminal_result(_, payload), do: {:error, {:invalid_terminal_turn, payload}}

  defp turn_timeout_budget do
    settings = Config.settings!().codex
    turn_timeout_ms = settings.turn_timeout_ms
    stall_timeout_ms = settings.stall_timeout_ms

    if is_integer(stall_timeout_ms) and stall_timeout_ms > 0 and stall_timeout_ms < turn_timeout_ms do
      %{timeout_ms: stall_timeout_ms, reason: :stall_timeout}
    else
      %{timeout_ms: turn_timeout_ms, reason: :turn_timeout}
    end
  end

  defp emit_turn_event(on_message, event, payload, payload_string, port, payload_details) do
    emit_message(
      on_message,
      event,
      %{
        payload: payload,
        raw: payload_string,
        details: payload_details
      },
      metadata_from_message(port, payload)
    )
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  defp handle_turn_method(
         port,
         on_message,
         payload,
         payload_string,
         method,
         timeout_budget,
         tool_executor,
         auto_approve_requests,
         elicitation_policy
       ) do
    metadata = metadata_from_message(port, payload)

    case maybe_handle_approval_request(
           port,
           method,
           payload,
           payload_string,
           on_message,
           metadata,
           tool_executor,
           auto_approve_requests,
           elicitation_policy
         ) do
      :input_required ->
        emit_message(
          on_message,
          :turn_input_required,
          %{payload: payload, raw: payload_string},
          metadata
        )

        {:error, {:turn_input_required, payload}}

      :approved ->
        receive_loop(port, on_message, timeout_budget, "", tool_executor, auto_approve_requests, elicitation_policy)

      :approval_required ->
        emit_message(
          on_message,
          :approval_required,
          %{payload: payload, raw: payload_string},
          metadata
        )

        {:error, {:approval_required, payload}}

      :unhandled ->
        if needs_input?(method, payload) do
          emit_message(
            on_message,
            :turn_input_required,
            %{payload: payload, raw: payload_string},
            metadata
          )

          {:error, {:turn_input_required, payload}}
        else
          emit_message(
            on_message,
            :notification,
            %{
              payload: payload,
              raw: payload_string
            },
            metadata
          )

          Logger.debug("Codex notification: #{inspect(method)}")
          receive_loop(port, on_message, timeout_budget, "", tool_executor, auto_approve_requests, elicitation_policy)
        end
    end
  end

  # credo:disable-for-lines:300 Credo.Check.Refactor.FunctionArity
  defp maybe_handle_approval_request(
         port,
         "item/commandExecution/requestApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests,
         _elicitation_policy
       ) do
    approve_or_require(
      port,
      id,
      "acceptForSession",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/tool/call",
         %{"id" => id, "params" => params} = payload,
         payload_string,
         on_message,
         metadata,
         tool_executor,
         _auto_approve_requests,
         _elicitation_policy
       ) do
    tool_name = tool_call_name(params)
    arguments = tool_call_arguments(params)

    result =
      tool_name
      |> tool_executor.(arguments)
      |> normalize_dynamic_tool_result()

    send_message(port, %{
      "id" => id,
      "result" => result
    })

    event =
      case result do
        %{"success" => true} -> :tool_call_completed
        _ when is_nil(tool_name) -> :unsupported_tool_call
        _ -> :tool_call_failed
      end

    emit_message(on_message, event, %{payload: payload, raw: payload_string}, metadata)

    :approved
  end

  defp maybe_handle_approval_request(
         port,
         "execCommandApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests,
         _elicitation_policy
       ) do
    approve_or_require(
      port,
      id,
      "approved_for_session",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "applyPatchApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests,
         _elicitation_policy
       ) do
    approve_or_require(
      port,
      id,
      "approved_for_session",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/fileChange/requestApproval",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests,
         _elicitation_policy
       ) do
    approve_or_require(
      port,
      id,
      "acceptForSession",
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  defp maybe_handle_approval_request(
         port,
         "item/tool/requestUserInput",
         %{"id" => id, "params" => params} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         auto_approve_requests,
         _elicitation_policy
       ) do
    maybe_auto_answer_tool_request_user_input(
      port,
      id,
      params,
      payload,
      payload_string,
      on_message,
      metadata,
      auto_approve_requests
    )
  end

  # Auto-decline MCP elicitation requests. Headless Symphony agents cannot
  # answer interactive elicitation prompts, so without this they block forever
  # (EVENT stays at mcpServer/elicitation/request, tokens flatline) until a
  # human rescues them. Declining keeps the agent's turn alive so it proceeds
  # without the requested input rather than wedging.
  defp maybe_handle_approval_request(
         port,
         "mcpServer/elicitation/request",
         %{"id" => id} = payload,
         payload_string,
         on_message,
         metadata,
         _tool_executor,
         _auto_approve_requests,
         "decline"
       ) do
    send_message(port, %{"id" => id, "result" => %{"action" => "decline"}})

    emit_message(
      on_message,
      :elicitation_auto_declined,
      %{payload: payload, raw: payload_string},
      metadata
    )

    :approved
  end

  defp maybe_handle_approval_request(
         _port,
         "mcpServer/elicitation/request",
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         _tool_executor,
         _auto_approve_requests,
         "block"
       ) do
    :input_required
  end

  defp maybe_handle_approval_request(
         _port,
         _method,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         _tool_executor,
         _auto_approve_requests,
         _elicitation_policy
       ) do
    :unhandled
  end

  defp normalize_dynamic_tool_result(%{"success" => success} = result) when is_boolean(success) do
    output =
      case Map.get(result, "output") do
        existing_output when is_binary(existing_output) -> existing_output
        _ -> dynamic_tool_output(result)
      end

    content_items =
      case Map.get(result, "contentItems") do
        existing_items when is_list(existing_items) -> existing_items
        _ -> dynamic_tool_content_items(output)
      end

    result
    |> Map.put("output", output)
    |> Map.put("contentItems", content_items)
  end

  defp normalize_dynamic_tool_result(result) do
    %{
      "success" => false,
      "output" => inspect(result),
      "contentItems" => dynamic_tool_content_items(inspect(result))
    }
  end

  defp dynamic_tool_output(%{"contentItems" => [%{"text" => text} | _]}) when is_binary(text), do: text
  defp dynamic_tool_output(result), do: Jason.encode!(result, pretty: true)

  defp dynamic_tool_content_items(output) when is_binary(output) do
    [
      %{
        "type" => "inputText",
        "text" => output
      }
    ]
  end

  defp approve_or_require(
         port,
         id,
         decision,
         payload,
         payload_string,
         on_message,
         metadata,
         true
       ) do
    send_message(port, %{"id" => id, "result" => %{"decision" => decision}})

    emit_message(
      on_message,
      :approval_auto_approved,
      %{payload: payload, raw: payload_string, decision: decision},
      metadata
    )

    :approved
  end

  defp approve_or_require(
         _port,
         _id,
         _decision,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         false
       ) do
    :approval_required
  end

  defp maybe_auto_answer_tool_request_user_input(
         port,
         id,
         params,
         payload,
         payload_string,
         on_message,
         metadata,
         true
       ) do
    case tool_request_user_input_approval_answers(params) do
      {:ok, answers, decision} ->
        send_message(port, %{"id" => id, "result" => %{"answers" => answers}})

        emit_message(
          on_message,
          :approval_auto_approved,
          %{payload: payload, raw: payload_string, decision: decision},
          metadata
        )

        :approved

      :error ->
        :input_required
    end
  end

  defp maybe_auto_answer_tool_request_user_input(
         _port,
         _id,
         _params,
         _payload,
         _payload_string,
         _on_message,
         _metadata,
         false
       ),
       do: :input_required

  defp tool_request_user_input_approval_answers(%{"questions" => questions}) when is_list(questions) do
    answers =
      Enum.reduce_while(questions, %{}, fn question, acc ->
        case tool_request_user_input_approval_answer(question) do
          {:ok, question_id, answer_label} ->
            {:cont, Map.put(acc, question_id, %{"answers" => [answer_label]})}

          :error ->
            {:halt, :error}
        end
      end)

    case answers do
      :error -> :error
      answer_map when map_size(answer_map) > 0 -> {:ok, answer_map, "Approve this Session"}
      _ -> :error
    end
  end

  defp tool_request_user_input_approval_answers(_params), do: :error

  defp tool_request_user_input_approval_answer(%{"id" => question_id, "options" => options})
       when is_binary(question_id) and is_list(options) do
    case if(String.starts_with?(question_id, "mcp_tool_call_approval_"),
           do: tool_request_user_input_approval_option_label(options)
         ) do
      nil -> :error
      answer_label -> {:ok, question_id, answer_label}
    end
  end

  defp tool_request_user_input_approval_answer(_question), do: :error

  defp tool_request_user_input_approval_option_label(options) do
    options
    |> Enum.map(&tool_request_user_input_option_label/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      labels ->
        Enum.find(labels, &(&1 == "Approve this Session")) ||
          Enum.find(labels, &(&1 == "Approve Once"))
    end
  end

  defp tool_request_user_input_option_label(%{"label" => label}) when is_binary(label), do: label
  defp tool_request_user_input_option_label(_option), do: nil

  defp await_response(port, request_id) do
    with_timeout_response(port, request_id, Config.settings!().codex.read_timeout_ms, "", [])
  end

  # Session-startup calls (initialize, thread/start) block on codex booting its
  # full MCP-server layer (npx cold-starts) before replying — measured at ~7s,
  # well past read_timeout_ms (5s). Give the handshake a longer budget so a cold
  # dispatch doesn't fail with :response_timeout before codex ever responds.
  defp await_startup_response(port, request_id) do
    with_timeout_response(port, request_id, Config.settings!().codex.startup_timeout_ms, "", :discard)
  end

  defp with_timeout_response(port, request_id, timeout_ms, pending_line, buffered) do
    receive do
      {^port, {:data, {:eol, chunk}}} ->
        complete_line = pending_line <> to_string(chunk)
        handle_response(port, request_id, complete_line, timeout_ms, buffered)

      {^port, {:data, {:noeol, chunk}}} ->
        with_timeout_response(port, request_id, timeout_ms, pending_line <> to_string(chunk), buffered)

      {^port, {:exit_status, status}} ->
        {:error, {:port_exit, status}}
    after
      timeout_ms ->
        {:error, :response_timeout}
    end
  end

  defp handle_response(port, request_id, data, timeout_ms, buffered) do
    payload = to_string(data)

    case Jason.decode(payload) do
      {:ok, %{"id" => ^request_id, "error" => error}} ->
        {:error, {:response_error, error}}

      {:ok, %{"id" => ^request_id, "result" => result}} ->
        replay_buffered_notifications(port, buffered)
        {:ok, result}

      {:ok, %{"id" => ^request_id} = response_payload} ->
        {:error, {:response_error, response_payload}}

      {:ok, %{"method" => _}} when is_list(buffered) ->
        with_timeout_response(port, request_id, timeout_ms, "", [data | buffered])

      {:ok, %{} = other} ->
        Logger.debug("Ignoring message while waiting for response: #{inspect(other)}")
        with_timeout_response(port, request_id, timeout_ms, "", buffered)

      {:error, _} ->
        log_non_json_stream_line(payload, "response stream")
        with_timeout_response(port, request_id, timeout_ms, "", buffered)
    end
  end

  defp replay_buffered_notifications(_port, :discard), do: :ok

  defp replay_buffered_notifications(port, buffered) do
    buffered
    |> Enum.reverse()
    |> Enum.each(fn data -> send(self(), {port, {:data, {:eol, data}}}) end)
  end

  defp log_non_json_stream_line(data, stream_label) do
    text =
      data
      |> to_string()
      |> String.trim()
      |> String.slice(0, @max_stream_log_bytes)

    if text != "" do
      if String.match?(text, ~r/\b(error|warn|warning|failed|fatal|panic|exception)\b/i) do
        Logger.warning("Codex #{stream_label} output: #{text}")
      else
        Logger.debug("Codex #{stream_label} output: #{text}")
      end
    end
  end

  defp protocol_message_candidate?(data) do
    data
    |> to_string()
    |> String.trim_leading()
    |> String.starts_with?("{")
  end

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp stop_port(port) when is_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        :ok

      _ ->
        try do
          Port.close(port)
          :ok
        rescue
          ArgumentError ->
            :ok
        end
    end
  end

  defp emit_message(on_message, event, details, metadata) when is_function(on_message, 1) do
    message = metadata |> Map.merge(details) |> Map.put(:event, event) |> Map.put(:timestamp, DateTime.utc_now())
    on_message.(message)
  end

  defp metadata_from_message(port, payload) do
    port |> port_metadata(nil) |> maybe_set_usage(payload)
  end

  defp maybe_set_usage(metadata, payload) when is_map(payload) do
    usage = Map.get(payload, "usage") || Map.get(payload, :usage)

    if is_map(usage) do
      Map.put(metadata, :usage, usage)
    else
      metadata
    end
  end

  defp maybe_set_usage(metadata, _payload), do: metadata

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp default_on_message(_message), do: :ok

  defp tool_call_name(params) when is_map(params) do
    case Map.get(params, "tool") || Map.get(params, :tool) || Map.get(params, "name") || Map.get(params, :name) do
      name when is_binary(name) ->
        case String.trim(name) do
          "" -> nil
          trimmed -> trimmed
        end

      _ ->
        nil
    end
  end

  defp tool_call_name(_params), do: nil

  defp tool_call_arguments(params) when is_map(params) do
    Map.get(params, "arguments") || Map.get(params, :arguments) || %{}
  end

  defp tool_call_arguments(_params), do: %{}

  defp send_message(port, message) do
    line = Jason.encode!(message) <> "\n"
    Port.command(port, line)
  end

  defp needs_input?("mcpServer/elicitation/request", payload) when is_map(payload), do: true

  defp needs_input?(method, payload)
       when is_binary(method) and is_map(payload) do
    String.starts_with?(method, "turn/") && input_required_method?(method, payload)
  end

  defp needs_input?(_method, _payload), do: false

  defp input_required_method?(method, payload) when is_binary(method) do
    method in [
      "turn/input_required",
      "turn/needs_input",
      "turn/need_input",
      "turn/request_input",
      "turn/request_response",
      "turn/provide_input",
      "turn/approval_required"
    ] || request_payload_requires_input?(payload)
  end

  defp request_payload_requires_input?(payload) do
    params = Map.get(payload, "params")
    needs_input_field?(payload) || needs_input_field?(params)
  end

  defp needs_input_field?(payload) when is_map(payload) do
    Map.get(payload, "requiresInput") == true or
      Map.get(payload, "needsInput") == true or
      Map.get(payload, "input_required") == true or
      Map.get(payload, "inputRequired") == true or
      Map.get(payload, "type") == "input_required" or
      Map.get(payload, "type") == "needs_input"
  end

  defp needs_input_field?(_payload), do: false
end
