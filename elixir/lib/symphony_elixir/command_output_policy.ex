defmodule SymphonyElixir.CommandOutputPolicy do
  @moduledoc """
  Classifies and compacts noisy agent command output for observability.

  Codex still owns command execution and decides what is placed in the model
  context. This module keeps Symphony's own status surfaces concise and gives
  agents stable prompt guidance for reducing future spend.
  """

  @type command_class :: :ci_watch | :ci_poll | :test | :build | :git | :unknown

  @guardrails """

  ## Symphony Runtime Efficiency Guardrails

  - Do not use `gh run watch`; poll CI sparsely with `gh pr checks` or `gh run view --json` every 30-60 seconds and report only changed statuses.
  - For long validations, redirect full output to a log file and print only a compact summary plus the final 80-120 lines on failure.
  - Keep passing test/build output quiet. Preserve full logs as files or artifacts, and cite the command, exit status, and log path.
  - Use compact reporters for Vitest, Playwright, and build commands when available.
  - Do not let proof images or `history/*.png` files expand affected-test scope unless source/test files require that validation.
  """

  @spec prompt_guardrails() :: String.t()
  def prompt_guardrails, do: @guardrails

  @spec classify_command(term()) :: command_class()
  def classify_command(command) do
    command
    |> normalize_text()
    |> do_classify_command()
  end

  @spec compact_codex_update(map()) :: map()
  def compact_codex_update(%{payload: %{} = payload} = update) do
    compacted_payload = compact_payload(payload)

    update
    |> Map.put(:payload, compacted_payload)
    |> maybe_compact_raw(compacted_payload)
  end

  def compact_codex_update(update), do: update

  @spec compact_output_delta(term()) :: String.t() | nil
  def compact_output_delta(delta) do
    delta
    |> normalize_text()
    |> summarize_output_text()
  end

  @spec compact_command_status(term()) :: String.t() | nil
  def compact_command_status(command) do
    normalized = normalize_text(command)

    case do_classify_command(normalized) do
      :ci_watch -> "ci watch started: use sparse polling instead of streaming `gh run watch`"
      :ci_poll -> "ci status poll: #{single_line(normalized)}"
      _ -> nil
    end
  end

  defp compact_payload(%{"method" => method} = payload) when is_binary(method) do
    case method do
      "item/commandExecution/outputDelta" ->
        compact_delta_payload(payload, ["params", "outputDelta"])

      "codex/event/exec_command_output_delta" ->
        compact_wrapper_delta_payload(payload)

      "codex/event/exec_command_begin" ->
        compact_exec_command_begin(payload)

      _ ->
        payload
    end
  end

  defp compact_payload(payload), do: payload

  defp compact_delta_payload(payload, path) do
    case get_in(payload, path) do
      delta when is_binary(delta) ->
        case compact_output_delta(delta) do
          nil -> payload
          compacted -> put_in(payload, path, compacted)
        end

      _ ->
        payload
    end
  end

  defp compact_wrapper_delta_payload(payload) do
    compact_delta_payload(payload, ["params", "msg", "delta"])
    |> compact_delta_payload(["params", "msg", "output"])
    |> compact_delta_payload(["params", "msg", "payload", "delta"])
  end

  defp compact_exec_command_begin(payload) do
    command =
      get_in(payload, ["params", "msg", "command"]) ||
        get_in(payload, ["params", "msg", "parsed_cmd"])

    case compact_command_status(command) do
      nil -> payload
      compacted -> put_in(payload, ["params", "msg", "command"], compacted)
    end
  end

  defp maybe_compact_raw(update, compacted_payload) do
    case Jason.encode(compacted_payload) do
      {:ok, encoded} -> Map.put(update, :raw, encoded)
      {:error, _reason} -> update
    end
  end

  defp summarize_output_text(""), do: nil

  defp summarize_output_text(text) do
    cond do
      ci_watch_output?(text) ->
        "ci watch output suppressed: GitHub Actions status table is streaming; use sparse polling/deltas"

      playwright_smoke_output?(text) ->
        "playwright smoke output: #{tail_summary(text)}"

      browser_test_output?(text) ->
        "browser test output: #{tail_summary(text)}"

      webserver_noise?(text) ->
        "webserver validation noise: #{tail_summary(text)}"

      repeated_warning?(text) ->
        "repeated validation warning: #{tail_summary(text)}"

      true ->
        nil
    end
  end

  defp do_classify_command(""), do: :unknown

  defp do_classify_command(command) do
    command_classifiers()
    |> Enum.find_value(:unknown, fn {class, matcher} ->
      if matcher.(command), do: class
    end)
  end

  defp command_classifiers do
    [
      {:ci_watch, &String.contains?(&1, "gh run watch")},
      {:ci_poll, &String.contains?(&1, "gh pr checks")},
      {:ci_poll, &String.contains?(&1, "gh run view")},
      {:test, &String.contains?(&1, "playwright test")},
      {:test, &String.contains?(&1, "vitest")},
      {:test, &String.contains?(&1, "bun run test")},
      {:test, &String.contains?(&1, "mix test")},
      {:build, &String.contains?(&1, "react-router build")},
      {:build, &String.contains?(&1, "build:ci")},
      {:build, &String.contains?(&1, "docker build")},
      {:git, &String.starts_with?(&1, "git ")}
    ]
  end

  defp ci_watch_output?(text) do
    String.contains?(text, "Refreshing run status every") or
      (String.contains?(text, "\nJOBS\n") and String.contains?(text, "github.com"))
  end

  defp playwright_smoke_output?(text) do
    String.contains?(text, "playwright test") or
      (String.contains?(text, "Running ") and String.contains?(text, " tests using "))
  end

  defp browser_test_output?(text) do
    String.contains?(text, "|browser") or String.contains?(text, "[browser]")
  end

  defp webserver_noise?(text) do
    String.contains?(text, "[WebServer]") and
      !String.match?(text, ~r/\b(error|failed|fatal|panic|exception)\b/i)
  end

  defp repeated_warning?(text) do
    String.contains?(text, "was reexported through module") or
      String.contains?(text, "NO_COLOR") or
      String.contains?(text, "DSN not configured")
  end

  defp tail_summary(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.take(-3)
    |> Enum.join(" | ")
    |> single_line()
  end

  defp single_line(text) do
    text
    |> sanitize_ansi_and_control_bytes()
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> truncate(220)
  end

  defp normalize_text(value) when is_binary(value), do: String.trim(value)
  defp normalize_text(value) when is_list(value), do: value |> to_string() |> String.trim()
  defp normalize_text(_value), do: ""

  defp sanitize_ansi_and_control_bytes(value) when is_binary(value) do
    value
    |> String.replace(~r/\x1B\[[0-9;]*[A-Za-z]/, "")
    |> String.replace(~r/\x1B./, "")
    |> String.replace(~r/[\x00-\x08\x0B-\x1F\x7F]/, "")
  end

  defp truncate(value, max_length) when byte_size(value) <= max_length, do: value

  defp truncate(value, max_length) do
    value
    |> String.slice(0, max(max_length - 1, 0))
    |> Kernel.<>("...")
  end
end
