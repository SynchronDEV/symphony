defmodule SymphonyElixir.Linear.Issue do
  @moduledoc """
  Normalized Linear issue representation used by the orchestrator.
  """

  defstruct [
    :id,
    :identifier,
    :title,
    :description,
    :priority,
    :state,
    :branch_name,
    :url,
    :assignee_id,
    blocked_by: [],
    labels: [],
    assigned_to_worker: true,
    created_at: nil,
    updated_at: nil
  ]

  @type t :: %__MODULE__{
          id: String.t() | nil,
          identifier: String.t() | nil,
          title: String.t() | nil,
          description: String.t() | nil,
          priority: integer() | nil,
          state: String.t() | nil,
          branch_name: String.t() | nil,
          url: String.t() | nil,
          assignee_id: String.t() | nil,
          labels: [String.t()],
          assigned_to_worker: boolean(),
          created_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  @spec label_names(t()) :: [String.t()]
  def label_names(%__MODULE__{labels: labels}) do
    labels
  end

  @doc """
  Returns true when the issue carries any label in `stop_labels`.

  Used to keep already-completed work (e.g. a review that passed and now waits
  for a human to merge) out of both dispatch and continuation, so the agent loop
  does not keep re-running it while it lingers in an active state.
  """
  @spec stop_continue_labeled?(t(), [String.t()]) :: boolean()
  def stop_continue_labeled?(%__MODULE__{labels: labels}, stop_labels)
      when is_list(labels) and is_list(stop_labels) do
    stop_labels != [] and Enum.any?(labels, &(&1 in stop_labels))
  end

  def stop_continue_labeled?(_issue, _stop_labels), do: false

  @doc """
  Returns true when the issue is assigned to this worker and carries every
  configured required label.
  """
  @spec routable?(t(), [String.t()]) :: boolean()
  def routable?(%__MODULE__{assigned_to_worker: false}, _required_labels), do: false

  def routable?(%__MODULE__{labels: labels}, required_labels)
      when is_list(labels) and is_list(required_labels) do
    normalized_labels =
      labels
      |> Enum.map(&normalize_label/1)
      |> Enum.reject(&(&1 == ""))
      |> MapSet.new()

    required_labels
    |> Enum.map(&normalize_label/1)
    |> Enum.all?(&MapSet.member?(normalized_labels, &1))
  end

  def routable?(%__MODULE__{}, required_labels) when required_labels in [[], nil], do: true
  def routable?(_issue, _required_labels), do: false

  defp normalize_label(label) when is_binary(label) do
    label
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_label(_label), do: ""
end
