defmodule SymphonyElixir.Linear.Adapter do
  @moduledoc """
  Linear-backed tracker adapter.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.Client

  @create_comment_mutation """
  mutation SymphonyCreateComment($issueId: String!, $body: String!) {
    commentCreate(input: {issueId: $issueId, body: $body}) {
      success
    }
  }
  """

  @update_state_mutation """
  mutation SymphonyUpdateIssueState($issueId: String!, $stateId: String!) {
    issueUpdate(id: $issueId, input: {stateId: $stateId}) {
      success
    }
  }
  """

  @update_labels_mutation """
  mutation SymphonyUpdateIssueLabels($issueId: String!, $labelIds: [String!]) {
    issueUpdate(id: $issueId, input: {labelIds: $labelIds}) {
      success
    }
  }
  """

  @state_lookup_query """
  query SymphonyResolveStateId($issueId: String!, $stateName: String!) {
    issue(id: $issueId) {
      team {
        states(filter: {name: {eq: $stateName}}, first: 1) {
          nodes {
            id
          }
        }
      }
    }
  }
  """

  @label_lookup_query """
  query SymphonyResolveLabelId($issueId: String!, $labelName: String!) {
    issue(id: $issueId) {
      labels {
        nodes {
          id
        }
      }
      team {
        labels(filter: {name: {eq: $labelName}}, first: 1) {
          nodes {
            id
          }
        }
      }
    }
  }
  """

  @spec fetch_candidate_issues() :: {:ok, [term()]} | {:error, term()}
  def fetch_candidate_issues, do: client_module().fetch_candidate_issues()

  @spec fetch_candidate_issues(DateTime.t() | nil) :: {:ok, [term()]} | {:error, term()}
  def fetch_candidate_issues(updated_after), do: client_module().fetch_candidate_issues(updated_after)

  @spec fetch_issues_by_states([String.t()]) :: {:ok, [term()]} | {:error, term()}
  def fetch_issues_by_states(states), do: client_module().fetch_issues_by_states(states)

  @spec fetch_issue_states_by_ids([String.t()]) :: {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids), do: client_module().fetch_issue_states_by_ids(issue_ids)

  @spec fetch_issue_rework_count(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fetch_issue_rework_count(issue_id), do: client_module().fetch_issue_rework_count(issue_id)

  @spec create_comment(String.t(), String.t()) :: :ok | {:error, term()}
  def create_comment(issue_id, body) when is_binary(issue_id) and is_binary(body) do
    with {:ok, response} <-
           client_module().graphql(@create_comment_mutation, %{issueId: issue_id, body: body}, critical?: true),
         true <- get_in(response, ["data", "commentCreate", "success"]) == true do
      :ok
    else
      false -> {:error, :comment_create_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :comment_create_failed}
    end
  end

  @spec apply_label(String.t(), String.t()) :: :ok | {:error, term()}
  def apply_label(issue_id, label_name) when is_binary(issue_id) and is_binary(label_name) do
    with {:ok, label_ids} <- resolve_label_ids(issue_id, label_name),
         {:ok, response} <-
           client_module().graphql(@update_labels_mutation, %{issueId: issue_id, labelIds: label_ids}, critical?: true),
         true <- get_in(response, ["data", "issueUpdate", "success"]) == true do
      :ok
    else
      false -> {:error, :issue_label_update_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :issue_label_update_failed}
    end
  end

  @spec update_issue_state(String.t(), String.t()) :: :ok | {:error, term()}
  def update_issue_state(issue_id, state_name)
      when is_binary(issue_id) and is_binary(state_name) do
    with {:ok, state_id} <- resolve_state_id(issue_id, state_name),
         {:ok, response} <-
           client_module().graphql(@update_state_mutation, %{issueId: issue_id, stateId: state_id}, critical?: true),
         true <- get_in(response, ["data", "issueUpdate", "success"]) == true do
      :ok
    else
      false -> {:error, :issue_update_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :issue_update_failed}
    end
  end

  defp client_module do
    Application.get_env(:symphony_elixir, :linear_client_module, Client)
  end

  defp resolve_state_id(issue_id, state_name) do
    with {:ok, response} <-
           client_module().graphql(@state_lookup_query, %{issueId: issue_id, stateName: state_name}, critical?: true),
         state_id when is_binary(state_id) <-
           get_in(response, ["data", "issue", "team", "states", "nodes", Access.at(0), "id"]) do
      {:ok, state_id}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :state_not_found}
    end
  end

  defp resolve_label_ids(issue_id, label_name) do
    with {:ok, response} <-
           client_module().graphql(@label_lookup_query, %{issueId: issue_id, labelName: label_name}, critical?: true),
         label_id when is_binary(label_id) <-
           get_in(response, ["data", "issue", "team", "labels", "nodes", Access.at(0), "id"]) do
      existing_label_ids =
        response
        |> get_in(["data", "issue", "labels", "nodes"])
        |> List.wrap()
        |> Enum.flat_map(fn
          %{"id" => id} when is_binary(id) -> [id]
          _ -> []
        end)

      {:ok, Enum.uniq(existing_label_ids ++ [label_id])}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :label_not_found}
    end
  end
end
