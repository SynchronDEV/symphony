defmodule SymphonyElixir.LinearAdapterBoundaryTest do
  use ExUnit.Case, async: false
  alias SymphonyElixir.Linear.Adapter

  defmodule Client do
    def graphql(query, variables, opts) do
      send(self(), {:graphql, query, variables, opts})
      [reply | rest] = Process.get(:adapter_replies)
      Process.put(:adapter_replies, rest)
      reply
    end
  end

  setup do
    previous = Application.get_env(:symphony_elixir, :linear_client_module)
    Application.put_env(:symphony_elixir, :linear_client_module, Client)

    on_exit(fn ->
      if previous do
        Application.put_env(:symphony_elixir, :linear_client_module, previous)
      else
        Application.delete_env(:symphony_elixir, :linear_client_module)
      end
    end)
  end

  test "label lookup errors and malformed replies fail without writing" do
    outcomes = [{{:error, :offline}, :offline}, {:malformed, :label_not_found}, {{:ok, %{}}, :label_not_found}]

    for {reply, expected} <- outcomes do
      Process.put(:adapter_replies, [reply])
      assert Adapter.apply_label("issue", "label") == {:error, expected}
      assert Process.get(:adapter_replies) == []
    end
  end

  test "label mutation failures preserve transport and mutation errors" do
    outcomes = [
      {{:error, :offline}, :offline},
      {:malformed, :issue_label_update_failed},
      {{:ok, %{}}, :issue_label_update_failed}
    ]

    for {reply, expected} <- outcomes do
      Process.put(:adapter_replies, [found_label(), reply])
      assert Adapter.apply_label("issue", "label") == {:error, expected}
      assert_receive {:graphql, _, %{labelIds: ["target"]}, [critical?: true]}
    end
  end

  test "label creation failures propagate and successful creation retries lookup only once" do
    for {reply, expected} <- [{{:error, :offline}, :offline}, {:malformed, :issue_label_create_failed}] do
      Process.put(:adapter_replies, [missing_label(), reply])
      assert Adapter.apply_label("issue", "label") == {:error, expected}
    end

    Process.put(:adapter_replies, [missing_label(), {:ok, %{"data" => %{"issueLabelCreate" => %{"success" => true}}}}, missing_label()])
    assert Adapter.apply_label("issue", "label") == {:error, :label_not_found}
    assert Process.get(:adapter_replies) == []
  end

  defp found_label do
    {:ok, %{"data" => %{"issue" => %{"labels" => %{"nodes" => [nil, %{"id" => 4}]}, "team" => %{"labels" => %{"nodes" => [%{"id" => "target"}]}}}}}}
  end

  defp missing_label do
    {:ok, %{"data" => %{"issue" => %{"team" => %{"id" => "team", "labels" => %{"nodes" => []}}}}}}
  end
end
