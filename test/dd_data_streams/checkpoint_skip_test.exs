defmodule DdDataStreams.CheckpointSkipTest do
  # Not async: the setup deletes the environment variables the configuration is
  # read from, which every test in the suite reads through `DdDataStreams.Config`.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias DdDataStreams.{Checkpoints, Config}

  setup do
    variables =
      ~w(DD_SERVICE DD_ENV DD_VERSION DD_PRIMARY_TAG DD_DATA_STREAMS_ENABLED) ++
        ~w(DD_TRACE_AGENT_URL DD_AGENT_HOST DD_TRACE_AGENT_PORT)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    on_exit(fn ->
      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    test = self()
    handler = "checkpoint-skip-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:dd_data_streams, :checkpoint, :skip],
      fn _event, measurements, metadata, _config ->
        send(test, {:checkpoint_skip, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok
  end

  test "a header that cannot be decoded starts a new pathway and reports the skip" do
    message = %{data: "payload", attributes: %{Config.header() => "not-base64!!!"}}

    assert {_pathway, %{parent_hash: 0, pathway_latency: 0, edge_latency: 0}} =
             Checkpoints.consume(message, ["direction:in"], System.system_time())

    assert_receive {:checkpoint_skip, %{count: 1}, %{reason: :malformed_header, extractors: nil}}
  end

  test "a message without the header starts a new pathway in silence" do
    message = %{data: "payload", attributes: %{}}

    assert {_pathway, %{parent_hash: 0, pathway_latency: 0, edge_latency: 0}} =
             Checkpoints.consume(message, ["direction:in"], System.system_time())

    refute_received {:checkpoint_skip, _measurements, _metadata}
  end

  test "a message no extractor names reports the miss with the extractors asked" do
    assert is_nil(DdDataStreams.consume(%{data: "payload", metadata: %{}}))

    assert_receive {:checkpoint_skip, %{count: 1},
                    %{
                      reason: :no_extractor_match,
                      extractors: [DdDataStreams.Extractor.GooglePubSub]
                    }}
  end
end
