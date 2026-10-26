defmodule DdDataStreams.TestingRecipeTest do
  # The integration-test recipe from the README: the library enabled with the
  # capture transport, so a checkpoint from produce/3 is assertable as soon as
  # flush(:all) sends it, with no Datadog Agent anywhere.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @capture_event [:dd_data_streams, :transport, :captured]

  setup do
    variables =
      ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED) ++
        ~w(DD_TRACE_AGENT_URL DD_AGENT_HOST DD_TRACE_AGENT_PORT)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    Application.put_env(:dd_data_streams, :transport, DdDataStreams.Transport.Capture)

    start_supervised!(DdDataStreams.Aggregator)

    test = self()
    handler = "recipe-capture-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      @capture_event,
      fn _event, _measurements, metadata, _config ->
        send(test, {:captured, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:dd_data_streams, :transport)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "a checkpoint from produce/3 is assertable immediately with flush(:all)" do
    [_pair] = DdDataStreams.produce("orders-out", "payload")

    # The checkpoint is stamped now, so its bucket is not complete: flush/0
    # keeps it, exactly as it does outside a test.
    assert :ok = DdDataStreams.Aggregator.flush()
    refute_received {:captured, _metadata}

    assert :ok = DdDataStreams.Aggregator.flush(:all)

    assert_receive {:captured, %{url: url, headers: headers, body: body}}
    assert url == "http://localhost:8126/v0.1/pipeline_stats"
    assert {"content-encoding", "gzip"} in headers
    assert :zlib.gunzip(body) =~ "topic:orders-out"
  end

  test "with the library disabled, produce/3 returns [] and nothing is captured" do
    Application.put_env(:dd_data_streams, :enabled, false)
    on_exit(fn -> Application.delete_env(:dd_data_streams, :enabled) end)

    assert DdDataStreams.produce("orders-out", "payload") == []

    assert :ok = DdDataStreams.Aggregator.flush(:all)
    refute_received {:captured, _metadata}
  end

  test "flush(:all) does nothing while the aggregator is not running" do
    stop_supervised!(DdDataStreams.Aggregator)

    assert :ok = DdDataStreams.Aggregator.flush(:all)
    refute_received {:captured, _metadata}
  end
end
