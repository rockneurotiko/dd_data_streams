defmodule DdDataStreams.CaptureTransportTest do
  # The capture transport keeps no state, so its own test shares nothing with
  # any other: a handler id unique to this test sees exactly its own posts.
  use ExUnit.Case, async: true

  alias DdDataStreams.Transport
  alias DdDataStreams.Transport.Capture

  @event [:dd_data_streams, :transport, :captured]

  setup do
    handler = "capture-transport-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      @event,
      fn _event, measurements, metadata, _config ->
        send(test, {:captured, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok
  end

  test "implements the transport behaviour" do
    assert Transport in Capture.module_info(:attributes)[:behaviour]
  end

  test "reports the post on telemetry and answers an accepted status" do
    assert {:ok, 202} =
             Capture.post(
               "http://localhost:8126/v0.1/pipeline_stats",
               [{"content-type", "application/msgpack"}, {"content-encoding", "gzip"}],
               ["gz", "ipped"]
             )

    assert_receive {:captured, %{count: 1},
                    %{
                      url: "http://localhost:8126/v0.1/pipeline_stats",
                      headers: [
                        {"content-type", "application/msgpack"},
                        {"content-encoding", "gzip"}
                      ],
                      body: "gzipped"
                    }}

    refute_received {:captured, _measurements, _metadata}
  end
end
