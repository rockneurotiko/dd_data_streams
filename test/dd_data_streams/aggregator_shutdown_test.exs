defmodule DdDataStreams.AggregatorShutdownTest do
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Transport

  @moduletag :capture_log

  setup do
    previous = System.get_env("DD_DATA_STREAMS_ENABLED")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    on_exit(fn ->
      if previous do
        System.put_env("DD_DATA_STREAMS_ENABLED", previous)
      else
        System.delete_env("DD_DATA_STREAMS_ENABLED")
      end
    end)

    test = self()

    stub(Transport.Req, :post, fn _url, _headers, body ->
      send(test, {:post, body})
      {:ok, 202}
    end)

    :ok
  end

  # The release path: the supervisor stops the aggregator with a shutdown exit,
  # which only reaches terminate/2 while the aggregator traps exits.
  test "stopping the supervised aggregator posts the bucket it is still filling" do
    supervisor = start_application()
    assert aggregator = Process.whereis(Aggregator)
    allow(Transport.Req, self(), aggregator)

    Aggregator.add(%{
      hash: 304_706_334_078_420_845,
      parent_hash: 2_140_133_010_089_266_238,
      tags: ["direction:in", "subscription:orders-sub", "type:google-pubsub"],
      timestamp: System.system_time(:millisecond),
      pathway_latency: 0,
      edge_latency: 0,
      payload_size: 12
    })

    assert Supervisor.stop(supervisor) == :ok
    refute Process.alive?(aggregator)

    assert_receive {:post, body}
    assert :zlib.gunzip(body) =~ "subscription:orders-sub"
  end

  defp start_application do
    {:ok, supervisor} = DdDataStreams.Application.start(:normal, [])

    # start/0 links the supervisor to this test process, but the on_exit
    # callback below runs after that process has exited, so the link would kill
    # the supervisor and race the stop.
    Process.unlink(supervisor)
    on_exit(fn -> stop(supervisor) end)
    supervisor
  end

  defp stop(supervisor) do
    Supervisor.stop(supervisor)
  catch
    :exit, _reason -> :ok
  end
end
