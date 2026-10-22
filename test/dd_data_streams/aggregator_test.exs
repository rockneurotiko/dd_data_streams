defmodule DdDataStreams.AggregatorTest do
  use ExUnit.Case, async: false

  alias DdDataStreams.Aggregator

  @moduletag :capture_log

  # The checkpoint a Pub/Sub consume edge produces: timestamp and latencies in
  # milliseconds, payload size in bytes.
  @checkpoint %{
    hash: 304_706_334_078_420_845,
    parent_hash: 2_140_133_010_089_266_238,
    tags: ["direction:in", "subscription:orders-sub", "type:google-pubsub"],
    timestamp: 1_700_000_000_000,
    pathway_latency: 250,
    edge_latency: 125,
    payload_size: 1234
  }

  # The expected body is that checkpoint encoded with python3's msgpack 1.1.2,
  # `msgpack.packb(fields, use_bin_type=True)`: the origin bucket (start of the
  # pathway, one bucket earlier than the checkpoint) first, then the current
  # bucket. The three sketch blobs are the dd-trace-go captures for 0.25 s,
  # 0.125 s and 1234 bytes, so this pins the millisecond-to-second scaling too.
  @aggregator_payload """
  88a3456e76a470726f64a753657276696365a8636865636b6f7574a553746174739286a55374
  617274cf17979cfbe21e1c00a84475726174696f6ecf00000002540be400a553746174739187
  a8456467655461677393ac646972656374696f6e3a696eb7737562736372697074696f6e3a6f
  72646572732d737562b2747970653a676f6f676c652d707562737562a448617368cf043a88cb
  e38e476daa506172656e7448617368cf1db347a23e754c3eae506174687761794c6174656e63
  79c4250a1209000000000040f03f1100d88afe9e04fe3f120d1208000000000000f03f18af01
  1a00ab456467654c6174656e6379c4250a1209000000000040f03f1100d88afe9e04fe3f120d
  1208000000000000f03f1889021a00ab5061796c6f616453697a65c4250a1209000000000040
  f03f1100d88afe9e04fe3f120d1208000000000000f03f1898071a00ad54696d657374616d70
  54797065a66f726967696ea84261636b6c6f677390ac5472616e73616374696f6e73c400b854
  72616e73616374696f6e436865636b706f696e74496473c40086a55374617274cf17979cfe36
  2a0000a84475726174696f6ecf00000002540be400a553746174739187a84564676554616773
  93ac646972656374696f6e3a696eb7737562736372697074696f6e3a6f72646572732d737562
  b2747970653a676f6f676c652d707562737562a448617368cf043a88cbe38e476daa50617265
  6e7448617368cf1db347a23e754c3eae506174687761794c6174656e6379c4250a1209000000
  000040f03f1100d88afe9e04fe3f120d1208000000000000f03f18af011a00ab456467654c61
  74656e6379c4250a1209000000000040f03f1100d88afe9e04fe3f120d1208000000000000f0
  3f1889021a00ab5061796c6f616453697a65c4250a1209000000000040f03f1100d88afe9e04
  fe3f120d1208000000000000f03f1898071a00ad54696d657374616d7054797065a763757272
  656e74a84261636b6c6f677390ac5472616e73616374696f6e73c400b85472616e7361637469
  6f6e436865636b706f696e74496473c400ad54726163657256657273696f6ea5302e312e30a4
  4c616e67a6656c69786972a756657273696f6ea5312e322e33ab50726f636573735461677390
  ab50726f647563744d61736b02
  """

  # The distribution dd-trace-go writes for a zero-byte payload sample, from its
  # own payload capture.
  @zero_sample_sketch "0a1209000000000040f03f1100d88afe9e04fe3f12001a0021000000000000f03f"

  defmodule FakeTransport do
    @behaviour DdDataStreams.Transport

    @impl true
    def post(url, headers, body) do
      Agent.update(:aggregator_test_transport, fn state ->
        %{state | calls: [{url, headers, body} | state.calls]}
      end)

      case Agent.get(:aggregator_test_transport, & &1.response) do
        {:raise, exception} -> raise exception
        response -> response
      end
    end
  end

  setup do
    variables =
      ~w(DD_SERVICE DD_ENV DD_VERSION DD_PRIMARY_TAG DD_DATA_STREAMS_ENABLED) ++
        ~w(DD_TRACE_AGENT_URL DD_AGENT_HOST DD_TRACE_AGENT_PORT)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_VERSION", "1.2.3")

    on_exit(fn ->
      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    start_supervised!(%{
      id: :transport,
      start:
        {Agent, :start_link,
         [fn -> %{response: {:ok, 202}, calls: []} end, [name: :aggregator_test_transport]]}
    })

    test = self()
    handler = "flush-error-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:dd_data_streams, :flush, :error],
      fn event, measurements, metadata, _config ->
        send(test, {:flush_error, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok
  end

  defp start_aggregator do
    start_supervised!({Aggregator, transport: FakeTransport})
  end

  defp respond_with(response) do
    Agent.update(:aggregator_test_transport, &%{&1 | response: response})
  end

  defp calls, do: Agent.get(:aggregator_test_transport, & &1.calls)

  defp hex(string), do: Base.decode16!(String.replace(string, ~r/\s/, ""), case: :mixed)

  test "a checkpoint lands in its current and origin buckets and is posted as expected" do
    start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert [{url, headers, body}] = calls()
    assert url == "http://localhost:8126/v0.1/pipeline_stats"
    assert {"content-type", "application/msgpack"} in headers
    assert {"content-encoding", "gzip"} in headers
    assert <<0x1F, 0x8B, _rest::binary>> = body
    assert :zlib.gunzip(body) == hex(@aggregator_payload)
  end

  test "flush keeps the bucket that is still being filled until the process stops" do
    pid = start_aggregator()
    checkpoint = %{@checkpoint | timestamp: System.system_time(:millisecond), pathway_latency: 0}
    Aggregator.add(checkpoint)

    Aggregator.flush()
    assert calls() == []

    GenServer.stop(pid)

    assert [{_url, _headers, body}] = calls()
    assert :zlib.gunzip(body) =~ "subscription:orders-sub"
  end

  test "the payload size distribution defaults to one zero-byte sample" do
    start_aggregator()
    Aggregator.add(Map.delete(@checkpoint, :payload_size))

    assert :ok = Aggregator.flush()

    assert [{_url, _headers, body}] = calls()
    assert :zlib.gunzip(body) =~ hex(@zero_sample_sketch)
  end

  test "a checkpoint added after a flush is still aggregated and flushed" do
    pid = start_aggregator()

    Aggregator.add(@checkpoint)
    assert :ok = Aggregator.flush()

    assert [{_url, _headers, _pre}] = calls()
    assert Process.alive?(pid)

    Aggregator.add(@checkpoint)
    Aggregator.add(%{@checkpoint | hash: 42})
    assert :ok = Aggregator.flush()

    assert [{_url1, _headers1, post_flush}, {_url2, _headers2, pre_flush}] = calls()
    assert Process.alive?(pid)
    assert point_count(pre_flush) == 2
    assert point_count(post_flush) == 4
    assert :zlib.gunzip(post_flush) =~ "subscription:orders-sub"
  end

  # One point in each of the current and the origin bucket counts one
  # TimestampType field per point.
  defp point_count(body), do: length(:binary.matches(:zlib.gunzip(body), "TimestampType"))

  test "a transport error is counted and the bucket is dropped without raising" do
    respond_with({:error, :econnrefused})
    pid = start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, [:dd_data_streams, :flush, :error], %{count: 1},
                    %{service: "checkout"}}

    assert Process.alive?(pid)
    assert [_call] = calls()

    Aggregator.flush()
    assert [_call] = calls()
    refute_receive {:flush_error, _event, _measurements, _metadata}
  end

  test "a failed flush drops the bucket and the next flush still delivers" do
    respond_with({:error, :econnrefused})
    pid = start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, [:dd_data_streams, :flush, :error], %{count: 1},
                    %{service: "checkout", reason: :econnrefused}}

    assert Process.alive?(pid)
    assert [_failed] = calls()

    # The dropped bucket must not come back: this flush carries only the
    # checkpoint added after the failure, one point per timestamp family.
    respond_with({:ok, 202})
    Aggregator.add(%{@checkpoint | hash: 42})

    assert :ok = Aggregator.flush()

    assert [{_url, _headers, body}, _failed] = calls()
    assert point_count(body) == 2
    assert :zlib.gunzip(body) =~ "subscription:orders-sub"
    assert Process.alive?(pid)
  end

  test "a transport that raises drops the bucket instead of crashing" do
    respond_with({:raise, %RuntimeError{message: "far end"}})
    pid = start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, [:dd_data_streams, :flush, :error], %{count: 1},
                    %{service: "checkout", reason: %RuntimeError{message: "far end"}}}

    assert Process.alive?(pid)
  end

  test "a non-2xx status is a failed flush as well" do
    respond_with({:ok, 500})
    pid = start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, [:dd_data_streams, :flush, :error], %{count: 1}, _metadata}

    assert Process.alive?(pid)
  end

  test "add/1 and flush/0 do nothing while the aggregator is not running" do
    refute Process.whereis(Aggregator)

    assert :ok = Aggregator.add(@checkpoint)
    assert :ok = Aggregator.flush()
  end

  if Code.ensure_loaded?(DdDataStreams.Transport.Req) do
    test "the Req implementation is the default transport" do
      pid = start_supervised!({Aggregator, []})

      assert :sys.get_state(pid).transport == DdDataStreams.Transport.Req
    end
  end
end
