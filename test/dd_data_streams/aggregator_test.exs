defmodule DdDataStreams.AggregatorTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  use Mimic

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Transport

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
  6f6e436865636b706f696e74496473c400ad54726163657256657273696f6ea5302e322e30a4
  4c616e67a6656c69786972a756657273696f6ea5312e322e33ab50726f636573735461677390
  ab50726f647563744d61736b03
  """

  # The distribution dd-trace-go writes for a zero-byte payload sample, from its
  # own payload capture.
  @zero_sample_sketch "0a1209000000000040f03f1100d88afe9e04fe3f12001a0021000000000000f03f"

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

    test = self()

    # The aggregator posts from its own process, so the stub is set here and
    # allowed into that process by start_aggregator/0.
    stub(Transport.Req, :post, fn url, headers, body ->
      send(test, {:post, url, headers, body})
      {:ok, 202}
    end)

    handler = "flush-error-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:dd_data_streams, :flush, :error],
      fn event, measurements, metadata, _config ->
        send(test, {:flush_error, event, measurements, metadata})
      end,
      nil
    )

    invalid_handler = "invalid-checkpoint-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      invalid_handler,
      [:dd_data_streams, :checkpoint, :invalid],
      fn _event, measurements, metadata, _config ->
        send(test, {:invalid_checkpoint, measurements, metadata})
      end,
      nil
    )

    stop_handler = "flush-stop-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      stop_handler,
      [:dd_data_streams, :flush, :stop],
      fn _event, measurements, metadata, _config ->
        send(test, {:flush_stop, measurements, metadata})
      end,
      nil
    )

    skip_handler = "checkpoint-skip-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      skip_handler,
      [:dd_data_streams, :checkpoint, :skip],
      fn _event, measurements, metadata, _config ->
        send(test, {:checkpoint_skip, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      :telemetry.detach(invalid_handler)
      :telemetry.detach(stop_handler)
      :telemetry.detach(skip_handler)
    end)

    :ok
  end

  defp start_aggregator do
    pid = start_supervised!({Aggregator, []})
    allow(Transport.Req, self(), pid)
    pid
  end

  defp respond_with({:raise, exception}) do
    stub(Transport.Req, :post, fn _url, _headers, _body -> raise exception end)
  end

  defp respond_with(response) do
    stub(Transport.Req, :post, fn _url, _headers, _body -> response end)
  end

  # Posts since the last call, oldest first.
  defp posts, do: calls(Transport.Req, :post, 3)

  defp hex(string), do: Base.decode16!(String.replace(string, ~r/\s/, ""), case: :mixed)

  test "a checkpoint lands in its current and origin buckets and is posted as expected" do
    start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert [[url, headers, body]] = posts()
    assert url == "http://localhost:8126/v0.1/pipeline_stats"
    assert {"content-type", "application/msgpack"} in headers
    assert {"content-encoding", "gzip"} in headers
    assert {"datadog-meta-lang", "elixir"} in headers
    assert <<0x1F, 0x8B, _rest::binary>> = body
    assert :zlib.gunzip(body) == hex(@aggregator_payload)
  end

  test "flush keeps the bucket that is still being filled until the process stops" do
    pid = start_aggregator()
    checkpoint = %{@checkpoint | timestamp: System.system_time(:millisecond), pathway_latency: 0}
    Aggregator.add(checkpoint)

    Aggregator.flush()
    assert posts() == []

    GenServer.stop(pid)

    assert [[_url, _headers, body]] = posts()
    assert :zlib.gunzip(body) =~ "subscription:orders-sub"
  end

  test "a checkpoint that cannot be aggregated is dropped and counted" do
    # Each of these reaches neither bucket, and crashes the aggregator if it
    # does.
    for {shape, checkpoint} <- [
          {"no timestamp", Map.delete(@checkpoint, :timestamp)},
          {"nil timestamp", %{@checkpoint | timestamp: nil}},
          {"nil tags", %{@checkpoint | tags: nil}},
          {"no hash", Map.delete(@checkpoint, :hash)},
          {"no parent hash", Map.delete(@checkpoint, :parent_hash)}
        ] do
      pid = start_aggregator()

      assert :ok = Aggregator.add(checkpoint), shape
      assert Process.alive?(pid), shape

      assert_receive {:invalid_checkpoint, %{count: 1}, %{service: "checkout"}}, 100, shape

      assert :ok = Aggregator.flush()
      assert posts() == [], shape
      assert %{buckets: %{}} = :sys.get_state(pid), shape

      stop_supervised!(DdDataStreams.Aggregator)
    end
  end

  test "latencies and a payload size that are nil count as zero" do
    pid = start_aggregator()

    checkpoint = %{
      @checkpoint
      | pathway_latency: nil,
        edge_latency: nil,
        payload_size: nil
    }

    assert :ok = Aggregator.add(checkpoint)
    assert Process.alive?(pid)
    refute_received {:invalid_checkpoint, _measurements, _metadata}

    assert [point] = points_of(pid, :current) |> Map.values()
    assert samples(point) == 1

    assert :ok = Aggregator.flush()
    assert [[_url, _headers, body]] = posts()
    assert point_count(body) == 2
  end

  test "a tag that is not a string is dropped instead of costing the whole flush" do
    start_aggregator()

    checkpoint = %{@checkpoint | tags: ["direction:in", :not_a_string]}
    assert :ok = Aggregator.add(checkpoint)

    assert :ok = Aggregator.flush()

    assert [[_url, _headers, body]] = posts()
    assert :zlib.gunzip(body) =~ "direction:in"
  end

  test "the payload size distribution defaults to one zero-byte sample" do
    start_aggregator()
    Aggregator.add(Map.delete(@checkpoint, :payload_size))

    assert :ok = Aggregator.flush()

    assert [[_url, _headers, body]] = posts()
    assert :zlib.gunzip(body) =~ hex(@zero_sample_sketch)
  end

  test "a checkpoint stamped far in the future is dropped instead of held forever" do
    pid = start_aggregator()

    far_future = System.system_time(:millisecond) + 3_600_000
    Aggregator.add(%{@checkpoint | timestamp: far_future})

    assert :ok = Aggregator.flush()

    assert posts() == []
    assert %{buckets: %{}} = :sys.get_state(pid)

    assert_receive {:flush_stop, %{buckets: 0, points: 0, dropped: 2},
                    %{service: nil, reason: :future_bucket}}
  end

  test "a successful flush reports the buckets and points it sent" do
    start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()
    assert [[_url, _headers, _body]] = posts()

    assert_receive {:flush_stop, %{buckets: 2, points: 2, dropped: 0},
                    %{service: "checkout", reason: nil}}
  end

  test "adding a checkpoint while no aggregator runs reports the skip" do
    assert is_nil(Process.whereis(Aggregator))

    assert :ok = Aggregator.add(@checkpoint)

    assert_receive {:checkpoint_skip, %{count: 1},
                    %{reason: :aggregator_not_running, extractors: nil}}
  end

  # A drop while the library is enabled is a misconfiguration, so it is
  # logged - but once per process, not once per checkpoint: a processor
  # records every message it handles and one line per message would bury the
  # log.
  test "a checkpoint dropped while enabled warns once per process, not per call" do
    Application.put_env(:dd_data_streams, :enabled, false)
    on_exit(fn -> Application.delete_env(:dd_data_streams, :enabled) end)

    disabled = capture_log(fn -> Aggregator.add(@checkpoint) end)
    refute disabled =~ "no aggregator is running"

    Application.put_env(:dd_data_streams, :enabled, true)

    enabled =
      capture_log(fn ->
        Aggregator.add(@checkpoint)
        Aggregator.add(@checkpoint)
      end)

    assert length(String.split(enabled, "no aggregator is running")) - 1 == 1
  end

  test "a checkpoint within clock skew is kept and flushed on shutdown" do
    pid = start_aggregator()

    # Ten seconds ahead is broker skew, not a bad timestamp.
    Aggregator.add(%{@checkpoint | timestamp: System.system_time(:millisecond) + 10_000})

    assert :ok = Aggregator.flush()
    assert posts() == []

    GenServer.stop(pid)

    assert [[_url, _headers, body]] = posts()
    assert :zlib.gunzip(body) =~ "subscription:orders-sub"
  end

  test "a checkpoint added after a flush is still aggregated and flushed" do
    pid = start_aggregator()

    Aggregator.add(@checkpoint)
    assert :ok = Aggregator.flush()

    assert [[_url, _headers, pre_flush]] = posts()
    assert Process.alive?(pid)

    Aggregator.add(@checkpoint)
    Aggregator.add(%{@checkpoint | hash: 42})
    assert :ok = Aggregator.flush()

    assert [[_url2, _headers2, post_flush]] = posts()
    assert Process.alive?(pid)
    assert point_count(pre_flush) == 2
    assert point_count(post_flush) == 4
    assert :zlib.gunzip(post_flush) =~ "subscription:orders-sub"
  end

  # One point in each of the current and the origin bucket counts one
  # TimestampType field per point.
  defp point_count(body), do: length(:binary.matches(:zlib.gunzip(body), "TimestampType"))

  # The points of one bucket family, read from the state of the aggregator. The
  # buckets of the other families are skipped, so the family asked for is the
  # one that is returned.
  defp points_of(pid, family) do
    %{buckets: buckets} = :sys.get_state(pid)

    Enum.find_value(
      buckets,
      %{},
      fn
        {{^family, _service, _start}, points} -> points
        _other -> nil
      end
    )
  end

  # How many samples the pathway distribution of a point holds.
  defp samples(point) do
    point.pathway.positive
    |> Map.values()
    |> Enum.sum()
    |> Kernel.+(point.pathway.zero_count)
  end

  test "two checkpoints of one node with different aggregation tags are two points" do
    # Every aggregation tag splits the statistics of the node the same way.
    for {tag, first, second} <- [
          {"group", "workers-a", "workers-b"},
          {"ds.name", "orders", "payments"},
          {"ds.namespace", "shop", "warehouse"},
          {"manual_checkpoint", "true", "false"},
          {"consumer_group", "workers-a", "workers-b"}
        ] do
      pid = start_aggregator()

      Aggregator.add(%{@checkpoint | tags: ["direction:in", "#{tag}:#{first}"]})
      Aggregator.add(%{@checkpoint | tags: ["direction:in", "#{tag}:#{second}"]})

      points = points_of(pid, :current)

      assert map_size(points) == 2

      assert points |> Map.values() |> Enum.map(& &1.edge_tags) |> Enum.sort() ==
               Enum.sort([
                 ["direction:in", "#{tag}:#{first}"],
                 ["direction:in", "#{tag}:#{second}"]
               ])

      assert points |> Map.values() |> Enum.map(&samples/1) |> Enum.sort() == [1, 1]

      assert :ok = Aggregator.flush()

      assert [[_url, _headers, body]] = posts()
      payload = :zlib.gunzip(body)

      assert point_count(body) == 4
      assert payload =~ "#{tag}:#{first}"
      assert payload =~ "#{tag}:#{second}"

      # The Hash field is the node hash as a uint64, right after its key.
      assert payload =~ <<0xA4, "Hash", 0xCF, @checkpoint.hash::64>>

      stop_supervised!(DdDataStreams.Aggregator)
    end
  end

  test "samples with the same aggregation tags merge into one point, whatever their order" do
    pid = start_aggregator()

    # The aggregation tags are the same set in another order, so the two
    # checkpoints are the same split of the same node.
    Aggregator.add(%{@checkpoint | tags: ["direction:in", "group:workers-a", "ds.name:orders"]})
    Aggregator.add(%{@checkpoint | tags: ["ds.name:orders", "group:workers-a", "direction:in"]})

    points = points_of(pid, :current)

    assert map_size(points) == 1
    assert [point] = Map.values(points)
    assert samples(point) == 2

    # The bucket of the other family is skipped, not raised on.
    assert map_size(points_of(pid, :origin)) == 1
  end

  test "a transport error is counted and the bucket is dropped without raising" do
    respond_with({:error, :econnrefused})
    pid = start_aggregator()
    Aggregator.add(@checkpoint)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, [:dd_data_streams, :flush, :error], %{count: 1},
                    %{service: "checkout"}}

    assert Process.alive?(pid)
    assert [_call] = posts()

    Aggregator.flush()
    assert posts() == []
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
    assert [_failed] = posts()

    # The dropped bucket must not come back: this flush carries only the
    # checkpoint added after the failure, one point per timestamp family.
    respond_with({:ok, 202})
    Aggregator.add(%{@checkpoint | hash: 42})

    assert :ok = Aggregator.flush()

    assert [[_url, _headers, body]] = posts()
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
      assert DdDataStreams.Config.transport() == DdDataStreams.Transport.Req
    end
  end

  test "the transport is read on every flush, so a runtime change applies" do
    start_aggregator()
    Aggregator.add(@checkpoint)

    # The behaviour module has no post/3 of its own, so naming it as the
    # transport makes the flush fail in a way that says which module was used.
    Application.put_env(:dd_data_streams, :transport, Transport)
    on_exit(fn -> Application.delete_env(:dd_data_streams, :transport) end)

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error, _event, %{count: 1},
                    %{reason: %UndefinedFunctionError{module: Transport}}}
  end
end
