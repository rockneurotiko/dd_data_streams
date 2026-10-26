defmodule DdDataStreams.BroadwayTest do
  # The library never calls Broadway: it hangs itself on the telemetry events
  # Broadway emits (broadway v1.2.1, `lib/broadway/topology/processor_stage.ex`
  # and `batch_processor_stage.ex`), so the events below are emitted directly,
  # with the measurements and metadata those two stages build.
  use ExUnit.Case, async: false

  use Mimic

  doctest DdDataStreams.Context

  alias DdDataStreams.{Aggregator, Context, Pathway, Propagator}

  @consume_event [:broadway, :processor, :message, :start]
  @fan_in_event [:broadway, :batch_processor, :start]
  @events [@consume_event, @fan_in_event]

  @headers Path.expand("../fixtures/propagation/headers.txt", __DIR__)
           |> File.read!()
           |> Code.eval_string()
           |> elem(0)

  @service "checkout"
  @env "prod"
  @subscription "orders-sub"
  # BroadwayCloudPubSub v2 stamps the subscription as its full resource name;
  # the extractor reduces it to the short id the tags below carry.
  @subscription_resource "projects/dsm-test/subscriptions/#{@subscription}"

  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]

  # Hard-pinned pathway hashes, so a change inside the hash functions cannot
  # make these assertions pass by agreeing with themselves. The parent-zero
  # values are the vectors pathway_test.exs pins and its comment credits to
  # dd-trace-py's fnv1_64; the header-chained ones were derived outside this
  # library by the same FNV-1 recipe (offset basis 0xcbf29ce484222325, prime
  # 0x100000001b3, over the little-endian child hash followed by the parent
  # hash), chained onto the dd-trace-py header the fixtures carry.
  @consume_pathway_hash 11_571_056_430_286_957_182
  @produce_pathway_hash 17_914_629_665_428_218_669
  # Chained onto hash 2_367_537_495_353_648_021, the dd-trace-py header the
  # fixtures carry.
  @header_chained_consume_hash 7_600_900_903_418_953_681
  @header_chained_produce_hash 7_510_542_000_147_840_257
  @manual_checkpoint_hash 2_935_049_296_382_441_740

  @absent :absent

  setup do
    variables =
      ~w(DD_SERVICE DD_ENV DD_PRIMARY_TAG DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_ENV", @env)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    detach_all()
    Context.clear()
    Application.delete_env(:dd_data_streams, :extractors)

    # The handlers run in the process that emits the telemetry event, which is
    # this one, so a private-mode stub records the checkpoints the library hands
    # the aggregator without an aggregator or a transport in the way.
    test = self()
    stub(Aggregator, :add, fn checkpoint -> send(test, {:checkpoint, checkpoint}) end)

    on_exit(fn ->
      detach_all()
      Context.clear()
      Application.delete_env(:dd_data_streams, :extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "a message event records exactly one consume checkpoint" do
    header = reference_header()
    received_ms = header.edge_start + 42
    attributes = %{"dd-pathway-ctx-base64" => header.base64}
    message = pubsub_message(attributes, received_at: native(received_ms))

    assert :ok = DdDataStreams.attach()
    consume_event(message)

    assert_receive {:checkpoint, checkpoint}
    refute_receive {:checkpoint, _second}

    assert checkpoint.tags == @consume_tags
    assert checkpoint.parent_hash == header.hash
    # Chained onto the hash dd-trace-py itself put on the message.
    assert checkpoint.hash == @header_chained_consume_hash
    assert checkpoint.timestamp == received_ms
    assert checkpoint.edge_latency == 42
    assert checkpoint.pathway_latency == received_ms - header.pathway_start

    # The same process runs the handler and the pipeline stage, so the pathway
    # the stage produces from is already here.
    assert Context.get().hash == checkpoint.hash
  end

  test "the consume checkpoint is stamped with the receive time, not the event time" do
    received_ms = 1_780_345_623_738
    message = pubsub_message(nil, received_at: native(received_ms))

    assert :ok = DdDataStreams.attach()
    # Broadway's own timestamps are when the processor picked the message up.
    consume_event(message, %{}, %{monotonic_time: -1, system_time: :dispatch_time})

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.timestamp == received_ms
  end

  test "a leaf consumer that produces nothing still records a consume checkpoint" do
    message = pubsub_message(nil)

    assert :ok = DdDataStreams.attach()
    consume_event(message)

    assert_receive {:checkpoint, checkpoint}
    refute_receive {:checkpoint, _second}

    assert checkpoint.tags == @consume_tags
    assert checkpoint.parent_hash == 0
    assert checkpoint.hash == @consume_pathway_hash
    # data 5 bytes + ordering key 10 bytes, no attributes and no inbound header.
    assert checkpoint.payload_size == 15
  end

  test "produce from handle_message chains the produce onto the consume checkpoint" do
    header = reference_header()
    message = pubsub_message(%{"dd-pathway-ctx-base64" => header.base64})

    assert :ok = DdDataStreams.attach()
    consume_event(message)
    assert_receive {:checkpoint, consumed}

    # What handle_message/3 does with the pathway its stage was given.
    [{key, value}] = DdDataStreams.produce("orders", "hello")

    assert_receive {:checkpoint, produced}
    refute_receive {:checkpoint, _second}

    assert key == "dd-pathway-ctx-base64"
    assert produced.tags == @produce_tags
    assert produced.parent_hash == consumed.hash
    assert produced.hash == @header_chained_produce_hash
    # data 5 bytes + the injected header key and value.
    assert produced.payload_size == 5 + byte_size(key) + byte_size(value)

    # Decoding the returned value reads this library's own encoding; the hash it
    # has to carry is the one pinned above, chained from the consume checkpoint.
    assert {:ok, %Pathway{hash: hash, pathway_start: pathway_start}} =
             Propagator.decode_str(value)

    assert hash == produced.hash
    assert pathway_start == header.pathway_start
  end

  test "both sides of one edge measure the same payload" do
    assert :ok = DdDataStreams.attach()

    [{key, value}] = DdDataStreams.produce("orders", "hello")
    assert_receive {:checkpoint, produced}

    # What the receiving pipeline gets: the data and the header this library
    # injected, neither an ordering key nor another attribute.
    consume_event(%{
      data: "hello",
      metadata: %{
        attributes: %{key => value},
        subscription: @subscription,
        received_at: native(1)
      }
    })

    assert_receive {:checkpoint, consumed}
    assert consumed.parent_hash == produced.hash
    assert consumed.payload_size == produced.payload_size
  end

  test "produce reports a payload size given in the options" do
    assert :ok = DdDataStreams.attach()

    assert [{key, value}] = DdDataStreams.produce("orders", "hello", payload_size: 42)

    assert_receive {:checkpoint, checkpoint}
    # The given size plus the injected header key and value.
    assert checkpoint.payload_size == 42 + byte_size(key) + byte_size(value)
    assert {:ok, _pathway} = Propagator.decode_str(value)
  end

  test "produce returns the configured header key" do
    Application.put_env(:dd_data_streams, :header, "x-pathway")
    on_exit(fn -> Application.delete_env(:dd_data_streams, :header) end)

    assert :ok = DdDataStreams.attach()

    assert [{"x-pathway", value}] = DdDataStreams.produce("orders", "hello")
    assert {:ok, %Pathway{hash: hash}} = Propagator.decode_str(value)

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.hash == hash
  end

  test "a batch processor event hands handle_batch one of the batch's pathways" do
    headers = distinct_headers(3)
    messages = batch_messages(headers)

    assert :ok = DdDataStreams.attach()

    consumed =
      headers
      |> Enum.with_index()
      |> Map.new(fn {header, index} ->
        consume_event(Enum.at(messages, index))
        assert_receive {:checkpoint, checkpoint}
        {header.hash, checkpoint.hash}
      end)

    # Fan-in samples one of the three pathways; seeding keeps the draw
    # reproducible run to run.
    :rand.seed(:exsss, {17, 42, 99})
    batch_event(messages)

    # Which of the three the draw keeps is not asserted; that it is
    # one of them, at the node this service consumed it at, is.
    merged = Context.get()
    assert merged.hash in Map.values(consumed)

    sampled = Enum.find(headers, &(consumed[&1.hash] == merged.hash))
    assert merged.pathway_start == sampled.pathway_start
    assert merged.edge_start == sampled.edge_start + 1

    # The batch's messages each got their checkpoint at their own event and the
    # batch event records nothing of its own.
    refute_receive {:checkpoint, _consume}
  end

  test "each produce from handle_batch records a checkpoint on the merged pathway" do
    headers = distinct_headers(3)
    messages = batch_messages(headers)

    assert :ok = DdDataStreams.attach()
    Enum.each(messages, &consume_event/1)
    batch_event(messages)
    flushed_consumes()

    merged = Context.get()
    assert %Pathway{hash: merged_hash} = merged

    # handle_batch/4 answering the batch with one message per consumed message.
    published =
      for _ <- messages do
        [{_key, value}] = DdDataStreams.produce("orders", "hello")
        assert_receive {:checkpoint, checkpoint}
        {checkpoint, value}
      end

    produced = Enum.map(published, &elem(&1, 0))

    assert length(produced) == 3
    assert Enum.all?(produced, &(&1.parent_hash == merged_hash))
    assert produced |> Enum.map(& &1.hash) |> Enum.uniq() |> length() == 1

    produce_hash = produced |> hd() |> Map.fetch!(:hash)

    for {_checkpoint, value} <- published do
      assert {:ok, %Pathway{hash: ^produce_hash}} = Propagator.decode_str(value)
    end
  end

  test "produce_batch records one checkpoint per payload and returns one pair" do
    header = reference_header()
    message = pubsub_message(%{"dd-pathway-ctx-base64" => header.base64})

    assert :ok = DdDataStreams.attach()
    consume_event(message)
    assert_receive {:checkpoint, consumed}

    [{key, value}] =
      DdDataStreams.produce_batch("orders", Stream.map(["one", "two", "three"], & &1))

    assert key == "dd-pathway-ctx-base64"
    assert {:ok, %Pathway{hash: hash}} = Propagator.decode_str(value)

    produced =
      for _ <- 1..3 do
        assert_receive {:checkpoint, checkpoint}
        checkpoint
      end

    assert Enum.all?(produced, &(&1.parent_hash == consumed.hash))
    assert Enum.all?(produced, &(&1.hash == hash))
    # Each payload plus the header the whole batch is published with.
    header = byte_size(key) + byte_size(value)

    assert produced |> Enum.map(& &1.payload_size) |> Enum.sort() == [
             3 + header,
             3 + header,
             5 + header
           ]

    assert produced |> Enum.map(& &1.timestamp) |> Enum.uniq() |> length() == 1
  end

  test "produce_batch of an empty enumerable returns []" do
    assert :ok = DdDataStreams.attach()

    assert DdDataStreams.produce_batch("orders", []) == []
    assert DdDataStreams.produce_batch("orders", Stream.map([], & &1)) == []
    refute_receive {:checkpoint, _checkpoint}
  end

  test "produce rejects an invalid payload size option" do
    assert :ok = DdDataStreams.attach()

    assert_raise ArgumentError, ~r/:payload_size/, fn ->
      DdDataStreams.produce("orders", "hello", payload_size: -1)
    end

    assert_raise ArgumentError, ~r/:payload_size/, fn ->
      DdDataStreams.produce_batch("orders", ["hello"], payload_size: 1)
    end

    refute_receive {:checkpoint, _checkpoint}
  end

  test "a message without a receive time is stamped when the checkpoint is taken" do
    assert :ok = DdDataStreams.attach()

    before = System.system_time(:millisecond)
    consume_event(pubsub_message(nil, received_at: @absent))
    after_emit = System.system_time(:millisecond)

    assert_receive {:checkpoint, checkpoint}

    assert checkpoint.tags == @consume_tags
    assert checkpoint.timestamp >= before
    assert checkpoint.timestamp <= after_emit
    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
  end

  test "several extractors name their own broker's edges in one application" do
    kafka_extractors()
    assert :ok = DdDataStreams.attach()

    header = reference_header()
    received_ms = header.edge_start + 7

    # A Kafka message: topic, partition, header list, millisecond timestamp.
    consume_event(%{
      data: "hello",
      metadata: %{
        topic: "orders",
        partition: 3,
        headers: [{"dd-pathway-ctx-base64", header.base64}],
        ts: received_ms
      }
    })

    assert_receive {:checkpoint, consumed}
    assert consumed.tags == ["direction:in", "topic:orders", "type:kafka", "partition:3"]
    assert consumed.parent_hash == header.hash
    assert consumed.timestamp == received_ms
    assert consumed.edge_latency == 7

    # The stage producing from that message reports its edge with the same
    # extractor, and returns the pair its draw client would write.
    [{_key, value}] = DdDataStreams.produce("orders-out", "hello")

    assert_receive {:checkpoint, produced}
    assert produced.tags == ["direction:out", "topic:orders-out", "type:kafka"]
    assert {:ok, _pathway} = Propagator.decode_str(value)

    # A Pub/Sub message in the same application is still named by its own
    # extractor, from its own metadata.
    consume_event(pubsub_message(%{"dd-pathway-ctx-base64" => header.base64}))

    assert_receive {:checkpoint, pubsub_consumed}
    assert pubsub_consumed.tags == @consume_tags
    assert pubsub_consumed.parent_hash == header.hash

    # And a produce after it is a Pub/Sub produce without being told so.
    _pair = DdDataStreams.produce("orders", "hello")

    assert_receive {:checkpoint, pubsub_produced}
    assert pubsub_produced.tags == @produce_tags
  end

  test "a stage that changes technology names the extractor it produces with" do
    kafka_extractors()
    assert :ok = DdDataStreams.attach()

    header = reference_header()

    consume_event(%{
      data: "hello",
      metadata: %{topic: "orders", headers: [{"dd-pathway-ctx-base64", header.base64}], ts: 1}
    })

    assert_receive {:checkpoint, _consumed}

    # Consumed from Kafka, published to Pub/Sub: the topic alone cannot say
    # which bus it goes to, so the outgoing extractor is named.
    _pair =
      DdDataStreams.produce("orders", "hello", extractor: DdDataStreams.Extractor.GooglePubSub)

    assert_receive {:checkpoint, produced}
    assert produced.tags == @produce_tags
  end

  test "a batch produces with the extractor that consumed its messages" do
    kafka_extractors()
    assert :ok = DdDataStreams.attach()

    messages =
      for partition <- 1..2 do
        %{
          data: "hello",
          metadata: %{topic: "orders", headers: [], ts: 1_700_000_000_000, partition: partition}
        }
      end

    Enum.each(messages, &consume_event/1)
    for _ <- messages, do: assert_receive({:checkpoint, _consume})

    # The batch stage is a different process, so the extractor has to arrive
    # with the fan-in pathway for its own produce edge to be named.
    batch_event(messages)

    [{_key, value}] = DdDataStreams.produce("orders-out", "hello")

    assert_receive {:checkpoint, produced}
    assert produced.tags == ["direction:out", "topic:orders-out", "type:kafka"]
    assert {:ok, _pathway} = Propagator.decode_str(value)

    # A batch no extractor can name leaves neither a pathway nor an extractor.
    batch_event([%{data: "hello", metadata: %{}}])

    assert Context.get() == %Pathway{}
    assert Context.get_extractor() == nil
  end

  test "fan-in keeps the sampled pathway and its extractor together (kafka first)" do
    kafka_extractors()
    assert :ok = DdDataStreams.attach()

    kafka_msg = %{
      data: "hello",
      metadata: %{topic: "orders", headers: [], ts: 1_700_000_000_000, partition: 1}
    }

    pubsub_msg = pubsub_message(nil)

    consume_event(kafka_msg)
    assert_receive {:checkpoint, kafka_consume}
    consume_event(pubsub_msg)
    assert_receive {:checkpoint, _pubsub_consume}

    batch_event([kafka_msg, pubsub_msg])

    sampled = Context.get()
    extractor = Context.get_extractor()
    assert %Pathway{} = sampled

    assert extractor in [
             DdDataStreams.BroadwayTest.KafkaExtractor,
             DdDataStreams.Extractor.GooglePubSub
           ]

    expected_type =
      if sampled.hash == kafka_consume.hash do
        "kafka"
      else
        "google-pubsub"
      end

    assert Enum.any?(extractor.produce("orders-out"), &(&1 == "type:" <> expected_type))
  end

  test "fan-in keeps the sampled pathway and its extractor together (pubsub first)" do
    kafka_extractors()
    assert :ok = DdDataStreams.attach()

    kafka_msg = %{
      data: "hello",
      metadata: %{topic: "orders", headers: [], ts: 1_700_000_000_000, partition: 1}
    }

    pubsub_msg = pubsub_message(nil)

    consume_event(pubsub_msg)
    assert_receive {:checkpoint, _pubsub_consume}
    consume_event(kafka_msg)
    assert_receive {:checkpoint, kafka_consume}

    batch_event([pubsub_msg, kafka_msg])

    sampled = Context.get()
    extractor = Context.get_extractor()
    assert %Pathway{} = sampled

    assert extractor in [
             DdDataStreams.BroadwayTest.KafkaExtractor,
             DdDataStreams.Extractor.GooglePubSub
           ]

    expected_type =
      if sampled.hash == kafka_consume.hash do
        "kafka"
      else
        "google-pubsub"
      end

    assert Enum.any?(extractor.produce("orders-out"), &(&1 == "type:" <> expected_type))
  end

  test "replacing a message's metadata leaves fan-in parentless" do
    assert :ok = DdDataStreams.attach()

    messages = [pubsub_message(nil), pubsub_message(nil)]
    Enum.each(messages, &consume_event/1)
    for _ <- messages, do: assert_receive({:checkpoint, _consume})

    # A handle_message/3 that replaces message.metadata drops the inbound
    # carrier, so fan-in cannot re-derive the consume checkpoint.
    mutated = Enum.map(messages, fn message -> %{message | metadata: %{foo: "bar"}} end)
    batch_event(mutated)

    assert Context.get() == %Pathway{}
    assert Context.get_extractor() == nil
    refute_receive {:checkpoint, _}
  end

  test "a produce with no consuming stage needs one extractor to be configured" do
    assert :ok = DdDataStreams.attach()

    assert [{"dd-pathway-ctx-base64", value}] = DdDataStreams.produce("orders", "hello")

    assert_receive {:checkpoint, produced}
    assert produced.tags == @produce_tags
    assert {:ok, _pathway} = Propagator.decode_str(value)

    Application.put_env(:dd_data_streams, :extractors, [
      DdDataStreams.Extractor.GooglePubSub,
      DdDataStreams.BroadwayTest.KafkaExtractor
    ])

    assert_raise ArgumentError, ~r/pass `extractor: MyApp.Extractor`/, fn ->
      DdDataStreams.produce("orders", "hello")
    end
  end

  test "a produce with a mistyped extractor option names the option" do
    assert_raise ArgumentError, ~r/extractor: "typo" must be a module exporting produce\/1/, fn ->
      DdDataStreams.produce("orders", "hello", extractor: "typo")
    end

    assert_raise ArgumentError, ~r/extractor: Enum must be a module exporting produce\/1/, fn ->
      DdDataStreams.produce("orders", "hello", extractor: Enum)
    end
  end

  test "a produce with no extractor configured says none is configured" do
    Application.put_env(:dd_data_streams, :extractors, [])

    error =
      assert_raise ArgumentError, fn ->
        DdDataStreams.produce("orders", "hello")
      end

    assert error.message =~ "no extractor is configured"
    assert error.message =~ "config :dd_data_streams, extractors:"
    assert error.message =~ "pass `extractor: MyApp.Extractor` to DdDataStreams.produce/3"
  end

  test "attaching refuses a module that is not an extractor" do
    Application.put_env(:dd_data_streams, :extractors, [Enum])

    assert_raise ArgumentError, ~r/is not a list of DdDataStreams.Extractor/, fn ->
      DdDataStreams.attach()
    end

    assert :telemetry.list_handlers(@consume_event) == []

    Application.put_env(:dd_data_streams, :extractors, DdDataStreams.Extractor.GooglePubSub)

    assert_raise ArgumentError, ~r/is not a list of DdDataStreams.Extractor/, fn ->
      DdDataStreams.attach()
    end

    assert :telemetry.list_handlers(@consume_event) == []
  end

  test "an extractor that exports consume/2 receives the producer that delivered the message" do
    Application.put_env(:dd_data_streams, :extractors, [
      DdDataStreams.BroadwayTest.ProducerAwareExtractor
    ])

    # The configured module exports consume/2 and no consume/1, so attaching
    # also pins that validation accepts an extractor of either arity.
    assert :ok = DdDataStreams.attach()

    consume_event(%{
      data: "hello",
      metadata: %{producer: {DdDataStreams.BroadwayTest.FakeProducer, [queue: "orders"]}}
    })

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.tags == ["direction:in", "type:test-broker"]

    assert_receive {:producer_info,
                    %{
                      producer: {DdDataStreams.BroadwayTest.FakeProducer, [queue: "orders"]},
                      topology: "TestPipeline"
                    }, %{producer: {DdDataStreams.BroadwayTest.FakeProducer, _opts}}}
  end

  test "a consume/2 extractor is given no producer for a message consumed by hand" do
    Application.put_env(:dd_data_streams, :extractors, [
      DdDataStreams.BroadwayTest.ProducerAwareExtractor
    ])

    # Called without attach and outside a telemetry event: no topology name is
    # known and this metadata carries no producer either - both are tolerated.
    assert %Pathway{} = DdDataStreams.consume(%{data: "hello", metadata: %{}})

    assert_receive {:checkpoint, _checkpoint}
    assert_receive {:producer_info, %{producer: nil, topology: nil}, %{}}
  end

  test "a message without a subscription records nothing" do
    assert :ok = DdDataStreams.attach()

    consume_event(pubsub_message(nil, subscription: @absent))

    refute_receive {:checkpoint, _checkpoint}
  end

  test "a message that records no checkpoint leaves no pathway to produce from" do
    header = reference_header()
    message = pubsub_message(%{"dd-pathway-ctx-base64" => header.base64})

    assert :ok = DdDataStreams.attach()
    consume_event(message)
    assert_receive {:checkpoint, _consumed}
    assert Context.get()

    # Metadata that carries the field on some messages only: this one records
    # nothing, so the earlier message's pathway must not survive it.
    consume_event(pubsub_message(nil, subscription: @absent))

    refute_receive {:checkpoint, _second}
    assert Context.get() == nil

    [{key, value}] = DdDataStreams.produce("orders", "hello")

    assert_receive {:checkpoint, produced}
    assert produced.parent_hash == 0
    assert produced.hash == @produce_pathway_hash

    assert key == "dd-pathway-ctx-base64"
    assert {:ok, %Pathway{hash: hash}} = Propagator.decode_str(value)
    assert hash == produced.hash
  end

  test "attaching twice keeps one handler and one checkpoint per message" do
    assert :ok = DdDataStreams.attach()
    assert :ok = DdDataStreams.attach()

    assert length(:telemetry.list_handlers(@consume_event)) == 1

    consume_event(pubsub_message(nil))

    assert_receive {:checkpoint, _checkpoint}
    refute_receive {:checkpoint, _second}
  end

  test "nothing is attached or recorded while the library is disabled" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "false")

    assert :ok = DdDataStreams.attach()
    assert :telemetry.list_handlers(@consume_event) == []

    consume_event(pubsub_message(nil))
    refute_receive {:checkpoint, _checkpoint}

    assert DdDataStreams.produce("orders", "hello") == []
    assert DdDataStreams.produce_batch("orders", ["hello"]) == []
    assert DdDataStreams.produce_batch("orders", []) == []
    refute_receive {:checkpoint, _checkpoint}
  end

  test "a checkpoint records a stage that is neither a consume nor a produce" do
    tags = ["direction:in", "type:internal-queue", "manual_checkpoint:true", "partition:3"]

    pathway = DdDataStreams.checkpoint(Pathway.new(hash: 234), tags)

    assert_receive {:checkpoint, checkpoint}

    assert checkpoint.tags == tags
    assert checkpoint.parent_hash == 234
    # partition and manual_checkpoint are reported; only the identifying tags are
    # hashed, which is what keeps a node one node across its partitions.
    assert checkpoint.hash == @manual_checkpoint_hash

    assert pathway.hash == checkpoint.hash
    assert pathway.edge_start == checkpoint.timestamp

    # A mid-pathway checkpoint does not become the stage's produce pathway.
    assert Context.get() == nil
  end

  test "checkpoint/1 defaults the pathway to the current context" do
    pathway = DdDataStreams.checkpoint(["direction:in", "type:internal-queue"])

    assert %DdDataStreams.Pathway{} = pathway
  end

  test "checkpoint tags must all be binaries" do
    assert_raise ArgumentError, fn ->
      DdDataStreams.checkpoint(Pathway.new(hash: 234), [:foo, 42])
    end

    assert_raise ArgumentError, fn ->
      DdDataStreams.checkpoint([:foo, 42])
    end
  end

  # The header dd-trace-py propagated in its own botocore test suite, i.e. a
  # value a real tracer put on a message rather than one this library encoded.
  defp reference_header do
    Enum.find(@headers, &(&1.source == :python_test_suite))
  end

  defp distinct_headers(count) do
    @headers
    |> Enum.reject(&(&1.hash == 0))
    |> Enum.uniq_by(& &1.hash)
    |> Enum.take(count)
  end

  # One message per header, each received one millisecond after the header's
  # own edge started, carrying that header's pathway as an inbound attribute.
  defp batch_messages(headers) do
    Enum.map(headers, fn header ->
      pubsub_message(
        %{"dd-pathway-ctx-base64" => header.base64},
        received_at: native(header.edge_start + 1)
      )
    end)
  end

  # A message shaped like the ones BroadwayCloudPubSub v2 delivers: the data on
  # the message and the rest on its metadata. A field passed as @absent is left
  # out entirely, as BroadwayCloudPubSub v1 does.
  defp pubsub_message(attributes, metadata \\ []) do
    [
      attributes: attributes,
      orderingKey: "customer-1",
      subscription: @subscription_resource,
      received_at: native(1_780_345_623_738)
    ]
    |> Keyword.merge(metadata)
    |> Enum.reject(fn {_key, value} -> value == @absent end)
    |> Map.new()
    |> then(&%{data: "hello", metadata: &1})
  end

  defp consume_event(message, metadata \\ %{}, measurements \\ %{}) do
    :telemetry.execute(
      @consume_event,
      Map.merge(%{monotonic_time: 0, system_time: 0}, measurements),
      Map.merge(
        %{
          processor_key: :default,
          topology_name: "TestPipeline",
          index: 0,
          name: "TestPipeline.Broadway.Processor_default_0",
          message: message,
          context: %{}
        },
        metadata
      )
    )
  end

  defp batch_event(messages) do
    :telemetry.execute(
      @fan_in_event,
      %{monotonic_time: 0, system_time: 0},
      %{
        topology_name: "TestPipeline",
        name: "TestPipeline.Broadway.BatchProcessor_default",
        index: 0,
        messages: messages,
        batch_info: %{
          batcher: :default,
          batch_key: :default,
          partition: nil,
          size: length(messages),
          trigger: 1
        },
        context: %{},
        producer: nil
      }
    )
  end

  defp kafka_extractors do
    Application.put_env(:dd_data_streams, :extractors, [
      DdDataStreams.BroadwayTest.KafkaExtractor,
      DdDataStreams.Extractor.GooglePubSub
    ])
  end

  defp flushed_consumes do
    for _ <- 1..3, do: assert_receive({:checkpoint, _consume})
    :ok
  end

  defp detach_all do
    Enum.each(@events, fn event ->
      for %{id: id} <- :telemetry.list_handlers(event), do: :telemetry.detach(id)
    end)
  end

  defp native(milliseconds), do: System.convert_time_unit(milliseconds, :millisecond, :native)
end

defmodule DdDataStreams.BroadwayTest.ProducerAwareExtractor do
  # The shape of a broker whose edge identity lives in the producer's
  # configuration rather than in the message metadata: it exports consume/2
  # and no consume/1, which also pins that either arity is a valid extractor.
  # The behaviour declares consume/1, so a consume/2-only module cannot
  # declare it without an implementation warning.
  def consume(metadata, producer_info) do
    # The handler runs in the process that emitted the event, so this reaches
    # the test that emitted it.
    send(self(), {:producer_info, producer_info, metadata})
    {:ok, ["direction:in", "type:test-broker"], %{}}
  end

  def produce(topic), do: ["direction:out", "topic:" <> topic, "type:test-broker"]
end

defmodule DdDataStreams.BroadwayTest.KafkaExtractor do
  # The shape BroadwayKafka stamps: the topic, the partition, the header list and
  # a millisecond timestamp. It exists to prove the library is not tied to one
  # broker, so it stays with the tests rather than in the library.
  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(%{topic: topic} = metadata) when is_binary(topic) do
    tags = ["direction:in", "topic:" <> topic, "type:kafka"]

    tags =
      case Map.get(metadata, :partition) do
        partition when is_integer(partition) -> tags ++ ["partition:#{partition}"]
        _ -> tags
      end

    {:ok, tags, %{attributes: Map.get(metadata, :headers), received_at: received_at(metadata)}}
  end

  def consume(_metadata), do: :error

  @impl true
  def produce(topic), do: ["direction:out", "topic:" <> topic, "type:kafka"]

  defp received_at(%{ts: milliseconds}) when is_integer(milliseconds),
    do: System.convert_time_unit(milliseconds, :millisecond, :native)

  defp received_at(_metadata), do: nil
end
