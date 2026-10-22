defmodule DdDataStreams.BroadwayTest do
  # The library never calls Broadway: it hangs itself on the telemetry events
  # Broadway emits (broadway v1.2.1, `lib/broadway/topology/processor_stage.ex`
  # and `batch_processor_stage.ex`), so the events below are emitted directly,
  # with the measurements and metadata those two stages build.
  use ExUnit.Case, async: false

  doctest DdDataStreams.Context

  alias DdDataStreams.{Context, Pathway, Propagator}

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

  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]

  @absent :absent

  # The aggregator is reached by its registered name, so a process with that
  # name records the checkpoints the library would have handed it instead of
  # requiring the real aggregator, its buckets and a fake transport.
  defmodule Recorder do
    use GenServer

    def start_link(test) do
      GenServer.start_link(__MODULE__, test, name: DdDataStreams.Aggregator)
    end

    @impl true
    def init(test), do: {:ok, test}

    @impl true
    def handle_cast({:add, checkpoint}, test) do
      send(test, {:checkpoint, checkpoint})
      {:noreply, test}
    end
  end

  setup do
    variables =
      ~w(DD_SERVICE DD_ENV DD_PRIMARY_TAG DD_DATA_STREAMS_ENABLED)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_ENV", @env)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    detach_all()
    Context.clear()
    start_supervised!({Recorder, self()})

    on_exit(fn ->
      detach_all()
      Context.clear()

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

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    consume_event(message)

    assert_receive {:checkpoint, checkpoint}
    refute_receive {:checkpoint, _second}

    assert checkpoint.tags == @consume_tags
    assert checkpoint.parent_hash == header.hash
    assert checkpoint.hash == consume_hash(header.hash)
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

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    # Broadway's own timestamps are when the processor picked the message up.
    consume_event(message, %{}, %{monotonic_time: -1, system_time: :dispatch_time})

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.timestamp == received_ms
  end

  test "a leaf consumer that produces nothing still records a consume checkpoint" do
    message = pubsub_message(nil)

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    consume_event(message)

    assert_receive {:checkpoint, checkpoint}
    refute_receive {:checkpoint, _second}

    assert checkpoint.tags == @consume_tags
    assert checkpoint.parent_hash == 0
    assert checkpoint.hash == consume_hash(0)
    # data 5 bytes + ordering key 10 bytes, no attributes and no inbound header.
    assert checkpoint.payload_size == 15
  end

  test "publish from handle_message chains the produce onto the consume checkpoint" do
    header = reference_header()
    message = pubsub_message(%{"dd-pathway-ctx-base64" => header.base64})

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    consume_event(message)
    assert_receive {:checkpoint, consumed}

    # What handle_message/3 does with the pathway its stage was given.
    published =
      DdDataStreams.publish("orders", %{data: "hello", attributes: nil, ordering_key: nil}, & &1)

    assert_receive {:checkpoint, produced}
    refute_receive {:checkpoint, _second}

    assert produced.tags == @produce_tags
    assert produced.parent_hash == consumed.hash
    assert produced.hash == produce_hash(consumed.hash)

    # Decoding the injected attribute reads this library's own encoding; the
    # hash it has to carry is the one derived above from Pathway.node_hash/4.
    assert %Pathway{hash: hash, pathway_start: pathway_start} =
             Propagator.get(published.attributes)

    assert hash == produced.hash
    assert pathway_start == header.pathway_start
  end

  test "a batch processor event hands handle_batch one of the batch's pathways" do
    headers = distinct_headers(3)
    messages = batch_messages(headers)

    assert :ok = DdDataStreams.attach(subscription: @subscription)

    consumed =
      headers
      |> Enum.with_index()
      |> Map.new(fn {header, index} ->
        consume_event(Enum.at(messages, index))
        assert_receive {:checkpoint, checkpoint}
        {header.hash, checkpoint.hash}
      end)

    batch_event(messages)

    # Fan-in is sampled, so which of the three it keeps is not fixed; that it is
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

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    Enum.each(messages, &consume_event/1)
    batch_event(messages)
    flushed_consumes()

    merged = Context.get()
    assert %Pathway{hash: merged_hash} = merged

    # handle_batch/4 answering the batch with one message per consumed message.
    published =
      for _ <- messages do
        DdDataStreams.publish(
          "orders",
          %{data: "hello", attributes: nil, ordering_key: nil},
          & &1
        )
      end

    produced =
      for _ <- published do
        assert_receive {:checkpoint, checkpoint}
        checkpoint
      end

    assert length(produced) == 3
    assert Enum.all?(produced, &(&1.parent_hash == merged_hash))
    assert produced |> Enum.map(& &1.hash) |> Enum.uniq() |> length() == 1

    produce_hash = produced |> hd() |> Map.fetch!(:hash)

    for outgoing <- published do
      assert %Pathway{hash: ^produce_hash} = Propagator.get(outgoing.attributes)
    end
  end

  test "a message without a subscription or receive time falls back to the configured values" do
    assert :ok = DdDataStreams.attach(subscription: "fallback-sub")

    before = System.system_time(:millisecond)
    consume_event(pubsub_message(nil, subscription: @absent, received_at: @absent))
    after_emit = System.system_time(:millisecond)

    assert_receive {:checkpoint, checkpoint}

    assert checkpoint.tags == [
             "direction:in",
             "subscription:fallback-sub",
             "type:google-pubsub"
           ]

    assert checkpoint.timestamp >= before
    assert checkpoint.timestamp <= after_emit
    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
  end

  test "a message without a subscription records nothing" do
    assert :ok = DdDataStreams.attach([])

    consume_event(pubsub_message(nil, subscription: @absent))

    refute_receive {:checkpoint, _checkpoint}
  end

  test "a message that records no checkpoint leaves no pathway to produce from" do
    header = reference_header()
    message = pubsub_message(%{"dd-pathway-ctx-base64" => header.base64})

    assert :ok = DdDataStreams.attach([])
    consume_event(message)
    assert_receive {:checkpoint, _consumed}
    assert Context.get()

    # Metadata that carries the field on some messages only: this one records
    # nothing, so the earlier message's pathway must not survive it.
    consume_event(pubsub_message(nil, subscription: @absent))

    refute_receive {:checkpoint, _second}
    assert Context.get() == nil

    published =
      DdDataStreams.publish("orders", %{data: "hello", attributes: nil, ordering_key: nil}, & &1)

    assert_receive {:checkpoint, produced}
    assert produced.parent_hash == 0
    assert produced.hash == produce_hash(0)

    assert %Pathway{hash: hash} = Propagator.get(published.attributes)
    assert hash == produced.hash
  end

  test "attaching twice keeps one handler and one checkpoint per message" do
    assert :ok = DdDataStreams.attach(subscription: @subscription)
    assert :ok = DdDataStreams.attach(subscription: @subscription)

    assert length(:telemetry.list_handlers(@consume_event)) == 1

    consume_event(pubsub_message(nil))

    assert_receive {:checkpoint, _checkpoint}
    refute_receive {:checkpoint, _second}
  end

  test "nothing is attached or injected while the library is disabled" do
    System.delete_env("DD_DATA_STREAMS_ENABLED")

    assert :ok = DdDataStreams.attach(subscription: @subscription)
    assert :telemetry.list_handlers(@consume_event) == []

    consume_event(pubsub_message(nil))
    refute_receive {:checkpoint, _checkpoint}

    message = %{data: "hello", attributes: %{"origin" => "eu"}, ordering_key: nil}
    assert DdDataStreams.publish("orders", message, & &1) == message
    refute_receive {:checkpoint, _checkpoint}
  end

  test "a checkpoint records a stage that is neither a consume nor a produce" do
    tags = ["direction:in", "type:internal-queue", "manual_checkpoint:true", "partition:3"]

    {pathway, checkpoint} = DdDataStreams.checkpoint(Pathway.new(hash: 234), tags)

    assert_receive {:checkpoint, ^checkpoint}

    assert checkpoint.tags == tags
    assert checkpoint.parent_hash == 234
    # partition and manual_checkpoint are reported; only the identifying tags are
    # hashed, which is what keeps a node one node across its partitions.
    assert checkpoint.hash ==
             Pathway.pathway_hash(Pathway.node_hash(@service, @env, "", tags), 234)

    assert pathway.hash == checkpoint.hash
    assert pathway.edge_start == checkpoint.timestamp

    # A mid-pathway checkpoint does not become the stage's produce pathway.
    assert Context.get() == nil
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
      subscription: @subscription,
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

  defp node_hash(tags), do: Pathway.node_hash(@service, @env, "", tags)

  defp consume_hash(parent), do: Pathway.pathway_hash(node_hash(@consume_tags), parent)
  defp produce_hash(parent), do: Pathway.pathway_hash(node_hash(@produce_tags), parent)
end
