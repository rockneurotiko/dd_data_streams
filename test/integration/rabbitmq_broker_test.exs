defmodule DdDataStreams.RabbitMQBrokerTest do
  # A real `BroadwayRabbitMQ.Producer` against a real RabbitMQ, so the metadata
  # the extractor reads is the metadata the client actually builds and the
  # headers are the typed triples `amqp` actually delivers.
  #
  # This is the `rabbitmq` tier: excluded from `mix test`, run with
  #
  #   just rabbitmq
  #
  # which starts the `rabbitmq` service from `compose.yaml`, runs this file and
  # leaves the container up when the tier fails.
  use ExUnit.Case, async: false

  alias DdDataStreams.{Carrier, Context, Extractor, Pathway, Propagator}

  @moduletag :rabbitmq

  @host System.get_env("DSM_RABBITMQ_HOST", "127.0.0.1")
  @port String.to_integer(System.get_env("DSM_RABBITMQ_PORT", "5672"))
  @connection [host: @host, port: @port, username: "dsm", password: "dsm"]
  @service "checkout"

  defmodule Pipeline do
    # The consumer under test: a real pipeline on the real producer. The
    # processor hands what Broadway delivered to the test process, which does
    # the asserting and the publishing.
    use Broadway

    def handle_message(_, message, _) do
      send(
        Application.fetch_env!(:dsm_test, :owner),
        {:handled, message.data, message.metadata, Context.get(), Context.get_extractor()}
      )

      message
    end
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)
    previous = Map.new(variables, &{&1, System.get_env(&1)})
    previous_extractors = Application.get_env(:dd_data_streams, :extractors)

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    System.delete_env("DD_ENV")
    System.delete_env("DD_DATA_STREAMS_HEADER")
    Application.put_env(:dd_data_streams, :extractors, [Extractor.RabbitMQ])
    Application.put_env(:dsm_test, :owner, self())

    {:ok, _} = Application.ensure_all_started(:amqp)
    {:ok, _} = Application.ensure_all_started(:broadway)
    DdDataStreams.attach()

    suffix = System.unique_integer([:positive])
    in_queue = "dsm-in-#{suffix}"
    out_queue = "dsm-out-#{suffix}"

    {:ok, connection} = AMQP.Connection.open(@connection)
    {:ok, channel} = AMQP.Channel.open(connection)

    # RabbitMQ 4 refuses transient queues by default, and `BroadwayRabbitMQ`
    # passes `declare: []` through to this same call, so both sides declare
    # durable.
    {:ok, _} = AMQP.Queue.declare(channel, in_queue, durable: true)
    {:ok, _} = AMQP.Queue.declare(channel, out_queue, durable: true)

    on_exit(fn ->
      DdDataStreams.detach()
      Context.clear()
      AMQP.Queue.delete(channel, in_queue)
      AMQP.Queue.delete(channel, out_queue)
      AMQP.Connection.close(connection)

      if previous_extractors,
        do: Application.put_env(:dd_data_streams, :extractors, previous_extractors),
        else: Application.delete_env(:dd_data_streams, :extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    %{channel: channel, in_queue: in_queue, out_queue: out_queue}
  end

  test "the queue delivers the typed header triples and the produce pair survives them", %{
    channel: channel,
    in_queue: in_queue,
    out_queue: out_queue
  } do
    header = DdDataStreams.Config.header()

    parent =
      Pathway.new(hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_000_000_000)

    AMQP.Basic.publish(channel, "", in_queue, "payload",
      headers: [{header, :longstr, Propagator.encode_str(parent)}]
    )

    start_supervised!(%{
      id: Pipeline,
      start:
        {Broadway, :start_link,
         [
           Pipeline,
           [
             name: Pipeline,
             producer: [
               module: {BroadwayRabbitMQ.Producer, producer_opts(in_queue)},
               concurrency: 1
             ],
             processors: [default: [concurrency: 1]],
             batchers: []
           ]
         ]}
    })

    assert_receive {:handled, "payload", metadata, pathway, extractor}, 20_000
    assert extractor == Extractor.RabbitMQ

    # The metadata BroadwayRabbitMQ built, handed to the extractor the way the
    # library hands it.
    info = %{producer: {BroadwayRabbitMQ.Producer, producer_opts(in_queue)}, topology: Pipeline}
    assert {:ok, consume_tags, parts} = Extractor.RabbitMQ.consume(metadata, info)

    assert consume_tags == ["direction:in", "topic:#{in_queue}", "type:rabbitmq"]
    assert Carrier.get(parts.attributes, header) == parent
    assert pathway.hash == expected_hash(consume_tags, parent.hash)
    assert pathway.pathway_start == parent.pathway_start

    # The pair `produce/3` returns still carries that pathway after RabbitMQ
    # has stored and returned the headers table. `produce/3` reads the pathway
    # from the process context, so putting the one the pipeline recorded back
    # here is the same hand-off a processor gets.
    Context.put(pathway)
    assert [{^header, encoded}] = DdDataStreams.produce(out_queue, "reply")

    AMQP.Basic.publish(channel, "", out_queue, "reply", headers: [{header, encoded}])

    # A `{key, value}` pair goes out and a typed triple comes back, which is
    # what the carrier is written for.
    assert {:ok, "reply", %{headers: headers}} = AMQP.Basic.get(channel, out_queue)
    assert {^header, :longstr, ^encoded} = List.keyfind(headers, header, 0)

    assert {:ok, produced} = Propagator.decode_str(encoded)
    assert produced.hash == expected_hash(produce_tags(out_queue), pathway.hash)
    assert produced.pathway_start == parent.pathway_start
  end

  defp producer_opts(queue) do
    [
      queue: queue,
      connection: @connection,
      metadata: [:headers, :timestamp],
      declare: [durable: true],
      on_failure: :reject,
      qos: [prefetch_count: 1]
    ]
  end

  defp produce_tags(queue) do
    Extractor.RabbitMQ.produce(queue, %{producer: nil, topology: nil}, [])
  end

  defp expected_hash(tags, parent_hash) do
    Pathway.node_hash(@service, "", nil, tags) |> Pathway.pathway_hash(parent_hash)
  end
end
