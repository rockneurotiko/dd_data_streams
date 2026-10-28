defmodule DdDataStreams.KafkaBrokerTest do
  # A real `BroadwayKafka.Producer` against a real Kafka, so the metadata the
  # extractor reads is the metadata `brod` actually builds and the headers are
  # the record headers Kafka actually stores.
  #
  # This is the `kafka` tier: excluded from `mix test`, run with
  #
  #   just kafka
  #
  # which starts the `kafka` service from `compose.yaml`, runs this file and
  # leaves the container up when the tier fails.
  use ExUnit.Case, async: false

  alias DdDataStreams.{Carrier, Context, Extractor, Pathway, Propagator}

  @moduletag :kafka

  @host System.get_env("DSM_KAFKA_HOST", "127.0.0.1")
  @port String.to_integer(System.get_env("DSM_KAFKA_PORT", "9092"))
  @endpoints [{@host, @port}]
  @service "checkout"
  @published_at 1_700_000_000_123

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

  defmodule Sink do
    # Reads the produced message back off the out topic with the same real
    # client, so the header is the one Kafka stored, not one built in the test.
    use Broadway

    def handle_message(_, message, _) do
      send(Application.fetch_env!(:dsm_test, :owner), {:received, message.data, message.metadata})
      message
    end
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:brod)

    {:ok, client} =
      :brod.start_link_client(@endpoints, __MODULE__.Publisher, auto_start_producers: true)

    on_exit(fn -> :brod.stop_client(client) end)
    %{client: client}
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)
    previous = Map.new(variables, &{&1, System.get_env(&1)})
    previous_extractors = Application.get_env(:dd_data_streams, :extractors)

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    System.delete_env("DD_ENV")
    System.delete_env("DD_DATA_STREAMS_HEADER")
    Application.put_env(:dd_data_streams, :extractors, [Extractor.Kafka])
    Application.put_env(:dsm_test, :owner, self())

    {:ok, _} = Application.ensure_all_started(:broadway)
    DdDataStreams.attach()

    suffix = System.unique_integer([:positive])
    in_topic = "dsm-in-#{suffix}"
    out_topic = "dsm-out-#{suffix}"

    :ok =
      :brod.create_topics(
        @endpoints,
        [
          %{
            name: in_topic,
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: []
          },
          %{
            name: out_topic,
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: []
          }
        ],
        %{timeout: 10_000}
      )

    on_exit(fn ->
      DdDataStreams.detach()
      Context.clear()

      Application.delete_env(:dd_data_streams, :extractors)

      if previous_extractors,
        do: Application.put_env(:dd_data_streams, :extractors, previous_extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    %{in_topic: in_topic, out_topic: out_topic}
  end

  test "the record delivers its headers and the produce pair survives the log", %{
    client: client,
    in_topic: in_topic,
    out_topic: out_topic
  } do
    header = DdDataStreams.Config.header()

    parent =
      Pathway.new(hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_000_000_000)

    publish(client, in_topic, "payload", [{header, Propagator.encode_str(parent)}])

    start_pipeline(Pipeline, in_topic, "dsm-in-group-#{System.unique_integer([:positive])}")

    assert_receive {:handled, "payload", metadata, pathway, extractor}, 30_000
    assert extractor == Extractor.Kafka

    # The metadata `brod` built, handed to the extractor the way the library
    # hands it.
    info = %{
      producer: {BroadwayKafka.Producer, producer_opts(in_topic, "g0")},
      topology: Pipeline
    }

    assert {:ok, consume_tags, parts} = Extractor.Kafka.consume(metadata, info)
    assert "direction:in" in consume_tags
    assert "type:kafka" in consume_tags
    assert "topic:#{in_topic}" in consume_tags
    assert "group:g0" in consume_tags
    assert Carrier.get(parts.attributes, header) == parent

    # Kafka keeps the producer's timestamp, and the pipeline reads it back.
    assert metadata.ts == @published_at
    assert pathway.hash == expected_hash(consume_tags, parent.hash)
    assert pathway.pathway_start == parent.pathway_start

    # The pairs `produce/3` returns are read back off the out topic, headers and
    # all, by a real consumer of the same client. `produce/3` reads the pathway
    # from the process context, so putting the one the pipeline recorded back
    # here is the same hand-off a processor gets.
    Context.put(pathway)
    assert [{^header, encoded} | topic_pairs] = DdDataStreams.produce(out_topic, "reply")

    publish(client, out_topic, "reply", [{header, encoded} | topic_pairs])

    start_pipeline(Sink, out_topic, "dsm-out-group-#{System.unique_integer([:positive])}")

    assert_receive {:received, "reply", metadata}, 30_000
    assert {^header, ^encoded} = List.keyfind(metadata.headers, header, 0)

    assert {:ok, produced} = Propagator.decode_str(encoded)
    assert produced.hash == expected_hash(produce_tags(out_topic), pathway.hash)
    assert produced.pathway_start == parent.pathway_start
  end

  defp start_pipeline(module, topic, group_id) do
    start_supervised!(%{
      id: module,
      start:
        {Broadway, :start_link,
         [
           module,
           [
             name: module,
             producer: [
               module: {BroadwayKafka.Producer, producer_opts(topic, group_id)},
               concurrency: 1
             ],
             processors: [default: [concurrency: 1]],
             batchers: []
           ]
         ]}
    })
  end

  defp producer_opts(topic, group_id) do
    [
      hosts: @endpoints,
      group_id: group_id,
      topics: [topic],
      offset_reset_policy: :earliest,
      begin_offset: :reset,
      receive_interval: 100
    ]
  end

  defp publish(client, topic, value, headers) do
    :ok =
      :brod.produce_sync(client, topic, 0, "", %{
        value: value,
        headers: headers,
        ts: @published_at
      })
  end

  defp produce_tags(topic) do
    Extractor.Kafka.produce(topic, %{producer: nil, topology: nil})
  end

  defp expected_hash(tags, parent_hash) do
    Pathway.node_hash(@service, "", nil, tags) |> Pathway.pathway_hash(parent_hash)
  end
end
