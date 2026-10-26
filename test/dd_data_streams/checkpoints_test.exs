defmodule DdDataStreams.CheckpointsTest do
  use ExUnit.Case, async: false

  alias DdDataStreams.Checkpoints
  alias DdDataStreams.Pathway

  @headers Path.expand("../fixtures/propagation/headers.txt", __DIR__)
           |> File.read!()
           |> Code.eval_string()
           |> elem(0)

  @service "checkout"
  @env "prod"

  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_PRIMARY_TAG DD_DATA_STREAMS_HEADER)
    previous = Map.new(variables, &{&1, System.get_env(&1)})

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_ENV", @env)
    System.delete_env("DD_PRIMARY_TAG")
    System.delete_env("DD_DATA_STREAMS_HEADER")

    on_exit(fn ->
      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  # The header dd-trace-py propagated in its own botocore test suite, i.e. a
  # value a real tracer put on a message rather than one this library encoded.
  defp reference_header do
    Enum.find(@headers, &(&1.source == :python_test_suite))
  end

  defp pubsub_message(attributes, opts \\ []) do
    %{
      data: Keyword.get(opts, :data, "hello"),
      attributes: attributes,
      ordering_key: Keyword.get(opts, :ordering_key, "customer-1")
    }
  end

  defp native(milliseconds) do
    System.convert_time_unit(milliseconds, :millisecond, :native)
  end

  defp node_hash(tags), do: Pathway.node_hash(@service, @env, "", tags)

  defp consume_hash(parent), do: Pathway.pathway_hash(node_hash(@consume_tags), parent)
  defp produce_hash(parent), do: Pathway.pathway_hash(node_hash(@produce_tags), parent)

  test "consume chains onto a pathway header written by another tracer" do
    header = reference_header()
    received_ms = header.edge_start + 42
    attributes = %{"origin" => "eu", "dd-pathway-ctx-base64" => header.base64}
    message = pubsub_message(attributes)

    {pathway, checkpoint} = Checkpoints.consume(message, @consume_tags, native(received_ms))

    assert checkpoint.tags == @consume_tags
    assert checkpoint.parent_hash == header.hash
    assert checkpoint.hash == consume_hash(header.hash)
    assert checkpoint.timestamp == received_ms
    assert checkpoint.edge_latency == 42
    assert checkpoint.pathway_latency == received_ms - header.pathway_start

    # data 5 + "origin"/"eu" 8 + the inbound header key 21 and value 28
    # + ordering key 10.
    assert checkpoint.payload_size == 5 + 8 + 21 + 28 + 10

    assert pathway.hash == checkpoint.hash
    assert pathway.pathway_start == header.pathway_start
    assert pathway.edge_start == received_ms
  end

  test "consume starts a fresh pathway when the header is absent or malformed" do
    received_ms = 1_780_345_623_738

    unusable = [
      nil,
      %{},
      %{"dd-pathway-ctx-base64" => ""},
      %{"dd-pathway-ctx-base64" => "not-base64!!"},
      %{"dd-pathway-ctx-base64" => Base.encode64(<<0::64>>)},
      %{"dd-pathway-ctx-base64" => Base.encode64(<<0::little-64, 0x80>>)}
    ]

    for attributes <- unusable do
      {pathway, checkpoint} =
        Checkpoints.consume(pubsub_message(attributes), @consume_tags, native(received_ms))

      assert checkpoint.parent_hash == 0
      assert checkpoint.hash == consume_hash(0)
      assert checkpoint.timestamp == received_ms
      assert checkpoint.pathway_latency == 0
      assert checkpoint.edge_latency == 0
      assert pathway.hash == checkpoint.hash
      assert pathway.pathway_start == received_ms
      assert pathway.edge_start == received_ms
    end
  end

  test "consume reports publish to consume latency from the broker's publish time" do
    received_ms = System.system_time(:millisecond)
    published_ms = received_ms - 40_000

    message = %{
      data: "hello",
      attributes: %{},
      ordering_key: "customer-1",
      pathway_start: native(published_ms),
      edge_start: native(published_ms)
    }

    {pathway, checkpoint} = Checkpoints.consume(message, @consume_tags, native(received_ms))

    assert checkpoint.parent_hash == 0
    assert checkpoint.timestamp == received_ms
    assert checkpoint.pathway_latency == 40_000
    assert checkpoint.edge_latency == 40_000
    assert pathway.pathway_start == published_ms
  end

  test "consume does not believe a publish time in the future" do
    received_ms = System.system_time(:millisecond)

    # Within a minute of this node's clock the timestamp is plausible and is
    # used; the latency simply clamps to zero.
    plausible = received_ms + 30_000

    {pathway, checkpoint} =
      Checkpoints.consume(
        %{data: "hello", attributes: %{}, pathway_start: native(plausible)},
        @consume_tags,
        native(received_ms)
      )

    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
    assert pathway.pathway_start == plausible

    # An hour ahead is garbage rather than skew, so the timestamp is dropped
    # and the pathway starts at the checkpoint time.
    implausible = received_ms + 3_600_000

    {pathway, checkpoint} =
      Checkpoints.consume(
        %{data: "hello", attributes: %{}, pathway_start: native(implausible)},
        @consume_tags,
        native(received_ms)
      )

    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
    assert pathway.pathway_start == received_ms
  end

  test "consume without a publish time keeps reporting zero latency" do
    received_ms = 1_780_345_623_738

    {pathway, checkpoint} =
      Checkpoints.consume(pubsub_message(%{}), @consume_tags, native(received_ms))

    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
    assert pathway.pathway_start == received_ms
    assert pathway.edge_start == received_ms
  end

  test "a publish time does not move the start a pathway header carries" do
    header = reference_header()
    received_ms = header.edge_start + 42
    published_ms = received_ms - 60_000

    message = %{
      data: "hello",
      attributes: %{"dd-pathway-ctx-base64" => header.base64},
      pathway_start: native(published_ms),
      edge_start: native(published_ms)
    }

    {pathway, checkpoint} = Checkpoints.consume(message, @consume_tags, native(received_ms))

    assert checkpoint.parent_hash == header.hash
    assert pathway.pathway_start == header.pathway_start
    assert checkpoint.pathway_latency == received_ms - header.pathway_start
    assert checkpoint.edge_latency == 42
  end

  test "produce chains the produce edge onto the consume checkpoint" do
    header = reference_header()
    {consumed, consume_checkpoint} = consume_reference(header)

    {pathway, checkpoint} = Checkpoints.produce(@produce_tags, consumed, 5)

    assert checkpoint.tags == @produce_tags
    assert checkpoint.parent_hash == consume_checkpoint.hash
    assert checkpoint.hash == produce_hash(consume_checkpoint.hash)
    assert checkpoint.payload_size == 5
    assert pathway.hash == checkpoint.hash
    assert pathway.pathway_start == consumed.pathway_start
    assert pathway.edge_start == checkpoint.timestamp
  end

  test "produce starts a fresh pathway when the caller has none" do
    {pathway, checkpoint} = Checkpoints.produce(@produce_tags, Pathway.new(%{}), 5)

    assert checkpoint.parent_hash == 0
    assert checkpoint.hash == produce_hash(0)
    assert pathway.pathway_start == checkpoint.timestamp
    assert pathway.edge_start == checkpoint.timestamp
  end

  test "produce reports the payload size it is given" do
    {_pathway, checkpoint} = Checkpoints.produce(@produce_tags, Pathway.new(%{}), 3)

    assert checkpoint.payload_size == 3
  end

  test "two produces from one consumed pathway share one produce node" do
    header = reference_header()
    {consumed, consume_checkpoint} = consume_reference(header)

    {_first_pathway, first} = Checkpoints.produce(@produce_tags, consumed, 5)
    {_second_pathway, second} = Checkpoints.produce(@produce_tags, consumed, 5)

    assert first.parent_hash == consume_checkpoint.hash
    assert second.parent_hash == consume_checkpoint.hash
    assert first.hash == second.hash
  end

  test "consume reads the configured header key" do
    Application.put_env(:dd_data_streams, :header, "x-pathway")
    on_exit(fn -> Application.delete_env(:dd_data_streams, :header) end)

    header = reference_header()
    attributes = %{"x-pathway" => header.base64}

    {_pathway, checkpoint} =
      Checkpoints.consume(
        pubsub_message(attributes),
        @consume_tags,
        native(header.edge_start + 1)
      )

    assert checkpoint.parent_hash == header.hash
  end

  test "a message carried in a header list is consumed in that shape" do
    header = reference_header()
    attributes = [{"traceparent", "00-abc"}, {"dd-pathway-ctx-base64", header.base64}]

    {consumed, inbound} =
      Checkpoints.consume(
        %{data: "hello", headers: attributes},
        @consume_tags,
        native(header.edge_start + 1)
      )

    assert inbound.parent_hash == header.hash
    assert consumed.hash == inbound.hash
  end

  defp consume_reference(header) do
    Checkpoints.consume(
      pubsub_message(%{"dd-pathway-ctx-base64" => header.base64}),
      @consume_tags,
      native(header.edge_start + 1)
    )
  end

  defp sqs_message(json, opts \\ []) do
    %{
      data: Keyword.get(opts, :data, "hello"),
      message_attributes: %{
        "_datadog" => %{
          name: "_datadog",
          data_type: "String",
          string_value: json,
          binary_value: "",
          value: json
        }
      },
      ordering_key: Keyword.get(opts, :ordering_key, "customer-1")
    }
  end

  test "consume reads a pathway from SQS _datadog message attributes" do
    header = reference_header()
    json = IO.iodata_to_binary(:json.encode(%{"dd-pathway-ctx-base64" => header.base64}))
    received_ms = header.edge_start + 42

    {pathway, checkpoint} =
      Checkpoints.consume(sqs_message(json), @consume_tags, native(received_ms))

    assert checkpoint.parent_hash == header.hash
    assert checkpoint.hash == consume_hash(header.hash)
    assert checkpoint.timestamp == received_ms
    assert checkpoint.edge_latency == 42
    assert checkpoint.payload_size == 5 + byte_size(json) + 10

    assert pathway.hash == checkpoint.hash
    assert pathway.pathway_start == header.pathway_start
  end

  test "consume leaves an SQS message without _datadog on a new pathway" do
    received_ms = 1_780_345_623_738

    {pathway, checkpoint} =
      Checkpoints.consume(
        %{data: "hello", message_attributes: %{}, ordering_key: "customer-1"},
        @consume_tags,
        native(received_ms)
      )

    assert checkpoint.parent_hash == 0
    assert checkpoint.payload_size == 5 + 0 + 10
    assert pathway.pathway_start == received_ms
  end

  test "Carrier.Sqs round-trips the pathway through _datadog" do
    alias DdDataStreams.Carrier.Sqs

    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
    key = "dd-pathway-ctx-base64"

    carrier = %{
      "_datadog" => %{
        name: "_datadog",
        data_type: "String",
        string_value: "{}",
        binary_value: "",
        value: "{}"
      }
    }

    updated = Sqs.put(carrier, pathway, key)
    assert Sqs.get(updated, key) == pathway
    assert Sqs.size(updated) > 0
  end
end
