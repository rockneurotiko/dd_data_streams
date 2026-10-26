defmodule DdDataStreams.Extractor.RabbitMQTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Carrier
  alias DdDataStreams.Extractor.RabbitMQ
  alias DdDataStreams.Propagator

  @info %{producer: {BroadwayRabbitMQ.Producer, [queue: "orders-q"]}, topology: nil}
  @none %{producer: nil, topology: nil}

  # The map BroadwayRabbitMQ 0.8.2 delivers with metadata: [:headers, :timestamp].
  defp metadata(headers, timestamp),
    do: %{headers: headers, timestamp: timestamp, amqp_channel: :channel}

  test "the queue from the producer names the consume edge" do
    headers = [{"dd-pathway-ctx-base64", :longstr, "abc"}]

    assert {:ok, tags, parts} = RabbitMQ.consume(metadata(headers, :undefined), @info)
    assert tags == ["direction:in", "topic:orders-q", "type:rabbitmq"]
    assert parts == %{attributes: headers, pathway_start: nil, edge_start: nil}
  end

  test "the exchange and routing key never name the consume edge" do
    metadata =
      Map.merge(metadata([], :undefined), %{exchange: "orders-x", routing_key: "created"})

    assert {:ok, ["direction:in", "topic:orders-q", "type:rabbitmq"], _} =
             RabbitMQ.consume(metadata, @info)
  end

  test "the pathway is read from an amqp header triple" do
    key = DdDataStreams.Config.header()
    pathway = DdDataStreams.Pathway.new(%{})
    headers = [{"x", :longstr, "1"}, {key, :longstr, Propagator.encode_str(pathway)}]

    assert {:ok, _tags, %{attributes: attributes}} =
             RabbitMQ.consume(metadata(headers, :undefined), @info)

    assert Carrier.Flat.get(attributes, key) == pathway
  end

  test "undefined headers and timestamp mean no attributes and no start" do
    assert {:ok, _tags, parts} = RabbitMQ.consume(metadata(:undefined, :undefined), @info)
    assert parts == %{attributes: nil, pathway_start: nil, edge_start: nil}
  end

  test "a message without headers or timestamp keys is tolerated" do
    assert {:ok, _tags, parts} = RabbitMQ.consume(%{}, @info)
    assert parts == %{attributes: nil, pathway_start: nil, edge_start: nil}
  end

  test "the timestamp is epoch seconds converted to native units" do
    assert {:ok, _tags, parts} = RabbitMQ.consume(metadata([], 1_700_000_000), @info)
    native = System.convert_time_unit(1_700_000_000, :second, :native)
    assert parts.pathway_start == native
    assert parts.edge_start == native
    refute Map.has_key?(parts, :received_at)
  end

  test "a timestamp that is not an integer is dropped" do
    for timestamp <- [nil, "1700000000", 1.7e9] do
      assert {:ok, _tags, %{pathway_start: nil}} =
               RabbitMQ.consume(metadata([], timestamp), @info)
    end
  end

  test "no producer, no queue or a server-named queue names no edge" do
    assert RabbitMQ.consume(metadata([], :undefined), @none) == :error

    for opts <- [[], [queue: ""], [queue: nil], :not_a_list] do
      info = %{producer: {BroadwayRabbitMQ.Producer, opts}, topology: nil}
      assert RabbitMQ.consume(metadata([], :undefined), info) == :error
    end
  end

  test "the default exchange with a routing key is named by the key" do
    expected = ["direction:out", "topic:orders-q", "type:rabbitmq"]

    assert RabbitMQ.produce("ignored", @none, exchange: "", routing_key: "orders-q") == expected
    assert RabbitMQ.produce("ignored", @none, routing_key: "orders-q") == expected
  end

  test "the topic stands in for the key on the default exchange only" do
    assert RabbitMQ.produce("orders-q", @none, []) ==
             ["direction:out", "topic:orders-q", "type:rabbitmq"]

    assert RabbitMQ.produce("orders-q", @none, exchange: "orders-x") == [
             "direction:out",
             "exchange:orders-x",
             "manual_checkpoint:false",
             "has_routing_key:false",
             "type:rabbitmq"
           ]
  end

  test "a named exchange is named by the exchange" do
    assert RabbitMQ.produce("t", @info, exchange: "orders-x", routing_key: "created") == [
             "direction:out",
             "exchange:orders-x",
             "manual_checkpoint:false",
             "has_routing_key:true",
             "type:rabbitmq"
           ]

    assert RabbitMQ.produce("t", @info, exchange: "orders-x", routing_key: "") == [
             "direction:out",
             "exchange:orders-x",
             "manual_checkpoint:false",
             "has_routing_key:false",
             "type:rabbitmq"
           ]
  end

  test "a default exchange with no key reports an empty exchange name" do
    assert RabbitMQ.produce("", @none, []) == [
             "direction:out",
             "exchange:",
             "manual_checkpoint:false",
             "has_routing_key:false",
             "type:rabbitmq"
           ]
  end

  test "unusable options never crash" do
    assert RabbitMQ.produce("orders-q", @none, :nope) ==
             ["direction:out", "topic:orders-q", "type:rabbitmq"]

    assert [_ | _] = RabbitMQ.produce("orders-q", @none, exchange: :atom, routing_key: 1)
  end
end
