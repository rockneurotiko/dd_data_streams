defmodule DdDataStreams.Extractor.KafkaTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Extractor.Kafka
  alias DdDataStreams.Tags

  @producer_info %{producer: nil, topology: nil}

  # The metadata BroadwayKafka 0.7.1 builds in wrap_message/4.
  @metadata %{
    topic: "orders",
    partition: 2,
    offset: 18_422,
    key: "customer-7",
    ts: 1_761_500_123_456,
    headers: [{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}]
  }

  test "a record names its consume edge by topic, without partition or offset" do
    native = System.convert_time_unit(1_761_500_123_456, :millisecond, :native)

    assert {:ok, tags, parts} = Kafka.consume(@metadata, @producer_info)
    assert tags == ["direction:in", "topic:orders", "type:kafka"]

    assert parts == %{
             attributes: @metadata.headers,
             ordering_key: "customer-7",
             pathway_start: native,
             edge_start: native
           }

    refute Map.has_key?(parts, :received_at)
  end

  test "the consumer group comes from the producer options" do
    info = %{
      producer: {BroadwayKafka.Producer, [group_id: "g1", topics: ["orders"]]},
      topology: Pipe
    }

    assert {:ok, tags, _parts} = Kafka.consume(@metadata, info)
    assert tags == ["direction:in", "topic:orders", "type:kafka", "group:g1"]
    assert Tags.filter(tags, :aggregation) == ["group:g1"]
    refute "group:g1" in Tags.filter(tags, :hash)
  end

  test "a producer without a group adds no group tag" do
    for producer <- [
          {BroadwayKafka.Producer, []},
          {BroadwayKafka.Producer, [group_id: ""]},
          {Other, :opts}
        ] do
      assert {:ok, tags, _parts} = Kafka.consume(@metadata, %{producer: producer, topology: nil})
      assert tags == ["direction:in", "topic:orders", "type:kafka"]
    end
  end

  test "the null shapes of a record are dropped, not crashed on" do
    metadata = %{topic: "orders", ts: :undefined, headers: nil, key: <<>>}

    assert {:ok, _tags, parts} = Kafka.consume(metadata, @producer_info)

    assert parts == %{attributes: nil, ordering_key: "", pathway_start: nil, edge_start: nil}

    assert {:ok, _tags, parts} =
             Kafka.consume(%{topic: "orders", headers: :undefined}, @producer_info)

    assert parts.attributes == nil
    assert parts.ordering_key == nil
  end

  test "a timestamp that is not positive is dropped" do
    for ts <- [-1, 0, "1761500123456", 1.5] do
      assert {:ok, _tags, parts} = Kafka.consume(%{topic: "orders", ts: ts}, @producer_info)
      assert parts.pathway_start == nil
      assert parts.edge_start == nil
    end
  end

  test "the record timestamp is a publish time in milliseconds, converted to native" do
    assert {:ok, _tags, parts} = Kafka.consume(%{topic: "orders", ts: 1000}, @producer_info)
    assert parts.pathway_start == System.convert_time_unit(1000, :millisecond, :native)
    assert parts.edge_start == parts.pathway_start
  end

  test "a record with no topic names no edge" do
    assert Kafka.consume(%{}, @producer_info) == :error
    assert Kafka.consume(%{topic: ""}, @producer_info) == :error
    assert Kafka.consume(%{topic: :orders}, @producer_info) == :error
  end

  test "the produce edge is named by topic" do
    assert Kafka.produce("orders-out", @producer_info) ==
             ["direction:out", "topic:orders-out", "type:kafka"]
  end
end
