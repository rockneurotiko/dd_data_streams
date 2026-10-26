defmodule DdDataStreams.Extractor.SQSTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.{Carrier, Checkpoints, Pathway}
  alias DdDataStreams.Extractor.SQS

  @url "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
  @sent_ms 1_700_000_123_456
  @datadog ~s({"dd-pathway-ctx-base64":"6gAAAAAAAACAoKv++WKA2uKB+mI="})
  @tags ["direction:in", "topic:orders", "type:sqs"]

  @metadata %{
    attributes: %{"message_group_id" => "customer-1", "sent_timestamp" => @sent_ms},
    message_attributes: %{
      "_datadog" => %{
        name: "_datadog",
        data_type: "String",
        string_value: @datadog,
        binary_value: "",
        value: @datadog
      }
    },
    receipt_handle: "AQEBzbVv...",
    message_id: "5fea7756-0ea4-451a-a703-a558b933e274",
    md5_of_body: "fafb00f5732ab283681e124bf8747ed1"
  }

  defp info(url, module \\ BroadwaySQS.Producer) do
    %{
      producer: {module, [queue_url: url, message_attribute_names: ["_datadog"]]},
      topology: Orders
    }
  end

  defp native(milliseconds), do: System.convert_time_unit(milliseconds, :millisecond, :native)

  test "consume names the queue from the producer and offers the attributes and send time" do
    assert {:ok, @tags, parts} = SQS.consume(@metadata, info(@url))

    assert parts == %{
             attributes: @metadata.message_attributes,
             pathway_start: native(@sent_ms),
             edge_start: native(@sent_ms)
           }
  end

  test "the queue is the last path segment of the queue URL" do
    for {url, queue} <- [
          {"https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo", "orders.fifo"},
          {"http://localhost:4566/000000000000/orders/", "orders"},
          {"orders", "orders"}
        ] do
      assert {:ok, tags, _parts} = SQS.consume(@metadata, info(url))
      assert tags == ["direction:in", "topic:" <> queue, "type:sqs"]
    end
  end

  test "two queues are two edges" do
    {:ok, orders, _} = SQS.consume(@metadata, info(@url))

    {:ok, refunds, _} =
      SQS.consume(@metadata, info("https://sqs.eu-west-1.amazonaws.com/1/refunds"))

    refute orders == refunds
  end

  test "no usable producer names no edge" do
    assert SQS.consume(@metadata, %{producer: nil, topology: nil}) == :error
    assert SQS.consume(@metadata, info(@url, Other.Producer)) == :error
    assert SQS.consume(@metadata, info("")) == :error
    assert SQS.consume(@metadata, info("/")) == :error
    assert SQS.consume(@metadata, info(nil)) == :error

    assert SQS.consume(@metadata, %{producer: {BroadwaySQS.Producer, []}, topology: nil}) ==
             :error
  end

  test "metadata without requested attributes offers no start" do
    metadata = %{message_id: "m1", attributes: [], message_attributes: []}

    assert {:ok, @tags, parts} = SQS.consume(metadata, info(@url))
    assert parts == %{attributes: [], pathway_start: nil, edge_start: nil}

    assert {:ok, _tags, parts} =
             SQS.consume(%{attributes: %{"sent_timestamp" => "1700000123456"}}, info(@url))

    assert parts.pathway_start == nil
  end

  test "produce reports the same queue for a URL and a bare name" do
    expected = ["direction:out", "topic:orders", "type:sqs"]
    none = %{producer: nil, topology: nil}

    assert SQS.produce(@url, none) == expected
    assert SQS.produce("orders", none) == expected
  end

  test "the _datadog attribute round-trips through the carrier into a checkpoint" do
    parent = Pathway.new(hash: 234, pathway_start: @sent_ms, edge_start: @sent_ms)
    attributes = Carrier.Sqs.put(%{}, parent)

    assert {:ok, @tags, parts} = SQS.consume(%{message_attributes: attributes}, info(@url))

    {_pathway, checkpoint} =
      Checkpoints.consume(Map.put(parts, :data, "hello"), @tags, native(@sent_ms + 50))

    assert checkpoint.parent_hash == 234
    assert checkpoint.payload_size == 5 + Carrier.Sqs.size(attributes)
  end

  test "a literal BroadwaySQS header is read, and a missing one starts a pathway" do
    {:ok, @tags, parts} = SQS.consume(@metadata, info(@url))

    {_pathway, checkpoint} =
      Checkpoints.consume(Map.put(parts, :data, "hello"), @tags, native(@sent_ms))

    assert checkpoint.parent_hash == 234
    assert checkpoint.payload_size == 5 + byte_size(@datadog)

    {:ok, @tags, parts} = SQS.consume(%{message_attributes: %{}}, info(@url))

    {_pathway, checkpoint} =
      Checkpoints.consume(Map.put(parts, :data, "hello"), @tags, native(@sent_ms))

    assert checkpoint.parent_hash == 0
  end
end
