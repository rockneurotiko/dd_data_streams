defmodule DdDataStreams.Extractor.GooglePubSubTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Extractor.GooglePubSub

  test "a subscription resource name is reduced to its short id" do
    metadata = %{
      subscription: "projects/my-project/subscriptions/orders-sub",
      attributes: %{"origin" => "test"},
      orderingKey: "customer-1",
      received_at: 42
    }

    assert {:ok, tags, parts} = GooglePubSub.consume(metadata)

    assert tags == ["direction:in", "subscription:orders-sub", "type:google-pubsub"]

    assert parts == %{
             attributes: %{"origin" => "test"},
             ordering_key: "customer-1",
             received_at: 42,
             pathway_start: nil,
             edge_start: nil
           }
  end

  test "the publish time starts the checkpoint of a message that starts a pathway" do
    publish_time = ~U[2026-10-26 19:00:00.123456Z]

    assert {:ok, _tags, parts} =
             GooglePubSub.consume(%{subscription: "orders-sub", publishTime: publish_time})

    assert parts.pathway_start == DateTime.to_unix(publish_time, :native)
    assert parts.edge_start == parts.pathway_start
  end

  test "a publish time that is not a DateTime is dropped" do
    for publish_time <- [nil, "2026-10-26T19:00:00Z", 1_780_345_623_738] do
      assert {:ok, _tags, parts} =
               GooglePubSub.consume(%{subscription: "orders-sub", publishTime: publish_time})

      assert parts.pathway_start == nil
      assert parts.edge_start == nil
    end
  end

  test "an already-short subscription is reported as it is" do
    assert {:ok, tags, _parts} = GooglePubSub.consume(%{subscription: "orders-sub"})
    assert tags == ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  end

  test "a message without a subscription names no edge" do
    assert GooglePubSub.consume(%{}) == :error
    assert GooglePubSub.consume(%{subscription: ""}) == :error
  end

  test "a topic resource name is reduced to its short id" do
    assert GooglePubSub.produce("projects/my-project/topics/orders", %{
             producer: nil,
             topology: nil
           }) ==
             ["direction:out", "topic:orders", "type:google-pubsub"]
  end

  test "an already-short topic is reported as it is" do
    assert GooglePubSub.produce("orders", %{producer: nil, topology: nil}) == [
             "direction:out",
             "topic:orders",
             "type:google-pubsub"
           ]
  end
end
