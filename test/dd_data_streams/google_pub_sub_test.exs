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
             received_at: 42
           }
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
    assert GooglePubSub.produce("projects/my-project/topics/orders") ==
             ["direction:out", "topic:orders", "type:google-pubsub"]
  end

  test "an already-short topic is reported as it is" do
    assert GooglePubSub.produce("orders") == [
             "direction:out",
             "topic:orders",
             "type:google-pubsub"
           ]
  end
end
