defmodule DdDataStreams.Extractor.GooglePubSubTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Extractor.GooglePubSub
  alias DdDataStreams.{Pathway, Propagator}

  test "a message that carries the produced topic reports it next to the subscription" do
    metadata = %{
      subscription: "projects/my-project/subscriptions/orders-sub",
      attributes: %{"origin" => "test", Propagator.topic_header() => "orders"}
    }

    assert {:ok, tags, _parts} = GooglePubSub.consume(metadata)

    assert tags == [
             "direction:in",
             "subscription:orders-sub",
             "topic:orders",
             "type:google-pubsub"
           ]

    assert is_binary(Propagator.topic_header())
  end

  test "a topic attribute that is not a string is ignored" do
    for topic <- [nil, 42, ""] do
      assert {:ok, tags, _parts} =
               GooglePubSub.consume(%{
                 subscription: "orders-sub",
                 attributes: %{Propagator.topic_header() => topic}
               })

      assert tags == ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
    end
  end

  test "the topic tag changes the consume node hash once" do
    {:ok, without_topic, _parts} = GooglePubSub.consume(%{subscription: "orders-sub"})

    {:ok, with_topic, _parts} =
      GooglePubSub.consume(%{
        subscription: "orders-sub",
        attributes: %{Propagator.topic_header() => "orders"}
      })

    assert Pathway.node_hash("checkout", "prod", nil, without_topic) ==
             7_951_410_199_073_889_140

    assert Pathway.node_hash("checkout", "prod", nil, with_topic) == 720_515_027_144_984_330
  end

  test "a subscription resource name is reduced to its short id" do
    metadata = %{
      subscription: "projects/my-project/subscriptions/orders-sub",
      attributes: %{"origin" => "test"},
      orderingKey: "customer-1",
      receivedAt: ~U[2026-10-26 19:00:00Z]
    }

    assert {:ok, tags, parts} = GooglePubSub.consume(metadata)

    assert tags == ["direction:in", "subscription:orders-sub", "type:google-pubsub"]

    assert parts == %{
             attributes: %{"origin" => "test"},
             ordering_key: "customer-1",
             received_at: DateTime.to_unix(~U[2026-10-26 19:00:00Z], :native),
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
