defmodule DdDataStreams.TagsTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Tags

  doctest DdDataStreams.Tags

  test "parse/1 splits at the first colon" do
    assert Tags.parse("subscription:orders-sub") == {:ok, {"subscription", "orders-sub"}}
    assert Tags.parse("topic:orders:eu") == {:ok, {"topic", "orders:eu"}}
    assert Tags.parse("type:") == {:ok, {"type", ""}}
  end

  test "parse/1 rejects anything that is not key and value" do
    assert Tags.parse("") == :error
    assert Tags.parse("orders") == :error
    assert Tags.parse(":orders") == :error
    assert Tags.parse(":") == :error
  end

  test "parse/1 rejects a tag that is not a string" do
    assert Tags.parse(:not_a_string) == :error
    assert Tags.parse(nil) == :error
  end

  test "encode/1 is the inverse of parse/1" do
    assert Tags.encode({"direction", "in"}) == "direction:in"
    assert Tags.encode({"topic", "orders:eu"}) == "topic:orders:eu"

    assert Tags.parse(Tags.encode({"topic", "orders:eu"})) == {:ok, {"topic", "orders:eu"}}
  end

  test "filter/2 :hash keeps only the tags that identify a node, in order" do
    tags = [
      "direction:in",
      "subscription:orders-sub",
      "partition:0",
      "group:workers",
      "has_routing_key:true",
      "ds.name:orders"
    ]

    assert Tags.filter(tags, :hash) == ["direction:in", "subscription:orders-sub"]
  end

  test "filter/2 :aggregation keeps the tags that split a node's statistics" do
    tags = [
      "direction:in",
      "subscription:orders-sub",
      "partition:0",
      "group:workers",
      "has_routing_key:true",
      "ds.name:orders"
    ]

    assert Tags.filter(tags, :aggregation) == ["group:workers", "ds.name:orders"]
  end

  test "filter/2 :edge keeps the identifying, aggregation and per-message tags" do
    tags = [
      "direction:in",
      "subscription:orders-sub",
      "partition:0",
      "group:workers",
      "has_routing_key:true",
      "ds.name:orders"
    ]

    assert Tags.filter(tags, :edge) == [
             "direction:in",
             "subscription:orders-sub",
             "partition:0",
             "group:workers",
             "has_routing_key:true",
             "ds.name:orders"
           ]
  end

  test "unknown and malformed tags are dropped, duplicates are kept" do
    tags = ["type:google-pubsub", "orders", ":", "unknown:value", "type:google-pubsub"]

    assert Tags.filter(tags, :hash) == ["type:google-pubsub", "type:google-pubsub"]
    assert Tags.filter(tags, :edge) == ["type:google-pubsub", "type:google-pubsub"]
  end
end
