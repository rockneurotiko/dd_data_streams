defmodule DdDataStreams.Tags do
  @moduledoc """
  The canonical tag vocabulary, encoded as `key:value` strings.

  Tags fall into three tiers:

    * hashed - `bus`, `direction`, `exchange`, `topic`, `type`, `subscription`,
      `kafka_cluster_id` identify a node and take part in its hash
    * aggregation - `ds.name`, `ds.namespace`, `manual_checkpoint`, `group`,
      `consumer_group` split a node's statistics without changing its identity
    * values - `partition`, `has_routing_key` describe one message and are
      reported, never hashed

  A tag is split at its first colon, so a value may itself contain colons. A
  key outside all three tiers is not a reason to drop a tag: it is reported on
  the edge like any other, it takes part in no node's hash and it splits no
  node's statistics. Only `filter/2` is selective, and only about the `:hash`
  and `:aggregation` tiers.

  A tag is always a string. A tag that is not a binary carries no key and no
  value, so `DdDataStreams.checkpoint/1,2` rejects it at the boundary, the
  extractors filter it out of a message's tags, and the aggregator drops any
  that reach it before the flush.
  """

  @hash_tags ~w(bus direction exchange topic type subscription kafka_cluster_id)
  @aggregation_tags ~w(ds.name ds.namespace manual_checkpoint group consumer_group)

  @type tier :: :hash | :aggregation

  @doc """
  Splits an encoded tag into its key and value.

      iex> DdDataStreams.Tags.parse("subscription:orders-sub")
      {:ok, {"subscription", "orders-sub"}}

      iex> DdDataStreams.Tags.parse("topic:orders:eu")
      {:ok, {"topic", "orders:eu"}}

      iex> DdDataStreams.Tags.parse("orders")
      :error
  """
  @spec parse(String.t()) :: {:ok, {String.t(), String.t()}} | :error
  def parse(tag) when is_binary(tag) do
    case String.split(tag, ":", parts: 2) do
      [key, value] when key != "" -> {:ok, {key, value}}
      _ -> :error
    end
  end

  # A tag that is not a string carries no key and no value.
  def parse(_tag), do: :error

  @doc """
  Encodes a key and value as a tag.

      iex> DdDataStreams.Tags.encode({"type", "google-pubsub"})
      "type:google-pubsub"
  """
  @spec encode({String.t(), String.t()}) :: String.t()
  def encode({key, value}), do: "#{key}:#{value}"

  @doc """
  Keeps the tags that belong to a tier, preserving their order.

  `:hash` keeps only the tags that identify a node. `:aggregation` keeps the
  tags that split a node's statistics without changing its identity. Neither
  tier keeps a tag whose key is unknown, nor one that has no key at all; such a
  tag is still reported, it simply does not belong to either tier.

      iex> tags = ["direction:in", "subscription:orders-sub", "partition:0", "group:workers"]
      iex> DdDataStreams.Tags.filter(tags, :hash)
      ["direction:in", "subscription:orders-sub"]
      iex> DdDataStreams.Tags.filter(tags, :aggregation)
      ["group:workers"]
  """
  @spec filter([String.t()], tier()) :: [String.t()]
  def filter(tags, :hash), do: Enum.filter(tags, &(key(&1) in @hash_tags))

  def filter(tags, :aggregation), do: Enum.filter(tags, &(key(&1) in @aggregation_tags))

  defp key(tag) do
    case parse(tag) do
      {:ok, {key, _value}} -> key
      :error -> nil
    end
  end
end
