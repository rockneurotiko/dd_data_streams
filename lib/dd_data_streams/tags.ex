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

  A tag is split at its first colon, so a value may itself contain colons. Tags
  whose key is unknown, or that have no key or no colon at all, are not edge
  tags and are dropped from an edge. A tag that is not a string is dropped as
  well.
  """

  @hash_tags ~w(bus direction exchange topic type subscription kafka_cluster_id)
  @aggregation_tags ~w(ds.name ds.namespace manual_checkpoint group consumer_group)
  @value_tags ~w(partition has_routing_key)

  @type tier :: :edge | :hash | :aggregation

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

  # A tag that is not a string carries no key and no value, so it is not an
  # edge tag.
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
  tags that split a node's statistics without changing its identity. `:edge`
  keeps every known tag: the identifying ones, the aggregation ones that split
  a node's statistics, and the per-message values such as `partition` that are
  reported but not hashed.

      iex> tags = ["direction:in", "subscription:orders-sub", "partition:0", "group:workers"]
      iex> DdDataStreams.Tags.filter(tags, :hash)
      ["direction:in", "subscription:orders-sub"]
      iex> DdDataStreams.Tags.filter(tags, :aggregation)
      ["group:workers"]
      iex> DdDataStreams.Tags.filter(tags, :edge)
      ["direction:in", "subscription:orders-sub", "partition:0", "group:workers"]
  """
  @spec filter([String.t()], tier()) :: [String.t()]
  def filter(tags, :hash), do: Enum.filter(tags, &(key(&1) in @hash_tags))

  def filter(tags, :aggregation), do: Enum.filter(tags, &(key(&1) in @aggregation_tags))

  def filter(tags, :edge) do
    Enum.filter(
      tags,
      &(key(&1) in @hash_tags or key(&1) in @aggregation_tags or key(&1) in @value_tags)
    )
  end

  defp key(tag) do
    case parse(tag) do
      {:ok, {key, _value}} -> key
      :error -> nil
    end
  end
end
