defmodule DdDataStreams.Extractor.GooglePubSub do
  @moduledoc """
  The extractor for `BroadwayCloudPubSub` v2.

  A Pub/Sub message names its consume edge by its subscription, and v2 stamps it
  as `metadata.subscription` together with `metadata.attributes`,
  `metadata.orderingKey` and `metadata.receivedAt`, which is what a checkpoint
  needs to name the edge. A message that carries no subscription - the shape
  `BroadwayCloudPubSub` v1 produces - records no consume checkpoint, because
  there is no name to report its edge under.

  Consume checkpoints therefore require `broadway_cloud_pub_sub` 2.0.0-rc or
  newer, tested with 2.0.0-rc.3, the first to stamp the receive time as
  `metadata.receivedAt`, a `%DateTime{}`. On stable 1.0.0 the v1 metadata has no
  `:subscription`, so `consume/1` returns `:error` for every message and a v1
  pipeline records nothing.

  Both edges are reported as `type:google-pubsub`, the technology Datadog files
  these streams under. Pub/Sub names its subscriptions and topics as full
  resource names (`projects/<project>/subscriptions/<id>`); this extractor
  reduces each to its short id, which is what dd-trace-java reports for Pub/Sub
  (both edges). dd-trace-py reports the full resource path of the subscription
  under `topic:` instead, so a Py consumer appears as a different node; the
  pathway still connects, because the link between two nodes is the hash
  carried on the message. An already-short name is reported as it is.

  The publish time Pub/Sub stamps on a message - `metadata.publishTime`, a
  `%DateTime{}` in v2 - starts the consume checkpoint of a message that starts a
  pathway, so a pipeline that only consumes reports how long the message waited
  in the bus instead of zero. A message that carries a pathway header keeps the
  start its producer wrote, because a first-hop start is only offered to a
  pathway that has not started. A publish time that is absent, is not a
  `%DateTime{}`, or falls outside the window a clock could plausibly have
  stamped is dropped rather than believed.
  """

  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(metadata) do
    case Map.get(metadata, :subscription) do
      subscription when is_binary(subscription) and subscription != "" ->
        {:ok, tags("in", "subscription:" <> short_id(subscription)), parts(metadata)}

      _ ->
        :error
    end
  end

  @impl true
  def produce(topic, _producer_info), do: tags("out", "topic:" <> short_id(topic))

  # The short id is the last segment of a resource name, and the whole value
  # when it carries no slashes.
  defp short_id(resource), do: resource |> String.split("/") |> List.last()

  defp tags(direction, name), do: ["direction:" <> direction, name, "type:google-pubsub"]

  defp parts(metadata) do
    %{
      attributes: Map.get(metadata, :attributes),
      ordering_key: Map.get(metadata, :orderingKey),
      received_at: received_at(metadata),
      pathway_start: publish_time(metadata),
      edge_start: publish_time(metadata)
    }
  end

  defp received_at(%{receivedAt: %DateTime{} = received_at}),
    do: DateTime.to_unix(received_at, :native)

  defp received_at(_metadata), do: nil

  # v2 stamps the publish time as a DateTime. Anything else - v1 metadata, a
  # hand-built fixture, a broken producer - leaves both starts nil, which is
  # what such a message reported before: zero latency rather than a wrong one.
  defp publish_time(%{publishTime: %DateTime{} = publish_time}),
    do: DateTime.to_unix(publish_time, :native)

  defp publish_time(_metadata), do: nil
end
