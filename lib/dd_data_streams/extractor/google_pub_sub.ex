defmodule DdDataStreams.Extractor.GooglePubSub do
  @moduledoc """
  The extractor for `BroadwayCloudPubSub` v2.

  A Pub/Sub message names its consume edge by its subscription, and v2 stamps it
  as `metadata.subscription` together with `metadata.attributes`,
  `metadata.orderingKey` and `metadata.received_at`, which is all a checkpoint
  needs. A message that carries no subscription - the shape
  `BroadwayCloudPubSub` v1 produces - records no consume checkpoint, because
  there is no name to report its edge under.

  Consume checkpoints therefore require `broadway_cloud_pub_sub` 2.0.0-rc or
  newer, tested with 2.0.0-rc.2. On stable 1.0.0 the v1 metadata has no
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
      received_at: Map.get(metadata, :received_at)
    }
  end
end
