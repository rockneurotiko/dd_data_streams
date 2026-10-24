defmodule DdDataStreams.Extractor.GooglePubSub do
  @moduledoc """
  The extractor for `BroadwayCloudPubSub` v2.

  A Pub/Sub message names its consume edge by its subscription, and v2 stamps it
  as `metadata.subscription` together with `metadata.attributes`,
  `metadata.orderingKey` and `metadata.received_at`, which is all a checkpoint
  needs. A message that carries no subscription - the shape
  `BroadwayCloudPubSub` v1 produces - records no consume checkpoint, because
  there is no name to report its edge under.

  Both edges are reported as `type:google-pubsub`, the technology Datadog files
  these streams under. The produce edge names its topic with `topic:<topic>`,
  which is what dd-trace-java and dd-trace-py report for Pub/Sub.
  """

  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(metadata) do
    case Map.get(metadata, :subscription) do
      subscription when is_binary(subscription) and subscription != "" ->
        {:ok, tags("in", "subscription:" <> subscription), parts(metadata)}

      _ ->
        :error
    end
  end

  @impl true
  def produce(topic), do: tags("out", "topic:" <> topic)

  defp tags(direction, name), do: ["direction:" <> direction, name, "type:google-pubsub"]

  defp parts(metadata) do
    %{
      attributes: Map.get(metadata, :attributes),
      ordering_key: Map.get(metadata, :orderingKey),
      received_at: Map.get(metadata, :received_at)
    }
  end
end
