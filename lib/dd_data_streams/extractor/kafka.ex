defmodule DdDataStreams.Extractor.Kafka do
  @moduledoc """
  The extractor for `BroadwayKafka` 0.7.1.

  A Kafka record names its consume edge by its topic, which `BroadwayKafka`
  stamps as `metadata.topic` together with `metadata.headers`, `metadata.key`
  and `metadata.ts`. A message that carries no topic records no consume
  checkpoint, because there is no name to report its edge under. It was checked
  against the source of `BroadwayKafka` 0.7.1 and brod 4.7.0, not against a live
  broker.

  Both edges are reported as `type:kafka`:

    * consume - `direction:in`, `topic:<topic>`, and `group:<consumer group>`
      when the producer's `:group_id` option is known
    * produce - `direction:out` and `topic:<topic>`

  The consumer group is read from the producer Broadway reports for the
  message, so it needs a core that hands the producer to `c:consume/2`; without
  one the `group:` tag is left out. `partition` and `offset` are not reported,
  as the Datadog tracers do not report them on a Data Streams edge either: a
  per-partition tag would name whichever partition arrived first for the node.
  The `kafka_cluster_id` tag the tracers add is not reported, because
  `BroadwayKafka` exposes no cluster id and rejects producer options it does not
  know; a node of this library therefore differs from a tracer node that reports
  one, though the pathway still connects through the header.

  The record headers are the carrier, so the pathway header round-trips with
  `DdDataStreams.Carrier.Flat`. A record without headers (`nil` or `:undefined`)
  is read as one with no inbound pathway. The Kafka key is the ordering key, so
  it counts towards the payload size as it does in the tracers.

  `metadata.ts` is a publish time, in Unix milliseconds: the producer's
  CreateTime, or the broker's LogAppendTime on a topic configured that way, and
  `BroadwayKafka` does not say which. It is offered as `:pathway_start` and
  `:edge_start`, converted to native time units, and never as `:received_at`,
  which stays the time the message reached the pipeline. A first-hop message
  therefore measures from it, as dd-trace-java does (dd-trace-go and
  dd-trace-py stamp the consume time); one that carries a pathway header keeps
  the start its producer wrote, and a consumer replaying a backlog reports the
  record's age. A timestamp that is `:undefined` (a magic-0 record) or not
  positive is dropped, as is one the core finds outside its window.
  """

  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(%{topic: topic} = metadata, producer_info)
      when is_binary(topic) and topic != "" do
    {:ok, ["direction:in", "topic:" <> topic, "type:kafka"] ++ group(producer_info),
     parts(metadata)}
  end

  def consume(_metadata, _producer_info), do: :error

  @impl true
  def produce(topic, _producer_info), do: ["direction:out", "topic:" <> topic, "type:kafka"]

  defp group(%{producer: {_module, opts}}) when is_list(opts) do
    case Keyword.get(opts, :group_id) do
      group when is_binary(group) and group != "" -> ["group:" <> group]
      _ -> []
    end
  end

  defp group(_producer_info), do: []

  defp parts(metadata) do
    published = published_at(metadata)

    %{
      attributes: headers(metadata),
      ordering_key: Map.get(metadata, :key),
      pathway_start: published,
      edge_start: published
    }
  end

  defp headers(%{headers: headers}) when is_list(headers), do: headers
  defp headers(_metadata), do: nil

  defp published_at(%{ts: milliseconds}) when is_integer(milliseconds) and milliseconds > 0,
    do: System.convert_time_unit(milliseconds, :millisecond, :native)

  defp published_at(_metadata), do: nil
end
