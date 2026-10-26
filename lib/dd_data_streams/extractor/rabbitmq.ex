defmodule DdDataStreams.Extractor.RabbitMQ do
  @moduledoc """
  The extractor for `BroadwayRabbitMQ` (tested against 0.8.2, over `amqp`).

  A consumed AMQP message does not say which queue it came from: the metadata
  holds no queue and neither does the channel. The queue is only in the
  producer's `queue:` option, which Broadway reports on the event that delivered
  the message, so this extractor implements `c:DdDataStreams.Extractor.consume/2`
  and names the consume edge by that queue. It never names it by the exchange or
  by `:routing_key`: both describe where the publisher sent the message, so a
  queue bound to several exchanges would be split into several nodes. dd-trace-java
  and dd-trace-py name the edge by the queue as well. A producer configured with
  a server-named queue (`queue: ""`) names no edge, because the real name is
  never visible to the pipeline. Checked against the source of
  `BroadwayRabbitMQ` 0.8.2, `amqp` and the AMQP 0-9-1 specification, not against
  a live broker.

  ## Edges

    * consume - `direction:in`, `topic:<queue>`, `type:rabbitmq`
    * produce to the default exchange (empty or no `:exchange`) with a routing
      key - `direction:out`, `topic:<routing key>`, `type:rabbitmq`, because the
      default exchange delivers to the queue named by the key
    * produce to a named exchange - `direction:out`, `exchange:<name>`,
      `manual_checkpoint:false`, `has_routing_key:<true|false>`, `type:rabbitmq`

  A produce edge is named by two values, so the host passes them as options of
  `DdDataStreams.produce/3`, which reach `c:DdDataStreams.Extractor.produce/3`
  unchanged:

      DdDataStreams.produce("orders", payload,
        extractor: DdDataStreams.Extractor.RabbitMQ,
        exchange: "orders-x",
        routing_key: "created"
      )

  Without `:exchange` the publish goes to the default exchange, and without
  `:routing_key` the topic argument stands in for it. A call whose options carry
  no usable exchange or key is still named rather than rejected: the default
  exchange with an empty key reports `exchange:` with an empty name, as both
  tracers do.

  ## Host options

  `BroadwayRabbitMQ` copies no AMQP properties into the message by default, so
  the pipeline must list what this extractor reads:

      producer: [
        module: {BroadwayRabbitMQ.Producer, queue: "orders-q", metadata: [:headers, :timestamp]},
        ...
      ]

  `:headers` is required for the pathway to arrive at all; `:timestamp` is
  optional and only feeds publish latency. The queue must be non-empty.

  A header table is the list `amqp` delivers, `[{name, type, value}]`, and is
  returned as `:attributes`, so the carrier reads the pathway from the last
  element of the triple. `:undefined` - what `amqp` delivers when the publisher
  sent no headers or no timestamp - and any other non-list is treated as absent.
  To publish the pathway, put the pair `produce/3` returns into `headers:` of
  `AMQP.Basic.publish/5`: `amqp` casts a `{name, binary}` pair to a long string
  itself.

  ## Publish time

  `:timestamp` is set by the publisher, in epoch seconds, and is usually absent.
  When it is an integer it is converted to native units and offered as the first
  hop's `:pathway_start` and `:edge_start`; it is never the receive time.

  ## Divergences from the official tracers

    * The produce edge follows dd-trace-java (`topic:` for a default-exchange
      publish with a key; exchange, `manual_checkpoint:false` and
      `has_routing_key` otherwise). dd-trace-py always reports `exchange:`,
      even when empty, and `has_routing_key:` without `topic:` or
      `manual_checkpoint:`, so a Python publisher appears as a different node;
      the pathway still connects through the hash carried on the message.
    * A consume checkpoint is recorded for every message, where dd-trace-java
      records one only when the delivery has a headers table.
    * dd-trace-py ignores the publish timestamp; dd-trace-java and this
      extractor use it.
  """

  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(metadata, %{producer: {_module, opts}}) when is_list(opts) and is_map(metadata) do
    case Keyword.get(opts, :queue) do
      queue when is_binary(queue) and queue != "" ->
        {:ok, ["direction:in", "topic:" <> queue, "type:rabbitmq"], parts(metadata)}

      _ ->
        :error
    end
  end

  def consume(_metadata, _producer_info), do: :error

  @impl true
  def produce(topic, _producer_info, opts) do
    opts = if Keyword.keyword?(opts), do: opts, else: []
    exchange = name(Keyword.get(opts, :exchange)) || ""
    key = name(Keyword.get(opts, :routing_key)) || if(exchange == "", do: name(topic))
    tags(exchange, key)
  end

  # The topic stands in for the key only on the default exchange: on a named
  # exchange it would claim a routing key the publish does not have.
  defp tags("", key) when is_binary(key) and key != "",
    do: ["direction:out", "topic:" <> key, "type:rabbitmq"]

  defp tags(exchange, key) do
    [
      "direction:out",
      "exchange:" <> exchange,
      "manual_checkpoint:false",
      "has_routing_key:" <> to_string(is_binary(key) and key != ""),
      "type:rabbitmq"
    ]
  end

  defp name(value) when is_binary(value), do: value
  defp name(_value), do: nil

  defp parts(metadata) do
    publish_time = publish_time(Map.get(metadata, :timestamp))

    %{
      attributes: headers(Map.get(metadata, :headers)),
      pathway_start: publish_time,
      edge_start: publish_time
    }
  end

  # amqp delivers :undefined when the publisher sent no headers table.
  defp headers(headers) when is_list(headers), do: headers
  defp headers(_headers), do: nil

  # The AMQP timestamp is publisher-set epoch seconds, or :undefined.
  defp publish_time(seconds) when is_integer(seconds),
    do: System.convert_time_unit(seconds, :second, :native)

  defp publish_time(_timestamp), do: nil
end
