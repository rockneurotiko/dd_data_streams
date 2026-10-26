defmodule DdDataStreams.Extractor do
  @moduledoc """
  The behaviour that tells the library what a Broadway message is.

  A Broadway producer hands its pipeline a `Broadway.Message` whose `metadata`
  map is whatever that producer decided to put there. Naming the edge a message
  was consumed on is therefore the one thing this library cannot do on its own:
  Pub/Sub reports the subscription it delivered from, Kafka the topic, SQS
  neither. An extractor answers that question, and there is one per broker.

  The extractors in use are the `:extractors` key of the application
  environment, a list defaulting to
  `[DdDataStreams.Extractor.GooglePubSub]`:

      config :dd_data_streams,
        extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub]

  An application that talks to several technologies lists one extractor per
  technology, and every message is given to them in order until one recognises
  its metadata: the first `c:consume/1` (or `c:consume/2`) that returns tags
  wins, so the more specific extractor goes first when two could claim the
  same message.

  ## Writing one

  `c:consume/1` gets the message's `metadata` and returns the consume edge tags
  together with the parts of the metadata a checkpoint measures, or `:error`
  when the metadata names no edge - the library then records nothing for that
  message rather than inventing a node. An extractor that raises is treated
  like one that named no edge, and the reason is logged and reported as a
  `[:dd_data_streams, :handler, :error]` telemetry event. `c:produce/2` gets
  the topic a message is published to and the `t:producer_info/0`, and returns
  the produce edge tags.

  `c:consume/1` is enough when the metadata alone names the edge. Some brokers
  keep the edge identity in the producer's configuration instead - the SQS
  queue, the Kafka consumer group, the RabbitMQ queue - and only Broadway knows
  which producer delivered a message. An extractor for such a broker implements
  `c:consume/2`, which receives `t:producer_info/0` next to the
  metadata; the library calls `c:consume/2` when the module exports it and
  `c:consume/1` otherwise, so adding it changes nothing for existing
  extractors. Both callbacks return the same things, and which of the two
  sources names the edge - metadata or producer info - is the extractor's own
  decision.

  Tags are `key:value` strings. A key `DdDataStreams.Tags` knows is hashed into
  the node or splits its statistics; one it does not know is reported on the
  edge and does neither. A consume edge needs `direction:in` and a `type:`; a
  produce edge `direction:out`, the topic and the same `type:`. The `type:` is
  the queueing technology Datadog files the stream under (`google-pubsub`,
  `kafka`, `sqs`, ...) and it is part of every node's identity, so it has to be
  the same value for every service in the pipeline.

  A `BroadwayKafka` pipeline, for example, stamps `metadata.topic`,
  `metadata.headers` and `metadata.ts`:

      defmodule MyApp.KafkaExtractor do
        @behaviour DdDataStreams.Extractor

        @impl true
        def consume(%{topic: topic} = metadata) when is_binary(topic) do
          {:ok, ["direction:in", "topic:" <> topic, "type:kafka"],
           %{
             attributes: Map.get(metadata, :headers),
             pathway_start:
               case Map.get(metadata, :ts) do
                 nil -> nil
                 milliseconds -> System.convert_time_unit(milliseconds, :millisecond, :native)
               end
           }}
        end

        def consume(_metadata), do: :error

        @impl true
        def produce(topic, _producer_info),
          do: ["direction:out", "topic:" <> topic, "type:kafka"]
      end

  The record timestamp the example reads is one of several publish times a
  broker stamps on a message: Pub/Sub's `publishTime`, Kafka's record timestamp,
  SQS's `SentTimestamp`, RabbitMQ's `:timestamp`. Returning one as
  `:pathway_start` makes a message that starts a pathway measure from it, so a
  pipeline that only ever consumes reports how long the message waited in the
  bus instead of zero. `:received_at` keeps its own meaning - when the message
  reached the pipeline - so a message with neither still reports zero latency,
  exactly as before.

  A pipeline that publishes several tags per edge, such as `partition:` or
  `group:`, adds them here; the keys in `DdDataStreams.Tags` decide whether they
  take part in the node's identity or only split its statistics.

  `c:produce/2` has no message metadata to look at, so it names every edge it
  is asked about. It does receive the `t:producer_info/0` of the message the
  stage is answering - the same one `c:consume/2` gets - so identity kept in
  the producer's configuration, such as a cluster id or an exchange, can still
  reach the produce tags; a stage that produces without consuming gets both
  fields nil. A produce in a stage that consumed a message is named by the
  extractor that consumed it, and a stage that produces without consuming
  passes its extractor to `DdDataStreams.produce/3`.
  """

  @typedoc """
  Fully built edge tags, direction included.

  A tag whose key `DdDataStreams.Tags` does not know is reported like any other:
  it takes part in no node's hash and splits no node's statistics.
  """
  @type tags :: [String.t()]

  @typedoc """
  What delivered the message, for `c:consume/2` and `c:produce/2`.

    * `:producer` - the Broadway producer as `{module, opts}`, the value the
      producer stamped into the message's metadata under `:producer`; `nil`
      when the producer stamps none (Broadway before 1.0.4) or the event was
      built by hand
    * `:topology` - the registered name of the Broadway pipeline the message
      is being delivered to, or `nil` when the checkpoint is taken outside a
      pipeline
  """
  @type producer_info :: %{producer: {module(), term()} | nil, topology: term() | nil}

  @typedoc """
  The parts of a message's metadata a checkpoint measures, under the names the
  library uses. Every key is optional.

    * `:attributes` - the inbound attributes as a map, or the inbound headers as
      a list of `{key, value}` pairs, carrying `dd-pathway-ctx-base64`
    * `:ordering_key` - the ordering key, counted towards the payload size
    * `:received_at` - when the message reached the pipeline, in native time
      units; `nil` (or absent) means the consume checkpoint is stamped now and
      reports no consume latency. `DdDataStreams.consume/1` turns `nil` into the
      current time before calling `DdDataStreams.Checkpoints.consume/3`, which
      requires an integer
    * `:pathway_start` - the broker's publish time for a first-hop message, in
      native time units, used as the pathway's start so the checkpoint reports
      publish to consume latency. `nil` (or absent) leaves the pathway starting
      at `received_at`, which reports zero latency. It is only taken from a
      message that starts a pathway: one carrying a pathway header already
      carries the start its producer wrote. A time before 2000-01-01 or more
      than a minute ahead of this node's clock is dropped rather than believed,
      so a garbage broker timestamp cannot report a decades-long latency or a
      start in the future
    * `:edge_start` - the same publish time taken as the consume edge's start,
      under the same rules; `nil` (or absent) leaves the edge starting at the
      checkpoint time
  """
  @type parts :: %{
          optional(:attributes) => map() | [{String.t(), String.t()}] | nil,
          optional(:ordering_key) => term(),
          optional(:received_at) => integer() | nil,
          optional(:pathway_start) => integer() | nil,
          optional(:edge_start) => integer() | nil
        }

  @doc """
  The consume edge of a message Broadway is about to deliver, from its metadata.

  Returns the edge tags and the metadata parts a checkpoint measures, or
  `:error` when the metadata names no edge for this broker. An extractor that
  raises is treated like one that returned `:error`, and the reason is logged
  and reported as a `[:dd_data_streams, :handler, :error]` telemetry event.
  """
  @callback consume(metadata :: map()) :: {:ok, tags(), parts()} | :error

  @doc """
  The consume edge of a message, from its metadata and the producer that
  delivered it.

  Optional: an extractor that exports this callback is called with two
  arguments instead of `c:consume/1`. Use it when the edge identity lives in
  the producer's configuration rather than in the message metadata. Everything
  else is `c:consume/1`'s contract: the return values, the tags, the parts, and
  `:error` when the metadata and producer together name no edge.
  """
  @callback consume(metadata :: map(), producer_info :: producer_info()) ::
              {:ok, tags(), parts()} | :error

  @doc """
  The produce edge of a message published to `topic`.

  `producer_info` is what delivered the message this produce answers: the same
  `t:producer_info/0` `c:consume/2` receives, so an extractor can report
  identity from the producer's configuration on the produce edge too. A stage
  that produces without consuming passes both fields nil.
  """
  @callback produce(topic :: String.t(), producer_info :: producer_info()) :: tags()

  @optional_callbacks consume: 2
end
