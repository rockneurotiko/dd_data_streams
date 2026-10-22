defmodule DdDataStreams.Integrations.GooglePubSub do
  @moduledoc """
  Produce and consume checkpoints for Google Pub/Sub messages.

  `consume/3` continues the pathway a received message carries in its
  `dd-pathway-ctx-base64` attribute, or starts a new one when the attribute is
  absent or unusable, and measures the consume edge. `publish/4` measures the
  produce edge, writes the pathway that continues it into the outgoing
  attributes, and hands the message to the caller's publish function. Neither
  function sends anything: each returns its checkpoint for
  `DdDataStreams.Aggregator.add/1`, which keeps the module pure and callable
  from anywhere in a pipeline.

  ## Messages

  Both functions take the parts of a message the checkpoints need, as a map:

    * `:data` - the message body, required
    * `:attributes` - the message attributes, `nil` when the broker sent none
    * `:ordering_key` - the ordering key, `nil` or `""` when it is unset

  These are exactly the values `BroadwayCloudPubSub` v2 exposes as
  `message.data`, `message.metadata.attributes` and `message.metadata.orderingKey`.
  Extra keys are left alone, so a caller can pass its whole message map and get
  it back from `publish/4` with the pathway attribute added.

  ## Edge tags

  A consume edge carries `direction:in`, `subscription:<subscription>` and
  `type:google-pubsub`, in that order. A produce edge carries `direction:out`,
  `topic:<topic>` and `type:google-pubsub`. The subscription is used verbatim,
  so a short name and a `projects/<project>/subscriptions/<id>` resource name
  stay distinguishable; the topic is the identifier the caller publishes to,
  normally the short topic id.

  The produce tags are what dd-trace-java and dd-trace-py report for Google
  Pub/Sub. dd-trace-java reports the same consume tags, from
  `createWithSubscription("google-pubsub", INBOUND, ...)` in `PubSubDecorator`
  (commit 05531e5), which reduces a subscription resource name to its id.
  dd-trace-py puts the subscription under the `topic:` key instead
  (`edge_tags = ["direction:in", f"topic:{subscription}", "type:google-pubsub"]`
  in `ddtrace/internal/datastreams/google_cloud_pubsub.py`, commit abbffc3), and
  its own comment there notes the difference. So a Python consumer and this
  library name the consume node differently; a Java consumer and this library
  agree unless the subscription is given as a resource name.

  ## Payload size

  The reported payload size in bytes is the data, the ordering key, and every
  attribute's key and value. `nil` values count as zero. The produce size is
  measured before the pathway attribute is written, as dd-trace-go v2.13 sizes
  an SQS send, so it is not inflated by this library. A consume size does
  include the inbound `dd-pathway-ctx-base64` entry (about 49 bytes); the two
  sides of one edge therefore differ by that header.

  ## Latency

  `consume/3` checkpoints at `received_at`, the native time unit
  `BroadwayCloudPubSub` stamps when a message reaches the pipeline, so the
  consume edge latency is the time the message spent waiting in the
  subscription rather than the time it spent queued inside Broadway. A message
  that carried no pathway starts at `received_at`, so both latencies are zero.
  `publish/4` checkpoints at the current time. Times are Unix milliseconds
  after conversion.

  ## Pathways

  A pathway with hash zero has never been checkpointed, and its first
  checkpoint starts it at the checkpoint time. Malformed input is not an error:
  an undecodable, truncated or non-base64 attribute is ignored and the message
  is consumed on a new pathway, as dd-trace-java does.

  `publish/4` returns the pathway the injected attribute carries. When one
  consumed message produces several messages, as a batch producer does, pass
  the *received* pathway to every `publish/4` call: each produce then has the
  consume hash as its parent and all of them share one produce node, instead of
  chaining produce edges onto each other.
  """

  alias DdDataStreams.{Config, Pathway, Propagator}

  @type message :: %{
          required(:data) => binary(),
          optional(:attributes) => %{optional(String.t()) => String.t()} | nil,
          optional(:ordering_key) => binary() | nil
        }

  @doc """
  Sets the consume checkpoint for a message received from `subscription`.

  `received_at` is when the message reached the pipeline, in native time units
  (`System.system_time/0`). The checkpoint's tags are `direction:in`,
  `subscription:<subscription>` and `type:google-pubsub`.

  Returns the pathway the checkpoint reached and the checkpoint itself, with
  its `:payload_size` in bytes. Hand the checkpoint to
  `DdDataStreams.Aggregator.add/1`, and the pathway to `publish/4` when this
  stage produces messages, so the produce edge continues the consumed one.
  """
  @spec consume(message(), String.t(), integer()) :: {Pathway.t(), Pathway.checkpoint()}
  def consume(%{data: _} = message, subscription, received_at)
      when is_binary(subscription) and is_integer(received_at) do
    pathway = Propagator.get(Map.get(message, :attributes)) || Pathway.new(%{})

    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, consume_tags(subscription),
        now: System.convert_time_unit(received_at, :native, :millisecond),
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    {pathway, Map.put(checkpoint, :payload_size, payload_size(message))}
  end

  @doc """
  Sets the produce checkpoint for a message about to be published to `topic`,
  injects the resulting pathway into the message's attributes, and calls
  `publish_fun` with that message.

  `pathway` is the pathway the publish continues: the one `consume/3` returned
  for a message received earlier, or a new pathway when there is none. The
  checkpoint's tags are `direction:out`, `topic:<topic>` and
  `type:google-pubsub`, and its `:payload_size` is the byte size of the message
  before the pathway attribute is written.

  `publish_fun` is the caller's own publish function. It receives the message
  map with `dd-pathway-ctx-base64` set, replacing any attribute the caller put
  under that name, and its return value is passed through:

      {pathway, checkpoint, :ok} =
        DdDataStreams.Integrations.GooglePubSub.publish(topic, message, pathway, fn outgoing ->
          MyApp.Publisher.publish(topic, outgoing.data, attributes: outgoing.attributes)
        end)

      DdDataStreams.Aggregator.add(checkpoint)

  Returns the checkpointed produce pathway, the checkpoint, and whatever
  `publish_fun` returned. The returned pathway is the one in the injected
  attribute; keep the input pathway for any further publishes of the same
  batch.
  """
  @spec publish(String.t(), message(), Pathway.t(), (message() -> result)) ::
          {Pathway.t(), Pathway.checkpoint(), result}
        when result: var
  def publish(topic, %{data: _} = message, %Pathway{} = pathway, publish_fun)
      when is_binary(topic) and is_function(publish_fun, 1) do
    payload_size = payload_size(message)

    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, produce_tags(topic),
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    attributes =
      message
      |> Map.get(:attributes)
      |> then(&(&1 || %{}))
      |> Propagator.put(pathway)

    published = Map.put(message, :attributes, attributes)
    checkpoint = Map.put(checkpoint, :payload_size, payload_size)

    {pathway, checkpoint, publish_fun.(published)}
  end

  @spec consume_tags(String.t()) :: [String.t()]
  defp consume_tags(subscription) do
    ["direction:in", "subscription:" <> subscription, "type:google-pubsub"]
  end

  @spec produce_tags(String.t()) :: [String.t()]
  defp produce_tags(topic) do
    ["direction:out", "topic:" <> topic, "type:google-pubsub"]
  end

  @spec payload_size(message()) :: non_neg_integer()
  defp payload_size(message) do
    data_size = bytes(Map.get(message, :data))
    ordering_key_size = bytes(Map.get(message, :ordering_key))
    attributes = Map.get(message, :attributes) || %{}

    Enum.reduce(attributes, data_size + ordering_key_size, fn {key, value}, size ->
      size + bytes(key) + bytes(value)
    end)
  end

  # Attributes reach this library from a broker, so a value that is not a
  # string is measured as zero rather than crashing the pipeline.
  defp bytes(value) when is_binary(value), do: byte_size(value)
  defp bytes(_value), do: 0
end
