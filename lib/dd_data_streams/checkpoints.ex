defmodule DdDataStreams.Checkpoints do
  @moduledoc """
  The consume and produce checkpoints of a message, without knowing the broker.

  `consume/3` continues the pathway a received message carries in its
  `dd-pathway-ctx-base64` header, or starts a new one when the header is absent
  or unusable, and measures the consume edge. `publish/4` measures the produce
  edge, writes the pathway that continues it into the outgoing headers, and
  hands the message to the caller's publish function. Neither function sends
  anything: each returns its checkpoint for `DdDataStreams.Aggregator.add/1`,
  which keeps the module pure and callable from anywhere in a pipeline.

  The edge tags are the caller's: this module records exactly the tags it is
  given, in the order it is given them. `DdDataStreams` builds them, from the
  configured broker type and the name the message metadata carries.

  ## Messages

  Both functions take the parts of a message the checkpoints need, as a map:

    * `:data` - the message body, required
    * `:attributes` - the message attributes, as a map: `nil` when the broker
      sent none
    * `:headers` - the message headers, as a list of `{key, value}` pairs, for a
      producer that exposes headers instead of attributes; used when
      `:attributes` is absent
    * `:ordering_key` - the ordering key, `nil` or `""` when it is unset

  These are the values `BroadwayCloudPubSub` v2 exposes as `message.data`,
  `message.metadata.attributes` and `message.metadata.orderingKey`, and the
  shape `BroadwayKafka` messages take once their `message.metadata.headers` list
  is passed as `:headers`. Extra keys are left alone, so a caller can pass its
  whole message map and get it back from `publish/4` with the pathway header
  added, in the same carrier it came in.

  ## Payload size

  The reported payload size in bytes is the data, the ordering key, and every
  attribute's or header's key and value. `nil` values count as zero. The produce
  size is measured before the pathway header is written, as dd-trace-go v2.13
  sizes an SQS send, so it is not inflated by this library. A consume size does
  include the inbound `dd-pathway-ctx-base64` entry (about 49 bytes); the two
  sides of one edge therefore differ by that header.

  ## Latency

  `consume/3` checkpoints at `received_at`, in native time units, so the consume
  edge latency is the time the message spent waiting in the bus rather than the
  time it spent queued inside Broadway. A message that carried no pathway starts
  at `received_at`, so both latencies are zero. `publish/4` checkpoints at the
  current time. Times are Unix milliseconds after conversion.

  ## Pathways

  A pathway with hash zero has never been checkpointed, and its first
  checkpoint starts it at the checkpoint time. Malformed input is not an error:
  an undecodable, truncated or non-base64 header is ignored and the message is
  consumed on a new pathway, as dd-trace-java does.

  `publish/4` returns the pathway the injected header carries. When one consumed
  message produces several messages, as a batch producer does, pass the
  *received* pathway to every `publish/4` call: each produce then has the
  consume hash as its parent and all of them share one produce node, instead of
  chaining produce edges onto each other.
  """

  alias DdDataStreams.{Config, Pathway, Propagator}

  @type message :: %{
          required(:data) => binary(),
          optional(:attributes) => %{optional(String.t()) => String.t()} | nil,
          optional(:headers) => [{String.t(), String.t()}] | nil,
          optional(:ordering_key) => binary() | nil
        }

  @doc """
  Sets the consume checkpoint for a message received on the edge described by
  `tags`, stamped at `received_at`.

  `tags` are the edge tags in full, direction included, such as
  `["direction:in", "subscription:orders-sub", "type:google-pubsub"]`;
  `DdDataStreams.consume/1` builds exactly that for a Broadway pipeline.

  Returns the pathway the checkpoint reached and the checkpoint itself, with
  its `:payload_size` in bytes. Hand the checkpoint to
  `DdDataStreams.Aggregator.add/1`, and the pathway to `publish/4` when this
  stage produces messages, so the produce edge continues the consumed one.
  """
  @spec consume(message(), [String.t()], integer()) :: {Pathway.t(), Pathway.checkpoint()}
  def consume(%{data: _} = message, tags, received_at)
      when is_list(tags) and is_integer(received_at) do
    {_field, carrier} = carrier(message)
    pathway = Propagator.get(carrier) || Pathway.new(%{})

    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, tags,
        now: System.convert_time_unit(received_at, :native, :millisecond),
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    {pathway, Map.put(checkpoint, :payload_size, payload_size(message))}
  end

  @doc """
  Sets the produce checkpoint for a message about to be published on the edge
  described by `tags`, injects the resulting pathway into the message's
  attributes or headers, and calls `publish_fun` with that message.

  `tags` are the edge tags in full, direction included, such as
  `["direction:out", "topic:orders", "type:google-pubsub"]`.

  `pathway` is the pathway the publish continues: the one `consume/3` returned
  for a message received earlier, or a new pathway when there is none. The
  checkpoint's `:payload_size` is the byte size of the message before the
  pathway header is written.

  `publish_fun` is the caller's own publish function. It receives the message
  map with `dd-pathway-ctx-base64` set, replacing any header the caller put
  under that name, and its return value is passed through:

      {pathway, checkpoint, :ok} =
        DdDataStreams.Checkpoints.publish(tags, message, pathway, fn outgoing ->
          MyApp.Publisher.publish(outgoing.data, attributes: outgoing.attributes)
        end)

      DdDataStreams.Aggregator.add(checkpoint)

  Returns the checkpointed produce pathway, the checkpoint, and whatever
  `publish_fun` returned. The returned pathway is the one in the injected
  header; keep the input pathway for any further publishes of the same batch.
  """
  @spec publish([String.t()], message(), Pathway.t(), (message() -> result)) ::
          {Pathway.t(), Pathway.checkpoint(), result}
        when result: var
  def publish(tags, %{data: _} = message, %Pathway{} = pathway, publish_fun)
      when is_list(tags) and is_function(publish_fun, 1) do
    payload_size = payload_size(message)

    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, tags,
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    {field, carrier} = carrier(message)
    published = Map.put(message, field, Propagator.put(carrier, pathway))
    checkpoint = Map.put(checkpoint, :payload_size, payload_size)

    {pathway, checkpoint, publish_fun.(published)}
  end

  # The carrier is whichever of the two shapes the message arrived with, so a
  # Kafka pipeline gets its header list back and a Pub/Sub pipeline its map.
  # A message with neither is published with an attributes map.
  @spec carrier(message()) :: {:attributes | :headers, map() | [{String.t(), String.t()}]}
  defp carrier(%{headers: headers}) when is_list(headers), do: {:headers, headers}
  defp carrier(message), do: {:attributes, Map.get(message, :attributes) || %{}}

  @spec payload_size(message()) :: non_neg_integer()
  defp payload_size(message) do
    {_field, carrier} = carrier(message)
    data_size = bytes(Map.get(message, :data))
    ordering_key_size = bytes(Map.get(message, :ordering_key))

    Enum.reduce(carrier, data_size + ordering_key_size, fn {key, value}, size ->
      size + bytes(key) + bytes(value)
    end)
  end

  # Attributes reach this library from a broker, so a value that is not a
  # string is measured as zero rather than crashing the pipeline.
  defp bytes(value) when is_binary(value), do: byte_size(value)
  defp bytes(_value), do: 0
end
