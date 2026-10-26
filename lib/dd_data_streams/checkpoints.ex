defmodule DdDataStreams.Checkpoints do
  @moduledoc """
  The consume and produce checkpoints of a message, without knowing the broker.

  `consume/3` continues the pathway a received message carries in its
  configured pathway header, or starts a new one when the header is absent or
  unusable, and measures the consume edge. `produce/3` measures the produce
  edge and returns the checkpointed pathway together with its checkpoint; it
  does not encode, send or mutate anything. Neither function sends anything:
  each returns its checkpoint for `DdDataStreams.Aggregator.add/1`. `consume/3`
  reads `DdDataStreams.Config` for the service, environment, primary tag and
  header key, so the module is not strictly pure, but it performs no I/O and
  sends nothing, and stays callable from anywhere in a pipeline.

  The edge tags are the caller's: this module records exactly the tags it is
  given, in the order it is given them. `DdDataStreams` builds them, from the
  configured broker type and the name the message metadata carries.

  ## Messages

  `consume/3` takes the parts of an inbound message the checkpoints need, as a
  map:

    * `:data` - the message body, required
    * `:attributes` - the message attributes, as a map: `nil` when the broker
      sent none
    * `:headers` - the message headers, as a list of `{key, value}` pairs, for a
      producer that exposes headers instead of attributes; used when
      `:attributes` is absent
    * `:message_attributes` - the SQS message attributes, as the map
      BroadwaySQS exposes under `message.metadata.message_attributes`; the
      pathway is read from its `_datadog` JSON attribute
    * `:ordering_key` - the ordering key, `nil` or `""` when it is unset

  These are the values `BroadwayCloudPubSub` v2 exposes as `message.data`,
  `message.metadata.attributes` and `message.metadata.orderingKey`, and the
  shape `BroadwayKafka` messages take once their `message.metadata.headers` list
  is passed as `:headers`. Extra keys are left alone. A message that carries
  headers is read through them, and headers take precedence over attributes; a
  header this library cannot read is measured as empty. Otherwise an attributes
  value that is neither a map nor a list is read as an empty attributes map, and
  a list of attributes is read as header pairs.

  `produce/3` takes no message: the caller measures the outgoing payload, or
  lets `DdDataStreams.produce/3` measure its `data`.

  ## Payload size

  The consume size in bytes is the data, the ordering key, and every attribute's
  or header's key and value. `nil` values count as zero. It is measured before
  the pathway is read, so it includes the inbound header (about 49 bytes).

  The produce size is the caller's: `DdDataStreams.produce/3` passes the
  `:payload_size` option when given, or the size of `data` otherwise, and adds
  the size of the pathway header it is about to write - the configured key plus
  the encoded value - so a produce measures the same message the consume side
  measures. A caller that measures its message itself measures it without that
  header.

  ## Latency

  `consume/3` checkpoints at `received_at`, in native time units, so the consume
  edge latency is the time the message spent waiting in the bus rather than the
  time it spent queued inside Broadway. A message that carried no pathway starts
  at `received_at`, so both latencies are zero. `produce/3` checkpoints at the
  current time. Times are Unix milliseconds after conversion.

  ## Pathways

  A pathway with hash zero has never been checkpointed, and its first
  checkpoint starts it at the checkpoint time. Malformed input is not an error:
  an undecodable, truncated or non-base64 header is ignored and the message is
  consumed on a new pathway, as dd-trace-java does. A header that is present
  but cannot be decoded is reported as a
  `[:dd_data_streams, :checkpoint, :skip]` telemetry event with reason
  `:malformed_header`, so a consumer can tell a header that was lost or broken
  on the way from the ordinary start of a new pathway.

  `produce/3` returns the pathway the caller encodes into the message. When one
  consumed message produces several messages, as a batch producer does, pass the
  *received* pathway to every `produce/3` call: each produce then has the
  consume hash as its parent and all of them share one produce node, instead of
  chaining produce edges onto each other.
  """

  alias DdDataStreams.{Carrier, Config, Pathway}

  @skip_event [:dd_data_streams, :checkpoint, :skip]

  @type message :: %{
          required(:data) => binary(),
          optional(:attributes) => %{optional(String.t()) => String.t()} | nil,
          optional(:headers) => [{String.t(), String.t()}] | nil,
          optional(:ordering_key) => binary() | nil
        }

  @doc """
  Sets the consume checkpoint for a message received on the edge described by
  `tags`, stamped at `received_at` (in native time units).

  `tags` are the edge tags in full, direction included, such as
  `["direction:in", "subscription:orders-sub", "type:google-pubsub"]`;
  `DdDataStreams.consume/1` builds exactly that for a Broadway pipeline.

  Returns the pathway the checkpoint reached and the checkpoint itself, with
  its `:payload_size` in bytes. Hand the checkpoint to
  `DdDataStreams.Aggregator.add/1`, and the pathway to `produce/3` when this
  stage produces messages, so the produce edge continues the consumed one.
  """
  @spec consume(message(), [String.t()], integer()) :: {Pathway.t(), Pathway.checkpoint()}
  def consume(%{data: _} = message, tags, received_at)
      when is_list(tags) and is_integer(received_at) do
    {_field, carrier} = carrier(message)
    pathway = continued(Carrier.fetch(carrier, Config.header()))

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
  described by `tags` and returns the checkpointed pathway together with its
  checkpoint.

  `tags` are the edge tags in full, direction included, such as
  `["direction:out", "topic:orders", "type:google-pubsub"]`.

  `pathway` is the pathway the produce continues: the one `consume/3` returned
  for a message received earlier, or a new pathway when there is none. The
  checkpoint's `:payload_size` is `payload_size`, which the caller measures on
  the message it sends; `DdDataStreams.produce/3` adds the size of the pathway
  header it writes on top, so here too the message is measured without it.

  This function records nothing and encodes nothing. Hand the checkpoint to
  `DdDataStreams.Aggregator.add/1`, encode the returned pathway with the
  configured header, and add that pair to the outgoing message:

      {pathway, checkpoint} =
        DdDataStreams.Checkpoints.produce(tags, pathway, byte_size(data))

      DdDataStreams.Aggregator.add(checkpoint)

      {DdDataStreams.Config.header(), DdDataStreams.Propagator.encode_str(pathway)}

  Keep the input pathway for any further produces of the same batch: every
  produce then has the consumed hash as its parent and they share one produce
  node.
  """
  @spec produce([String.t()], Pathway.t(), non_neg_integer()) ::
          {Pathway.t(), Pathway.checkpoint()}
  def produce(tags, %Pathway{} = pathway, payload_size)
      when is_list(tags) and is_integer(payload_size) and payload_size >= 0 do
    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, tags,
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    {pathway, Map.put(checkpoint, :payload_size, payload_size)}
  end

  # The carrier is whichever of the two shapes the message arrived with, so a
  # Kafka pipeline reads its header list back and a Pub/Sub pipeline its map.
  # A message with neither, or with attributes of a shape the library cannot
  # read, is read as an empty attributes map.
  @spec carrier(message()) :: {:attributes | :headers | :message_attributes, map() | [tuple()]}
  @doc false
  def carrier(%{headers: headers}) when is_list(headers), do: {:headers, headers}

  def carrier(%{message_attributes: attributes}) when is_map(attributes),
    do: {:message_attributes, attributes}

  def carrier(%{message_attributes: attributes}) when is_list(attributes),
    do: {:message_attributes, attributes}

  def carrier(%{attributes: attributes}) when is_map(attributes), do: {:attributes, attributes}
  def carrier(%{attributes: attributes}) when is_list(attributes), do: {:attributes, attributes}
  def carrier(_message), do: {:attributes, %{}}

  @spec payload_size(message()) :: non_neg_integer()
  defp payload_size(message) do
    {_field, carrier} = carrier(message)

    bytes(Map.get(message, :data)) +
      bytes(Map.get(message, :ordering_key)) +
      Carrier.size(carrier)
  end

  # A header the producer wrote that this consumer cannot read is reported, so
  # broken propagation is distinguishable from the ordinary start of a new
  # pathway; a message that carries no header at all starts one in silence.
  @spec continued({:ok, Pathway.t()} | :absent | {:error, :malformed}) :: Pathway.t()
  defp continued({:ok, pathway}), do: pathway
  defp continued(:absent), do: Pathway.new(%{})

  defp continued({:error, :malformed}) do
    :telemetry.execute(@skip_event, %{count: 1}, %{reason: :malformed_header, extractors: nil})

    Pathway.new(%{})
  end

  # Attributes reach this library from a broker, so a value that is not a
  # string is measured as zero rather than crashing the pipeline.
  defp bytes(value) when is_binary(value), do: byte_size(value)
  defp bytes(_value), do: 0
end
