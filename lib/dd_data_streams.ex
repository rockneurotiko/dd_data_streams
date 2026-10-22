defmodule DdDataStreams do
  @moduledoc """
  Data Streams Monitoring for Google Pub/Sub pipelines.

  Datadog's Data Streams Monitoring follows a payload from the service that
  produced it to the service that consumed it, so it can report how long the
  payload waited in the queue between them and how the services are connected.
  Doing that needs a checkpoint on each side of every edge and a pathway carried
  on the message itself. This module supplies the Pub/Sub side of that and the
  Broadway glue that decides where a checkpoint belongs; the pathway hashing,
  propagation, aggregation and transport live in the modules it calls.

  ## Using it in a Broadway pipeline

  The library is off until it is enabled, and a Broadway pipeline reaches it
  through telemetry instead of through a dependency:

      # DD_DATA_STREAMS_ENABLED=true in the environment, then at start up:
      DdDataStreams.attach(subscription: "orders-sub")

      # in handle_message/3 or handle_batch/4:
      def handle_message(_processor, message, _context) do
        DdDataStreams.publish("orders-out", %{data: outgoing_data}, &MyApp.Publisher.publish/1)
        message
      end

  `attach/1` installs two telemetry handlers:

    * `[:broadway, :processor, :message, :start]`, which Broadway emits once for
      each message it is about to hand to `handle_message/3`. The handler
      records the consume checkpoint - the one checkpoint per message, whatever
      the pipeline does with it afterwards, including a leaf consumer that
      produces nothing - and leaves the resulting pathway in
      `DdDataStreams.Context` for a produce call in the same stage process.
    * `[:broadway, :batch_processor, :start]`, once per batch. The handler
      samples one of the pathways the batch's messages were consumed on and
      leaves that in `DdDataStreams.Context`, so a produce call in
      `handle_batch/4` continues a pathway that connects to the messages being
      answered. Fan-in is a sample rather than a join, which is what
      dd-trace-go v2.13 does in `Merge`/`MergeContexts`: it keeps one pathway.

  Both handlers record only what each event is: the batch handler adds no
  consume checkpoint, because every message of the batch already got one at its
  own message event.

  ## Pathways that are not a Broadway stage

  `consume/2` and `publish/3` are the same two checkpoints as functions, for a
  pipeline that drives the library itself, and `checkpoint/2` records one for a
  stage that is neither a Pub/Sub consume nor a Pub/Sub produce.

  Checkpoints are handed to `DdDataStreams.Aggregator.add/1`, which does nothing
  when the aggregator is not running, so none of these needs to be guarded at
  the call site while the library is disabled.

  ## Latency

  A consume checkpoint is stamped with the time BroadwayCloudPubSub recorded for
  the message's arrival (`message.metadata.received_at`), not the time the
  processor picked it up, so the consume edge reports the time the message spent
  in the subscription rather than Broadway's own backlog.
  """

  alias DdDataStreams.{Aggregator, Config, Context, Pathway}
  alias DdDataStreams.Integrations.GooglePubSub

  @handler_id :dd_data_streams
  @consume_event [:broadway, :processor, :message, :start]
  @fan_in_event [:broadway, :batch_processor, :start]
  @events [@consume_event, @fan_in_event]

  @doc """
  Installs the Broadway telemetry handlers that record consume checkpoints and
  fan-in pathways.

  Options:

    * `:subscription` - the subscription to attribute a message to when its
      metadata carries none, for a producer that predates
      BroadwayCloudPubSub v2. Without it such a message records no checkpoint.

  Call it once, when the application starts. It does nothing when the library is
  disabled, and calling it again is harmless: telemetry keeps one handler per id
  and the second call returns `:ok` instead of failing, so a hot code upgrade
  does not double the checkpoints.
  """
  @spec attach(keyword()) :: :ok
  def attach(opts) do
    if Config.enabled?() do
      case :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, %{
             subscription: Keyword.get(opts, :subscription)
           }) do
        :ok -> :ok
        {:error, :already_exists} -> :ok
      end
    else
      :ok
    end
  end

  @doc """
  Records the consume checkpoint of a message received from `subscription`, and
  leaves the checkpointed pathway in `DdDataStreams.Context`.

  `message` is a map with the parts of the message the checkpoint needs:

    * `:data` - the message body
    * `:attributes` - the message attributes carrying `dd-pathway-ctx-base64`,
      or `nil`; a missing or unusable header starts a new pathway
    * `:ordering_key` - the ordering key, or `nil`
    * `:received_at` - when the message reached the pipeline, in native time
      units, as BroadwayCloudPubSub v2 stamps it; defaults to now, which reports
      no consume latency

  `subscription` is used verbatim in the `subscription:` tag, so a short name
  and a `projects/<project>/subscriptions/<id>` resource name stay
  distinguishable.

  Returns the pathway the checkpoint reached and the checkpoint itself, with
  its payload size, for an application that wants to record or forward them
  itself. The checkpoint has already been given to the aggregator, so adding it
  again counts the message twice.
  """
  @spec consume(map(), String.t()) :: {Pathway.t(), Pathway.checkpoint()}
  def consume(%{data: _} = message, subscription) when is_binary(subscription) do
    {pathway, checkpoint} = GooglePubSub.consume(message, subscription, received_at(message))

    Aggregator.add(checkpoint)
    Context.put(pathway)

    {pathway, checkpoint}
  end

  @doc """
  Records the produce checkpoint of a message published to `topic`, injects the
  resulting pathway into the message's attributes, and calls `publish_fun` with
  that message.

  The pathway continued is the one in `DdDataStreams.Context`, so this is meant
  to be called from the stage that consumed the message being answered: inside
  `handle_message/3` it chains the produce onto that message's consume
  checkpoint, inside `handle_batch/4` onto the batch's fan-in pathway. With no
  context the produce starts a new pathway.

  `message` is a map with `:data`, `:attributes` and `:ordering_key`, as
  `consume/2` documents. `publish_fun` receives that map with
  `dd-pathway-ctx-base64` set and publishes it; its return value is returned
  here. Publishing several messages that answer one consumed message means
  calling this once per message, not chaining one call's result into the next,
  so all of them share one produce node.

  When the library is disabled nothing is recorded and nothing is injected:
  `publish_fun` is called with `message` as it is.
  """
  @spec publish(String.t(), map(), (map() -> result)) :: result when result: var
  def publish(topic, message, publish_fun) when is_function(publish_fun, 1) do
    if Config.enabled?() do
      {_pathway, checkpoint, result} =
        GooglePubSub.publish(topic, message, Context.get() || Pathway.new(%{}), publish_fun)

      Aggregator.add(checkpoint)
      result
    else
      publish_fun.(message)
    end
  end

  @doc """
  Records a checkpoint for the stage reached by `tags` and returns the
  checkpointed pathway together with the measurement.

  `pathway` is the pathway to continue - `DdDataStreams.Context.get/0` for the
  one the current stage consumed - and `tags` are the edge tags reported for
  this stage, such as `["direction:in", "type:my-queue"]`. Only the tag keys in
  `DdDataStreams.Tags` are reported or hashed, so an application-specific
  `group:` or `manual_checkpoint:true` tag is a way to split one node's
  statistics without splitting its identity.

  The checkpoint is stamped with the current time, has no payload size, and is
  given to the aggregator. The caller's context is left alone: this is a
  checkpoint in the middle of a pathway, not a new stage to produce from.
  """
  @spec checkpoint(Pathway.t(), [String.t()]) :: {Pathway.t(), Pathway.checkpoint()}
  def checkpoint(%Pathway{} = pathway, tags) when is_list(tags) do
    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, tags,
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    Aggregator.add(checkpoint)

    {pathway, checkpoint}
  end

  @doc false
  # Telemetry wants arity four and a remote capture, and its handler ids are
  # global to the VM, so an attached handler is reachable before an application
  # starts and must not be able to raise: telemetry detaches a handler that
  # does, for every pipeline in the VM, and never reattaches it.
  @spec handle_event([atom()], map(), map(), map()) :: :ok
  def handle_event(@consume_event, _measurements, metadata, config) do
    with %{} = message <- received(Map.get(metadata, :message)),
         subscription when is_binary(subscription) <- subscription(message, config) do
      consume(message, subscription)
    else
      # A message that records no checkpoint must not leave the pathway of the
      # previous message behind for a later produce in this stage to continue.
      _ -> Context.clear()
    end

    :ok
  end

  def handle_event(@fan_in_event, _measurements, metadata, config) do
    pathways =
      for message <- List.wrap(Map.get(metadata, :messages)),
          pathway when not is_nil(pathway) <- [consumed(message, config)],
          do: pathway

    Context.put(Pathway.merge(pathways))
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # The pathway a batch message was consumed on, without recording it again: the
  # batch's produce edges continue the consume node of a message that already
  # has its checkpoint from the message event.
  defp consumed(message, config) do
    with %{} = message <- received(message),
         subscription when is_binary(subscription) <- subscription(message, config) do
      {pathway, _checkpoint} = GooglePubSub.consume(message, subscription, received_at(message))
      pathway
    end
  end

  # A telemetry metadata map holds one Broadway message, whose own metadata holds
  # the fields this library needs. Every field is read with a default because the
  # event carries whatever the emitter put there, and `nil` values are expected:
  # BroadwayCloudPubSub v1 sets none of them.
  defp received(%{metadata: metadata} = message) when is_map(metadata) do
    %{
      data: Map.get(message, :data),
      attributes: Map.get(metadata, :attributes),
      ordering_key: Map.get(metadata, :orderingKey),
      subscription: Map.get(metadata, :subscription),
      received_at: Map.get(metadata, :received_at)
    }
  end

  defp received(_message), do: nil

  defp subscription(message, config) do
    case Map.get(message, :subscription) do
      value when is_binary(value) and value != "" -> value
      _ -> Map.get(config, :subscription)
    end
  end

  defp received_at(message) do
    case Map.get(message, :received_at) do
      received_at when is_integer(received_at) -> received_at
      _ -> System.system_time()
    end
  end
end
