defmodule DdDataStreams do
  @moduledoc """
  Data Streams Monitoring for Broadway pipelines.

  Datadog's Data Streams Monitoring follows a payload from the service that
  produced it to the service that consumed it, so it can report how long the
  payload waited in the queue between them and how the services are connected.
  Doing that needs a checkpoint on each side of every edge and a pathway carried
  on the message itself. This module supplies the Broadway glue that decides
  where a checkpoint belongs; the pathway hashing, propagation, aggregation and
  transport live in the modules it calls.

  Nothing here is specific to one broker, or to one broker per application. The
  library hangs itself on the telemetry events Broadway already emits, so it
  works with any producer, and the parts that are broker-specific - what a
  message's metadata says about the edge it arrived on, and which technology to
  report - are modules behind `DdDataStreams.Extractor`:

      config :dd_data_streams, extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub]

  A message is named by the first configured extractor that recognises its
  metadata, so an application can run a Kafka pipeline and a Pub/Sub pipeline
  side by side.

  ## Using it in a Broadway pipeline

  The library is off until it is enabled, and a Broadway pipeline reaches it
  through telemetry instead of through a dependency:

      # DD_DATA_STREAMS_ENABLED=true in the environment, then at start up:
      DdDataStreams.attach()

      # in handle_message/3 or handle_batch/4:
      def handle_message(_processor, message, _context) do
        DdDataStreams.publish("orders-out", %{data: outgoing_data}, &MyApp.Publisher.publish/1)
        message
      end

  `attach/0` installs two telemetry handlers:

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

  `consume/1` and `publish/3` are the same two checkpoints as functions, for a
  pipeline that drives the library itself, and `checkpoint/2` records one for a
  stage that is neither a consume nor a produce.

  Checkpoints are handed to `DdDataStreams.Aggregator.add/1`, which does nothing
  when the aggregator is not running, so none of these needs to be guarded at
  the call site while the library is disabled.
  """

  alias DdDataStreams.{Aggregator, Checkpoints, Config, Context, Pathway}

  @handler_id :dd_data_streams
  @consume_event [:broadway, :processor, :message, :start]
  @fan_in_event [:broadway, :batch_processor, :start]
  @events [@consume_event, @fan_in_event]

  @default_extractors [DdDataStreams.Extractor.GooglePubSub]
  @extractors_key :extractors

  @doc """
  Installs the Broadway telemetry handlers that record consume checkpoints and
  fan-in pathways.

  Call it once, when the application starts, before Broadway starts. It does
  nothing when the library is disabled, and calling it again is harmless:
  telemetry keeps one handler per id and the second call returns `:ok` instead
  of failing, so a hot code upgrade does not double the checkpoints.

  Which broker a message comes from, and which edge tags it is reported under,
  is the configured `DdDataStreams.Extractor`'s business. Attaching fails with an
  `ArgumentError` when `config :dd_data_streams, extractors:` is not a non-empty
  list of extractors, rather than leaving a handler attached that cannot name an
  edge.
  """
  @spec attach() :: :ok
  def attach do
    if Config.enabled?() do
      ensure_extractors!(extractors())

      case :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, %{}) do
        :ok -> :ok
        {:error, :already_exists} -> :ok
      end
    else
      :ok
    end
  end

  @doc """
  Records the consume checkpoint of a message, and leaves the checkpointed
  pathway in `DdDataStreams.Context`.

  `message` is a Broadway message, or a map shaped like one: the body in
  `:data` and the producer's own metadata map in `:metadata`. The configured
  extractor names the edge from that metadata and hands back the parts a
  checkpoint measures, so a pipeline that drives the library itself only has to
  build the metadata the producer would have built.

  Returns `nil` when the extractor finds no edge in the metadata - a message
  with no subscription, no topic, nothing to report it under - and records
  nothing in that case. Otherwise returns the pathway the checkpoint reached and
  the checkpoint itself, with its payload size, for an application that wants to
  record or forward them itself. The checkpoint has already been given to the
  aggregator, so adding it again counts the message twice.
  """
  @spec consume(map()) :: {Pathway.t(), Pathway.checkpoint()} | nil
  def consume(%{data: _, metadata: _} = message) do
    case checkpointed(message) do
      nil ->
        nil

      {extractor, {pathway, checkpoint}} ->
        Aggregator.add(checkpoint)
        Context.put(pathway, extractor)

        {pathway, checkpoint}
    end
  end

  def consume(_message), do: nil

  @doc """
  Records the produce checkpoint of a message published to `topic`, injects the
  resulting pathway into the message's attributes or headers, and calls
  `publish_fun` with that message.

  The pathway continued is the one in `DdDataStreams.Context`, so this is meant
  to be called from the stage that consumed the message being answered: inside
  `handle_message/3` it chains the produce onto that message's consume
  checkpoint, inside `handle_batch/4` onto the batch's fan-in pathway. With no
  context the produce starts a new pathway.

  `message` is a map with `:data` and the attributes the producer takes, either
  a `:attributes` map or a list of `{key, value}` `:headers`; the pathway is
  written back into the same carrier. `publish_fun` receives that map with
  `dd-pathway-ctx-base64` set and publishes it; its return value is returned
  here. Publishing several messages that answer one consumed message means
  calling this once per message, not chaining one call's result into the next,
  so all of them share one produce node.

  The edge tags come from the extractor that named the consume edge of the
  message being answered, which is the one `attach/0` left in the context, so a
  pipeline that consumes and produces on the same technology needs nothing more.
  A pipeline that changes technology - consuming from Kafka and publishing to
  Pub/Sub - names the outgoing one, because the topic alone does not say which
  bus it goes to:

      DdDataStreams.publish("orders-out", message, &publish/1, extractor: MyApp.PubSubExtractor)

  When the library is disabled nothing is recorded and nothing is injected:
  `publish_fun` is called with `message` as it is.
  """
  @spec publish(String.t(), map(), (map() -> result), keyword()) :: result when result: var
  def publish(topic, message, publish_fun, opts \\ []) when is_function(publish_fun, 1) do
    if Config.enabled?() do
      {_pathway, checkpoint, result} =
        Checkpoints.publish(
          produce_tags(topic, opts),
          message,
          Context.get() || Pathway.new(%{}),
          publish_fun
        )

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
  # does, for every pipeline in the VM, and never reattaches it. A message the
  # extractor cannot name records nothing instead of failing.
  @spec handle_event([atom()], map(), map(), map()) :: :ok
  def handle_event(@consume_event, _measurements, metadata, _config) do
    case consume(Map.get(metadata, :message)) do
      # A message that records no checkpoint must not leave the pathway of the
      # previous message behind for a later produce in this stage to continue.
      nil -> Context.clear()
      _recorded -> :ok
    end

    :ok
  end

  def handle_event(@fan_in_event, _measurements, metadata, _config) do
    resolved =
      metadata
      |> Map.get(:messages)
      |> List.wrap()
      |> Enum.map(&checkpointed/1)
      |> Enum.reject(&is_nil/1)

    pathways = for {_extractor, {pathway, _checkpoint}} <- resolved, do: pathway

    # The batch stage is another process, so it needs the extractor the messages
    # were consumed with for its own produce edges.
    extractor =
      case resolved do
        [{extractor, _checkpointed} | _] -> extractor
        [] -> nil
      end

    Context.put(Pathway.merge(pathways), extractor)
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # The consume checkpoint of a message together with the extractor that named it.
  # Every configured extractor is asked in order, and the first one that
  # recognises the metadata wins; recording is the caller's business, so the
  # batch handler can reuse this without counting a message twice.
  defp checkpointed(%{data: data, metadata: metadata}) when is_map(metadata) do
    Enum.find_value(extractors(), fn extractor ->
      case extractor.consume(metadata) do
        {:ok, tags, parts} ->
          checkpoint = Checkpoints.consume(Map.put(parts, :data, data), tags, received_at(parts))
          {extractor, checkpoint}

        :error ->
          nil
      end
    end)
  end

  defp checkpointed(_message), do: nil

  defp received_at(parts) do
    case Map.get(parts, :received_at) do
      received_at when is_integer(received_at) -> received_at
      _ -> System.system_time()
    end
  end

  defp extractors do
    Application.get_env(:dd_data_streams, @extractors_key, @default_extractors)
  end

  # The extractor named on the call, then the one the stage consumed with, then
  # the only configured one: a topic name alone does not say which bus it goes
  # to, so an application with several extractors has to say so once the stage
  # that is producing did not consume the message itself.
  defp produce_tags(topic, opts) do
    (Keyword.get(opts, :extractor) || Context.get_extractor() || only_extractor!()).produce(topic)
  end

  defp only_extractor! do
    case extractors() do
      [extractor] ->
        extractor

      configured ->
        raise ArgumentError,
              "several extractors are configured (#{inspect(configured)}): pass " <>
                "`extractor: MyApp.Extractor` to DdDataStreams.publish/4"
    end
  end

  defp ensure_extractors!(configured) do
    valid? = fn module ->
      Code.ensure_loaded?(module) and function_exported?(module, :consume, 1) and
        function_exported?(module, :produce, 1)
    end

    case configured do
      [_ | _] = extractors ->
        unless Enum.all?(extractors, valid?) do
          raise ArgumentError,
                "#{inspect(extractors)} is not a list of DdDataStreams.Extractor modules: " <>
                  "set `config :dd_data_streams, extractors: [MyApp.Extractor]`"
        end

        :ok

      other ->
        raise ArgumentError,
              "#{inspect(other)} is not a list of DdDataStreams.Extractor modules: " <>
                "set `config :dd_data_streams, extractors: [MyApp.Extractor]`"
    end
  end
end
