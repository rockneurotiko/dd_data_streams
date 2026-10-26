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
  library hangs itself on the telemetry events Broadway already emits, so the
  pathway and propagation layer works with any producer, and the parts that are
  broker-specific - what a message's metadata says about the edge it arrived
  on, and which technology to report - are modules behind
  `DdDataStreams.Extractor`:

      config :dd_data_streams, extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub]

  A message is named by the first configured extractor that recognises its
  metadata, so an application can run a Kafka pipeline and a Pub/Sub pipeline
  side by side. One extractor ships today -
  `DdDataStreams.Extractor.GooglePubSub` - so tagging a message from any other
  broker needs an extractor module of your own; the carriers `Flat` and `Sqs`
  ship as well.

  ## Using it in a Broadway pipeline

  A Broadway pipeline reaches the library through telemetry instead of through a
  dependency, so attaching is the whole integration:

      # at start up, before Broadway does:
      DdDataStreams.attach()

      # in handle_message/3 or handle_batch/4:
      def handle_message(_processor, message, _context) do
        data = encode(message.data)

        attributes = existing_attributes

        attributes =
          case DdDataStreams.produce("orders-out", data) do
            [{key, value}] -> Map.put(attributes, key, value)
            [] -> attributes
          end

        MyApp.Publisher.publish("orders-out", data, attributes: attributes)
        message
      end

  `produce/3` records the produce checkpoint and returns the key/value pair to
  put on the outgoing message; it does not publish. `produce_batch/3` does the
  same for several payloads published to one topic and returns one pair for the
  whole batch.

  `attach/0` installs two telemetry handlers:

    * `[:broadway, :processor, :message, :start]`, which Broadway emits once for
      each message it is about to hand to `handle_message/3`. The handler
      records the consume checkpoint - one per message that reaches
      `handle_message/3`, including a leaf consumer that produces nothing - and
      leaves the resulting pathway in `DdDataStreams.Context` for a produce
      call in the same stage process.
    * `[:broadway, :batch_processor, :start]`, once per batch. The handler
      samples one of the pathways the batch's messages were consumed on and
      leaves that in `DdDataStreams.Context`, so a produce call in
      `handle_batch/4` continues a pathway that connects to the messages being
      answered. Fan-in is a sample rather than a join, which is what
      dd-trace-go v2.11.2 does in `Merge`/`MergeContexts`: it keeps one pathway.

  Both handlers record only what each event is: the batch handler adds no
  consume checkpoint, because every message of the batch already got one at its
  own message event.

  A message that never reaches `handle_message/3` therefore gets no consume
  checkpoint. That happens when `prepare_messages/2` raises: Broadway marks
  every message of the batch failed before the per-message start event
  (`deps/broadway/lib/broadway/topology/processor_stage.ex`, the
  `maybe_prepare_messages/2` wrapper), so the start event never fires and a
  raise silences the checkpoint for the whole batch, not just the message that
  raised. Marking a message failed with `Broadway.Message.failed/2` is a
  different case: those messages still reach `handle_message/3`, so they still
  checkpoint, and failed messages take Broadway's failed-ack path either way.
  A redelivery is a new attempt and counts a new checkpoint; the wait is still
  measured from the receive time, so per-attempt counting does not distort it.

  ## Messages the library cannot record

  The handlers never raise. Telemetry detaches a handler that raises, for every
  pipeline in the VM, and never reattaches it, so a single malformed message
  would stop monitoring for the whole application. A message the library cannot
  record is skipped instead: the pathway and the extractor of the previous
  message are cleared, so a later produce does not continue them, and the reason
  is logged and reported as a `[:dd_data_streams, :handler, :error]` telemetry
  event.

  ## Pathways that are not a Broadway stage

  `consume/1` and `produce/3` are the same two checkpoints as functions, for a
  pipeline that drives the library itself, and `checkpoint/2` records one for a
  stage that is neither a consume nor a produce.

  Checkpoints are handed to `DdDataStreams.Aggregator.add/1`, which does nothing
  when the aggregator is not running, so none of these needs to be guarded at
  the call site while the library is disabled.
  """

  alias DdDataStreams.{Aggregator, Checkpoints, Config, Context, Pathway, Propagator}

  require Logger

  @handler_id :dd_data_streams
  @consume_event [:broadway, :processor, :message, :start]
  @fan_in_event [:broadway, :batch_processor, :start]
  @events [@consume_event, @fan_in_event]
  @error_event [:dd_data_streams, :handler, :error]
  @skip_event [:dd_data_streams, :checkpoint, :skip]

  @default_extractors [DdDataStreams.Extractor.GooglePubSub]
  @extractors_key :extractors

  @typedoc """
  The metadata key and base64 pathway value to add to an outgoing message, as
  returned by `produce/3` and `produce_batch/3`.

  The key is `DdDataStreams.Config.header/0`, `"dd-pathway-ctx-base64"` unless
  configured; the value is the encoded pathway.
  """
  @type propagation :: {String.t(), String.t()}

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
  edge. The extractors are validated even while the library is disabled, so a
  configuration mistake raises when the application starts instead of waiting
  for the flag to be flipped.
  """
  @spec attach() :: :ok
  def attach do
    ensure_extractors!(extractors())

    if Config.enabled?() do
      case :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, %{}) do
        :ok -> :ok
        {:error, :already_exists} -> :ok
      end
    else
      :ok
    end
  end

  @doc """
  Removes the telemetry handlers `attach/0` installed, so recording stops
  without restarting the application.

  Broadway events are no longer recorded afterwards; `consume/1`, `produce/3`
  and `checkpoint/1` keep working as plain functions while the library is
  enabled, and checkpoints already handed to the aggregator are still flushed.

  Calling it when nothing is attached returns `:ok` too, so a test can end with
  it unconditionally, and `attach/0` and `detach/0` may be interleaved freely.
  """
  @spec detach() :: :ok
  def detach do
    case :telemetry.detach(@handler_id) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end

  @doc """
  The library's effective configuration and liveness, as one map, answered from
  the local VM: no Datadog Agent, no aggregator flush, nothing that can block.

    * `:enabled` - what `attach/0`, `produce/3` and `produce_batch/3` obey.
    * `:attached` - whether the Broadway telemetry handlers are installed.
    * `:aggregator_running` - whether the process that batches checkpoints and
      posts them is running.
    * `:transport`, `:agent_url`, `:header`, `:service`, `:env`,
      `:extractors` - the effective values of the same settings the recording
      itself reads, as of this call.

  "Is it recording?" is all three of `:enabled`, `:attached` and
  `:aggregator_running`: the flag decides whether new checkpoints are made, the
  handlers decide whether Broadway events make any, and the aggregator decides
  whether what they make is ever posted.
  """
  @spec status() :: %{
          enabled: boolean(),
          attached: boolean(),
          aggregator_running: boolean(),
          transport: module() | nil,
          agent_url: String.t(),
          header: String.t(),
          service: String.t(),
          env: String.t(),
          extractors: [module()]
        }
  def status do
    %{
      enabled: Config.enabled?(),
      attached: :telemetry.list_handlers(@consume_event) != [],
      aggregator_running: not is_nil(Process.whereis(Aggregator)),
      transport: Config.transport(),
      agent_url: Config.agent_url(),
      header: Config.header(),
      service: Config.service(),
      env: Config.env(),
      extractors: extractors()
    }
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
  nothing in that case; the miss is reported as a
  `[:dd_data_streams, :checkpoint, :skip]` telemetry event whose `:extractors`
  metadata is the configured list that was asked. It also clears the process
  context, so a later
  `produce/3` cannot chain onto the previous message's pathway. An extractor
  that raises is treated the same way, and
  the reason is logged and reported as a `[:dd_data_streams, :handler, :error]`
  telemetry event. Otherwise returns the pathway the checkpoint reached. The
  checkpoint has already been given to the aggregator, so it is not returned;
  use `DdDataStreams.Checkpoints.consume/3` if you need the checkpoint to
  record or forward it yourself, and do not add it to this aggregator again.
  """
  @spec consume(map()) :: Pathway.t() | nil
  def consume(message), do: consume_message(message, nil, nil)

  # The topology name and the producer only exist on the telemetry event, so a
  # consume called by hand reports neither; the producer still comes from the
  # message's metadata when the caller put it there.
  defp consume_message(%{data: _, metadata: _} = message, topology, producer) do
    case checkpointed(message, topology, producer) do
      nil ->
        Context.clear()
        nil

      {pathway, checkpoint} ->
        Aggregator.add(checkpoint)
        Context.put(pathway)

        pathway
    end
  end

  defp consume_message(_message, _topology, _producer) do
    Context.clear()
    nil
  end

  @doc """
  Records the produce checkpoint for `data` published to `topic` and returns the
  `{key, value}` pair to add to the outgoing message's attributes or headers.

  It does not publish. The caller adds the pair to the same metadata the
  receiver will read:

      attributes = existing_attributes

      attributes =
        case DdDataStreams.produce("orders-out", data) do
          [{key, value}] -> Map.put(attributes, key, value)
          [] -> attributes
        end

      MyApp.PubSub.publish("orders-out", data, attributes: attributes)

  The key is `DdDataStreams.Config.header/0`, `"dd-pathway-ctx-base64"` unless
  configured. The pathway continued is the one in `DdDataStreams.Context`, so
  this is meant to be called from the stage that consumed the message being
  answered: inside `handle_message/3` it chains the produce onto that message's
  consume checkpoint, inside `handle_batch/4` onto the batch's fan-in pathway.
  With no context the produce starts a new pathway. This function does not
  change the context.

  `data` is the outgoing payload. It is only measured, never inspected: a binary
  is measured with `byte_size/1`, and anything else counts as zero. The size of
  the `{key, value}` pair this function returns is added to it, because that
  pair travels with the message and the consumer measures every attribute it
  receives: measure the message without the pair. Pass `payload_size:` when the
  payload's size is known another way, or when the message's own attributes
  should be counted.

  Call it immediately before sending: the checkpoint is recorded when this
  function runs, so a send that never happens still reports a produce.
  Producing several messages from one consumed message means calling this once
  per message, not chaining one call's result into the next, so all of them
  share one produce node.

  The edge tags come from the extractor that named the consume edge of the
  message being answered, which is the one `attach/0` left in the context, so a
  pipeline that consumes and produces on the same technology needs nothing more.
  That extractor also receives the producer info of the message being answered,
  so identity kept in the producer's configuration - a Kafka cluster id, a
  RabbitMQ exchange - can be reported on the produce edge; a stage that
  produces without consuming passes both fields nil. A pipeline that changes
  technology - consuming from Kafka and publishing to
  Pub/Sub - names the outgoing one, because the topic alone does not say which
  bus it goes to:

      DdDataStreams.produce("orders-out", data, extractor: MyApp.PubSubExtractor)

  Options:

    * `:extractor` - the extractor that names the outgoing edge. Defaults to the
      extractor that consumed the message being answered, then to the only
      configured one; a stage that produces without consuming and has several
      extractors must pass it.
    * `:payload_size` - the size in bytes to report, a non-negative integer.
      Defaults to the size of `data`. The pair this function returns is added to
      it, so pass the size of the message without that pair.

  Any other option is passed to an extractor that implements the optional
  `c:DdDataStreams.Extractor.produce/3`, which is how an edge that needs more
  than a topic is named - a RabbitMQ publish names its exchange and routing key.
  The library neither reads nor validates those options, and an extractor that
  implements only `produce/2` never sees them.

  Returns `[]` when the library is disabled, and records nothing.
  """
  @spec produce(String.t(), term(), keyword()) :: [propagation()]
  def produce(topic, data, opts \\ []) when is_binary(topic) do
    size = payload_size(data, opts)

    if Config.enabled?() do
      {pathway, checkpoint} =
        Checkpoints.produce(
          produce_tags(topic, opts),
          Context.get() || Pathway.new(%{}),
          size
        )

      pair = {Config.header(), Propagator.encode_str(pathway)}
      Aggregator.add(%{checkpoint | payload_size: checkpoint.payload_size + header_size(pair)})
      [pair]
    else
      []
    end
  end

  @doc """
  Records one produce checkpoint per payload in `datas` and returns one
  `{key, value}` pair for the whole batch.

  All the payloads are published to the same `topic`; the pair is the same for
  every message because the batch shares one produce node. Add it to each
  message's metadata:

      attributes = existing_attributes

      attributes =
        case DdDataStreams.produce_batch("orders-out", payloads) do
          [{key, value}] -> Map.put(attributes, key, value)
          [] -> attributes
        end

      MyApp.PubSub.publish_batch("orders-out", payloads, attributes: attributes)

  Each payload records its own checkpoint with its own size, but all of them
  have the same hash and timestamp, so the batch is one produce edge. `datas`
  is any `Enumerable`; it is materialised once. An empty enumerable returns
  `[]`; a disabled library returns `[]`.

  Payload size is `byte_size/1` per binary payload, or zero otherwise, plus the
  size of the pair this function returns, as `produce/3` does. There is no
  `:payload_size` option here: call `produce/3` once per message when each size
  needs to be measured differently.

  The `:extractor` option is the same as for `produce/3`, as is any option an
  extractor implementing `c:DdDataStreams.Extractor.produce/3` reads. Call it
  immediately before the batch send, for the same reason as `produce/3`.
  """
  @spec produce_batch(String.t(), Enumerable.t(), keyword()) :: [propagation()]
  def produce_batch(topic, datas, opts \\ []) when is_binary(topic) do
    if Keyword.has_key?(opts, :payload_size) do
      raise ArgumentError,
            ":payload_size is not supported by produce_batch/3; call produce/3 per message"
    end

    if Config.enabled?() do
      case produce_all(topic, Enum.to_list(datas), opts) do
        [] -> []
        pair -> [pair]
      end
    else
      []
    end
  end

  @doc """
  Records a checkpoint for the stage reached by `tags` and returns the
  checkpointed pathway. The checkpoint has already been given to the
  aggregator, so it is not returned; use `Pathway.set_checkpoint/3` if you
  need the measurement itself.

  `checkpoint/1` defaults `pathway` to `DdDataStreams.Context.get/0` (or a new
  pathway when there is none); `checkpoint/2` takes it explicitly, for a stage
  that wants to continue a pathway it already holds. `tags` are the edge tags
  reported for this stage, such as `["direction:in", "type:my-queue"]`.
  `DdDataStreams.Tags`
  decides what each key does with them: some are hashed into the node, and
  others, such as `group:` or `manual_checkpoint:true`, split one node's
  statistics without splitting its identity. A key the vocabulary does not know
  is reported on the edge and does neither.

  The checkpoint is stamped with the current time, has no payload size, and is
  given to the aggregator. The caller's context is left alone: this is a
  checkpoint in the middle of a pathway, not a new stage to produce from.
  """
  @spec checkpoint([String.t()]) :: Pathway.t()
  def checkpoint(tags) when is_list(tags) do
    validate_checkpoint_tags!(tags)

    checkpoint(Context.get() || Pathway.new(%{}), tags)
  end

  @spec checkpoint(Pathway.t(), [String.t()]) :: Pathway.t()
  def checkpoint(%Pathway{} = pathway, tags) when is_list(tags) do
    validate_checkpoint_tags!(tags)

    {pathway, checkpoint} =
      Pathway.set_checkpoint(pathway, tags,
        service: Config.service(),
        env: Config.env(),
        primary_tag: Config.primary_tag()
      )

    Aggregator.add(checkpoint)

    pathway
  end

  @doc false
  # Telemetry wants arity four and a remote capture, and its handler ids are
  # global to the VM, so an attached handler is reachable before an application
  # starts and must not be able to raise: telemetry detaches a handler that
  # does, for every pipeline in the VM, and never reattaches it. A message the
  # extractor cannot name records nothing instead of failing.
  @spec handle_event([atom()], map(), map(), map()) :: :ok
  def handle_event(@consume_event, _measurements, metadata, _config) do
    try do
      case consume_message(
             Map.get(metadata, :message),
             Map.get(metadata, :topology_name),
             Map.get(metadata, :producer)
           ) do
        # A message that records no checkpoint must not leave the pathway of the
        # previous message behind for a later produce in this stage to continue.
        nil -> Context.clear()
        _recorded -> :ok
      end
    rescue
      error -> skipped(@consume_event, error, __STACKTRACE__)
    catch
      kind, reason -> skipped(@consume_event, {kind, reason}, __STACKTRACE__)
    end

    :ok
  end

  def handle_event(@fan_in_event, _measurements, metadata, _config) do
    try do
      topology = Map.get(metadata, :topology_name)
      producer = Map.get(metadata, :producer)

      resolved =
        metadata
        |> Map.get(:messages)
        |> List.wrap()
        |> Enum.map(&checkpointed(&1, topology, producer))
        |> Enum.reject(&is_nil/1)

      # Sample one consumed message's pathway, with the extractor and producer
      # info it was consumed with, so a mixed-broker batch cannot pair one
      # broker's pathway with another's identity. Fan-in re-derives consume
      # checkpoints from the batch messages' metadata, so a handle_message/3
      # that replaces metadata entirely leaves the batch parentless (documented
      # as unsupported).
      pathway =
        case resolved do
          [] ->
            Pathway.merge([])

          _ ->
            {sampled, _checkpoint} = Enum.random(resolved)
            sampled
        end

      # The batch stage is another process, so it needs the extractor and the
      # producer info of the sampled message for its own produce edges.
      Context.put(pathway)
    rescue
      error -> skipped(@fan_in_event, error, __STACKTRACE__)
    catch
      kind, reason -> skipped(@fan_in_event, {kind, reason}, __STACKTRACE__)
    end

    :ok
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # A message or a batch the library cannot record is skipped, not failed: the
  # stage's previous pathway and extractor are cleared, so a later produce does
  # not continue them, and the reason is reported once.
  defp skipped(event, reason, stacktrace) do
    Context.clear()

    report("skipped a Data Streams checkpoint", %{
      event: event,
      reason: reason,
      stacktrace: stacktrace
    })

    :ok
  end

  defp report(message, metadata) do
    Logger.warning("#{message}: #{inspect(metadata[:reason])}")

    :telemetry.execute(@error_event, %{count: 1}, error_metadata(metadata))

    :ok
  end

  # Every report of the error event carries the same keys, so a consumer does
  # not have to know which branch reported it.
  defp error_metadata(metadata) do
    Map.merge(%{event: nil, extractor: nil, reason: nil, stacktrace: nil}, metadata)
  end

  # The consume checkpoint of a message together with the pathway fields a
  # produce in the same stage reports from: the extractor that named the edge
  # and the producer that delivered the message. Every configured extractor is
  # asked in order, and the first one that
  # recognises the metadata wins; recording is the caller's business, so the
  # batch handler can reuse this without counting a message twice.
  defp checkpointed(%{data: data, metadata: metadata}, topology, producer)
       when is_map(metadata) do
    extractors = extractors()

    # What Broadway knows about the producer, for an extractor that names the
    # edge from it: the {module, opts} the event reports - a message that
    # carries its own wins, since that is the one actually delivered - and the
    # pipeline's name from the event the checkpoint is taken at.
    producer_info = %{producer: Map.get(metadata, :producer) || producer, topology: topology}

    case Enum.find_value(extractors, &extractor_checkpoint(&1, data, metadata, producer_info)) do
      nil ->
        # No extractor names an edge for the message: nothing is recorded, and
        # the miss is reported so the drop is observable without a log line
        # per message.
        :telemetry.execute(@skip_event, %{count: 1}, %{
          reason: :no_extractor_match,
          extractors: extractors
        })

        nil

      found ->
        found
    end
  end

  defp checkpointed(_message, _topology, _producer), do: nil

  defp extractor_checkpoint(extractor, data, metadata, producer_info) do
    case edge(extractor, metadata, producer_info) do
      {:ok, tags, parts} ->
        {pathway, checkpoint} =
          Checkpoints.consume(
            Map.put(parts, :data, data),
            Enum.filter(tags, &is_binary/1),
            received_at(parts)
          )

        # The extractor and the producer that delivered the message ride on the
        # pathway, so a produce in this stage reports the outgoing edge with the
        # identity the consume side saw.
        {%{pathway | extractor: extractor, producer_info: producer_info}, checkpoint}

      :error ->
        nil
    end
  end

  # An extractor is the application's own code: it may raise, and it may hand
  # back a shape the library cannot use. Both mean this extractor names no edge
  # for the message, and both are reported so the failure is not silent.
  defp edge(extractor, metadata, producer_info) do
    case consume_edge(extractor, metadata, producer_info) do
      {:ok, tags, parts} when is_list(tags) and is_map(parts) ->
        {:ok, tags, parts}

      :error ->
        :error

      # A shape the library cannot use names no edge, like an extractor that
      # found none.
      _unusable ->
        report("a Data Streams extractor handed back an unusable edge", %{
          extractor: extractor,
          reason: :unusable_edge
        })

        :error
    end
  rescue
    error -> extractor_failed(extractor, error, __STACKTRACE__)
  catch
    kind, reason -> extractor_failed(extractor, {kind, reason}, __STACKTRACE__)
  end

  # consume/2 is optional: an extractor that exports it sees which producer
  # delivered the message, one that exports only consume/1 keeps its old
  # contract. attach/0 loads and validates every configured extractor, so the
  # load branch only runs for a consume/1 called before that.
  defp consume_edge(extractor, metadata, producer_info) do
    cond do
      function_exported?(extractor, :consume, 2) ->
        extractor.consume(metadata, producer_info)

      function_exported?(extractor, :consume, 1) ->
        extractor.consume(metadata)

      Code.ensure_loaded?(extractor) and function_exported?(extractor, :consume, 2) ->
        extractor.consume(metadata, producer_info)

      true ->
        extractor.consume(metadata)
    end
  end

  defp extractor_failed(extractor, reason, stacktrace) do
    report("a Data Streams extractor failed", %{
      extractor: extractor,
      reason: reason,
      stacktrace: stacktrace
    })

    :error
  end

  defp received_at(parts) do
    case Map.get(parts, :received_at) do
      received_at when is_integer(received_at) -> received_at
      _ -> System.system_time()
    end
  end

  defp produce_all(_topic, [], _opts), do: []

  defp produce_all(topic, datas, opts) do
    {pathway, checkpoint} =
      Checkpoints.produce(
        produce_tags(topic, opts),
        Context.get() || Pathway.new(%{}),
        0
      )

    pair = {Config.header(), Propagator.encode_str(pathway)}

    Enum.each(datas, fn data ->
      Aggregator.add(%{checkpoint | payload_size: bytes(data) + header_size(pair)})
    end)

    pair
  end

  defp payload_size(data, opts) do
    case Keyword.get(opts, :payload_size) do
      nil ->
        bytes(data)

      size when is_integer(size) and size >= 0 ->
        size

      other ->
        raise ArgumentError,
              ":payload_size must be a non-negative integer, got: #{inspect(other)}"
    end
  end

  # The producer's payload is only measured; a value that is not a binary counts
  # as zero rather than crashing the send path.
  defp bytes(data) when is_binary(data), do: byte_size(data)
  defp bytes(_data), do: 0

  # The pathway pair goes out with the message and the consume side measures
  # every attribute it arrives with, so the produce side counts the pair it is
  # about to send; attributes the publisher added stay unseen on this side.
  defp header_size({key, value}), do: byte_size(key) + byte_size(value)

  # Checkpoint tags are reported verbatim, so a non-binary can never be split
  # into a key and value later; reject it at the boundary instead of dropping
  # it silently in the aggregator.
  defp validate_checkpoint_tags!(tags) do
    unless Enum.all?(tags, &is_binary/1) do
      raise ArgumentError,
            "checkpoint tags must all be binaries, got: #{inspect(tags)}"
    end
  end

  defp extractors do
    Application.get_env(:dd_data_streams, @extractors_key, @default_extractors)
  end

  # The extractor named on the call, then the one the stage consumed with, then
  # the only configured one: a topic name alone does not say which bus it goes
  # to, so an application with several extractors has to say so once the stage
  # that is producing did not consume the message itself.
  # produce/3 is optional: an extractor that exports it names the outgoing edge
  # from the options the host passed, one that exports only produce/2 keeps its
  # old contract. attach/0 validates every configured extractor, so the load
  # branch only runs for a produce called before that.
  defp produce_tags(topic, opts) do
    extractor =
      explicit_extractor(Keyword.get(opts, :extractor)) || Context.get_extractor() ||
        only_extractor!()

    producer_info = Context.get_producer_info()

    cond do
      function_exported?(extractor, :produce, 3) ->
        extractor.produce(topic, producer_info, opts)

      function_exported?(extractor, :produce, 2) ->
        extractor.produce(topic, producer_info)

      Code.ensure_loaded?(extractor) and function_exported?(extractor, :produce, 3) ->
        extractor.produce(topic, producer_info, opts)

      true ->
        extractor.produce(topic, producer_info)
    end
  end

  # A mistyped extractor named on the call would otherwise fail much later as a
  # call on a non-function, so it is rejected here with the option named.
  defp explicit_extractor(nil), do: nil

  defp explicit_extractor(extractor) do
    if is_atom(extractor) and Code.ensure_loaded?(extractor) and
         (function_exported?(extractor, :produce, 2) or function_exported?(extractor, :produce, 3)) do
      extractor
    else
      raise ArgumentError,
            "extractor: #{inspect(extractor)} must be a module exporting produce/2 or produce/3"
    end
  end

  defp only_extractor! do
    case extractors() do
      [extractor] ->
        extractor

      [] ->
        raise ArgumentError,
              "no extractor is configured: set " <>
                "`config :dd_data_streams, extractors: [MyApp.Extractor]` or pass " <>
                "`extractor: MyApp.Extractor` to DdDataStreams.produce/3"

      configured ->
        raise ArgumentError,
              "several extractors are configured (#{inspect(configured)}): pass " <>
                "`extractor: MyApp.Extractor` to DdDataStreams.produce/3"
    end
  end

  defp ensure_extractors!(configured) do
    valid? = fn module ->
      Code.ensure_loaded?(module) and
        (function_exported?(module, :consume, 1) or function_exported?(module, :consume, 2)) and
        (function_exported?(module, :produce, 2) or function_exported?(module, :produce, 3))
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
