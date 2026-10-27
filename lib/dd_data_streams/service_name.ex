defmodule DdDataStreams.ServiceName do
  @moduledoc """
  The behaviour that names the service a checkpoint is reported under.

  `DdDataStreams.Config.service/0` names one service for the whole application.
  A pipeline that handles messages for several services - one per tenant, one
  per Broadway pipeline, one per topic - needs a name per checkpoint instead,
  and the name is part of the node's identity: it is hashed into `hash`, it
  keys the aggregator's buckets, and it is the `Service` of the payload the
  agent receives. Naming it per message therefore needs to reach every
  checkpoint, which is what this behaviour is for.

  Configure one module, and every consume, produce and checkpoint asks it:

      config :dd_data_streams, service_name: MyApp.ServiceNames

      defmodule MyApp.ServiceNames do
        @behaviour DdDataStreams.ServiceName

        @impl true
        def call(%{direction: :in, metadata: metadata}) do
          case metadata do
            %{subscription: subscription} -> "my_service." <> short_id(subscription)
            _ -> nil
          end
        end

        def call(%{direction: :out, topic: topic}), do: "my_service." <> short_id(topic)

        defp short_id(resource), do: resource |> String.split("/") |> List.last()
      end

  ## Precedence

  A `:service` option passed to `DdDataStreams.produce/3` or
  `DdDataStreams.produce_batch/3` wins, because the call site is the most
  specific place to say it. Then the configured module, then
  `DdDataStreams.Config.service/0`. A module that returns `nil`, `""` or
  anything that is not a string names no service, and the configured one
  applies; a module that raises is reported as
  `[:dd_data_streams, :handler, :error]` and the checkpoint is still recorded
  under the configured service, so a mistake in naming does not lose
  measurements. `DdDataStreams.checkpoint/1,2` does not ask: it is a stage in
  the middle of a pathway, with no message and no topic to name a service for,
  and reports `DdDataStreams.Config.service/0`.

  Unlike the extractor, this module is not broker-specific and does not have to
  be: it decides a name from data the library already has, and every broker
  reaches it the same way.

  ## What it is given

  `t:DdDataStreams.ServiceName.context/0` carries what the checkpoint knows.
  The `:in` direction has the message's broker metadata, read the same way the
  consuming extractor read it; the `:out` direction has the publish topic, the
  options the host passed to `produce/3`, and the pathway the produce continues,
  whose `tags` are the consumed edge's. Both have the edge tags the extractor
  named and the producer info of the message being answered, so
  `producer_info.topology` - the registered name of the Broadway pipeline - is
  often enough to name a service without looking at a message at all.

  Reading the tags of the pathway is how a produce names itself after what it
  answers rather than after what it publishes:

      def call(%{direction: :out, pathway: %DdDataStreams.Pathway{} = pathway}) do
        case DdDataStreams.Tags.get(pathway.tags, "topic") do
          nil -> nil
          topic -> "my_service." <> topic
        end
      end

  A consume cannot do the same: the pathway header carries the hashes and the
  starts, not the tags, so `:pathway` is `nil` for `:in` - the upstream edge's
  tags are not on the message.

  ## Cost

  The module runs once per checkpoint, on the path that records it, for every
  message the pipeline handles. Keep it a decision, not a lookup: no database,
  no HTTP call. It is also a cardinality budget rather than a labelling
  convenience - one payload and one request go to the agent per service per
  ten-second flush, and the service is hashed into every node, so a name
  derived from an order id multiplies services and nodes instead of
  describing them. Name a small, known set of logical services.
  """

  alias DdDataStreams.Extractor

  @typedoc """
  What the service namer is asked, for the checkpoint being recorded.

    * `:direction` - `:in` for a consume checkpoint, `:out` for a produce one
    * `:tags` - the edge tags the extractor named, in full and in order, so a
      `"topic:"`, `"subscription:"` or `"type:"` tag can be read here instead of
      parsed again
    * `:metadata` - the message's broker metadata for `:in`; `nil` for `:out`
    * `:topic` - the publish topic for `:out`; `nil` for `:in`
    * `:opts` - the options the host passed to `DdDataStreams.produce/3` for
      `:out`, including any an extractor read; `[]` for `:in`
    * `:pathway` - the pathway the checkpoint continues: for `:out` the one
      `DdDataStreams.Context` holds, whose `tags` are the consumed edge's tags;
      `nil` when there is none (a produce that consumed nothing) and for `:in`,
      whose header carries no tags
    * `:producer_info` - what delivered the message being answered; see
      `t:DdDataStreams.Extractor.producer_info/0`

  The outgoing payload itself is not here. `produce/3` never inspects it, and
  a name derived from message content is the cardinality trap the module
  documentation warns about.
  """
  @type context :: %{
          direction: :in | :out,
          tags: [String.t()],
          metadata: map() | nil,
          topic: String.t() | nil,
          opts: keyword(),
          pathway: DdDataStreams.Pathway.t() | nil,
          producer_info: Extractor.producer_info()
        }

  @doc """
  The service the checkpoint is reported under, or `nil` to use
  `DdDataStreams.Config.service/0`.
  """
  @callback call(context()) :: String.t() | nil
end
