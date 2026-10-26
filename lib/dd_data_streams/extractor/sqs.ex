defmodule DdDataStreams.Extractor.SQS do
  @moduledoc """
  The extractor for `BroadwaySQS` 1.0.

  An SQS message names neither its queue nor its region: the metadata
  `BroadwaySQS` stamps (`:message_id`, `:receipt_handle`, `:md5_of_body`,
  `:attributes`, `:message_attributes`) carries no queue identity at all. The
  consume edge is therefore named from the producer's configuration, which is
  why this extractor implements `c:DdDataStreams.Extractor.consume/2`: it reads
  `:queue_url` from the options of the `BroadwaySQS.Producer` that delivered the
  message and reduces it to its last path segment, so
  `https://sqs.eu-west-1.amazonaws.com/123456789012/orders` is the queue
  `orders`. Two queues in one application are two producers, so they report two
  different edges. A `.fifo` suffix stays in the name, as the official tracers
  report it. A message delivered by any other producer, a producer without a
  usable `:queue_url`, or a hand-called `consume/2` with no producer names no
  edge and records nothing. Checked against the source of `BroadwaySQS` 1.0.1
  and the AWS API reference, not against a live queue.

  The produce edge is named by the topic the host passes to
  `DdDataStreams.produce/3`: either the queue URL or the bare queue name, which
  report the same queue.

  | edge    | tags                                              |
  | ------- | ------------------------------------------------- |
  | consume | `direction:in`, `topic:<queue>`, `type:sqs`       |
  | produce | `direction:out`, `topic:<queue>`, `type:sqs`      |

  These are the tags dd-trace-py reports for SQS on both edges, so a node joins
  with a Python service's; dd-trace-go reports the same produce tags and has no
  consume side to compare. Nothing else is reported: SQS has no partition, no
  consumer group and no routing key.

  ## Host options

  SQS returns no message attributes unless asked, so the pipeline must request
  the one that carries the pathway header, or every message starts a new
  pathway:

      producer: [
        module:
          {BroadwaySQS.Producer,
           queue_url: "https://sqs.eu-west-1.amazonaws.com/123456789012/orders",
           message_attribute_names: ["_datadog"],
           attribute_names: [:sent_timestamp]}
      ]

    * `message_attribute_names: ["_datadog"]` - required. The pathway header
      travels in the `_datadog` message attribute, a JSON object, which this
      extractor hands to the carrier as `:attributes`. Prefer the explicit list
      to `:all`
    * `attribute_names: [:sent_timestamp]` - optional. `SentTimestamp`, epoch
      milliseconds, starts the pathway of a message that carries no header, so
      a pipeline that only consumes reports how long the message waited in the
      queue instead of zero. Without it, or when it falls outside the window a
      clock could plausibly have stamped, both starts are left unset

  ## Where it differs from the official tracers

    * `:received_at` is not set. `BroadwaySQS` stamps no receive time, so the
      checkpoint is taken when the message reaches the handler, and the consume
      latency includes the time the message waited inside the pipeline;
      dd-trace-py measures at the receive call
    * A FIFO queue's `MessageGroupId` (`attribute_names: [:message_group_id]`) is
      not read: it is not an ordering key here, because dd-trace-py counts it in
      the payload size on produce only and mapping it would inflate consume sizes
      against Python, and no tag is built from it. A publisher that wants the
      same produce size passes `:payload_size` to `DdDataStreams.produce/3`
    * The payload size of a consume is the body plus the `_datadog` attribute;
      dd-trace-py also counts every other message attribute
    * A `Binary` `_datadog` attribute and an SNS envelope nested in the body (an
      SNS subscription without raw delivery) are not read; the message then
      starts a new pathway

  The host writing the produce side puts the header into `SendMessage` itself,
  as `%{"DataType" => "String", "StringValue" => json}` under `"_datadog"`, with
  at most 10 message attributes per message.
  """

  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(metadata, %{producer: {BroadwaySQS.Producer, opts}})
      when is_map(metadata) and is_list(opts) do
    case queue_name(Keyword.get(opts, :queue_url)) do
      nil -> :error
      queue -> {:ok, tags("in", queue), parts(metadata)}
    end
  end

  def consume(_metadata, _producer_info), do: :error

  @impl true
  def produce(queue, _producer_info), do: tags("out", queue_name(queue) || queue)

  # The queue is the last path segment of its URL, and the whole value when it
  # carries no slashes.
  defp queue_name(url) when is_binary(url) do
    case url |> String.trim_trailing("/") |> String.split("/") |> List.last() do
      "" -> nil
      name -> name
    end
  end

  defp queue_name(_url), do: nil

  defp tags(direction, queue), do: ["direction:" <> direction, "topic:" <> queue, "type:sqs"]

  defp parts(metadata) do
    sent_at = sent_at(Map.get(metadata, :attributes))

    %{
      attributes: Map.get(metadata, :message_attributes),
      pathway_start: sent_at,
      edge_start: sent_at
    }
  end

  # BroadwaySQS parses SentTimestamp to an integer of epoch milliseconds, and
  # hands over `[]` when no system attribute was requested.
  defp sent_at(%{"sent_timestamp" => milliseconds}) when is_integer(milliseconds),
    do: System.convert_time_unit(milliseconds, :millisecond, :native)

  defp sent_at(_attributes), do: nil
end
