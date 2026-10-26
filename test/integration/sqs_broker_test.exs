defmodule DdDataStreams.SQSBrokerTest do
  # A real `BroadwaySQS.Producer` against a real SQS API (goaws), so the
  # metadata the extractor reads is the metadata the client actually builds and
  # the `_datadog` message attribute is the one the queue actually returns.
  #
  # This is the `sqs` tier: excluded from `mix test`, run with
  #
  #   just sqs
  #
  # which starts the `sqs` service from `compose.yaml`, runs this file and
  # leaves the container up when the tier fails.
  #
  # goaws stamps `SentTimestamp` when the message is received, not when it was
  # sent, so this tier proves the extractor reads a send time off the queue but
  # not that the time is the producer's. It also returns every message
  # attribute whatever `message_attribute_names` asks for, so the "you must
  # request `_datadog`" rule is not exercised here either: those two are what a
  # run against real SQS would add.
  use ExUnit.Case, async: false

  alias DdDataStreams.{Carrier, Context, Extractor, Pathway, Propagator}

  @moduletag :sqs

  @endpoint System.get_env("DSM_SQS_ENDPOINT", "http://127.0.0.1:4100")
  @client_config [
    endpoint: @endpoint,
    region: "us-east-1",
    access_key_id: "dsm",
    secret_access_key: "dsm"
  ]
  @service "checkout"

  defmodule Pipeline do
    # The consumer under test: a real pipeline on the real producer. The
    # processor hands what Broadway delivered to the test process, which does
    # the asserting and the publishing.
    use Broadway

    def handle_message(_, message, _) do
      send(
        Application.fetch_env!(:dsm_test, :owner),
        {:handled, message.data, message.metadata, Context.get(), Context.get_extractor()}
      )

      message
    end
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)
    previous = Map.new(variables, &{&1, System.get_env(&1)})
    previous_extractors = Application.get_env(:dd_data_streams, :extractors)

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    System.delete_env("DD_ENV")
    System.delete_env("DD_DATA_STREAMS_HEADER")
    Application.put_env(:dd_data_streams, :extractors, [Extractor.SQS])
    Application.put_env(:dsm_test, :owner, self())

    {:ok, _} = Application.ensure_all_started(:broadway)
    DdDataStreams.attach()

    suffix = System.unique_integer([:positive])
    in_queue = create_queue("dsm-in-#{suffix}")
    out_queue = create_queue("dsm-out-#{suffix}")

    on_exit(fn ->
      DdDataStreams.detach()
      Context.clear()

      Enum.each([in_queue, out_queue], &delete_queue/1)

      if previous_extractors,
        do: Application.put_env(:dd_data_streams, :extractors, previous_extractors),
        else: Application.delete_env(:dd_data_streams, :extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    %{in_queue: in_queue, out_queue: out_queue}
  end

  test "the queue returns the pathway attribute and the produce pair survives it", %{
    in_queue: in_queue,
    out_queue: out_queue
  } do
    header = DdDataStreams.Config.header()

    parent =
      Pathway.new(hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_000_000_000)

    # An SQS producer wraps the pair in the `_datadog` JSON attribute, as
    # `Carrier.Sqs.put/3` does; publishing it under the bare header name would
    # not be readable by the consumer.
    %{"_datadog" => datadog} = Carrier.Sqs.put(%{}, parent)
    send_message(in_queue, "payload", datadog.string_value)

    start_supervised!(%{
      id: Pipeline,
      start:
        {Broadway, :start_link,
         [
           Pipeline,
           [
             name: Pipeline,
             producer: [module: {BroadwaySQS.Producer, producer_opts(in_queue)}, concurrency: 1],
             processors: [default: [concurrency: 1]],
             batchers: []
           ]
         ]}
    })

    assert_receive {:handled, "payload", metadata, pathway, extractor}, 20_000
    assert extractor == Extractor.SQS

    # The metadata BroadwaySQS built, handed to the extractor the way the
    # library hands it. Fixtures assert this shape from the source; this asserts
    # it from the running client.
    info = %{producer: {BroadwaySQS.Producer, producer_opts(in_queue)}, topology: Pipeline}
    assert {:ok, consume_tags, parts} = Extractor.SQS.consume(metadata, info)

    assert "direction:in" in consume_tags
    assert "type:sqs" in consume_tags
    assert "topic:#{queue_name(in_queue)}" in consume_tags

    sent_at = metadata.attributes["sent_timestamp"]
    assert is_integer(sent_at)
    assert parts.pathway_start == System.convert_time_unit(sent_at, :millisecond, :native)
    assert Carrier.get(parts.attributes, header) == parent

    # The checkpoint the pipeline process produces from chains onto the
    # published hash, so the queue and the carrier delivered a usable pathway.
    assert pathway.hash == expected_hash(consume_tags, parent.hash)
    assert pathway.pathway_start == parent.pathway_start

    # The pair `produce/3` returns still carries that pathway after the queue
    # has stored and returned it. `produce/3` reads the pathway from the process
    # context, so putting the one the pipeline recorded back here is the same
    # hand-off a processor gets.
    Context.put(pathway)

    out_name = queue_name(out_queue)
    assert [{^header, encoded}] = DdDataStreams.produce(out_name, "reply")

    # The documented producer side: the pair wrapped into `_datadog`'s JSON.
    send_message(out_queue, "reply", IO.iodata_to_binary(:json.encode(%{header => encoded})))

    %{"MessageAttributes" => %{"_datadog" => %{"StringValue" => datadog}}} =
      receive_message(out_queue)

    assert %{^header => ^encoded} = :json.decode(datadog)

    assert {:ok, produced} = Propagator.decode_str(encoded)
    assert produced.hash == expected_hash(produce_tags(out_name), pathway.hash)
    assert produced.pathway_start == parent.pathway_start
  end

  defp producer_opts(queue_url) do
    [
      queue_url: queue_url,
      message_attribute_names: ["_datadog"],
      attribute_names: [:sent_timestamp],
      config: @client_config,
      receive_interval: 100
    ]
  end

  defp produce_tags(queue_name) do
    Extractor.SQS.produce(queue_name, %{producer: nil, topology: nil})
  end

  defp expected_hash(tags, parent_hash) do
    Pathway.node_hash(@service, "", nil, tags) |> Pathway.pathway_hash(parent_hash)
  end

  defp queue_name(queue_url), do: queue_url |> String.split("/") |> List.last()

  # goaws answers the query protocol in XML and the JSON protocol in JSON; the
  # helpers use the protocol that is easiest to read for what they return.
  defp create_queue(name) do
    %{body: body} = Req.post!(@endpoint, form: [Action: "CreateQueue", QueueName: name])
    [_, queue_url] = Regex.run(~r"<QueueUrl>(.+)</QueueUrl>", body)
    queue_url
  end

  defp delete_queue(queue_url) do
    Req.post!(@endpoint, form: [Action: "DeleteQueue", QueueUrl: queue_url])
  end

  defp send_message(queue_url, body, datadog) do
    Req.post!(@endpoint,
      form: [
        {"Action", "SendMessage"},
        {"QueueUrl", queue_url},
        {"MessageBody", body},
        {"MessageAttribute.1.Name", "_datadog"},
        {"MessageAttribute.1.Value.DataType", "String"},
        {"MessageAttribute.1.Value.StringValue", datadog}
      ]
    )
  end

  defp receive_message(queue_url) do
    %{body: body} =
      Req.post!(@endpoint,
        headers: [
          {"content-type", "application/x-amz-json-1.0"},
          {"x-amz-target", "AmazonSQS.ReceiveMessage"}
        ],
        json: %{
          "QueueUrl" => queue_url,
          "MaxNumberOfMessages" => 1,
          "AttributeNames" => ["SentTimestamp"],
          "MessageAttributeNames" => ["_datadog"]
        },
        decode_body: false
      )

    %{"Messages" => [message]} = :json.decode(body)
    message
  end
end
