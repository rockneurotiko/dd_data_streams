defmodule DdDataStreams.PubSubEmulatorTest do
  # A receive/publish round trip against the Google Pub/Sub emulator over its
  # REST API, so the broker side of the integration is exercised against a real
  # Pub/Sub server rather than a function. The message that comes back out of
  # the emulator is handed to the library in the shape BroadwayCloudPubSub v2
  # produces, and the reply is published with the pair the library's produce
  # checkpoint returns.
  #
  # The emulator is not part of the default suite: this is the `emulator` tier,
  # excluded from `mix test` and run with
  #
  #   PUBSUB_EMULATOR_HOST=127.0.0.1:8085 mix test --only emulator
  #
  # The project must be the one the emulator was started with, because it rejects
  # any other:
  #
  #   docker run -d -p 127.0.0.1:8085:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:582.0.0-emulators \
  #     gcloud beta emulators pubsub start --project=dsm-test --host-port=0.0.0.0:8085
  #
  # DSM_PAYLOAD_DECODER may name a command that takes the path of a captured
  # pipeline stats body, decodes it and prints it. `e2e/decoder/decode` is that
  # command: a Python decoder over the msgpack package and the ddsketch
  # protobuf, pinned alongside dd-trace-py 3.17.0 in
  # `e2e/decoder/requirements.txt`. When it is set the flushed body is handed to
  # it; when it is not, that assertion is skipped because the library
  # deliberately carries no MsgPack decoder. The pipeline itself is proven by
  # the harness in `e2e/`, a separate Mix project in this repository where
  # Broadway and BroadwayCloudPubSub are real dependencies; here Broadway is not
  # a dependency, so its message metadata is built by hand.
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Checkpoints
  alias DdDataStreams.Context
  alias DdDataStreams.Extractor.GooglePubSub
  alias DdDataStreams.Pathway
  alias DdDataStreams.Propagator
  alias DdDataStreams.Transport

  @moduletag :emulator

  @emulator System.get_env("PUBSUB_EMULATOR_HOST")
  @project System.get_env("PUBSUB_EMULATOR_PROJECT", "dsm-test")
  @base (case System.get_env("PUBSUB_EMULATOR_HOST") do
           nil -> ""
           host -> "http://" <> host
         end)

  @reference Path.expand("../fixtures/interop/reference_decoders.txt", __DIR__)
             |> File.read!()
             |> Code.eval_string()
             |> elem(0)

  # dd-trace-go v2 and dd-trace-py 3.17.0 both read this header back as the
  # pathway below, and both re-encode it to the same bytes.
  @header Enum.find(
            @reference,
            &(&1.provenance =~ "1700003600000" and &1.hash == 2_367_537_495_353_648_021)
          )

  @service "checkout"
  @env "prod"
  @consume_latency_ms 250

  # The aggregator is reached by its registered name. This transport records the
  # request the agent would have received instead of sending it; the stub below
  # replaces it, and is allowed into the aggregator's own process.

  setup_all do
    if @emulator in [nil, ""] do
      raise """
      PUBSUB_EMULATOR_HOST is not set, so there is no broker to talk to.

      Start the Pub/Sub emulator (see e2e/README.md) and run:

          PUBSUB_EMULATOR_HOST=127.0.0.1:8085 mix test --only emulator
      """
    end

    {:ok, _apps} = Application.ensure_all_started(:req)
    :ok
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)
    previous = Map.new(variables, &{&1, System.get_env(&1)})

    System.put_env("DD_SERVICE", @service)
    System.put_env("DD_ENV", @env)
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    System.delete_env("DD_DATA_STREAMS_HEADER")

    suffix = System.unique_integer([:positive])

    names = %{
      in_topic: "interop-in-#{suffix}",
      out_topic: "interop-out-#{suffix}",
      in_subscription: "interop-in-sub-#{suffix}",
      out_subscription: "interop-out-sub-#{suffix}"
    }

    assert put_topic(names.in_topic) == 200
    assert put_topic(names.out_topic) == 200
    assert put_subscription(names.in_subscription, names.in_topic) == 200
    assert put_subscription(names.out_subscription, names.out_topic) == 200

    test = self()

    stub(Transport.Req, :post, fn url, headers, body ->
      send(test, {:flush, url, headers, IO.iodata_to_binary(body)})
      {:ok, 202}
    end)

    on_exit(fn ->
      Context.clear()
      delete(@base <> "/v1/projects/#{@project}/subscriptions/#{names.out_subscription}")
      delete(@base <> "/v1/projects/#{@project}/subscriptions/#{names.in_subscription}")
      delete(@base <> "/v1/projects/#{@project}/topics/#{names.out_topic}")
      delete(@base <> "/v1/projects/#{@project}/topics/#{names.in_topic}")

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    %{names: names}
  end

  test "the broker delivers the published attributes unchanged", %{names: names} do
    attributes = %{"dd-pathway-ctx-base64" => @header.base64, "origin" => "go"}

    assert [_message_id] = publish(names.in_topic, "payload", attributes)

    assert [%{"ackId" => ack_id, "message" => message}] = pull(names.in_subscription)
    assert message["attributes"] == attributes

    assert acknowledge(names.in_subscription, [ack_id]) == 200
    assert pull(names.in_subscription) == []
  end

  test "a received message yields a consume checkpoint and the reply carries the pathway",
       %{names: names} do
    started = start_supervised!({Aggregator, []})
    allow(Transport.Req, self(), started)

    attributes = %{"dd-pathway-ctx-base64" => @header.base64}

    assert [_message_id] = publish(names.in_topic, "payload", attributes)
    assert [%{"ackId" => ack_id, "message" => message}] = pull(names.in_subscription)

    # What BroadwayCloudPubSub v2 hands a processor: the broker's attributes and
    # ordering key, the subscription the producer was configured with (as the
    # full resource name), the receive time in native units, and the broker's
    # own identifiers.
    received_ms = @header.edge_start + @consume_latency_ms

    received_at = System.convert_time_unit(received_ms, :millisecond, :native)

    metadata = %{
      attributes: message["attributes"],
      orderingKey: message["orderingKey"],
      subscription: "projects/#{@project}/subscriptions/#{names.in_subscription}",
      receivedAt: DateTime.from_unix!(received_at, :native),
      messageId: message["messageId"],
      publishTime: message["publishTime"],
      deliveryAttempt: nil
    }

    data = Base.decode64!(message["data"])

    pathway = DdDataStreams.consume(%{data: data, metadata: metadata})

    # The consume checkpoint is recorded by `consume/1` and no longer
    # returned, so recompute it here for the assertions below.
    {:ok, consume_tags, consume_parts} =
      GooglePubSub.consume(metadata)

    {_recomputed_pathway, checkpoint} =
      Checkpoints.consume(
        Map.put(consume_parts, :data, data),
        Enum.filter(consume_tags, &is_binary/1),
        received_at
      )

    assert checkpoint.tags == [
             "direction:in",
             "subscription:#{names.in_subscription}",
             "type:google-pubsub"
           ]

    assert checkpoint.parent_hash == @header.hash
    assert checkpoint.timestamp == received_ms
    assert checkpoint.pathway_latency == received_ms - @header.pathway_start
    assert checkpoint.edge_latency == @consume_latency_ms

    assert checkpoint.payload_size ==
             byte_size(data) + byte_size("dd-pathway-ctx-base64") + byte_size(@header.base64)

    produce_tags = ["direction:out", "topic:#{names.out_topic}", "type:google-pubsub"]

    reply_data = "reply:" <> data
    [{key, value}] = DdDataStreams.produce(names.out_topic, reply_data)
    assert [out_message_id] = publish(names.out_topic, reply_data, %{key => value})

    assert is_binary(out_message_id)
    assert acknowledge(names.in_subscription, [ack_id]) == 200

    # The injected attribute survives the broker and continues the consumed node.
    assert [%{"message" => reply}] = pull(names.out_subscription)
    assert encoded = reply["attributes"][key]
    assert {:ok, decoded} = Propagator.decode_str(encoded)

    assert decoded.hash ==
             Pathway.pathway_hash(
               Pathway.node_hash(@service, @env, nil, produce_tags),
               pathway.hash
             )

    assert decoded.pathway_start == @header.pathway_start

    # Stopping the aggregator flushes every bucket, including the one the
    # checkpoints just landed in, through the transport the agent would have
    # received.
    ref = Process.monitor(started)
    assert :ok = GenServer.stop(started)
    assert_receive {:DOWN, ^ref, :process, ^started, :normal}

    body = flushed_body()
    payload = :zlib.gunzip(body)

    assert String.contains?(payload, "direction:in")
    assert String.contains?(payload, "direction:out")

    assert String.contains?(
             payload,
             "subscription:#{names.in_subscription}"
           )

    assert String.contains?(payload, "topic:#{names.out_topic}")
    assert String.contains?(payload, <<0xCF, checkpoint.hash::big-64>>)
    assert String.contains?(payload, <<0xCF, decoded.hash::big-64>>)

    assert_reference_decoder(body)
  end

  defp flushed_body do
    assert_receive {:flush, url, headers, body}, 2_000
    assert String.ends_with?(url, "/v0.1/pipeline_stats")

    assert headers == [
             {"content-type", "application/msgpack"},
             {"content-encoding", "gzip"},
             {"datadog-meta-lang", "elixir"}
           ]

    refute_receive {:flush, _, _, _}, 100
    body
  end

  # The reference decoder is e2e/decoder/decode, run on the flushed body because
  # the library carries no MsgPack code.
  defp assert_reference_decoder(body) do
    case System.get_env("DSM_PAYLOAD_DECODER") do
      nil ->
        :ok

      decoder ->
        path =
          Path.join(
            System.tmp_dir!(),
            "dsm-flushed-#{System.unique_integer([:positive])}.msgpack.gz"
          )

        File.write!(path, body)
        on_exit(fn -> File.rm(path) end)

        assert {output, 0} = System.cmd(decoder, [path], stderr_to_stdout: true)

        for tag <- ["direction:in", "direction:out", "type:google-pubsub"] do
          assert String.contains?(output, tag)
        end
    end
  end

  defp put_topic(topic) do
    {:ok, response} =
      Req.put("#{@base}/v1/projects/#{@project}/topics/#{topic}",
        json: %{},
        retry: false,
        receive_timeout: 5_000
      )

    response.status
  end

  defp put_subscription(subscription, topic) do
    {:ok, response} =
      Req.put("#{@base}/v1/projects/#{@project}/subscriptions/#{subscription}",
        json: %{topic: "projects/#{@project}/topics/#{topic}", ackDeadlineSeconds: 10},
        retry: false,
        receive_timeout: 5_000
      )

    response.status
  end

  defp publish(topic, data, attributes) do
    message = %{"data" => Base.encode64(data)}

    message =
      if attributes in [nil, %{}], do: message, else: Map.put(message, "attributes", attributes)

    {:ok, response} =
      Req.post("#{@base}/v1/projects/#{@project}/topics/#{topic}:publish",
        json: %{"messages" => [message]},
        retry: false,
        receive_timeout: 5_000
      )

    assert response.status == 200
    response.body["messageIds"]
  end

  # `returnImmediately` matters: an empty subscription makes a pull block until a
  # message arrives, which would hang the suite instead of returning nothing.
  defp pull(subscription) do
    {:ok, response} =
      Req.post("#{@base}/v1/projects/#{@project}/subscriptions/#{subscription}:pull",
        json: %{"maxMessages" => 10, "returnImmediately" => true},
        retry: false,
        receive_timeout: 5_000
      )

    assert response.status == 200
    response.body["receivedMessages"] || []
  end

  defp acknowledge(subscription, ack_ids) do
    {:ok, response} =
      Req.post("#{@base}/v1/projects/#{@project}/subscriptions/#{subscription}:acknowledge",
        json: %{"ackIds" => ack_ids},
        retry: false,
        receive_timeout: 5_000
      )

    response.status
  end

  defp delete(url) do
    Req.delete(url, retry: false, receive_timeout: 5_000)
  end
end
