defmodule E2e.Resources do
  # Every run gets its own topic and subscription names, because the emulator
  # rejects re-creating a name that already exists and a run must not need a
  # clean emulator.
  def init! do
    id = System.get_env("E2E_RUN_ID") || Integer.to_string(System.system_time(:millisecond))
    :persistent_term.put({__MODULE__, :id}, id)
    IO.puts("run id: #{id}")
    id
  end

  def id, do: :persistent_term.get({__MODULE__, :id})
  def in_topic, do: "in-topic-#{id()}"
  def out_topic, do: "out-topic-#{id()}"
  def sub, do: "e2e-sub-#{id()}"
  def out_sub, do: "e2e-out-sub-#{id()}"
  def sub_resource, do: "projects/dsm-test/subscriptions/#{sub()}"
end

defmodule E2e.Token do
  def fetch, do: {:ok, "emulator-token"}
end

defmodule E2e.PubSub do
  @base (case System.get_env("PUBSUB_EMULATOR_HOST") do
           nil -> "http://127.0.0.1:8085"
           host -> "http://" <> host
         end)

  def base, do: @base

  def create_topic(topic) do
    Req.put!("#{@base}/v1/projects/dsm-test/topics/#{topic}",
      json: %{},
      headers: [{"authorization", "Bearer emulator"}]
    )
  end

  def create_subscription(sub, topic) do
    Req.put!("#{@base}/v1/projects/dsm-test/subscriptions/#{sub}",
      json: %{topic: "projects/dsm-test/topics/#{topic}", ackDeadlineSeconds: 10},
      headers: [{"authorization", "Bearer emulator"}]
    )
  end

  def publish(topic, %{data: data, attributes: attributes, ordering_key: ordering_key}) do
    message = %{"data" => Base.encode64(data)}

    message =
      if attributes && attributes != %{},
        do: Map.put(message, "attributes", attributes),
        else: message

    message = if ordering_key, do: Map.put(message, "orderingKey", ordering_key), else: message

    response =
      Req.post!("#{@base}/v1/projects/dsm-test/topics/#{topic}:publish",
        json: %{"messages" => [message]},
        headers: [{"authorization", "Bearer emulator"}]
      )

    {:ok, response.body["messageIds"]}
  end

  def pull(sub) do
    Req.post!("#{@base}/v1/projects/dsm-test/subscriptions/#{sub}:pull",
      json: %{"maxMessages" => 10, "returnImmediately" => true},
      headers: [{"authorization", "Bearer emulator"}]
    ).body
  end
end

defmodule E2e.Observed do
  # What the pipeline saw, so the run can be checked instead of only printed.
  def init, do: :persistent_term.put(__MODULE__, [])
  def put(entry), do: :persistent_term.put(__MODULE__, [entry | entries()])
  def entries, do: :persistent_term.get(__MODULE__, [])
end

defmodule E2e.Pipeline do
  use Broadway

  def handle_message(_processor, message, _context) do
    IO.puts("\n== CONSUMED BROADWAY MESSAGE ==")
    IO.puts("data: #{inspect(message.data)}")
    IO.puts("metadata: #{inspect(message.metadata, pretty: true, limit: :infinity)}")

    metadata = message.metadata
    attributes = metadata.attributes || %{}
    inbound = DdDataStreams.Propagator.get(attributes)
    IO.puts("decoded inbound pathway: #{inspect(inbound)}")

    # attach/0 recorded the consume checkpoint for this message before Broadway
    # called this function, and left the checkpointed pathway in the context.
    consume = DdDataStreams.Context.get()
    IO.puts("consume pathway from attach: #{inspect(consume)}")

    outgoing_data = "reply:#{message.data}"

    attributes = Enum.into(DdDataStreams.produce(E2e.Resources.out_topic(), outgoing_data), attributes)

    outgoing = %{data: outgoing_data, attributes: attributes, ordering_key: nil}
    IO.puts("produce attributes: #{inspect(outgoing.attributes)}")

    E2e.Observed.put(%{
      data: message.data,
      inbound: inbound,
      subscription: metadata.subscription,
      received_at: DateTime.to_unix(metadata.receivedAt, :native),
      publish_time: metadata.publishTime,
      consume: consume,
      produce: outgoing.attributes
    })

    E2e.PubSub.publish(E2e.Resources.out_topic(), outgoing)

    message
  end
end

defmodule E2e.FlushErrors do
  def init, do: :persistent_term.put(__MODULE__, 0)

  def handle(event, measurements, metadata, _config) do
    IO.puts("flush error: #{inspect({event, measurements, metadata})}")
    :persistent_term.put(__MODULE__, count() + 1)
  end

  def count, do: :persistent_term.get(__MODULE__, 0)
end

defmodule E2e.Verify do
  # What the run has to be able to state, rather than what it prints. A failure
  # here is a broken pipeline, so the script exits non-zero.

  alias DdDataStreams.{Config, Pathway, Propagator}

  def run!(out_messages, captured, decoder_output) do
    observed = E2e.Observed.entries()
    entries = Map.new(observed, &{&1.data, &1})

    check!(
      map_size(entries) == 2,
      "expected two consumed messages, saw #{inspect(map_size(entries))}"
    )

    with_header = entries["hello-with-header"]
    without_header = entries["hello-no-header"]
    check!(with_header != nil and without_header != nil, "a message was not consumed")

    inbound = with_header.inbound
    check!(inbound != nil, "the dd-trace-go header did not decode")

    checked =
      for {_data, entry} <- entries do
        check!(entry.consume != nil, "attach/0 recorded no consume checkpoint")
        check!(is_binary(entry.subscription), "the message metadata carries no subscription")
        check!(is_integer(entry.received_at), "the message metadata carries no receive time")

        parent = if entry.inbound, do: entry.inbound.hash, else: 0
        consume_hash = Pathway.pathway_hash(node_hash(consume_tags(entry.subscription)), parent)
        received_ms = System.convert_time_unit(entry.received_at, :native, :millisecond)

        check!(
          entry.consume.hash == consume_hash,
          "the consume pathway does not chain onto the inbound pathway"
        )

        check!(
          entry.consume.edge_start == received_ms,
          "the consume checkpoint is not stamped with the receive time"
        )

        check!(
          entry.consume.pathway_start == pathway_start(entry, received_ms),
          "the consume checkpoint moved the pathway start"
        )

        encoded = entry.produce["dd-pathway-ctx-base64"]
        check!(encoded != nil, "a published message carries no pathway attribute")

        check!(
          Map.has_key?(entry.produce, "origin"),
          "merged inbound attributes did not survive the produce"
        )

        produce_hash =
          Pathway.pathway_hash(node_hash(produce_tags(E2e.Resources.out_topic())), consume_hash)

        {:ok, decoded} = Propagator.decode_str(encoded)

        check!(
          decoded.hash == produce_hash,
          "the produce header does not chain onto the consume checkpoint"
        )

        check!(
          decoded.pathway_start == entry.consume.pathway_start,
          "the produce header moved the pathway start"
        )

        %{parent: parent, consume_hash: consume_hash, produce_hash: produce_hash}
      end

    published = Enum.map(out_messages, & &1["message"]["attributes"]["dd-pathway-ctx-base64"])

    check!(
      length(published) == 2,
      "expected two messages on the out topic, saw #{length(published)}"
    )

    check!(
      Enum.all?(published, &is_binary/1),
      "a published message reached the broker without a pathway attribute"
    )

    check!(
      Enum.all?(out_messages, fn
        %{"message" => %{"attributes" => attrs}} when is_map(attrs) -> Map.has_key?(attrs, "origin")
        _ -> false
      end),
      "inbound attributes did not survive to the broker"
    )

    check!(captured != [], "the aggregator flushed nothing to the agent")

    check!(
      E2e.FlushErrors.count() == 0,
      "the aggregator reported #{E2e.FlushErrors.count()} flush errors"
    )

    if decoder_output do
      for tag <- ["direction:in", "direction:out", "type:google-pubsub"] do
        check!(
          String.contains?(decoder_output, tag),
          "the decoder did not find #{tag} in the payload"
        )
      end

      for entry <- checked,
          hash <- [entry.consume_hash, entry.produce_hash, entry.parent],
          hash > 0 do
        check!(
          String.contains?(decoder_output, Integer.to_string(hash)),
          "the payload the agent would receive does not contain hash #{hash}"
        )
      end
    end

    IO.puts("\n== VERIFIED ==")
  end

  defp node_hash(tags) do
    Pathway.node_hash(Config.service(), Config.env(), Config.primary_tag(), tags)
  end

  defp consume_tags(subscription) do
    ["direction:in", "subscription:" <> short_id(subscription), "type:google-pubsub"]
  end

  # The Pub/Sub extractor reports the short id, the last segment of the
  # resource name the subscription metadata carries.
  defp short_id(resource), do: resource |> String.split("/") |> List.last()

  defp produce_tags(topic) do
    ["direction:out", "topic:" <> topic, "type:google-pubsub"]
  end

  # A message that carries a header keeps the start its producer wrote. One that
  # does not starts a pathway at the publish time the bus stamped on it, so the
  # consume side reports how long the message waited; only a message with no
  # publish time falls back to the receive time.
  defp pathway_start(%{inbound: %Pathway{pathway_start: start}}, _received_ms), do: start

  defp pathway_start(%{publish_time: %DateTime{} = time}, _received_ms),
    do: DateTime.to_unix(time, :millisecond)

  defp pathway_start(_entry, received_ms), do: received_ms

  defp check!(true, _message), do: :ok
  defp check!(false, message), do: raise("verification failed: #{message}")
end

E2e.Resources.init!()
System.put_env("CAPTURE_DIR", Path.join(__DIR__, "capture-#{E2e.Resources.id()}"))

System.put_env(%{
  "DD_DATA_STREAMS_ENABLED" => "true",
  "DD_DATA_STREAMS_HEADER" => "dd-pathway-ctx-base64",
  "DD_SERVICE" => "checkout",
  "DD_ENV" => "prod",
  "DD_TRACE_AGENT_URL" => "http://127.0.0.1:8126"
})

# Set before the application starts, which is why this script runs with `mix
# run --no-start`: whether the aggregator runs is decided once at start from
# `enabled?`, so the configuration has to be in place before the application
# boots. The transport itself is re-read from configuration on every flush.
Application.put_env(:dd_data_streams, :transport, E2e.Capture)

{:ok, _} = Application.ensure_all_started(:req)
{:ok, _} = Application.ensure_all_started(:dd_data_streams)
{:ok, _} = Application.ensure_all_started(:broadway)

E2e.Observed.init()
E2e.FlushErrors.init()

:telemetry.attach(
  "capture-flush",
  [:dd_data_streams, :flush, :error],
  &E2e.FlushErrors.handle/4,
  nil
)

E2e.PubSub.create_topic(E2e.Resources.in_topic())
E2e.PubSub.create_topic(E2e.Resources.out_topic())
E2e.PubSub.create_subscription(E2e.Resources.sub(), E2e.Resources.in_topic())
E2e.PubSub.create_subscription(E2e.Resources.out_sub(), E2e.Resources.out_topic())

DdDataStreams.attach()

{:ok, _pid} =
  Broadway.start_link(E2e.Pipeline,
    name: E2e.Pipeline,
    producer: [
      module:
        {BroadwayCloudPubSub.Pull.Producer,
         subscription: E2e.Resources.sub_resource(),
         base_url: E2e.PubSub.base(),
         token_generator: {E2e.Token, :fetch, []},
         receive_interval: 200},
      concurrency: 1
    ],
    processors: [default: [concurrency: 1]]
  )

# A header written by dd-trace-go, so the consume checkpoint has an upstream
# parent to chain onto.
real_header = "6gAAAAAAAACAoKv++WKA2uKB+mI="

{:ok, ids} =
  E2e.PubSub.publish(E2e.Resources.in_topic(), %{
    data: "hello-with-header",
    attributes: %{"dd-pathway-ctx-base64" => real_header, "origin" => "go"},
    ordering_key: nil
  })

IO.puts("published with header: #{inspect(ids)}")

{:ok, ids2} =
  E2e.PubSub.publish(E2e.Resources.in_topic(), %{
    data: "hello-no-header",
    attributes: %{"origin" => "none"},
    ordering_key: nil
  })

IO.puts("published without header: #{inspect(ids2)}")

Process.sleep(3_000)

# The outbound messages must carry the injected attribute; read them back.
out = E2e.PubSub.pull(E2e.Resources.out_sub())

IO.puts("\n== PUBLISHED MESSAGES READ BACK FROM THE BROKER ==")

for %{"message" => message} <- out["receivedMessages"] || [] do
  IO.puts("data: #{Base.decode64!(message["data"])}")
  IO.puts("attributes: #{inspect(message["attributes"])}")

  case message["attributes"]["dd-pathway-ctx-base64"] do
    nil ->
      IO.puts("no pathway attribute")

    b64 ->
      IO.puts("decoded produce pathway: #{inspect(DdDataStreams.Propagator.decode_str(b64))}")
  end
end

IO.puts("\n== TELEMETRY ==")

# Stopping the aggregator flushes every bucket, including the partial one.
GenServer.stop(DdDataStreams.Aggregator)
Process.sleep(500)

files = Path.wildcard(Path.join(System.get_env("CAPTURE_DIR"), "*.bin"))
IO.puts("captured payloads: #{inspect(files)}")

latest = files |> Enum.sort() |> List.last()

decoder = Path.join(__DIR__, "decoder/decode")
venv = Path.join(__DIR__, "decoder/.venv/bin/python")

decoder_output =
  if File.exists?(latest) and File.exists?(venv) do
    {output, status} = System.cmd(decoder, [latest], stderr_to_stdout: true)

    IO.puts("\n== REFERENCE DECODER (dd-trace-py toolchain) ==")
    IO.puts(output)

    if status != 0, do: raise("reference decoder rejected #{latest}")
    output
  else
    IO.puts("""
    reference decoder not run: set up decoder/.venv first
      cd decoder && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    then run decoder/decode #{latest || "<captured.bin>"}
    """)

    nil
  end

E2e.Verify.run!(out["receivedMessages"] || [], files, decoder_output)
