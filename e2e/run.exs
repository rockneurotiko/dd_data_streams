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
  @base "http://127.0.0.1:8091"

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

defmodule E2e.Pipeline do
  use Broadway

  def handle_message(_processor, message, _context) do
    IO.puts("\n== CONSUMED BROADWAY MESSAGE ==")
    IO.puts("data: #{inspect(message.data)}")
    IO.puts("metadata: #{inspect(message.metadata, pretty: true, limit: :infinity)}")

    attributes = message.metadata.attributes || %{}
    IO.puts("decoded inbound pathway: #{inspect(DdDataStreams.Propagator.get(attributes))}")

    {_pathway, checkpoint} =
      DdDataStreams.consume(
        %{
          data: message.data,
          attributes: attributes,
          ordering_key: message.metadata.orderingKey,
          received_at: message.metadata.received_at
        },
        message.metadata.subscription
      )

    IO.puts("consume checkpoint: #{inspect(checkpoint, pretty: true, limit: :infinity)}")
    IO.puts("context pathway: #{inspect(DdDataStreams.Context.get())}")

    DdDataStreams.publish(
      E2e.Resources.out_topic(),
      %{data: "reply:#{message.data}", attributes: %{}, ordering_key: nil},
      fn outgoing ->
        IO.puts("produce attributes: #{inspect(outgoing.attributes)}")
        E2e.PubSub.publish(E2e.Resources.out_topic(), outgoing)
      end
    )

    message
  end
end

defmodule E2e.FlushErrors do
  def handle(event, measurements, metadata, _config) do
    IO.puts("flush error: #{inspect({event, measurements, metadata})}")
  end
end

E2e.Resources.init!()
System.put_env("CAPTURE_DIR", Path.join(__DIR__, "capture-#{E2e.Resources.id()}"))

System.put_env(%{
  "DD_DATA_STREAMS_ENABLED" => "true",
  "DD_SERVICE" => "checkout",
  "DD_ENV" => "prod",
  "DD_TRACE_AGENT_URL" => "http://127.0.0.1:8126"
})

# Set before the application starts: the aggregator reads its transport once, at
# start. This is why this script runs with `mix run --no-start`.
Application.put_env(:dd_data_streams, :transport, E2e.Capture)

{:ok, _} = Application.ensure_all_started(:req)
{:ok, _} = Application.ensure_all_started(:dd_data_streams)
{:ok, _} = Application.ensure_all_started(:broadway)

E2e.PubSub.create_topic(E2e.Resources.in_topic())
E2e.PubSub.create_topic(E2e.Resources.out_topic())
E2e.PubSub.create_subscription(E2e.Resources.sub(), E2e.Resources.in_topic())
E2e.PubSub.create_subscription(E2e.Resources.out_sub(), E2e.Resources.out_topic())

DdDataStreams.attach([])

{:ok, _pid} =
  Broadway.start_link(E2e.Pipeline,
    name: E2e.Pipeline,
    producer: [
      module:
        {BroadwayCloudPubSub.Pull.Producer,
         subscription: E2e.Resources.sub_resource(),
         base_url: "http://127.0.0.1:8091",
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

:telemetry.attach(
  "capture-flush",
  [:dd_data_streams, :flush, :error],
  &E2e.FlushErrors.handle/4,
  nil
)

# Stopping the aggregator flushes every bucket, including the partial one.
GenServer.stop(DdDataStreams.Aggregator)
Process.sleep(500)

files = Path.wildcard(Path.join(System.get_env("CAPTURE_DIR"), "*.bin"))
IO.puts("\ncaptured payloads: #{inspect(files)}")

latest = files |> Enum.sort() |> List.last()

decoder = Path.join(__DIR__, "decoder/decode")
venv = Path.join(__DIR__, "decoder/.venv/bin/python")

if File.exists?(latest) and File.exists?(venv) do
  {output, status} = System.cmd(decoder, [latest], stderr_to_stdout: true)

  IO.puts("\n== REFERENCE DECODER (dd-trace-py toolchain) ==")
  IO.puts(output)

  if status != 0, do: raise("reference decoder rejected #{latest}")
else
  IO.puts("""
  reference decoder skipped: set up decoder/.venv first
    cd decoder && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
  then run decoder/decode #{latest || "<captured.bin>"}
  """)
end
