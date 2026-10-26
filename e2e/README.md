# End-to-end harness

A separate Mix project that drives the library through a real Broadway pipeline
on `BroadwayCloudPubSub.Pull.Producer`, against the Google Pub/Sub emulator, and
decodes the body the aggregator would have posted to the Datadog Agent.

It is deliberately not part of the library: the root project has no Broadway,
Pub/Sub or network dependency, and `mix compile`, `mix test`, `mix format` and
`mix deps.get` at the repository root neither compile nor format anything here.

## What each piece proves

- `run.exs` — a real Broadway pipeline on the client's pull producer:
  `DdDataStreams.attach()` installs the consumption handlers, which record one
  checkpoint per message with the broker's receive time and leave the
  checkpointed pathway in the process context, and `produce/3` records the
  produce checkpoint on that pathway and returns the `dd-pathway-ctx-base64`
  pair added to a message published to a second topic. The capture transport writes the flushed
  body to disk instead of posting it, so the run is observable without an agent.
  At the end it *checks* what happened — the context pathway is the consume
  checkpoint chained onto the inbound hash, the produce header chains onto that
  consume checkpoint, both replies reach the broker with the attribute, nothing
  failed to flush, and the decoded payload carries both hashes — and exits
  non-zero when any of that is missing. Printing is for the reader; the exit
  status is what CI uses.
- `lib/e2e/capture.ex` — the transport the script installs before the
  application starts. It saves the exact bytes and headers the agent would have
  received.
- `decoder/` — reads a captured body with the reference payload schema
  (MsgPack outer payload, DDSketch protobuf distributions) and prints the
  service, environment, language and every point with its edge tags, hash,
  parent hash, timestamp family and sample counts. This is what proves the wire
  shape and that a consume checkpoint chains onto the upstream hash and a
  produce checkpoint chains onto the consume hash.
- `stub_agent.exs` — an HTTP stub that answers `202 Accepted` to any request. It
  lets the library's agent acceptance test run on a machine with no Datadog
  Agent. It accepts any body and validates nothing.

## Prerequisites

- Elixir 1.19 / OTP 28
- Docker, for the Pub/Sub emulator
- Python 3, for the decoder

## Run the pipeline

Start the emulator once. The project name must be `dsm-test`, because the
emulator rejects any other project:

```sh
docker run --rm -d --name dsm-e2e-emulator \
  -p 127.0.0.1:8085:8085 \
  gcr.io/google.com/cloudsdktool/cloud-sdk:582.0.0-emulators \
  gcloud beta emulators pubsub start --project=dsm-test --host-port=0.0.0.0:8085
```

Fetch the dependencies and run the script:

```sh
cd e2e
mix deps.get
mix run --no-start run.exs
```

`--no-start` matters for startup ordering, not for the transport. The
aggregator re-reads `:dd_data_streams, :transport` from configuration on every
flush, so it would pick the capture transport up whenever it is set. What must
be settled before the application boots is whether the aggregator runs at all:
`DdDataStreams.Config.enabled?/0` is read once at start and the aggregator keeps
that answer. The script therefore puts the environment and the capture
transport in place first, and starts the application only once that state is
reached.

The run talks to the emulator at `PUBSUB_EMULATOR_HOST` when it is set and at
`127.0.0.1:8085` otherwise, so a second emulator on another port needs only that
variable, not an edit.

Every invocation creates its own topics and subscriptions (`in-topic-<run id>`,
`out-topic-<run id>`, `e2e-sub-<run id>`, `e2e-out-sub-<run id>`), so a second
run against the same emulator does not collide with the first. Set `E2E_RUN_ID`
to reuse a run id.

The run prints the consumed messages, their metadata, the decoded inbound
pathway, the consume checkpoint, the produce attributes, and the messages read
back from the second topic. The inbound pathway is stable because the header is
fixed; consume and produce hashes and the latencies vary with each run's receive
time. Expected essentials:

```text
run id: 1792699014834
published with header: ["75"]
published without header: ["76"]

== CONSUMED BROADWAY MESSAGE ==
metadata: %{
  attributes: %{
    "dd-pathway-ctx-base64" => "6gAAAAAAAACAoKv++WKA2uKB+mI=",
    "origin" => "go"
  },
  received_at: 1792699014982577561,
  subscription: "projects/dsm-test/subscriptions/e2e-sub-1792699014834",
  ...
}
decoded inbound pathway: %DdDataStreams.Pathway{hash: 234, pathway_start: 1700000000000, edge_start: 1700003600000}
consume pathway from attach: %DdDataStreams.Pathway{hash: 2940366034965408889, pathway_start: 1700000000000, edge_start: 1792699014982}
produce attributes: %{"dd-pathway-ctx-base64" => "iEc/sgQzhYOAoKv++WKs/YDTrGg="}

== PUBLISHED MESSAGES READ BACK FROM THE BROKER ==
data: reply:hello-with-header
attributes: %{"dd-pathway-ctx-base64" => "iEc/sgQzhYOAoKv++WKs/YDTrGg="}
decoded produce pathway: {:ok, %DdDataStreams.Pathway{hash: 9477037086134454152, ...}}

captured payloads: [".../e2e/capture-1792699014834/1792699018008378.bin"]

== VERIFIED ==
```

The second message has no inbound header, so its consume pathway starts at the
receive time and its parent hash is zero.

## Decode the captured body

Set up the decoder once:

```sh
cd e2e/decoder
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Then decode a body. `run.exs` prints the captured path, and runs the decoder
automatically when the virtualenv exists:

```sh
cd e2e
decoder/decode capture-<run id>/<timestamp>.bin
```

Or call the script directly from its own directory:

```sh
cd e2e/decoder
.venv/bin/python decode_payload.py ../capture-<run id>/<timestamp>.bin
```

`requirements.txt` pins the reference tracer (`ddtrace==3.17.0`) and the two
primitives the payload is read with (`msgpack`, `ddsketch`).

Expected:

```text
service: checkout
env: prod
lang: elixir
tracer_version: 0.1.0
version:
product_mask: 2
buckets: 3

bucket start=1700000000000000000 duration=10000000000
  tags=['direction:in', 'subscription:e2e-sub-...', 'type:google-pubsub'] hash=2940366034965408889 parent_hash=234 timestamp_type=origin samples={'pathway': 2, 'edge': 2, 'payload': 2}
  tags=['direction:out', 'topic:out-topic-...', 'type:google-pubsub'] hash=9477037086134454152 parent_hash=2940366034965408889 timestamp_type=origin ...
```

Every checkpoint appears twice, once in the `current` bucket and once in the
`origin` bucket, as the aggregator documents. A `pathway`/`edge`/`payload`
sample count of `2` means two consume checkpoints reached that point. The tags,
the parent/child relationship and the sample counts are stable; the hashes and
bucket starts change with the run's timestamps.

The same decoder can be handed to the library's emulator tier, which is the
`emulator` ExUnit tag. From the repository root:

```sh
DSM_PAYLOAD_DECODER=e2e/decoder/decode \
  PUBSUB_EMULATOR_HOST=127.0.0.1:8085 \
  mix test --only emulator
```

## Exercise the agent tier without an agent

From the repository root, start the stub:

```sh
elixir e2e/stub_agent.exs
```

In a second shell, also from the repository root:

```sh
DD_TRACE_AGENT_URL=http://127.0.0.1:8126 mix test --only agent
```

The test posts a flushed payload with the library's Req transport and asserts
the `202` it gets back. The stub prints the request line and body size and
accepts anything.

## Honest limits

- The stub agent is not a real agent. It validates nothing; a `202` from it
  shows the request was sent and the status handled, not that the pipeline stats
  schema is accepted by Datadog.
- A real agent does not decode the body either: `/v0.1/pipeline_stats` is a
  reverse proxy to the intake on `DD_SITE`, and the status the test sees is the
  intake's answer. So a keyed agent proves acceptance by the backend, while a
  keyless agent answers `500` (`Pipeline stats forwarder is OFF`) and proves
  nothing about the payload. That keyed run is manual: CI uses the stub, because
  the check needs a secret and sends data to a real account.
- The emulator's streaming surface is unused. The pipeline uses the pull
  producer, and the script's broker calls use the emulator's REST API. The
  client's streaming/gRPC path is not exercised.
- Only the pull producer is exercised. A push producer, ordering keys across a
  stream, and any service other than Pub/Sub are out of scope.
- The decoder is not dd-trace-go's generated `StatsPayload.DecodeMsg`: that
  decoder lives in a Go internal package and cannot be imported by a separate
  Go module. The decoder reads the same MsgPack payload and DDSketch protobuf
  with the msgpack and ddsketch packages, pinned alongside dd-trace-py 3.17.0.
- The emulator is not cleaned up by the script. Per-run resource names make
  reruns safe; remove the container when done.

## Stop the emulator

```sh
docker stop dsm-e2e-emulator
```
