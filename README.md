# DdDataStreams

[![CI](https://github.com/rockneurotiko/dd_data_streams/actions/workflows/ci.yml/badge.svg)](https://github.com/rockneurotiko/dd_data_streams/actions/workflows/ci.yml)

[Datadog Data Streams Monitoring][dsm] for Elixir pipelines running on
[Broadway][broadway].

Data Streams Monitoring follows a payload from the service that produced it to
the service that consumed it, so it can report how long the payload waited in
the queue between them and how the services are connected. This library adds the
Broadway side of that: it records a checkpoint on each side of every edge,
writes the pathway onto the outgoing message, and ships the aggregated
statistics to the Datadog Agent.

It is a library, not an integration package: it has no dependency on Broadway
and none on any broker client. It hangs itself on the telemetry events Broadway
already emits, so it works with any producer, and the two things that are
broker-specific - what a message's metadata says about the edge it arrived on,
and the `type:` technology to report - are one module behind
`DdDataStreams.Extractor`. One extractor is provided for
`BroadwayCloudPubSub` v2; any other broker is one small module of your own, and
an application can list several to run pipelines on more than one bus at once.

[dsm]: https://docs.datadoghq.com/data_streams/
[broadway]: https://hexdocs.pm/broadway

## What's in the box

* `DdDataStreams`: the entry point. `attach/0` installs the Broadway telemetry
  handlers, `publish/3` records a produce checkpoint and injects the pathway into
  the outgoing message, and `consume/1` and `checkpoint/2` do the same for a
  pipeline that does not run on Broadway (`publish/4` takes the extractor to use
  when the stage publishes to a different bus than it consumed from).
* `DdDataStreams.Extractor`: the behaviour that names the edge of a message from
  its metadata. Configured with `config :dd_data_streams, extractors:`.
* `DdDataStreams.Extractor.GooglePubSub`: the extractor for
  `BroadwayCloudPubSub` v2, and the default.
* `DdDataStreams.Checkpoints`: the consume and produce checkpoints of a message,
  given its edge tags. It knows no broker.
* `DdDataStreams.Pathway`: the pathway struct and its hashing, the identity of a
  node and of an edge.
* `DdDataStreams.Propagator`: the `dd-pathway-ctx-base64` header codec, byte
  compatible with the official Datadog tracers, over an attribute map or a
  header list.
* `DdDataStreams.Aggregator`: a GenServer that buckets checkpoints and flushes
  them to the Agent every ten seconds.
* `DdDataStreams.Payload` and `DdDataStreams.Sketch`: the StatsPayload MsgPack
  encoder and the DDSketch distributions the intake expects.
* `DdDataStreams.Tags`: the canonical tag vocabulary.
* `DdDataStreams.Context`: the pathway the current process produces from.
* `DdDataStreams.Config`: the environment variables that configure the library.
* `DdDataStreams.Transport`: the behaviour for posting to the Agent, with a Req
  implementation as the default.

## Installation

Add `:dd_data_streams` to your dependencies:

```elixir
def deps do
  [
    {:dd_data_streams, "~> 0.1.0"}
  ]
end
```

`:req` is an optional dependency of this library. It provides the default
transport to the Agent; if you pass your own transport (see
[Application configuration](#application-configuration)), you do not need it, and
if you do not have it the startup logs an error telling you to configure one.

The library requires Elixir 1.17 or later.

## Usage

The library is off until it is enabled:

```sh
DD_DATA_STREAMS_ENABLED=true
```

When the application starts, attach the handlers once, before Broadway starts.
It takes no options: the configured extractors decide what each message means.

```elixir
def start(_type, _args) do
  DdDataStreams.attach()

  children = [
    MyApp.Pipeline
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

`attach/0` installs two telemetry handlers:

* `[:broadway, :processor, :message, :start]` records the consume checkpoint of
  each message, once per message, including for a leaf consumer that produces
  nothing.
* `[:broadway, :batch_processor, :start]` samples one of the batch's pathways so
  a `handle_batch/4` that publishes continues a pathway that connects back to
  the consumed messages. Fan-in keeps one pathway, not a join of all of them.

Both events are emitted by Broadway itself, for every producer, so a pipeline
that runs on Kafka, SQS, RabbitMQ or anything else is attached the same way. The
only thing that changes is the extractors.

Calling `attach/0` twice is harmless: telemetry keeps one handler per id, so a
hot code upgrade does not double the checkpoints.

### Choosing extractors

An extractor turns a Broadway message's `metadata` into the edge it arrived on.
Configure a list of them, one per technology the application talks to:

```elixir
config :dd_data_streams,
  extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub]
```

Every message is offered to the extractors in order, and the first one that
recognises its metadata names its edge, so put the more specific one first. The
Pub/Sub extractor is the default when the key is absent, so a Pub/Sub pipeline
configures nothing: a message names its consume edge by its subscription, and
`BroadwayCloudPubSub` v2 stamps the subscription, the attributes, the ordering
key and the receive time, which is everything a checkpoint needs. A message that
carries no subscription records no consume checkpoint, because there is no name
to report it under.

Another broker is one module implementing `DdDataStreams.Extractor`. Its
moduledoc has a `BroadwayKafka` example: the same three lines of metadata
mapping, and the `type:` Datadog files those streams under.

On the produce side there is no metadata to look at, only a topic, so a produce
is reported by the extractor that consumed the message being answered. A stage
that changes technology names the outgoing one:

```elixir
def handle_message(_processor, message, _context) do
  # Consumed from Kafka, published to Pub/Sub.
  DdDataStreams.publish("orders-out", outgoing, &MyApp.PubSub.publish/1,
    extractor: DdDataStreams.Extractor.GooglePubSub
  )

  message
end
```

### Producing from a Broadway pipeline

In `handle_message/3`, call `DdDataStreams.publish/3` with the outgoing message
and your own publish function. The pathway is taken from the process context
that `attach/0` filled in for this message, and the function receives the
message with `dd-pathway-ctx-base64` added to its attributes:

```elixir
def handle_message(_processor, message, _context) do
  DdDataStreams.publish(
    "orders-out",
    %{
      data: encode(message.data),
      attributes: %{"source" => "checkout"},
      ordering_key: nil
    },
    fn outgoing ->
      MyApp.PubSub.publish("orders-out", outgoing.data, attributes: outgoing.attributes)
    end
  )

  message
end
```

The outgoing message carries its headers in `:attributes` (a map) or in
`:headers` (a list of `{key, value}` pairs, as `BroadwayKafka` calls them); the
pathway goes into whichever one the message already uses, so the call site hands
back the same shape its producer takes.

The publish function's return value is returned unchanged, so nothing has to be
unwrapped:

```elixir
:ok =
  DdDataStreams.publish("orders-out", message, fn outgoing ->
    MyApp.PubSub.publish(outgoing)
  end)
```

In `handle_batch/4`, the context already holds the fan-in pathway, so the same
call works. Publishing one message per consumed message means calling
`publish/3` once per message, not chaining one call's result into the next: all
of them then share one produce node with the consume node as their parent.

When the library is disabled, `publish/3` injects nothing and simply calls your
function with the message as it is, so the call site needs no guard.

Call `publish/3` from the stage process itself, not from a `Task` it spawns: the
context is kept per process, so a task sees no pathway and starts a new one.

### Without Broadway

`consume/1` and `publish/3` are the same two checkpoints as functions, for a
pipeline that drives the library itself. `consume/1` takes a Broadway message,
or a map shaped like one - the body in `:data`, the producer's metadata in
`:metadata` - so the extractor names the edge exactly as it does for Broadway:

```elixir
{pathway, _checkpoint} =
  DdDataStreams.consume(%{
    data: message.data,
    metadata: %{
      subscription: "orders-sub",
      attributes: message.attributes,
      orderingKey: nil,
      received_at: received_at
    }
  })

# LATER, when the message is answered:
DdDataStreams.Context.put(pathway)

DdDataStreams.publish("orders-out", outgoing, &MyApp.PubSub.publish/1)
```

It returns `nil`, and records nothing, when the extractor finds no edge in the
metadata. The metadata is the extractor's own contract: for the Pub/Sub
extractor those are the values `BroadwayCloudPubSub` v2 exposes as
`message.metadata.subscription`, `.attributes`, `.orderingKey` and `.received_at`
(native time units).

Do not call `consume/1` yourself while `attach/0` is installed: the telemetry
handler already records that checkpoint, and the message would be counted twice.

### Checkpoints for other stages

`checkpoint/2` records a checkpoint for a stage that is not a bus edge, such as
an internal queue, and takes its edge tags verbatim:

```elixir
{pathway, _checkpoint} =
  DdDataStreams.checkpoint(
    DdDataStreams.Context.get() || DdDataStreams.Pathway.new(%{}),
    ["direction:in", "type:internal-queue", "manual_checkpoint:true"]
  )
```

It does not change the process context; a produce in that stage should continue
the pathway you passed in.

All checkpoints are handed to the aggregator, which is not running while the
library is disabled, so these calls need no guard at the call site.

## Configuration

Everything is read from the process environment when it is needed, except the
transport and the extractor, which are application configuration.

### Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_DATA_STREAMS_ENABLED` | `false` | Turns the library on. `true` or `1`, case-insensitive. |
| `DD_SERVICE` | `unnamed-elixir-service` | The service name reported for this pipeline. |
| `DD_ENV` | empty | The deployment environment. |
| `DD_VERSION` | empty | The application version, reported as `Version` in the payload. |
| `DD_PRIMARY_TAG` | empty | The primary tag, hashed into every node identity. |
| `DD_TRACE_AGENT_URL` | unset | Agent base URL; wins over host and port when set and non-empty. |
| `DD_AGENT_HOST` | `localhost` | Agent host, when `DD_TRACE_AGENT_URL` is unset. |
| `DD_TRACE_AGENT_PORT` | `8126` | Agent port, when `DD_TRACE_AGENT_URL` is unset. |

Unix domain sockets are not supported. The Agent adds the API key itself, so no
key is configured here and no authentication header is sent.

### Application configuration

Two keys are application configuration:

```elixir
config :dd_data_streams,
  # Defaults to [DdDataStreams.Extractor.GooglePubSub].
  extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub],
  # Defaults to DdDataStreams.Transport.Req when Req is available.
  transport: MyApp.AgentTransport
```

The extractors are resolved on every attach, publish and consume call, so a
change applies without a restart. The transport is read when the aggregator starts, so
a change to it needs an application restart. When the transport key is absent,
`DdDataStreams.Transport.Req` is used if Req is available.

### When the enable flag takes effect

The flag is read in two different places, and the difference matters:

* `attach/0` and `publish/3` read it on every call, so they follow a runtime
  change: attaching after boot installs handlers, and disabling stops header
  injection.
* `DdDataStreams.Application` reads it once, when the application starts. The
  aggregator is only started if the library was enabled at boot; turning the
  flag on later leaves handlers recording checkpoints that the aggregator drops
  in silence, and turning it off later leaves the aggregator running.

Set `DD_DATA_STREAMS_ENABLED` in the environment before the VM starts, and
restart the application to change it.

## What is reported

### Edge tags

Edge tags come from the extractor. The Pub/Sub one reports a consume edge as
`direction:in`, `subscription:<subscription>`, `type:google-pubsub` and a produce
edge as `direction:out`, `topic:<topic>`, `type:google-pubsub`. The subscription
is used verbatim, so a short name and a
`projects/<project>/subscriptions/<id>` resource name stay distinguishable.

`type:` is the queueing technology Datadog files the stream under, and it is
hashed into every node, so it has to be the same for every service in a pipeline,
and every edge of one bus is reported by one extractor.

Only tags in the vocabulary of `DdDataStreams.Tags` are reported:

* hashed, identifying a node: `bus`, `direction`, `exchange`, `topic`, `type`,
  `subscription`, `kafka_cluster_id`
* aggregation, splitting a node's statistics without changing its identity:
  `ds.name`, `ds.namespace`, `manual_checkpoint`, `group`, `consumer_group`
* per-message values, reported but never hashed: `partition`, `has_routing_key`

A produce measured for one topic is one node even when the messages are spread
over partitions, which is what lets downstream produce edges join up.

### Latency

A consume checkpoint is stamped with the time the message reached the pipeline
(the metadata's receive time), not the time the processor picked it up. The
consume edge therefore reports the time the message spent in the queue, not
Broadway's own backlog. A produce checkpoint is stamped at the time of the
publish call.

A message that carried no pathway starts a new one at its consume checkpoint, so
both latencies are zero for it.

### Payload size

The reported payload size is the data, the ordering key, and every attribute's
or header's key and value, in bytes. The produce size is measured before the
pathway header is added, so it is not inflated by this library; a consume size
does include the inbound header (about 49 bytes), so the two sides of one edge
differ by that header.

### Flushing

`DdDataStreams.Aggregator` buckets checkpoints in ten-second windows and posts
one MsgPack, gzip-compressed payload per service to
`<agent-url>/v0.1/pipeline_stats` every ten seconds. Each checkpoint is counted
twice, in the bucket it belongs to and in the bucket that contains the start of
its pathway, as the official tracers do; the two entries differ only in bucket
start and timestamp type. A bucket is dropped before it is sent, so a failed
send loses that bucket rather than growing memory; the failure is logged and
reported through telemetry, and nothing is raised. Stopping the aggregator sends
the partial bucket too.

The aggregator is started and supervised by `:dd_data_streams` itself, so the
host does not add it to its own supervision tree.

## Telemetry

The library emits one event:

* `[:dd_data_streams, :flush, :error]`, once per failed Agent flush, with
  `%{count: 1}` measurements and `%{service: service, reason: reason}` metadata.

The HTTP request is best effort and is not retried.

## Interoperability

The pathway travels in the `dd-pathway-ctx-base64` header, which is the same
format dd-trace-go, dd-trace-java and dd-trace-py use: the 64-bit FNV-1 hash of
the node in little-endian byte order followed by two zigzag varints of Unix
milliseconds, base64 encoded. Headers written by any of those tracers decode
here, and headers written here decode there. A malformed header is ignored and
the message starts a new pathway.

The Agent payload matches the `StatsPayload` shape of dd-trace-go v2.13, and the
DDSketch bins use the same mapping, so the distributions line up with the Datadog
backend.

For the Pub/Sub extractor there are two known naming differences: dd-trace-py
tags a consume edge with `topic:<subscription>` instead of
`subscription:<subscription>`, and dd-trace-java reduces a subscription resource
name to its id. Both change the node's identity, so those consumers appear as a
different node rather than joining the one reported here; the pathway itself
still connects, because the link between two nodes is the hash carried on the
message.

## Limitations

* A message is named by the first extractor that recognises its metadata, and a
  produce by the extractor that consumed the message being answered. A stage
  that produces without consuming, or that publishes to a different bus than it
  consumed from, says which extractor by passing `extractor:` to
  `DdDataStreams.publish/4`.
* This is Data Streams Monitoring only: no APM traces, backlogs or transactions
  are collected.
* The Agent must be reachable over HTTP; Unix domain sockets are not supported.
* A message whose metadata names no edge records no consume checkpoint and
  leaves no pathway behind for a later produce to continue.
* Fan-in is sampled, not joined: a batch that consumed several pathways
  continues exactly one of them.

## Development

```sh
mix deps.get
mix test          # default tier: the offline protocol core
mix ci            # format, compile, credo, cover over the default tier
mix docs          # module documentation
```

The suite has tiers for the parts that need something running. They are excluded
by default, so a plain `mix test` cannot report them as passing while nothing is
listening.

### `emulator`

A receive/publish round trip against the Google Pub/Sub emulator over its REST
API, with the message handed to the library in the shape
`BroadwayCloudPubSub` v2 produces. It exercises the broker side: resource-name
subscriptions, attributes surviving a publish, the receive time.

```sh
docker run --rm -d -p 127.0.0.1:8085:8085 \
  gcr.io/google.com/cloudsdktool/cloud-sdk:582.0.0-emulators \
  gcloud beta emulators pubsub start --project=dsm-test --host-port=0.0.0.0:8085

PUBSUB_EMULATOR_HOST=127.0.0.1:8085 mix test --only emulator
```

### `agent`

One flushed payload posted to `<DD_TRACE_AGENT_URL>/v0.1/pipeline_stats`. The
Agent proxies that route to the pipeline stats intake, so the status code is the
intake's own answer: `202` means the backend accepted the schema, `500` means
the Agent has no API key to forward with, and a `4xx` means the intake rejected
the payload. This is the tier that validates the wire format.

```sh
DD_TRACE_AGENT_URL=http://127.0.0.1:8126 mix test --only agent
```

`e2e/stub_agent.exs` answers `202` to any request, so the tier also runs on a
machine with no Datadog Agent. That proves the transport and nothing about the
payload: a stub accepts any body by design, so the schema is only checked by a
real Agent with an API key.

### The end-to-end harness

`e2e/README.md` describes it: a separate Mix project running a real Broadway
pipeline on `BroadwayCloudPubSub.Pull.Producer` against the same emulator, with a
capture transport instead of the Agent and a reference decoder that reads the
body back.

## License

Beerware. See [LICENSE](LICENSE).
