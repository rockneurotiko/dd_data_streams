# DdDataStreams

[![CI](https://github.com/rockneurotiko/dd_data_streams/actions/workflows/ci.yml/badge.svg)](https://github.com/rockneurotiko/dd_data_streams/actions/workflows/ci.yml)

[Datadog Data Streams Monitoring][dsm] for Elixir pipelines running on Google
Pub/Sub.

Data Streams Monitoring follows a payload from the service that produced it to
the service that consumed it, so it can report how long the payload waited in
the queue between them and how the services are connected. This library adds the
Pub/Sub side of that: it records a checkpoint on each side of every edge, writes
the pathway onto the outgoing message, and ships the aggregated statistics to
the Datadog Agent.

It is a library, not an integration package: it has no dependency on Broadway or
on a Pub/Sub client. It hangs itself on the telemetry events Broadway already
emits and reads the message metadata that `BroadwayCloudPubSub` v2 produces.

[dsm]: https://docs.datadoghq.com/data_streams/

## What's in the box

* `DdDataStreams`: the entry point. `attach/1` installs the Broadway telemetry
  handlers, `publish/3` records a produce checkpoint and injects the pathway into
  the outgoing message, and `consume/2` and `checkpoint/2` do the same for a
  pipeline that does not run on Broadway.
* `DdDataStreams.Integrations.GooglePubSub`: the Pub/Sub consume and produce
  checkpoints, including the edge tags and the payload size measurement.
* `DdDataStreams.Pathway`: the pathway struct and its hashing, the identity of a
  node and of an edge.
* `DdDataStreams.Propagator`: the `dd-pathway-ctx-base64` header codec, byte
  compatible with the official Datadog tracers.
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

When the application starts, attach the handlers once, before Broadway starts:

```elixir
def start(_type, _args) do
  DdDataStreams.attach(subscription: "orders-sub")

  children = [
    MyApp.Pipeline
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

`attach/1` installs two telemetry handlers:

* `[:broadway, :processor, :message, :start]` records the consume checkpoint of
  each message, once per message, including for a leaf consumer that produces
  nothing.
* `[:broadway, :batch_processor, :start]` samples one of the batch's pathways so
  a `handle_batch/4` that publishes continues a pathway that connects back to
  the consumed messages. Fan-in keeps one pathway, not a join of all of them.

The only option is `:subscription`, used for a message whose metadata carries no
subscription. That is the case with `BroadwayCloudPubSub` v1, which also does
not stamp the receive time; such a message then reports no consume latency.

Calling `attach/1` twice is harmless: telemetry keeps one handler per id, so a
hot code upgrade does not double the checkpoints.

### Producing from a Broadway pipeline

In `handle_message/3`, call `DdDataStreams.publish/3` with the outgoing message
and your own publish function. The pathway is taken from the process context
that `attach/1` filled in for this message, and the function receives the
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

`consume/2` and `publish/3` are the same two checkpoints as functions, for a
pipeline that drives the library itself:

```elixir
{pathway, _checkpoint} =
  DdDataStreams.consume(
    %{
      data: message.data,
      attributes: message.attributes,
      ordering_key: nil,
      received_at: received_at
    },
    "orders-sub"
  )

# LATER, when the message is answered:
DdDataStreams.Context.put(pathway)

DdDataStreams.publish("orders-out", outgoing, &MyApp.PubSub.publish/1)
```

A message map needs `:data` and may carry `:attributes`, `:ordering_key` and
`:received_at` (in native time units). These are the values
`BroadwayCloudPubSub` v2 exposes as `message.data`,
`message.metadata.attributes`, `message.metadata.orderingKey` and
`message.metadata.received_at`.

Do not call `consume/2` yourself while `attach/1` is installed: the telemetry
handler already records that checkpoint, and the message would be counted twice.

### Checkpoints for other stages

`checkpoint/2` records a checkpoint for a stage that is neither a Pub/Sub
consume nor a produce, such as an internal queue:

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
transport, which is application configuration read once when the aggregator
starts.

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

The aggregator posts through the `DdDataStreams.Transport` implementation in the
application environment, so a host can reuse its own HTTP pool:

```elixir
config :dd_data_streams, transport: MyApp.AgentTransport
```

The key is read when the aggregator starts, so a change needs an application
restart. When it is absent, `DdDataStreams.Transport.Req` is used if Req is
available.

### When the enable flag takes effect

The flag is read in two different places, and the difference matters:

* `attach/1` and `publish/3` read it on every call, so they follow a runtime
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

A consume edge carries `direction:in`, `subscription:<subscription>` and
`type:google-pubsub`; a produce edge carries `direction:out`, `topic:<topic>` and
`type:google-pubsub`. The subscription is used verbatim, so a short name and a
`projects/<project>/subscriptions/<id>` resource name stay distinguishable.

Only tags in the vocabulary of `DdDataStreams.Tags` are reported:

* hashed, identifying a node: `bus`, `direction`, `exchange`, `topic`, `type`,
  `subscription`, `kafka_cluster_id`
* aggregation, splitting a node's statistics without changing its identity:
  `ds.name`, `ds.namespace`, `manual_checkpoint`, `group`, `consumer_group`
* per-message values, reported but never hashed: `partition`, `has_routing_key`

A produce measured for one topic is one node even when the messages are spread
over partitions, which is what lets downstream produce edges join up.

### Latency

A consume checkpoint is stamped with the time the broker handed the message to
the pipeline (`metadata.received_at`), not the time the processor picked it up.
The consume edge therefore reports the time the message spent in the
subscription, not Broadway's own backlog. A produce checkpoint is stamped at the
time of the publish call.

A message that carried no pathway starts a new one at its consume checkpoint, so
both latencies are zero for it.

### Payload size

The reported payload size is the data, the ordering key, and every attribute's
key and value, in bytes. The produce size is measured before the pathway
attribute is added, so it is not inflated by this library; a consume size does
include the inbound header (about 49 bytes), so the two sides of one edge differ
by that header.

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

The pathway travels in the `dd-pathway-ctx-base64` attribute, which is the same
format dd-trace-go, dd-trace-java and dd-trace-py use: the 64-bit FNV-1 hash of
the node in little-endian byte order followed by two zigzag varints of Unix
milliseconds, base64 encoded. Headers written by any of those tracers decode
here, and headers written here decode there. A malformed header is ignored and
the message starts a new pathway.

The Agent payload matches the `StatsPayload` shape of dd-trace-go v2.13, and the
DDSketch bins use the same mapping, so the distributions line up with the Datadog
backend.

One known naming difference: dd-trace-py tags a Pub/Sub consume edge with
`topic:<subscription>` instead of `subscription:<subscription>`, so a Python
consumer and this library do not join up on that node. dd-trace-java uses the
same consume tags as this library, except that it reduces a subscription
resource name to its id.

## Limitations

* Only Google Pub/Sub edges are provided. For another broker, build the
  checkpoint with `DdDataStreams.checkpoint/2` and the tags above.
* This is Data Streams Monitoring only: no APM traces, backlogs or transactions
  are collected.
* The Agent must be reachable over HTTP; Unix domain sockets are not supported.
* A message with no subscription anywhere records no consume checkpoint and
  leaves no pathway behind for a later produce to continue.
* Fan-in is sampled, not joined: a batch that consumed several pathways
  continues exactly one of them.

## Development

```sh
mix deps.get
mix test          # offline protocol core, no broker or agent
mix ci            # format, compile, credo, cover
mix docs          # module documentation
```

The test suite has tiers for the parts that need something running. They are
excluded by default, so a plain `mix test` cannot report them as passing while
nothing is listening:

```sh
PUBSUB_EMULATOR_HOST=127.0.0.1:8085 mix test --only emulator
DD_TRACE_AGENT_URL=http://127.0.0.1:8126 mix test --only agent
```

`e2e/README.md` describes the end-to-end harness: a real Broadway pipeline
against the Pub/Sub emulator, with a reference decoder for the payload the Agent
would receive.

## License

Beerware. See [LICENSE](LICENSE).
