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
  handlers, `produce/3` records a produce checkpoint and returns the `{key,
  value}` pair to add to the outgoing message, `produce_batch/3` does the same
  for a batch, and `consume/1` and `checkpoint/2` cover a pipeline that does not
  run on Broadway (`produce/3` takes `extractor:` when the stage publishes to a
  different bus than it consumed from).
* `DdDataStreams.Extractor`: the behaviour that names the edge of a message from
  its metadata. Configured with `config :dd_data_streams, extractors:`.
* `DdDataStreams.Extractor.GooglePubSub`: the extractor for
  `BroadwayCloudPubSub` v2, and the default.
* `DdDataStreams.Checkpoints`: the consume and produce checkpoints of a
  pathway, given edge tags. It knows no broker.
* `DdDataStreams.Pathway`: the pathway struct and its hashing, the identity of a
  node and of an edge.
* `DdDataStreams.Propagator`: the pathway header codec (default key
  `dd-pathway-ctx-base64`), byte compatible with the official Datadog tracers,
  over an attribute map or a header list.
* `DdDataStreams.Carrier`: broker-specific carriers for the pathway header.
  `Flat` handles attribute maps and header lists; `Sqs` unwraps the `_datadog`
  JSON message attribute BroadwaySQS exposes.
* `DdDataStreams.Aggregator`: a GenServer that buckets checkpoints and flushes
  them to the Agent every ten seconds.
* `DdDataStreams.Payload` and `DdDataStreams.Sketch`: the StatsPayload MsgPack
  encoder and the DDSketch distributions the intake expects.
* `DdDataStreams.Tags`: the canonical tag vocabulary.
* `DdDataStreams.Context`: the pathway the current process produces from.
* `DdDataStreams.Config`: the environment and application settings that
  configure the library.
* `DdDataStreams.Transport`: the behaviour for posting to the Agent, with a Req
  implementation as the default and a `DdDataStreams.Transport.Capture`
  implementation for tests.

## SQS wrapping

For SQS, BroadwaySQS exposes the pathway under the `_datadog` message
attribute as a JSON object. `DdDataStreams.Carrier.Sqs` unwraps that JSON to
read the pathway and merges a produced header into it, so a producer wraps the
`{key, value}` pair returned by `DdDataStreams.produce/3` into the `_datadog`
attribute's JSON rather than adding it as a separate attribute:

```elixir
attributes = existing_attributes

attributes =
  case DdDataStreams.produce("orders", data) do
    [{key, value}] -> Map.put(attributes, "_datadog", Jason.encode!(%{key => value}))
    [] -> attributes
  end
```

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

## Full workflow

One service publishes to a topic; a Broadway pipeline consumes from a
subscription on that topic and publishes to another topic. The library records
one checkpoint on each side of each edge and carries the pathway in a metadata
key between them. It never publishes or consumes anything itself.

### 1. Producer

Measure the payload, ask the library for the metadata pair, and add it to the
message your client sends. For Pub/Sub:

```elixir
defmodule MyApp.Orders do
  def publish(order) do
    data = Jason.encode!(order)

    attributes = existing_attributes

    attributes =
      case DdDataStreams.produce("orders", data) do
        [{key, value}] -> Map.put(attributes, key, value)
        [] -> attributes
      end

    MyApp.PubSub.publish("orders", data, attributes: attributes)
  end
end
```

`produce/3` records the produce checkpoint and returns a list containing
`{key, value}`; `[]` means the library is disabled, so there is nothing to add. The key is
`"dd-pathway-ctx-base64"` unless configured (see
[Configuration](#configuration)), and the value is the encoded pathway.

For a batch published to one topic, use `produce_batch/3`. It records one
checkpoint per payload and returns one pair for the whole batch:

```elixir
def publish_orders(orders) do
  payloads = Enum.map(orders, &Jason.encode!/1)

  attributes = existing_attributes

  attributes =
    case DdDataStreams.produce_batch("orders", payloads) do
      [{key, value}] -> Map.put(attributes, key, value)
      [] -> attributes
    end

  MyApp.PubSub.publish_batch("orders", payloads, attributes: attributes)
end
```

Kafka uses the same pair as a header:

```elixir
headers = existing_headers

headers =
  case DdDataStreams.produce("orders", data) do
    [{key, value}] -> headers ++ [{key, value}]
    [] -> headers
  end

MyApp.Kafka.produce("orders", data, headers: headers)
```

The `[...]` pattern assumes the library is enabled; the Pub/Sub example
shows the `[]` branch.

Call `produce/3` immediately before the send. The checkpoint is recorded when
the function runs, so a send that never happens still reports a produce.

### 2. Consumer

Attach once at start up, before Broadway starts:

```elixir
def start(_type, _args) do
  DdDataStreams.attach()
  Supervisor.start_link([MyApp.Pipeline], strategy: :one_for_one)
end
```

The handler records the consume checkpoint for every message, using the
producer's pathway value when it is present. In `handle_message/3`, answer with
`produce/3` exactly as the producer did:

```elixir
defmodule MyApp.Pipeline do
  use Broadway

  def handle_message(_processor, message, _context) do
    data = Jason.encode!(handle(message.data))

    attributes = existing_attributes

    attributes =
      case DdDataStreams.produce("orders-out", data) do
        [{key, value}] -> Map.put(attributes, key, value)
        [] -> attributes
      end

    MyApp.PubSub.publish("orders-out", data, attributes: attributes)
    message
  end
end
```

The pathway is taken from the process context, so the produce edge continues
the consumed one. In `handle_batch/4` the context holds one pathway sampled from
the batch; use `produce_batch/3` when one send answers the whole batch.

A pipeline that does not use Broadway can call `consume/1` itself:

```elixir
pathway =
  DdDataStreams.consume(%{
    data: data,
    metadata: %{
      subscription: "orders-sub",
      attributes: attributes,
      orderingKey: nil,
      received_at: received_at
    }
  })

# In the same process:
out_attributes = existing_attributes

out_attributes =
  case DdDataStreams.produce("orders-out", out_data) do
    [{key, value}] -> Map.put(out_attributes, key, value)
    [] -> out_attributes
  end

MyApp.PubSub.publish("orders-out", out_data, attributes: out_attributes)
```

`consume/1` already leaves the pathway in the context. If the produce happens in
another process, pass the pathway with `DdDataStreams.Context.put/1` there.

### 3. What travels on the message

The producer adds one entry: the key from `DdDataStreams.Config.header/0`, by
default `dd-pathway-ctx-base64`, with a base64 value containing the node hash and
the pathway and edge start times. Merge it into the metadata the message already
carries; do not replace the other attributes or headers. The broker must
preserve it on the way through.

The consumer reads it automatically. A missing or malformed value is not an
error: the message starts a new pathway, with zero latency.

### 4. What Datadog shows

Once the Agent receives the flushed stats (about 10 seconds, then a few minutes
for the first UI data), Data Streams Monitoring shows:

- the service and topic/subscription nodes on the Data Streams map;
- throughput per edge;
- `data_streams.latency` distributions (p50/p75/p95/p99) for the edge and the
  full pathway;
- `data_streams.payload_size` distributions.

There are no APM traces or spans, and no message-level detail. A consumer-only
deployment starts a new pathway, so its edge latency is zero.

### 5. Checklist

- The Agent is reachable at `DD_TRACE_AGENT_URL` (or `DD_AGENT_HOST` /
  `DD_TRACE_AGENT_PORT`) and has an API key.
- `DD_SERVICE` is set (defaults to `unnamed-elixir-service`); `DD_ENV` is
  optional and an empty value is sent as-is.
- `:req` is a dependency, or a custom `DdDataStreams.Transport` is configured.
- `DdDataStreams.attach/0` runs once before Broadway starts.
- The producer and consumer use the same `DdDataStreams.Config.header/0` value.
- The broker preserves the attribute or header.

## Usage

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

`attach/0` validates the configured extractors before it attaches and raises
`ArgumentError` when `config :dd_data_streams, extractors:` is not a non-empty
list of extractor modules. That validation runs even while the library is
disabled, so a configuration mistake fails when the application starts instead
of waiting for the flag to be flipped.

To stop recording without restarting the application, `DdDataStreams.detach/0`
removes the handlers `attach/0` installed:

```elixir
DdDataStreams.attach()
# ...
DdDataStreams.detach()
```

It returns `:ok` whether or not anything was attached, so a test can end with it
unconditionally; checkpoints already handed to the aggregator are still flushed.
`DdDataStreams.status/0` answers "is it recording?" from the local VM - no
Datadog Agent needed - with the enabled flag, whether the handlers are attached,
whether the aggregator is running, and the effective transport, agent URL,
header, service, environment and extractors.

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
  # Consumed from Kafka, publishing to Pub/Sub.
  data = encode(message.data)

  attributes = existing_attributes

  attributes =
    case DdDataStreams.produce("orders-out", data,
           extractor: DdDataStreams.Extractor.GooglePubSub
         ) do
      [{key, value}] -> Map.put(attributes, key, value)
      [] -> attributes
    end

  MyApp.PubSub.publish("orders-out", data, attributes: attributes)
  message
end
```

### Producing from a Broadway pipeline

In `handle_message/3`, call `DdDataStreams.produce/3` with the outgoing payload.
It records the produce checkpoint and returns the `{key, value}` pair to add to
the message; it does not publish. The pathway is taken from the process context
that `attach/0` filled in for this message:

```elixir
def handle_message(_processor, message, _context) do
  data = encode(message.data)

  attributes = existing_attributes

  attributes =
    case DdDataStreams.produce("orders-out", data) do
      [{key, value}] -> Map.put(attributes, key, value)
      [] -> attributes
    end

  MyApp.PubSub.publish("orders-out", data, attributes: attributes)
  message
end
```

The key is `DdDataStreams.Config.header/0`, `"dd-pathway-ctx-base64"` unless
configured. A client that takes a header list uses the same pair:

```elixir
headers = existing_headers

headers =
  case DdDataStreams.produce("orders-out", data) do
    [{key, value}] -> headers ++ [{key, value}]
    [] -> headers
  end

MyApp.Kafka.produce("orders-out", data, headers: headers)
```

Fan-in re-derives each message's consume checkpoint from the batch
messages' metadata, so a `handle_message/3` that returns the message unchanged
keeps the batch linked. Replacing `message.metadata` entirely (for example by
building a new `%Broadway.Message{}`) drops the inbound carrier: the batch then
fans in without a parent pathway and the produce edge starts a new one. Preserve
the metadata, or at least carry its inbound pathway, when transforming a message.

In `handle_batch/4`, the context holds one pathway sampled from the batch.
`produce_batch/3` records one checkpoint per payload and returns one pair for
the whole batch, so one send can answer the whole batch:

```elixir
def handle_batch(_processor, messages, _batch_info, _context) do
  payloads = Enum.map(messages, &encode(&1.data))

  attributes = existing_attributes

  attributes =
    case DdDataStreams.produce_batch("orders-out", payloads) do
      [{key, value}] -> Map.put(attributes, key, value)
      [] -> attributes
    end

  MyApp.PubSub.publish_batch("orders-out", payloads, attributes: attributes)
  messages
end
```

`produce_batch/3` returns `[]` for an empty batch; both functions return `nil`
when the library is disabled, so the call site needs no guard beyond that.

Producing one message per consumed message means calling `produce/3` once per
message, not chaining one call's result into the next: all of them then share
one produce node with the consume node as their parent.

Call `produce/3` from the stage process itself, not from a `Task` it spawns: the
context is kept per process, so a task sees no pathway and starts a new one.
Call it immediately before the send: the checkpoint is recorded when the
function runs, so a send that never happens still reports a produce.

### Without Broadway

`consume/1` and `produce/3` are the same two checkpoints as functions, for a
pipeline that drives the library itself. `consume/1` takes a Broadway message,
or a map shaped like one - the body in `:data`, the producer's metadata in
`:metadata` - so the extractor names the edge exactly as it does for Broadway:

```elixir
pathway =
  DdDataStreams.consume(%{
    data: message.data,
    metadata: %{
      subscription: "orders-sub",
      attributes: message.attributes,
      orderingKey: nil,
      received_at: received_at
    }
  })

# LATER, when the message is answered, in the same process:
data = encode(reply)

attributes = existing_attributes

attributes =
  case DdDataStreams.produce("orders-out", data) do
    [{key, value}] -> Map.put(attributes, key, value)
    [] -> attributes
  end

MyApp.PubSub.publish("orders-out", data, attributes: attributes)
```

`consume/1` returns `nil`, and records nothing, when the extractor finds no edge
in the metadata. It leaves the pathway in the context of the calling process; if
the answer is produced in another process, set it there with
`DdDataStreams.Context.put(pathway)` and pass `extractor:` to `produce/3` when
several extractors are configured.

The metadata is the extractor's own contract: for the Pub/Sub extractor those
are the values `BroadwayCloudPubSub` v2 exposes as
`message.metadata.subscription`, `.attributes`, `.orderingKey` and `.received_at`
(native time units).

Do not call `consume/1` yourself while `attach/0` is installed: the telemetry
handler already records that checkpoint, and the message would be counted twice.

### Checkpoints for other stages

`checkpoint/2` records a checkpoint for a stage that is not a bus edge, such as
an internal queue, and takes its edge tags verbatim:

```elixir
pathway =
  DdDataStreams.checkpoint(
    ["direction:in", "type:internal-queue", "manual_checkpoint:true"]
  )
```

It does not change the process context; a produce in that stage should continue
the pathway you passed in.

`manual_checkpoint:true` is an aggregation tag, so it splits the statistics of
a node: two checkpoints of the same node, meaning the same hash-tier tags and
therefore the same hash, that differ in their aggregation tags are counted
apart and reported as separate points that share that hash.

All checkpoints are handed to the aggregator, which is not running while the
library is disabled, so these calls need no guard at the call site.

## Configuration

Every value is read when it is needed, never at compile time: from the
application configuration below when that key is set, and from the process
environment otherwise. Application configuration wins over the environment, and
an unset, empty or blank value counts as unset, so one setting can be given in
either place.

### Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_DATA_STREAMS_ENABLED` | unset, on | Turns the library off when it reads as false: `false`, `0`, `no`, `off`. |
| `DD_SERVICE` | `unnamed-elixir-service` | The service name reported for this pipeline. |
| `DD_ENV` | empty | The deployment environment, sent as-is (the library does not normalize it). |
| `DD_VERSION` | empty | The application version, reported as `Version` in the payload. |
| `DD_PRIMARY_TAG` | empty | The primary tag, hashed into every node identity. |
| `DD_DATA_STREAMS_HEADER` | `dd-pathway-ctx-base64` | Attribute/header key the pathway travels under; the producer and consumer must use the same value. |
| `DD_TRACE_AGENT_URL` | unset | Agent base URL; wins over host and port when set and non-empty. |
| `DD_AGENT_HOST` | `localhost` | Agent host, when `DD_TRACE_AGENT_URL` is unset. |
| `DD_TRACE_AGENT_PORT` | `8126` | Agent port, when `DD_TRACE_AGENT_URL` is unset. |

Unix domain sockets are not supported. The Agent adds the API key itself, so no
key is configured here and no authentication header is sent.

### Application configuration

Each environment variable in the table above has a key of the same meaning
here, and the key wins when both are set:

```elixir
config :dd_data_streams,
  # Defaults to true. A boolean here wins over DD_DATA_STREAMS_ENABLED.
  enabled: true,
  service: "checkout",
  env: "prod",
  version: "1.2.3",
  primary_tag: "region:eu",
  # The attribute/header key `produce/3` returns and `consume/1` reads.
  header: "dd-pathway-ctx-base64",
  # A port is accepted as a string or an integer.
  agent_url: "http://localhost:8126",
  agent_host: "localhost",
  agent_port: 8126,
  # Defaults to [DdDataStreams.Extractor.GooglePubSub].
  extractors: [MyApp.KafkaExtractor, DdDataStreams.Extractor.GooglePubSub],
  # Defaults to DdDataStreams.Transport.Req when Req is available.
  transport: MyApp.AgentTransport
```

When each setting is read:

| Setting | Read when |
|---|---|
| `enabled` | at attach, produce and consume time, and once when the application starts (for the aggregator) |
| `service`, `env`, `primary_tag` | per checkpoint; `service` and `env` again when building the payload |
| `version` | when building the payload (on flush) |
| `header` | on every produce and consume call |
| `extractors` | on attach, on every consume, and on produce only as a fallback |
| `agent_url`, `agent_host`, `agent_port` | on every flush |
| `transport` | on every flush, and once when the application starts (for the default) |

So changing any of them at runtime with `Application.put_env/3` or
`System.put_env/2` affects the next use, except that a runtime change cannot
start or stop the aggregator; see below. When the transport key is absent,
`DdDataStreams.Transport.Req` is used if Req is available.

### Turning it off

The library is on as soon as you depend on it: adding the dependency and calling
`attach/0` is the opt-in. Datadog's other clients default the other way and
require `DD_DATA_STREAMS_ENABLED=true`, but their tracer is already running and
data streams monitoring is one more product on top of it, while this library does
nothing else.

The flag stays supported, as a kill switch an operator can flip without a
release. Two ways to set it, in this order:

* `config :dd_data_streams, enabled:` when it is a boolean. This wins, which is
  what makes `config/test.exs` a sensible place to keep the aggregator out of a
  test suite.
* otherwise `DD_DATA_STREAMS_ENABLED`, read as false from `false`, `0`, `no` or
  `off` (case-insensitive). Unset, empty or anything else leaves the library on.

The flag is read in two different places, and the difference matters:

* `attach/0`, `produce/3` and `produce_batch/3` read it on every call, so they
  follow a runtime change: attaching after boot installs handlers, and disabling
  stops producing the pair.
* `DdDataStreams.Application` reads it once, when the application starts. The
  aggregator is only started if the library was enabled at boot; turning the
  flag on later leaves handlers recording checkpoints that the aggregator drops
  in silence, and turning it off later leaves the aggregator running.

Restart the application to change whether the aggregator runs.

## Testing your pipeline

What a test suite should do depends on what it is testing: the pipeline's own
code, or the library's effect on it. Both recipes start in `config/test.exs`.

### Unit tests: turn the library off

```elixir
# config/test.exs
config :dd_data_streams, enabled: false
```

With the flag off, `attach/0` installs no handlers, `produce/3` and
`produce_batch/3` return `[]`, `consume/1` and `checkpoint/1` record nothing,
and no aggregator is started when the application boots. The pipeline code
under test makes the same calls and takes the same branches as in production,
but touches nothing global, so every test can be `async: true`: there is no
handler id and no aggregator process for two tests to fight over.

### Integration tests: keep it on and capture the flushes

```elixir
# config/test.exs
config :dd_data_streams,
  enabled: true,
  transport: DdDataStreams.Transport.Capture
```

`DdDataStreams.Transport.Capture` posts nothing: each flush is reported on the
`[:dd_data_streams, :transport, :captured]` telemetry event, whose metadata is
`%{url: url, headers: headers, body: body}` with the exact gzip-compressed
payload the agent would have received (`:zlib.gunzip/1` reads it back). The
aggregator is started when the application boots, reading this transport like
any other.

A test attaches a handler under its own id, so it receives only what is
flushed while it runs, and calls `DdDataStreams.Aggregator.flush(:all)` to
send every bucket at once - including the one still being filled, which
`flush/0` keeps until its ten-second window completes. A checkpoint recorded a
moment ago is therefore assertable immediately:

```elixir
# In an async: false test: it observes the shared aggregator's flushes.
defmodule MyApp.OrdersTest do
  use ExUnit.Case, async: false

  setup do
    test = self()
    handler = "capture-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:dd_data_streams, :transport, :captured],
      fn _event, _measurements, metadata, _config -> send(test, {:captured, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok
  end

  test "the produce checkpoint reaches the aggregator" do
    [{_key, _value}] = DdDataStreams.produce("orders-out", Jason.encode!(order))

    :ok = DdDataStreams.Aggregator.flush(:all)

    assert_receive {:captured, %{body: body}}
    assert :zlib.gunzip(body) =~ "topic:orders-out"
  end
end
```

A test that exercises the Broadway handlers ends with `DdDataStreams.detach/0`,
which removes the handlers `attach/0` installed and returns `:ok` whether or
not anything was attached, so `on_exit` can call it unconditionally.

### The handler id is fixed

`attach/0` installs its handlers under one VM-global telemetry id,
`:dd_data_streams`, and `detach/0` removes that one id for the whole VM. Two
async tests that each attach and detach would therefore race: one test's
cleanup removes the handlers the other is still recording with. Tests that
call `attach/0` or `detach/0` - anything exercising the Broadway path - run
with `async: false`. Tests that never attach are unaffected: every unit test
with the library disabled, and any test that calls `consume/1`, `produce/3`
or `checkpoint/1` directly, never touches the id. The capture handler above
uses a per-test id of its own, so it never collides either.

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

Every tag from the extractor is reported. What the vocabulary of
`DdDataStreams.Tags` decides is what else each key does:

* hashed, identifying a node: `bus`, `direction`, `exchange`, `topic`, `type`,
  `subscription`, `kafka_cluster_id`
* aggregation, splitting a node's statistics without changing its identity:
  `ds.name`, `ds.namespace`, `manual_checkpoint`, `group`, `consumer_group`
* per-message values, reported but never hashed: `partition`, `has_routing_key`

Any other key is reported as it is: it hashes into no node and splits no
statistics.

A produce measured for one topic is one node even when the messages are spread
over partitions, which is what lets downstream produce edges join up.

### Latency

A consume checkpoint is stamped with the time the message reached the pipeline
(the metadata's receive time), not the time the processor picked it up. The
consume edge therefore reports the time the message spent in the queue, not
Broadway's own backlog. A produce checkpoint is stamped at the time of the
produce call.

A message that carried no pathway starts a new one at its consume checkpoint, so
both latencies are zero for it.

### Payload size

The consume size is the data, the ordering key, and every attribute's or
header's key and value, in bytes. It includes the inbound pathway header (about
49 bytes).

The produce size is the size of `data` by default, or the `:payload_size` you
pass to `produce/3`; `produce_batch/3` measures each payload separately. The
library adds the size of the `{key, value}` pair it returns - the configured
header key plus the encoded value - so both sides of one edge measure the same
message. Measure the message without that pair.

### Flushing

`DdDataStreams.Aggregator` buckets checkpoints in ten-second windows and posts
one MsgPack, gzip-compressed payload per service to
`<agent-url>/v0.1/pipeline_stats` every ten seconds. Each checkpoint is counted
twice, in the bucket it belongs to and in the bucket that contains the start of
its pathway, as the official tracers do; the two entries differ only in bucket
start and timestamp type. A bucket is dropped before it is sent, so a failed
send loses that bucket rather than growing memory; the failure is reported via
telemetry, and nothing is raised. Stopping the aggregator sends
the partial bucket too, unless the agent has not answered within ten seconds,
in which case the node keeps shutting down without it.

`DdDataStreams.Aggregator.flush(:all)` sends every bucket immediately, the
partial one included; `flush/0` keeps its meaning of complete buckets only.
That is the seam a test uses to assert on a checkpoint without waiting for the
window - see [Testing your pipeline](#testing-your-pipeline).

The aggregator is started and supervised by `:dd_data_streams` itself, so the
host does not add it to its own supervision tree.

## Telemetry

The library emits three events:

* `[:dd_data_streams, :transport, :captured]`, once per flush through
  `DdDataStreams.Transport.Capture`, with `%{count: 1}` measurements and
  `%{url: url, headers: headers, body: body}` metadata. Only a suite that
  configured the capture transport (see
  [Testing your pipeline](#testing-your-pipeline)) ever sees it.

* `[:dd_data_streams, :flush, :error]`, once per failed Agent flush, with
  `%{count: 1}` measurements and `%{service: service, reason: reason}` metadata.

* `[:dd_data_streams, :handler, :error]`, once per failure: one per failing
  extractor, one per edge the library cannot use, and one per message or batch
  the handler could not record. The measurements are `%{count: 1}` and the
  metadata is `%{event: event, extractor: extractor, reason: reason,
  stacktrace: stacktrace}`, with `nil` for the keys that do not apply. The
  reason is the exception that was raised, `:unusable_edge` for an edge the
  library cannot use, or `{kind, reason}` for a call that threw or exited.

The HTTP request is best effort and is not retried.

## Interoperability

The pathway travels in the `dd-pathway-ctx-base64` header by default, which is
the same format dd-trace-go, dd-trace-java and dd-trace-py use: the 64-bit FNV-1
hash of the node in little-endian byte order followed by two zigzag varints of
Unix milliseconds, base64 encoded. Headers written by any of those tracers
decode here, and headers written here decode there. A malformed header is
ignored and the message starts a new pathway.

For backwards compatibility, when the configured key is absent the library also
reads a legacy binary header under `dd-pathway-ctx`, decoding its raw
(non-base64) bytes. The configured key always wins when present.

The header key is configurable with `header:` / `DD_DATA_STREAMS_HEADER`, but the
value format is the official one; changing the key stops the official tracers
from finding the pathway.

The Agent payload matches the `StatsPayload` shape of dd-trace-go v2.11.2, and the
DDSketch bins use the same mapping as dd-trace-go v2.11.2 and
  dd-trace-java 1.67.1 (gamma 1.015625, offset 1.8761281912861705), which is
  self-describing in the payload; dd-trace-py uses offset 1338.5, so its bins do
  not line up with Go/Java.

For the Pub/Sub extractor there are two known naming differences: dd-trace-py
tags a consume edge with `topic:<subscription>` instead of
`subscription:<subscription>`, and dd-trace-java reduces a subscription resource
name to its id. Both change the node's identity, so those consumers appear as a
different node rather than joining the one reported here; the pathway itself
still connects, because the link between two nodes is the hash carried on the
message.

### Node hash

The node hash is FNV-1 over the service, environment, primary tag and the
hashable edge tags. The hashable set here is the same as dd-trace-java 1.67.1's
(`bus`, `direction`, `exchange`, `topic`, `type`, `subscription`,
`kafka_cluster_id`), but the tags are sorted rather than written in Java's
fixed order. dd-trace-go v2.11.2 hashes a different set (it includes `group`,
`event_type` and `segment_name` and omits `bus` and `subscription`) and adds
process tags and an optional container hash by default. dd-trace-py 3.17.0
hashes every tag, including per-message ones such as `partition`. None of the
four agree for an arbitrary set of tags, and Go and Java add runtime-specific
process tags, so numeric equality between tracers is not attainable; the
library does not try. The tags that are not hashed are still reported on the
edge, and those that split a node's statistics do so without changing its
identity.

## Limitations

* A message is named by the first extractor that recognises its metadata, and a
  produce by the extractor that consumed the message being answered. A stage
  that produces without consuming, or that publishes to a different bus than it
  consumed from, says which extractor by passing `extractor:` to
  `DdDataStreams.produce/3`.
* This is Data Streams Monitoring only: no APM traces, backlogs or transactions
  are collected.
* The Agent must be reachable over HTTP; Unix domain sockets are not supported.
* A message whose metadata names no edge records no consume checkpoint and
  leaves no pathway behind for a later produce to continue.
* Fan-in is sampled, not joined: a batch that consumed several pathways
  continues exactly one of them.

## Requirements and troubleshooting

* The Datadog Agent must be version 7.34 or later.
* The Agent must have an API key configured in its own config or environment;
  the library sends none.
* A `500` response means the Agent's forwarder is off (no API key to forward
  with); a `4xx` means the intake rejected the payload.
* The HTTP request is best effort and is not retried.
* Stats are flushed about every ten seconds, so telemetry
  (`[:dd_data_streams, :flush, :error]`) reports a failure at that cadence.

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
intake's own answer: `202` means the backend accepted for processing, `500` means
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
