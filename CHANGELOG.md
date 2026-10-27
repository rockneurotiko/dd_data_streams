# Changelog

## Unreleased

### Added

- `flush_concurrency:`, the number of services a flush posts at once. Defaults
  to 8; `1` posts them one after another, as before.

### Changed

- The aggregator posts the per-service payloads of one flush in parallel
  instead of one after another, so an application that reports for many
  services no longer spends the whole ten-second window in sequential agent
  requests, and a slow one does not hold up the others.

## 0.3.0

### Added

- `DdDataStreams.Backend`, the behaviour that decides whether the calling
  process is recording and where one checkpoint goes. The default,
  `DdDataStreams.Backend.Aggregator`, wraps the previous behaviour exactly, so
  an application that configures no backend is unaffected.
- `DdDataStreams.Testing.Backend`, an owner-scoped backend built on
  `NimbleOwnership`. It records into the test process that opted in and nowhere
  else, so tests that use it can run with `async: true`. `nimble_ownership` is
  an optional dependency of the library and is only needed by a suite that uses
  this backend.
- `DdDataStreams.Testing.start/1`, `allow/2`, `checkpoints/1`, `subscribe/0`,
  `set_private/0`, `set_global/1`, `set_from_context/1` and the assert helpers
  `assert_produced/1`, `assert_consumed/1` and `assert_chained/2` are the
  public owner-scoped API. `use DdDataStreams.Testing` installs the mode setup.
- `DdDataStreams.ServiceName`, the behaviour that names the service per
  checkpoint. Configured with `service_name:` it is asked for every consume,
  produce and checkpoint, so one pipeline can report several services; the
  `:service` option on `produce/3` and `produce_batch/3` overrides it for one
  call, and the configured `service` remains the fallback.
- The checkpoint map carries `:service`, and the aggregator groups buckets and
  payloads by it instead of by the application-wide setting. `Checkpoints.consume/4`
  and `produce/4` take the `:service` option that sets it.
- A pathway carries `tags`, the edge tags of the node it reached, so a produce
  in the stage that consumed a message can read the consumed edge from the
  pathway in `DdDataStreams.Context`. `DdDataStreams.Tags.get/2` returns one
  tag's value. The tags stay in the VM: the propagated header does not carry
  them. `ServiceName`'s context gained the `:pathway` it continues.

### Changed

- `produce/3`, `produce_batch/3`, `consume/1`, `checkpoint/1` and the Broadway
  handlers ask the configured backend whether to record instead of reading the
  enabled flag directly. The default backend keeps the old answer.
- The handlers installed by `attach/0` are safe to leave attached for a whole
  test suite: an unowned process is a no-op. `attach/0` stays idempotent.
- `DdDataStreams.Testing.enable!/1` and `capture/1` are documented as the global,
  `async: false` tools for the aggregator and wire-format path.

### Docs

- The testing section of the README documents the owner-scoped flow first, with
  the global capture recipe as the `async: false` exception. It no longer claims
  that every test using the library must be `async: false`.

## 0.2.0

### Added

- `DdDataStreams.Testing.enable!/1` turns the library on for a single test:
  it sets the configuration (with `transport:`, `service:` and `env:`
  options), attaches the handlers and starts the aggregator when it is not
  running, and returns the cleanup that stops what it started, detaches what
  it attached and restores the previous application environment. A test suite
  no longer has to restart the application around each test.
- `DdDataStreams.Testing.capture/1` runs a function with the capture
  transport in place, flushes every bucket and returns the points that were
  sent, decoded by the new `DdDataStreams.Testing.decode_payload/1` into
  plain maps with `edge_tags`, `hash`, `parent_hash` and the latency fields.
  A test asserts on the pathway instead of on gunzipped MsgPack bytes.

### Changed

- `DdDataStreams.Aggregator.add/1` now logs a warning, once per process, when
  it drops a checkpoint because no aggregator is running while the library is
  enabled. The drop was silent before, which is how an aggregator that was
  never started went unnoticed.

### Fixed

- The default Req transport no longer passes `request_timeout` to `Req.post/2`.
  Req only learned that option in 0.7.0, so on Req 0.5.x and 0.6.x every flush
  died with `ArgumentError: unknown option :request_timeout` and the stats were
  lost. The hard deadline on one request now comes from `Task.yield/2` instead,
  and the transport passes only options every Req since 0.5 accepts.
- `DdDataStreams.Transport.Req.post/3` never raises: an exception from Req or
  from building the request comes back as `{:error, exception}`, so a transport
  bug cannot crash the aggregator and take its buckets with it.

### Docs

- The testing section of the README documents the testing API and spells out
  why a test that records with the library on runs with `async: false`: the
  handler id, the aggregator and the application environment are shared by
  the whole VM, and per-process filtering would not change the last two.
