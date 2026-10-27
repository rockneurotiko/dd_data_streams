# Changelog

## 0.2.0 (unreleased)

### Fixed

- The default Req transport no longer passes `request_timeout` to `Req.post/2`.
  Req only learned that option in 0.7.0, so on Req 0.5.x and 0.6.x every flush
  died with `ArgumentError: unknown option :request_timeout` and the stats were
  lost. The hard deadline on one request now comes from `Task.yield/2` instead,
  and the transport passes only options every Req since 0.5 accepts.
- `DdDataStreams.Transport.Req.post/3` never raises: an exception from Req or
  from building the request comes back as `{:error, exception}`, so a transport
  bug cannot crash the aggregator and take its buckets with it.
