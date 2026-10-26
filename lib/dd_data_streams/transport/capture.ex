defmodule DdDataStreams.Transport.Capture do
  @moduledoc """
  A transport that reports each flush on telemetry instead of posting it.

  Configured in place of the agent transport - typically in `config/test.exs` -
  it turns a flush into something a test can assert on: it answers
  `{:ok, 202}` and emits `[:dd_data_streams, :transport, :captured]` with
  `%{count: 1}` measurements and `%{url: url, headers: headers, body: body}`
  metadata, where the body is the exact gzip-compressed MsgPack payload the
  agent would have received; `:zlib.gunzip/1` reads it back.

  It keeps no state: a test attaches a telemetry handler under its own id and
  receives only what is flushed while that handler is attached, so concurrent
  tests never capture each other's flushes through the transport itself.
  Combined with `DdDataStreams.Aggregator.flush(:all)`, which sends every
  bucket including the one still being filled, a checkpoint recorded a moment
  before is assertable without waiting for the ten-second bucket window.
  """

  @behaviour DdDataStreams.Transport

  @event [:dd_data_streams, :transport, :captured]

  @impl true
  def post(url, headers, body) do
    :telemetry.execute(@event, %{count: 1}, %{
      url: url,
      headers: headers,
      body: IO.iodata_to_binary(body)
    })

    {:ok, 202}
  end
end
