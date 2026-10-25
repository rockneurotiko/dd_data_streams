defmodule DdDataStreams.Transport do
  @moduledoc """
  How a payload reaches the Datadog Agent.

  The aggregator knows only this behaviour, so an application can reuse an
  existing HTTP pool instead of the default. `DdDataStreams.Transport.Req` is the
  default and is compiled only when the optional `Req` dependency is available.

  An implementation posts the body to the agent's `/v0.1/pipeline_stats`
  endpoint and reports the HTTP status, or the reason the request did not
  complete. The agent adds the API key itself, so no authentication header is
  passed.

  `post/3` must return promptly: the aggregator waits for it while handling
  casts and flushes, so a transport that never returns blocks aggregation and
  lets the mailbox grow. A transport is expected to enforce its own connect,
  receive and request timeouts.
  """

  @doc """
  Posts a request body and returns the response status or the transport error.
  """
  @callback post(url :: String.t(), headers :: [{String.t(), String.t()}], body :: iodata()) ::
              {:ok, non_neg_integer()} | {:error, term()}
end
