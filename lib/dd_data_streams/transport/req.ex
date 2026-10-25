if Code.ensure_loaded?(Req) do
  defmodule DdDataStreams.Transport.Req do
    @moduledoc """
    The default agent transport, backed by Req.

    Req is an optional dependency, so this module is compiled only when it is
    available; an application that supplies its own transport does not pull in an
    HTTP client.
    """

    @behaviour DdDataStreams.Transport

    @impl true
    def post(url, headers, body) do
      # The agent connection is best effort and the caller drops the bucket on
      # failure, so a retry here would only delay the next flush. The timeouts
      # keep a blackholed agent from blocking aggregation indefinitely.
      case Req.post(
             url,
             headers: headers,
             body: IO.iodata_to_binary(body),
             retry: false,
             connect_options: [timeout: 2_000],
             receive_timeout: 5_000,
             request_timeout: 10_000
           ) do
        {:ok, %Req.Response{status: status}} -> {:ok, status}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
