if Code.ensure_loaded?(Req) do
  defmodule DdDataStreams.Transport.Req do
    @moduledoc """
    The default agent transport, backed by Req.

    Req is an optional dependency, so this module is compiled only when it is
    available; an application that supplies its own transport does not pull in an
    HTTP client.

    Only options Req has accepted since 0.5 are used, because `mix.exs` allows
    any `Req ~> 0.5`: an option a version does not know makes Req raise
    `ArgumentError` instead of posting. `:request_timeout` is the example - Req
    added it in 0.7.0 - so the hard deadline on the whole request is enforced
    with `Task.yield/2` below rather than with that option.
    """

    @behaviour DdDataStreams.Transport

    # Connect and receive timeouts are per socket operation, so neither bounds
    # the whole call against a server that trickles its response; the aggregator
    # waits for this call while handling casts and flushes, so it needs one hard
    # deadline of its own.
    @deadline_ms 10_000

    @impl true
    def post(url, headers, body) do
      # The rescue belongs inside the task as well as around it: the task is
      # linked to the caller, so an exception escaping it would take the caller
      # down instead of coming back as a result. A transport bug must not crash
      # the aggregator, which would drop every bucket in silence.
      task = Task.async(fn -> request(url, headers, body) end)

      case Task.yield(task, @deadline_ms) || Task.shutdown(task, :brutal_kill) do
        {:ok, result} -> result
        {:exit, reason} -> {:error, reason}
        nil -> {:error, :timeout}
      end
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end

    # The agent connection is best effort and the caller drops the bucket on
    # failure, so a retry here would only delay the next flush. The timeouts
    # keep a blackholed agent from blocking aggregation indefinitely.
    defp request(url, headers, body) do
      case Req.post(
             url,
             headers: headers,
             body: IO.iodata_to_binary(body),
             retry: false,
             connect_options: [timeout: 2_000],
             receive_timeout: 5_000
           ) do
        {:ok, %Req.Response{status: status}} -> {:ok, status}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end
end
