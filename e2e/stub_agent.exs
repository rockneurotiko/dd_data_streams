# A stand-in for a Datadog Agent on a machine that has none.
#
# This is a stub: it accepts any method, any path and any body, prints the
# request line and body size, and answers 202 Accepted. It does not parse,
# validate, forward or store the pipeline stats, so a 202 from here proves only
# that the library sent a request to the configured URL and handled the status;
# it does not prove that a real agent accepts the payload.
#
#   elixir stub_agent.exs              # listens on 8126
#   PORT=8126 elixir stub_agent.exs
#
# The library's agent acceptance test posts to whatever DD_TRACE_AGENT_URL
# names, so point it here from a second shell:
#
#   DD_TRACE_AGENT_URL=http://127.0.0.1:8126 MIX_BUILD_PATH=_build/e2e \
#     mix test test/integration/agent_test.exs

defmodule StubAgent do
  def accept(listen) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        serve(socket)
        accept(listen)

      {:error, reason} ->
        IO.puts("stub agent stopped: #{inspect(reason)}")
    end
  end

  defp serve(socket) do
    with {:ok, head} <- read_head(socket, ""),
         {:ok, body} <- read_body(socket, head) do
      [request_line | _] = String.split(head, "\r\n")
      IO.puts("stub agent: #{request_line} (#{byte_size(body)} body bytes)")
    end

    :gen_tcp.send(
      socket,
      "HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
    )

    :gen_tcp.close(socket)
  end

  defp read_head(socket, acc) do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      case :gen_tcp.recv(socket, 0, 5_000) do
        {:ok, data} -> read_head(socket, acc <> data)
        error -> error
      end
    end
  end

  defp read_body(socket, head) do
    [headers, rest] = String.split(head, "\r\n\r\n", parts: 2)
    read_body(socket, content_length(headers), rest)
  end

  defp read_body(_socket, length, acc) when byte_size(acc) >= length, do: {:ok, acc}

  defp read_body(socket, length, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} -> read_body(socket, length, acc <> data)
      error -> error
    end
  end

  defp content_length(headers) do
    case Regex.run(~r/content-length:\s*(\d+)/i, headers) do
      [_, length] -> String.to_integer(length)
      _ -> 0
    end
  end
end

port = String.to_integer(System.get_env("PORT", "8126"))

{:ok, listen} =
  :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true])

IO.puts("stub agent listening on http://127.0.0.1:#{port} (accepts any body, answers 202)")
StubAgent.accept(listen)
