defmodule DdDataStreams.TransportTest do
  use ExUnit.Case, async: false

  alias DdDataStreams.Config
  alias DdDataStreams.Transport
  alias DdDataStreams.Transport.Req, as: ReqTransport

  doctest DdDataStreams.Config

  @headers [{"content-type", "application/msgpack"}, {"content-encoding", "gzip"}]

  test "the transport behaviour has one callback" do
    assert Transport.behaviour_info(:callbacks) == [post: 3]
  end

  test "the Req implementation exists exactly when Req is available" do
    assert Code.ensure_loaded?(ReqTransport) == Code.ensure_loaded?(Req)
  end

  if Code.ensure_loaded?(Req) do
    test "the Req implementation implements the behaviour" do
      assert function_exported?(ReqTransport, :post, 3)

      assert Transport in ReqTransport.module_info(:attributes)[:behaviour]
    end

    test "posts the body with the given headers and reports the status" do
      port = serve_once(202)

      assert {:ok, 202} =
               ReqTransport.post(
                 "http://127.0.0.1:#{port}/v0.1/pipeline_stats",
                 @headers,
                 ["gzip", "ped"]
               )

      assert_receive {:request, request}
      assert request.request_line == "POST /v0.1/pipeline_stats HTTP/1.1"
      assert request.headers["content-type"] == "application/msgpack"
      assert request.headers["content-encoding"] == "gzip"
      assert request.body == "gzipped"
    end

    test "a refused connection is reported as an error" do
      assert {:error, _reason} =
               ReqTransport.post(
                 "http://127.0.0.1:1/v0.1/pipeline_stats",
                 @headers,
                 "body"
               )
    end
  end

  describe "agent_url/1" do
    test "DD_TRACE_AGENT_URL wins over the host and port" do
      assert Config.agent_url(%{
               "DD_TRACE_AGENT_URL" => "http://agent:8126",
               "DD_AGENT_HOST" => "other",
               "DD_TRACE_AGENT_PORT" => "8127"
             }) == "http://agent:8126"
    end

    test "the host and port build the URL when no URL is set" do
      assert Config.agent_url(%{"DD_AGENT_HOST" => "agent", "DD_TRACE_AGENT_PORT" => "8127"}) ==
               "http://agent:8127"

      assert Config.agent_url(%{"DD_TRACE_AGENT_PORT" => "8127"}) == "http://localhost:8127"
    end

    test "the default is the agent on localhost" do
      assert Config.agent_url(%{}) == "http://localhost:8126"
    end

    test "empty and blank values count as unset" do
      assert Config.agent_url(%{
               "DD_TRACE_AGENT_URL" => "",
               "DD_AGENT_HOST" => " ",
               "DD_TRACE_AGENT_PORT" => ""
             }) == "http://localhost:8126"
    end
  end

  # A one-shot HTTP server: it accepts a single request, reports the request
  # line, the headers and the body to the test process, and answers with the
  # given status.
  defp serve_once(status) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])
    {:ok, port} = :inet.port(listen)
    test = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      :gen_tcp.close(listen)
      send(test, {:request, read_request(socket, "")})

      :gen_tcp.send(
        socket,
        "HTTP/1.1 #{status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
      )

      :gen_tcp.close(socket)
    end)

    port
  end

  defp read_request(socket, acc) do
    case :binary.split(acc, "\r\n\r\n") do
      [head, rest] ->
        [request_line | header_lines] = String.split(head, "\r\n")

        headers =
          Map.new(header_lines, fn line ->
            [name, value] = String.split(line, ":", parts: 2)
            {String.downcase(name), String.trim(value)}
          end)

        length = String.to_integer(Map.get(headers, "content-length", "0"))

        %{request_line: request_line, headers: headers, body: read_body(socket, rest, length)}

      [_incomplete] ->
        {:ok, more} = :gen_tcp.recv(socket, 0, 5000)
        read_request(socket, acc <> more)
    end
  end

  defp read_body(_socket, body, length) when byte_size(body) >= length, do: body

  defp read_body(socket, body, length) do
    {:ok, more} = :gen_tcp.recv(socket, 0, 5000)
    read_body(socket, body <> more, length)
  end
end
