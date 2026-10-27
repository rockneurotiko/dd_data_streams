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

    # Req validates the options it is given and raises on one it does not know,
    # so an option outside the supported Req range would crash here rather than
    # post. The transport must never raise either way: an exception would take
    # the aggregator down with it and drop every bucket in silence.
    test "an exception from Req or the body comes back as an error, never raised" do
      assert {:error, %ArgumentError{}} = ReqTransport.post(nil, @headers, "body")

      assert {:error, %ArgumentError{}} =
               ReqTransport.post("http://127.0.0.1:1/v0.1/pipeline_stats", @headers, [:bad])
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

  describe "application configuration" do
    setup do
      names = ~w(DD_TRACE_AGENT_URL DD_AGENT_HOST DD_TRACE_AGENT_PORT)
      previous = Map.new(names, &{&1, System.get_env(&1)})
      Enum.each(names, &System.delete_env/1)

      on_exit(fn ->
        for {name, value} <- previous do
          if value, do: System.put_env(name, value), else: System.delete_env(name)
        end

        for key <- [:agent_url, :agent_host, :agent_port, :transport] do
          Application.delete_env(:dd_data_streams, key)
        end
      end)

      :ok
    end

    test "agent_url/0 reads the configured keys, which win over the environment" do
      System.put_env("DD_AGENT_HOST", "env-host")
      System.put_env("DD_TRACE_AGENT_PORT", "9999")

      configure(agent_host: "configured", agent_port: 8127)
      assert Config.agent_url() == "http://configured:8127"

      configure(agent_url: "http://configured:8126/")
      assert Config.agent_url() == "http://configured:8126"
    end

    # A URL is a whole setting and beats a host and port, wherever each came from.
    test "an environment URL still wins over a configured host and port" do
      System.put_env("DD_TRACE_AGENT_URL", "http://env:8126")
      configure(agent_host: "configured", agent_port: 8127)

      assert Config.agent_url() == "http://env:8126"
    end

    test "a blank configured value counts as unset" do
      System.put_env("DD_AGENT_HOST", "env-host")
      configure(agent_host: " ")

      assert Config.agent_url() == "http://env-host:8126"
    end

    test "transport/0 reads the configured module" do
      configure(transport: Transport)

      assert Config.transport() == Transport
    end
  end

  defp configure(settings) do
    Enum.each(settings, fn {key, value} ->
      Application.put_env(:dd_data_streams, key, value)
    end)
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
