defmodule DdDataStreams.Testing do
  @moduledoc """
  Turning the library on around a single test, and reading back what it flushed.

  `DdDataStreams.Application` decides once, at boot, whether the aggregator
  runs, so a test that flips `enabled` by itself records into an aggregator
  that is not there. `enable!/1` performs the whole flip for the test: it sets
  the configuration, attaches the Broadway handlers and starts the aggregator
  when it is not already running, and returns the function that puts
  everything back:

      setup do
        on_exit(DdDataStreams.Testing.enable!(transport: DdDataStreams.Transport.Capture))
        :ok
      end

  `capture/1` then runs a function with that transport in place and hands back
  the points that reached it, decoded by `decode_payload/1` into plain maps, so
  a test asserts on `hash` and `parent_hash` instead of on gunzipped bytes.

  A test using either runs with `async: false`: the aggregator, the application
  environment and the Broadway handler id are all shared by the whole VM.
  """

  alias DdDataStreams.Aggregator

  @config_keys [:enabled, :transport, :service, :env]
  @option_keys [:transport, :service, :env]
  @capture_event [:dd_data_streams, :transport, :captured]

  @doc """
  Enables the library for one test and returns the function that undoes it.

  Sets `enabled: true` and any of the options `:transport`, `:service` and
  `:env:` that are given, attaches the handlers `DdDataStreams.attach/0`
  installs, and starts the aggregator if it is not already running, so
  checkpoints recorded during the test are aggregated instead of dropped. Any
  other option raises an `ArgumentError`, so a typo does not silently configure
  nothing.

  The returned zero-arity function stops the aggregator this call started,
  detaches the handlers this call attached and restores the application
  environment to what it was before the call; anything the call found already
  running or already attached it leaves alone. It is safe to call twice, and
  the aggregator is linked to the calling process, so a test that never runs
  the cleanup still cannot leak one. Pass it to `ExUnit.Callbacks.on_exit/1`:

      on_exit(DdDataStreams.Testing.enable!())
  """
  @spec enable!(keyword()) :: (-> :ok)
  def enable!(opts \\ []) when is_list(opts) do
    validate_options!(opts)

    previous = Map.new(@config_keys, &{&1, Application.fetch_env(:dd_data_streams, &1)})
    attached? = DdDataStreams.status().attached

    Application.put_env(:dd_data_streams, :enabled, true)

    for key <- @option_keys, Keyword.has_key?(opts, key) do
      Application.put_env(:dd_data_streams, key, Keyword.fetch!(opts, key))
    end

    :ok = DdDataStreams.attach()
    started = start_aggregator()

    fn -> restore(previous, started, attached?) end
  end

  # A silently ignored typo would show up much later as a flush going
  # nowhere, so it is rejected here, where the test still points at it.
  defp validate_options!(opts) do
    case Keyword.keys(opts) -- @option_keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown enable! options: #{inspect(unknown)}; " <>
                "expected #{inspect(@option_keys)}"
    end
  end

  # nil when the aggregator was already running, so the cleanup only ever
  # stops what this call started.
  defp start_aggregator do
    case Aggregator.start_link([]) do
      {:ok, pid} -> pid
      {:error, {:already_started, _pid}} -> nil
    end
  end

  defp restore(previous, started, attached?) do
    stop_aggregator(started)
    unless attached?, do: DdDataStreams.detach()

    for {key, value} <- previous do
      case value do
        {:ok, configured} -> Application.put_env(:dd_data_streams, key, configured)
        :error -> Application.delete_env(:dd_data_streams, key)
      end
    end

    :ok
  end

  # The aggregator may already be on its way out - it is linked to the test
  # process, which an on_exit callback outlives - so a stop that races its
  # exit is not an error.
  defp stop_aggregator(nil), do: :ok

  defp stop_aggregator(pid) do
    if Process.whereis(Aggregator) == pid do
      try do
        GenServer.stop(pid)
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  @doc """
  Runs `fun`, flushes every bucket, and returns the points the transport
  captured while it ran.

  Attaches a handler of its own under a unique id to
  `[:dd_data_streams, :transport, :captured]`, so it sees exactly what is
  flushed between the start of the call and the flush it makes itself, and
  detaches it again whether `fun` returns or raises. The transport must report
  on that event, which `DdDataStreams.Transport.Capture` does - pass it to
  `enable!/1` - and the aggregator must be running, or the flush has nothing
  to send and the result is `[]`.

  The flush happens after `fun` returns, so a checkpoint recorded a moment
  earlier is assertable without waiting for the ten-second bucket window, the
  way `DdDataStreams.Aggregator.flush(:all)` sends it. Every point of every
  payload flushed during the call is returned as one flat list of maps with
  `:edge_tags`, `:hash`, `:parent_hash`, the three distributions and
  `:timestamp_type`, in the shape `decode_payload/1` gives each point.
  """
  @spec capture((-> any)) :: [map()]
  def capture(fun) when is_function(fun, 0) do
    handler = {__MODULE__, make_ref()}
    :telemetry.attach(handler, @capture_event, &__MODULE__.handle_capture/4, self())

    try do
      fun.()
      :ok = Aggregator.flush(:all)
      drain()
    after
      :telemetry.detach(handler)
    end
  end

  @doc false
  # Telemetry wants a module function rather than a closure, and the config is
  # the collecting process, so a flush from anywhere reaches the caller of
  # capture/1.
  @spec handle_capture([atom()], map(), map(), pid()) :: :ok
  def handle_capture(_event, _measurements, %{body: body}, collector) do
    send(collector, {__MODULE__, body})
    :ok
  end

  # The flush is synchronous, so every body arrives before flush(:all) returns
  # and an empty mailbox after that means there is nothing left to collect.
  defp drain(acc \\ []) do
    receive do
      {__MODULE__, body} -> drain([body | acc])
    after
      0 ->
        acc
        |> Enum.reverse()
        |> Enum.flat_map(fn body -> body |> decode_payload() |> Map.get(:stats, []) end)
        |> Enum.flat_map(&Map.get(&1, :stats, []))
    end
  end

  @doc """
  Decodes a captured transport body into a plain map.

  The body is the gzip-compressed MsgPack payload the agent would have
  received, as `[:dd_data_streams, :transport, :captured]` reports it; it is
  gunzipped and read back into the same shape the library builds it in: atom
  keys in this library's vocabulary at every level.

      iex> payload = %DdDataStreams.Payload{env: "prod", service: "checkout"}
      iex> body = payload |> DdDataStreams.Payload.encode() |> IO.iodata_to_binary() |> :zlib.gzip()
      iex> decoded = DdDataStreams.Testing.decode_payload(body)
      iex> {decoded.env, decoded.service, decoded.stats}
      {"prod", "checkout", []}

  A payload carries `:env`, `:service`, `:stats`, `:tracer_version`, `:lang`,
  `:version`, `:process_tags` and `:product_mask`; each bucket in `:stats`
  carries `:start`, `:duration` and its own `:stats` points, and each point
  carries `:edge_tags`, `:hash`, `:parent_hash`, `:pathway_latency`,
  `:edge_latency`, `:payload_size` and `:timestamp_type`. The three
  distributions stay binaries, exactly as they went on the wire; they are
  `DdDataStreams.Sketch` protos, not numbers. A key the payload vocabulary
  does not know keeps the name it arrived with, so nothing is dropped.

  It reads what `DdDataStreams.Payload` encodes; anything else raises.
  """
  @spec decode_payload(binary()) :: map()
  def decode_payload(body) when is_binary(body) do
    {payload, _rest} = body |> :zlib.gunzip() |> unpack()
    payload
  end

  # The MsgPack forms DdDataStreams.Payload writes: unsigned integers,
  # strings, binaries, arrays and maps, each in its smallest form. A byte
  # outside those is a payload this library did not write, and says so instead
  # of decoding half of it.
  defp unpack(<<byte, rest::binary>>) when byte <= 0x7F, do: {byte, rest}
  defp unpack(<<0xCC, value, rest::binary>>), do: {value, rest}
  defp unpack(<<0xCD, value::16, rest::binary>>), do: {value, rest}
  defp unpack(<<0xCE, value::32, rest::binary>>), do: {value, rest}
  defp unpack(<<0xCF, value::64, rest::binary>>), do: {value, rest}

  defp unpack(<<byte, rest::binary>>) when byte in 0xA0..0xBF,
    do: take(byte - 0xA0, rest)

  defp unpack(<<0xD9, length, rest::binary>>), do: take(length, rest)
  defp unpack(<<0xDA, length::16, rest::binary>>), do: take(length, rest)
  defp unpack(<<0xDB, length::32, rest::binary>>), do: take(length, rest)
  defp unpack(<<0xC4, length, rest::binary>>), do: take(length, rest)
  defp unpack(<<0xC5, length::16, rest::binary>>), do: take(length, rest)
  defp unpack(<<0xC6, length::32, rest::binary>>), do: take(length, rest)

  defp unpack(<<byte, rest::binary>>) when byte in 0x90..0x9F,
    do: unpack_array(byte - 0x90, rest, [])

  defp unpack(<<0xDC, length::16, rest::binary>>), do: unpack_array(length, rest, [])
  defp unpack(<<0xDD, length::32, rest::binary>>), do: unpack_array(length, rest, [])

  defp unpack(<<byte, rest::binary>>) when byte in 0x80..0x8F,
    do: unpack_map(byte - 0x80, rest, %{})

  defp unpack(<<0xDE, length::16, rest::binary>>), do: unpack_map(length, rest, %{})
  defp unpack(<<0xDF, length::32, rest::binary>>), do: unpack_map(length, rest, %{})

  defp unpack(<<>>), do: raise(ArgumentError, "the MsgPack payload ends early")

  defp unpack(<<byte, _rest::binary>>),
    do: raise(ArgumentError, "unsupported MsgPack byte 0x#{Integer.to_string(byte, 16)}")

  defp take(length, rest) do
    <<value::binary-size(^length), rest::binary>> = rest
    {value, rest}
  end

  defp unpack_array(0, rest, acc), do: {Enum.reverse(acc), rest}

  defp unpack_array(length, rest, acc) do
    {value, rest} = unpack(rest)
    unpack_array(length - 1, rest, [value | acc])
  end

  defp unpack_map(0, rest, acc), do: {acc, rest}

  defp unpack_map(length, rest, acc) do
    {key, rest} = unpack(rest)
    {value, rest} = unpack(rest)
    unpack_map(length - 1, rest, Map.put(acc, wire_key(key), value))
  end

  # The wire names in the vocabulary the rest of the library uses for the same
  # fields, so a decoded point reads like the point the aggregator built. A key
  # outside the vocabulary keeps the name it arrived with.
  defp wire_key("Env"), do: :env
  defp wire_key("Service"), do: :service
  defp wire_key("Stats"), do: :stats
  defp wire_key("TracerVersion"), do: :tracer_version
  defp wire_key("Lang"), do: :lang
  defp wire_key("Version"), do: :version
  defp wire_key("ProcessTags"), do: :process_tags
  defp wire_key("ProductMask"), do: :product_mask
  defp wire_key("Start"), do: :start
  defp wire_key("Duration"), do: :duration
  defp wire_key("Backlogs"), do: :backlogs
  defp wire_key("Transactions"), do: :transactions
  defp wire_key("TransactionCheckpointIds"), do: :transaction_checkpoint_ids
  defp wire_key("EdgeTags"), do: :edge_tags
  defp wire_key("Hash"), do: :hash
  defp wire_key("ParentHash"), do: :parent_hash
  defp wire_key("PathwayLatency"), do: :pathway_latency
  defp wire_key("EdgeLatency"), do: :edge_latency
  defp wire_key("PayloadSize"), do: :payload_size
  defp wire_key("TimestampType"), do: :timestamp_type
  defp wire_key(key), do: key
end
