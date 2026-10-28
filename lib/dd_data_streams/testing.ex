defmodule DdDataStreams.Testing do
  @moduledoc """
  Testing support for a host application.

  There are two ways to record with this library in a test, and the first one
  is the default:

    * Owner-scoped. Configure `DdDataStreams.Testing.Backend`, start it from
      `test_helper.exs`, and call `start/1` in a test that wants checkpoints.
      Recording is per test, so tests can run with `async: true`, and nothing
      has to be flushed or waited for. This is the API to reach for.
    * Global. `enable!/1` and `capture/1` flip the application environment,
      the shared aggregator and the VM-wide telemetry handler id. They are the
      `async: false` tools for tests that need the full wire-format path
      through the aggregator and the capture transport.

  ## Owner-scoped tests

      # config/test.exs
      config :dd_data_streams, backend: DdDataStreams.Testing.Backend

      # test/test_helper.exs
      {:ok, _} = DdDataStreams.Testing.Backend.start_link()

      defmodule MyApp.OrdersTest do
        use ExUnit.Case, async: true
        use DdDataStreams.Testing

        test "the consume checkpoint links to the produce checkpoint" do
          DdDataStreams.Testing.start()

          DdDataStreams.consume(%{
            data: payload,
            metadata: %{
              subscription: "projects/demo/subscriptions/orders-sub",
              attributes: Map.new(DdDataStreams.produce("orders-out", payload))
            }
          })

          produce = DdDataStreams.Testing.assert_produced("orders-out")
          consume = DdDataStreams.Testing.assert_consumed("orders-sub")
          DdDataStreams.Testing.assert_chained(produce, consume)
        end
      end

  `use DdDataStreams.Testing` only picks private mode for an async case and
  shared mode otherwise. A test opts in by calling `start/1`; a test that never
  does sees `produce/3` return `[]`, records nothing, and can run alongside the
  others. A process a test did not start, such as a Broadway processor, is
  allowed with `allow/2`, by pid or by registered name. `checkpoints/1` reads
  the result, and `subscribe/0` turns the same points into messages when a test
  prefers `assert_receive`.

  ## Global tests

  `enable!/1` and `capture/1` keep their old meaning. They set `:enabled` and
  the transport, start the shared aggregator and attach the shared handler id,
  so a test using either runs with `async: false`. Use them when the test needs
  what only that path gives: an encoded payload through the real aggregator,
  or the wire-format bytes the agent would receive.
  """

  import ExUnit.Assertions, only: [assert: 2]

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Testing.Backend

  # The backend module exists only when the optional :nimble_ownership dependency
  # does; the owner-scoped functions below say so at call time instead of
  # failing to compile in an application that never uses them.
  @compile {:no_warn_undefined, DdDataStreams.Testing.Backend}

  @config_keys [:enabled, :transport, :service, :env]
  @option_keys [:transport, :service, :env]
  @capture_event [:dd_data_streams, :transport, :captured]

  @doc """
  Sets up the owner-scoped mode from the ExUnit context.

  Picks private mode when the test module is `async: true` and shared mode
  otherwise, so a test module only needs:

      use ExUnit.Case, async: true
      use DdDataStreams.Testing

  In private mode each test still opts in with `start/1`. In shared mode every
  process records into the test process, so `allow/2` is not needed. A module
  whose context already chose a mode can call `set_private/0` or `set_global/1`
  itself instead.
  """
  defmacro __using__(_opts) do
    quote do
      setup context do
        DdDataStreams.Testing.set_from_context(context)
      end
    end
  end

  @doc """
  Makes `owner` (the calling process by default) an owner of the checkpoints.

  After this call, `produce/3`, `produce_batch/3`, `consume/1`, `checkpoint/1`
  and the Broadway handlers record into `owner`, and `checkpoints/1` reads what
  they recorded. Calling it again is harmless. A `Task` records into the owner
  that spawned it without this call, because it inherits `$callers`; any other
  process needs `allow/2`.

  Returns `:ok`. Raises when the ownership server is not running, because a
  test that thinks it is recording and is not is worse than a failed test.

  In shared mode the shared owner receives the checkpoints, so `start/1` is not
  needed there; calling it with the shared owner is harmless.
  """
  @spec start(pid() | atom()) :: :ok
  def start(owner \\ self()) do
    ensure_backend!()
    Backend.start(owner)
  end

  @doc """
  Allows another process to record into `owner`.

  Broadway processor and batcher processes are started by Broadway, not by the
  test, so they are not in the test's `$callers` and need this call. Both
  arguments accept a pid or a registered name:

      DdDataStreams.Testing.start()
      DdDataStreams.Testing.allow(self(), :"my_pipeline.Broadway.Processor_0")

  Returns `:ok`, or `{:error, %NimbleOwnership.Error{}}` when the ownership
  server refuses the allowance (for example in shared mode, where every process
  already records into the same owner).
  """
  @spec allow(pid() | atom(), pid() | atom()) :: :ok | {:error, struct()}
  def allow(owner, allowed) do
    ensure_backend!()
    Backend.allow(owner, allowed)
  end

  @doc """
  The checkpoints recorded for `owner` (the calling process by default), oldest
  first.

  Each point is a plain map shaped like the points `decode_payload/1` returns:
  `:edge_tags`, `:hash`, `:parent_hash`, `:pathway_latency`, `:edge_latency`,
  `:payload_size` and `:timestamp_type`. The latencies and the size are the
  integers the checkpoint measured, not the flushed distributions, and
  `:timestamp_type` is `"current"` because this is the checkpoint itself rather
  than an aggregator bucket.

  Reading is synchronous: `record/1` in the backend has already stored the
  point by the time the call that produced it returns, so no flush or wait is
  needed. A test that never called `start/1` gets `[]`. In shared mode the
  shared owner's points are returned.
  """
  @spec checkpoints(pid() | atom()) :: [map()]
  def checkpoints(owner \\ self()) do
    ensure_backend!()
    Backend.checkpoints(owner)
  end

  @doc """
  Subscribes the calling owner to checkpoint messages.

  Every point recorded after this call is also sent to the owner as
  `{DdDataStreams.Testing, :checkpoint, point}`, with the same map
  `checkpoints/1` returns. It is the push alternative for tests that prefer
  `assert_receive`; `checkpoints/1` is the default because it cannot lose a
  point to a mailbox order.
  """
  @spec subscribe() :: :ok
  def subscribe do
    ensure_backend!()
    Backend.subscribe()
  end

  @doc """
  Asserts that a produce checkpoint was recorded for `topic`.

  Returns the point, so a test can chain more assertions on it:

      produce = DdDataStreams.Testing.assert_produced("orders-out")
      assert "type:google-pubsub" in produce.edge_tags
  """
  @spec assert_produced(String.t(), pid() | atom()) :: map()
  def assert_produced(topic, owner \\ self()) do
    assert_point(owner, ["direction:out", "topic:#{topic}"], "produce on topic #{topic}")
  end

  @doc """
  Asserts that a consume checkpoint was recorded for `subscription`.

  Returns the point, like `assert_produced/2`.
  """
  @spec assert_consumed(String.t(), pid() | atom()) :: map()
  def assert_consumed(subscription, owner \\ self()) do
    assert_point(
      owner,
      ["direction:in", "subscription:#{subscription}"],
      "consume on subscription #{subscription}"
    )
  end

  @doc """
  Asserts that `consume` continues the pathway `produce` started.

  `produce` and `consume` are points returned by `assert_produced/2` and
  `assert_consumed/2`, or read from `checkpoints/1`. Returns `consume`.
  """
  @spec assert_chained(map(), map()) :: map()
  def assert_chained(produce, consume) do
    assert consume.parent_hash == produce.hash,
           "expected the consume parent_hash #{inspect(consume.parent_hash)} " <>
             "to be the produce hash #{inspect(produce.hash)}"

    consume
  end

  @doc """
  Switches the ownership server to private mode.

  This is the default mode and the one `set_from_context/1` picks for an
  `async: true` case.
  """
  @spec set_private() :: :ok
  @spec set_private(map()) :: :ok
  def set_private(_context \\ %{}) do
    ensure_backend!()
    Backend.set_private()
  end

  @doc """
  Switches the ownership server to shared mode: every process records into the
  calling test process.

  Raises when the ExUnit context is `async: true`, because a shared owner would
  receive another test's checkpoints.
  """
  @spec set_global(map()) :: :ok
  def set_global(context) do
    ensure_backend!()
    Backend.set_global(context)
  end

  @doc """
  Picks `set_private/0` for an `async: true` case and `set_global/1` otherwise.
  """
  @spec set_from_context(map()) :: :ok
  def set_from_context(context) do
    ensure_backend!()
    Backend.set_from_context(context)
  end

  defp assert_point(owner, tags, description) do
    point = Enum.find(checkpoints(owner), &Enum.all?(tags, fn tag -> tag in &1.edge_tags end))

    assert point,
           "expected a #{description} checkpoint, got: #{inspect(checkpoints(owner))}"

    point
  end

  defp ensure_backend! do
    unless Code.ensure_loaded?(Backend) do
      raise "the owner-scoped testing API needs the optional :nimble_ownership dependency; " <>
              "add {:nimble_ownership, \"~> 1.0\"} to your deps"
    end

    :ok
  end

  @doc """
  Global, `async: false` mode: enables the library for one test and returns the
  function that undoes it.

  This is the aggregator-and-wire-format path. For tests that only want to see
  the checkpoints a pipeline records, prefer the owner-scoped API (`start/1`
  and `checkpoints/1`), which leaves the shared application environment and
  aggregator alone and can run with `async: true`.

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
  Global, `async: false` mode: runs `fun`, flushes every bucket, and returns
  the points the transport captured while it ran.

  What it flushes is the one shared aggregator, so a test using it must not run
  next to another test that records. For the owner-scoped checkpoints a
  pipeline records, use `checkpoints/1` instead.

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
