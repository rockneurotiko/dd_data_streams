defmodule DdDataStreams.Aggregator do
  @moduledoc """
  Aggregates checkpoints into ten-second buckets and flushes them to the agent.

  A checkpoint is counted twice, as dd-trace-go v2.11.2 does it: once in the bucket
  that contains the checkpoint (`TimestampType: "current"`) and once in the
  bucket that contains the start of the pathway (`TimestampType: "origin"`). The
  two entries carry the same distributions; only the bucket start and the
  timestamp type differ. A bucket is identified by its service, its start and
  which of the two families it belongs to, and its points by their hash
  together with their aggregation tags, so that one node whose statistics are
  split by a `group:` or a `manual_checkpoint:` tag is reported as one point
  per split.

  The three values a point carries are the pathway latency and the edge latency
  in seconds, and the payload size in bytes. Checkpoints measure both latencies
  in milliseconds, so they are scaled here; the payload size defaults to zero.

  `flush/0` sends the buckets that are complete, meaning their start is not
  newer than ten seconds ago, and keeps the bucket still being filled.
  `flush(:all)` sends every bucket, the still-being-filled one included, as
  stopping the process does.

  A bucket is removed before it is sent, so a failed send drops it: it is
  counted in
  `[:dd_data_streams, :flush, :error]` telemetry with a `count` measurement,
  and nothing is raised. A successful send is counted in
  `[:dd_data_streams, :flush, :stop]` telemetry with the buckets and points it
  carried, and a bucket dropped because its start sits beyond the clock-skew
  horizon is counted in the same event by its `dropped` points. Stopping the
  process sends everything,
  including the partial bucket, unless the agent has not answered within ten
  seconds, in which case the send is abandoned and the node keeps shutting
  down.

  One payload and one request are built per service, because the intake accepts
  one service per payload. The requests go out in parallel, up to
  `DdDataStreams.Config.flush_concurrency/0` at a time, and the flush returns
  once every one of them has been attempted. The transport and the agent URL
  are read from `DdDataStreams.Config` on every flush, so a change to either
  applies to the next one.

  A checkpoint that cannot be aggregated is dropped instead of counted: it is
  reported in `[:dd_data_streams, :checkpoint, :invalid]` telemetry with a
  `count` measurement, and nothing is raised.
  """

  use GenServer

  require Logger

  alias DdDataStreams.Config
  alias DdDataStreams.Pathway
  alias DdDataStreams.Payload
  alias DdDataStreams.Sketch
  alias DdDataStreams.Tags

  @bucket_duration_ns 10_000_000_000

  # How far a checkpoint may sit in the future before its bucket is dropped
  # instead of kept: a minute is well past any broker clock skew.
  @future_horizon_ns 60_000_000_000
  @flush_interval_ms 10_000

  # The final flush runs inside the supervisor's shutdown budget, so it is
  # bounded well below it: an agent that does not answer must not hold the
  # node for the whole budget.
  @shutdown_flush_ms 10_000
  # Both verified publishers of this endpoint (dd-trace-go, dd-trace-py) send
  # Datadog-Meta-Lang on the POST itself, not only in the body.
  @headers [
    {"content-type", "application/msgpack"},
    {"content-encoding", "gzip"},
    {"datadog-meta-lang", "elixir"}
  ]
  @error_event [:dd_data_streams, :flush, :error]
  @stop_event [:dd_data_streams, :flush, :stop]
  @invalid_event [:dd_data_streams, :checkpoint, :invalid]
  @skip_event [:dd_data_streams, :checkpoint, :skip]
  # Keyed by module and message, so the process that is told once does not say
  # it again for the rest of its life.
  @dropped_key {__MODULE__, :no_aggregator_warned}

  @doc """
  Starts the aggregator under its registered name.

  The transport is not an option: it is read from `DdDataStreams.Config` on
  every flush, so it can be changed while the aggregator runs.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Adds a checkpoint to its current and origin buckets.

  Takes the map `DdDataStreams.Pathway.set_checkpoint/3` returns, with an
  optional `:payload_size` in bytes. Does nothing when the aggregator is not
  running, which is the case while the library is disabled; that skip is
  reported in `[:dd_data_streams, :checkpoint, :skip]` telemetry. While the
  library is enabled such a drop is a misconfiguration - the flag was flipped
  after the application booted - so the first one in each process is also
  logged at warning level instead of vanishing silently.
  """
  @spec add(Pathway.checkpoint()) :: :ok
  def add(checkpoint) when is_map(checkpoint) do
    case Process.whereis(__MODULE__) do
      nil ->
        :telemetry.execute(@skip_event, %{count: 1}, %{
          reason: :aggregator_not_running,
          extractors: nil
        })

        warn_if_enabled()

        :ok

      pid ->
        GenServer.cast(pid, {:add, checkpoint})
    end
  end

  # A drop while the library is enabled means the aggregator is not where the
  # recording expects it, and a silent one is how that goes unnoticed. Once
  # per process, not once per call: a Broadway processor records every message
  # it handles, and one line per message would bury the log.
  defp warn_if_enabled do
    if Config.enabled?() and not Process.get(@dropped_key, false) do
      Process.put(@dropped_key, true)

      Logger.warning(
        "a checkpoint was dropped: data streams monitoring is enabled but no " <>
          "aggregator is running; in tests call DdDataStreams.Testing.enable!/1, " <>
          "otherwise set :enabled before the application boots"
      )
    end

    :ok
  end

  @doc """
  Sends every bucket that is complete, dropping each one before it is sent.

  Returns once the sends have finished, whether or not they succeeded. The wait
  is bounded by the transport's own timeouts; the default transport aborts a
  request after ten seconds.
  """
  @spec flush() :: :ok
  def flush, do: call(:flush)

  @doc """
  Sends every bucket, including the one still being filled.

  This is the test seam: a checkpoint recorded a moment ago lands in the
  current bucket, which `flush/0` keeps until the window completes, while
  `flush(:all)` sends it at once - so with a capture transport in place a test
  asserts on it immediately. `flush/0` keeps its meaning: only complete
  buckets go, and the partial one waits for the next flush. Like `flush/0`,
  this does nothing when the aggregator is not running.
  """
  @spec flush(:all) :: :ok
  def flush(:all), do: call({:flush, :all})

  defp call(request) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      # The transport enforces its own timeouts; the caller waits for the sends
      # either way.
      pid -> GenServer.call(pid, request, :infinity)
    end
  end

  @impl true
  def init(_opts) do
    # Trapping exits is what lets terminate/2 run when the supervisor shuts the
    # aggregator down, so the partial bucket is sent on that path too.
    Process.flag(:trap_exit, true)
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:ok, %{buckets: %{}}}
  end

  @impl true
  def handle_cast({:add, checkpoint}, state) do
    case normalise(checkpoint) do
      {:ok, checkpoint} ->
        {:noreply, %{state | buckets: add_checkpoint(state.buckets, checkpoint)}}

      :error ->
        invalid(Config.service(), checkpoint)
        {:noreply, state}
    end
  end

  # Only a checkpoint that every bucket can carry is kept. The latencies and
  # the payload size default to zero, because a checkpoint that measured
  # nothing is incomplete rather than malformed; only binary tags are kept, so
  # one tag that cannot be encoded does not cost the whole flush.
  defp normalise(checkpoint) when is_map(checkpoint) do
    tags = Map.get(checkpoint, :tags)

    with true <- is_integer(Map.get(checkpoint, :timestamp)),
         true <- is_list(tags),
         true <- present?(checkpoint, :hash),
         true <- present?(checkpoint, :parent_hash),
         {:ok, service} <- service(Map.get(checkpoint, :service)),
         {:ok, pathway_latency} <- quantity(Map.get(checkpoint, :pathway_latency)),
         {:ok, edge_latency} <- quantity(Map.get(checkpoint, :edge_latency)),
         {:ok, payload_size} <- quantity(Map.get(checkpoint, :payload_size)) do
      {:ok,
       Map.merge(checkpoint, %{
         service: service,
         tags: Enum.filter(tags, &is_binary/1),
         pathway_latency: pathway_latency,
         edge_latency: edge_latency,
         payload_size: payload_size
       })}
    else
      _ -> :error
    end
  end

  defp normalise(_checkpoint), do: :error

  # A checkpoint written by an older version of this library, or by hand,
  # carries no service and is reported under the configured one, as every
  # checkpoint used to be. A blank name counts as no name.
  defp service(value) when is_binary(value) do
    case String.trim(value) do
      "" -> {:ok, Config.service()}
      trimmed -> {:ok, trimmed}
    end
  end

  defp service(_value), do: {:ok, Config.service()}

  defp present?(checkpoint, key), do: not is_nil(Map.get(checkpoint, key))

  defp quantity(nil), do: {:ok, 0}
  defp quantity(value) when is_integer(value), do: {:ok, value}
  defp quantity(_value), do: :error

  @impl true
  def handle_call(:flush, _from, state) do
    {:reply, :ok, flush_due(state, System.system_time(:millisecond) * 1_000_000)}
  end

  # :infinity is the cutoff the shutdown flush uses: every bucket goes,
  # including the partial one and any stamped ahead of the clock.
  def handle_call({:flush, :all}, _from, state) do
    {:reply, :ok, flush_due(state, :infinity)}
  end

  @impl true
  def handle_info(:flush, state) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, flush_due(state, System.system_time(:millisecond) * 1_000_000)}
  end

  # Exits are trapped, so a linked process dying arrives as a message instead of
  # killing the aggregator.
  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    task = Task.async(fn -> flush_due(state, :infinity) end)
    _ = Task.yield(task, @shutdown_flush_ms) || Task.shutdown(task)
    :ok
  end

  defp add_checkpoint(buckets, checkpoint) do
    timestamp_ns = checkpoint.timestamp * 1_000_000
    pathway_latency = Map.get(checkpoint, :pathway_latency, 0)
    edge_latency = Map.get(checkpoint, :edge_latency, 0)
    payload_size = Map.get(checkpoint, :payload_size, 0)
    service = checkpoint.service

    buckets
    |> add_point(
      :current,
      service,
      timestamp_ns,
      checkpoint,
      pathway_latency,
      edge_latency,
      payload_size
    )
    |> add_point(
      :origin,
      service,
      timestamp_ns - pathway_latency * 1_000_000,
      checkpoint,
      pathway_latency,
      edge_latency,
      payload_size
    )
  end

  defp add_point(
         buckets,
         family,
         service,
         timestamp_ns,
         checkpoint,
         pathway_latency,
         edge_latency,
         payload_size
       ) do
    key = {family, service, align(timestamp_ns)}
    points = Map.get(buckets, key, %{})

    points =
      Map.update(
        points,
        point_key(checkpoint),
        new_point(checkpoint, pathway_latency, edge_latency, payload_size),
        fn point ->
          %{
            point
            | pathway: Sketch.add(point.pathway, pathway_latency / 1000),
              edge: Sketch.add(point.edge, edge_latency / 1000),
              size: Sketch.add(point.size, payload_size)
          }
        end
      )

    Map.put(buckets, key, points)
  end

  # The aggregation tags split a node's statistics without changing its
  # identity, so they are part of the key of a point; sorted, because the same
  # tag set in another order is the same split.
  defp point_key(checkpoint) do
    {checkpoint.hash, checkpoint.tags |> Tags.filter(:aggregation) |> Enum.sort()}
  end

  defp new_point(checkpoint, pathway_latency, edge_latency, payload_size) do
    %{
      edge_tags: checkpoint.tags,
      parent_hash: checkpoint.parent_hash,
      pathway: Sketch.new() |> Sketch.add(pathway_latency / 1000),
      edge: Sketch.new() |> Sketch.add(edge_latency / 1000),
      size: Sketch.new() |> Sketch.add(payload_size)
    }
  end

  # Buckets are aligned down to the bucket width, so a bucket start is a multiple
  # of ten seconds in nanoseconds.
  defp align(timestamp_ns), do: div(timestamp_ns, @bucket_duration_ns) * @bucket_duration_ns

  defp flush_due(state, now_ns) do
    cutoff = if now_ns == :infinity, do: :infinity, else: now_ns - @bucket_duration_ns
    horizon = if now_ns == :infinity, do: :infinity, else: now_ns + @future_horizon_ns

    {due, buckets} =
      Map.split_with(state.buckets, fn {{_f, _s, start}, _} -> due?(start, cutoff) end)

    # A checkpoint stamped far in the future (a replayed message, a skewed clock)
    # would never become due and would be held until the process stops.
    {future, buckets} = Map.split_with(buckets, fn {{_f, _s, start}, _} -> start > horizon end)
    dropped(future)

    transport = Config.transport()

    due
    |> Enum.group_by(fn {{_family, service, _start}, _points} -> service end)
    |> Task.async_stream(
      fn {service, entries} ->
        counts = %{buckets: length(entries), points: points_count(entries), dropped: 0}
        post(transport, service, build_payload(service, entries), counts)
      end,
      max_concurrency: Config.flush_concurrency(),
      ordered: false,
      # The transport owns its request deadline and post/4 rescues and catches,
      # so a task never raises: nothing here can wait forever.
      timeout: :infinity
    )
    |> Stream.run()

    %{state | buckets: buckets}
  end

  defp due?(_start, :infinity), do: true
  defp due?(start, cutoff), do: start <= cutoff

  defp build_payload(service, entries) do
    payload = %{Payload.new() | service: service}

    entries
    |> Enum.sort_by(fn {{family, _service, start}, _points} -> {start, family} end)
    |> Enum.reduce(payload, fn {{family, _service, start}, points}, payload ->
      Payload.add_bucket(payload, %{
        start: start,
        duration: @bucket_duration_ns,
        stats: points_to_maps(points, family)
      })
    end)
  end

  defp points_to_maps(points, family) do
    points
    |> Enum.sort_by(fn {{hash, aggregation_tags}, _point} -> {hash, aggregation_tags} end)
    |> Enum.map(fn {{hash, _aggregation_tags}, point} ->
      %{
        edge_tags: point.edge_tags,
        hash: hash,
        parent_hash: point.parent_hash,
        pathway_latency: Sketch.to_proto(point.pathway),
        edge_latency: Sketch.to_proto(point.edge),
        payload_size: Sketch.to_proto(point.size),
        timestamp_type: to_string(family)
      }
    end)
  end

  defp post(nil, service, _payload, _counts), do: failed(service, :no_transport)

  defp post(transport, service, payload, counts) do
    body = Payload.encode(payload) |> IO.iodata_to_binary() |> :zlib.gzip()
    url = Config.agent_url() <> "/v0.1/pipeline_stats"

    case transport.post(url, @headers, body) do
      {:ok, status} when status < 400 -> flushed(service, counts)
      {:ok, status} -> failed(service, {:http_status, status})
      {:error, reason} -> failed(service, reason)
      other -> failed(service, {:unexpected_response, other})
    end
  rescue
    error -> failed(service, error)
  catch
    kind, reason -> failed(service, {kind, reason})
  end

  defp flushed(service, counts) do
    :telemetry.execute(@stop_event, counts, %{service: service, reason: nil})

    :ok
  end

  # A bucket beyond the horizon never becomes due; the points it carried are
  # counted as dropped so the loss is observable.
  defp dropped(future) when map_size(future) == 0, do: :ok

  defp dropped(future) do
    :telemetry.execute(@stop_event, %{buckets: 0, points: 0, dropped: points_count(future)}, %{
      service: nil,
      reason: :future_bucket
    })

    :ok
  end

  defp points_count(entries) do
    Enum.reduce(entries, 0, fn {{_family, _service, _start}, points}, total ->
      total + map_size(points)
    end)
  end

  defp failed(service, reason) do
    :telemetry.execute(@error_event, %{count: 1}, %{service: service, reason: reason})

    :ok
  end

  defp invalid(service, checkpoint) do
    :telemetry.execute(@invalid_event, %{count: 1}, %{service: service, checkpoint: checkpoint})

    :ok
  end
end
