defmodule DdDataStreams.Aggregator do
  @moduledoc """
  Aggregates checkpoints into ten-second buckets and flushes them to the agent.

  A checkpoint is counted twice, as dd-trace-go v2.13 does it: once in the bucket
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
  newer than ten seconds ago, and keeps the bucket still being filled. A bucket
  is removed before it is sent, so a failed send drops it: it is logged and
  counted in `[:dd_data_streams, :flush, :error]` telemetry with a `count`
  measurement, and nothing is raised. Stopping the process sends everything,
  including the partial bucket.

  One payload and one request are built per service. The transport and the
  agent URL are read from `DdDataStreams.Config` on every flush, so a change to
  either applies to the next one.
  """

  use GenServer

  require Logger

  alias DdDataStreams.Config
  alias DdDataStreams.Payload
  alias DdDataStreams.Sketch
  alias DdDataStreams.Tags

  @bucket_duration_ns 10_000_000_000

  # How far a checkpoint may sit in the future before its bucket is dropped
  # instead of kept: a minute is well past any broker clock skew.
  @future_horizon_ns 60_000_000_000
  @flush_interval_ms 10_000
  @headers [{"content-type", "application/msgpack"}, {"content-encoding", "gzip"}]
  @error_event [:dd_data_streams, :flush, :error]

  @typedoc """
  A checkpoint with the size of the message it was recorded for, which
  `DdDataStreams.Pathway.set_checkpoint/3` leaves out.
  """
  @type checkpoint :: %{
          required(:hash) => non_neg_integer(),
          required(:parent_hash) => non_neg_integer(),
          required(:tags) => [String.t()],
          required(:timestamp) => integer(),
          required(:pathway_latency) => non_neg_integer(),
          required(:edge_latency) => non_neg_integer(),
          optional(:payload_size) => non_neg_integer()
        }

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
  running, which is the case while the library is disabled.
  """
  @spec add(checkpoint()) :: :ok
  def add(checkpoint) when is_map(checkpoint) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:add, checkpoint})
    end
  end

  @doc """
  Sends every bucket that is complete, dropping each one before it is sent.

  Returns once the sends have finished, whether or not they succeeded.
  """
  @spec flush() :: :ok
  def flush do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      # The transport may take longer than the default five seconds, and the
      # caller waits for the sends either way.
      pid -> GenServer.call(pid, :flush, :infinity)
    end
  end

  @impl true
  def init(_opts) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:ok, %{buckets: %{}}}
  end

  @impl true
  def handle_cast({:add, checkpoint}, state) do
    {:noreply, %{state | buckets: add_checkpoint(state.buckets, checkpoint)}}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {:reply, :ok, flush_due(state, System.system_time(:millisecond) * 1_000_000)}
  end

  @impl true
  def handle_info(:flush, state) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, flush_due(state, System.system_time(:millisecond) * 1_000_000)}
  end

  @impl true
  def terminate(_reason, state) do
    flush_due(state, :infinity)
  end

  defp add_checkpoint(buckets, checkpoint) do
    timestamp_ns = checkpoint.timestamp * 1_000_000
    pathway_latency = Map.get(checkpoint, :pathway_latency, 0)
    edge_latency = Map.get(checkpoint, :edge_latency, 0)
    payload_size = Map.get(checkpoint, :payload_size, 0)
    service = Config.service()

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
    buckets = Map.reject(buckets, fn {{_f, _s, start}, _} -> start > horizon end)

    transport = Config.transport()

    due
    |> Enum.group_by(fn {{_family, service, _start}, _points} -> service end)
    |> Enum.each(fn {service, entries} ->
      post(transport, service, build_payload(service, entries))
    end)

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

  defp post(nil, service, _payload), do: failed(service, :no_transport)

  defp post(transport, service, payload) do
    body = Payload.encode(payload) |> IO.iodata_to_binary() |> :zlib.gzip()
    url = Config.agent_url() <> "/v0.1/pipeline_stats"

    case transport.post(url, @headers, body) do
      {:ok, status} when status < 400 -> :ok
      {:ok, status} -> failed(service, {:http_status, status})
      {:error, reason} -> failed(service, reason)
      other -> failed(service, {:unexpected_response, other})
    end
  rescue
    error -> failed(service, error)
  catch
    kind, reason -> failed(service, {kind, reason})
  end

  defp failed(service, reason) do
    Logger.warning(
      "dropped pipeline stats of #{service} after a failed flush: #{inspect(reason)}"
    )

    :telemetry.execute(@error_event, %{count: 1}, %{service: service, reason: reason})

    :ok
  end
end
