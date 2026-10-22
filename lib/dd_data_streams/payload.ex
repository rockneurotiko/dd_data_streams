defmodule DdDataStreams.Payload do
  @moduledoc """
  The MsgPack body the Datadog Agent forwards to the pipeline stats intake.

  The wire shape matches `StatsPayload` in dd-trace-go v2.13, so the field names,
  their order and their types are the ones the intake already accepts from the
  official tracers:

    * the payload carries `Env`, `Service`, `Stats`, `TracerVersion`, `Lang`,
      `Version`, `ProcessTags` and `ProductMask`
    * a bucket carries `Start` and `Duration` in Unix nanoseconds, its `Stats`
      points, `Backlogs`, `Transactions` and `TransactionCheckpointIds`
    * a point carries `EdgeTags`, `Hash`, `ParentHash`, the three DDSketch
      distributions and `TimestampType`

  The sketches are `bin` on the wire, not `str`: the reference decoder reads them
  with a bytes method and rejects a string. Integers are written in the smallest
  unsigned form and strings in the smallest string form, because the reference
  encoder does the same.

  Backlogs and transactions are part of the shape but are always empty here;
  nothing in this library collects them.
  """

  alias DdDataStreams.Config

  import Bitwise

  @version Mix.Project.config()[:version]

  # DSM is bit 1 of the product mask. The Go and Java tracers also set the APM
  # bit; the intake honours either.
  @product_mask 2

  @bucket_duration_ns 10_000_000_000

  @typedoc """
  A point: the node's edge tags, its hash and parent hash, and the three
  distributions. Latencies are in seconds and the payload size in bytes; the
  distributions are the `DdDataStreams.Sketch.to_proto/1` blobs.
  """
  @type point :: %{
          optional(:edge_tags) => [String.t()],
          optional(:hash) => non_neg_integer(),
          optional(:parent_hash) => non_neg_integer(),
          optional(:pathway_latency) => binary(),
          optional(:edge_latency) => binary(),
          optional(:payload_size) => binary(),
          optional(:timestamp_type) => String.t()
        }

  @typedoc """
  A bucket: its start in Unix nanoseconds, its width in nanoseconds and its
  points. `:current` and `:origin` buckets differ only in `:start` and in the
  points' `:timestamp_type`.
  """
  @type bucket :: %{
          optional(:start) => non_neg_integer(),
          optional(:duration) => non_neg_integer(),
          optional(:stats) => [point()]
        }

  @type t :: %__MODULE__{
          env: String.t(),
          service: String.t(),
          stats: [bucket()],
          tracer_version: String.t(),
          lang: String.t(),
          version: String.t(),
          process_tags: [String.t()],
          product_mask: non_neg_integer()
        }

  defstruct env: "",
            service: "",
            stats: [],
            tracer_version: @version,
            lang: "elixir",
            version: "",
            process_tags: [],
            product_mask: @product_mask

  @doc """
  Returns an empty payload for the service configured in the environment.

  The identity fields are read from `DdDataStreams.Config`, so the aggregator can
  build the payload without passing them around. `:stats` is filled by
  `add_bucket/2`.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{
      env: Config.env(),
      service: Config.service(),
      version: Config.version()
    }
  end

  @doc """
  Appends a bucket to the payload.
  """
  @spec add_bucket(t(), bucket()) :: t()
  def add_bucket(%__MODULE__{} = payload, bucket) when is_map(bucket) do
    %{payload | stats: payload.stats ++ [bucket]}
  end

  @doc """
  Encodes the payload as MsgPack iodata.
  """
  @spec encode(t()) :: iodata()
  def encode(%__MODULE__{} = payload) do
    map([
      {"Env", str(payload.env)},
      {"Service", str(payload.service)},
      {"Stats", array(Enum.map(payload.stats, &bucket/1))},
      {"TracerVersion", str(payload.tracer_version)},
      {"Lang", str(payload.lang)},
      {"Version", str(payload.version)},
      {"ProcessTags", array(Enum.map(payload.process_tags, &str/1))},
      {"ProductMask", uint(payload.product_mask)}
    ])
  end

  defp bucket(bucket) do
    map([
      {"Start", uint(Map.get(bucket, :start, 0))},
      {"Duration", uint(Map.get(bucket, :duration, @bucket_duration_ns))},
      {"Stats", array(Enum.map(Map.get(bucket, :stats, []), &point/1))},
      {"Backlogs", array([])},
      {"Transactions", bin("")},
      {"TransactionCheckpointIds", bin("")}
    ])
  end

  defp point(point) do
    map([
      {"EdgeTags", array(Enum.map(Map.get(point, :edge_tags, []), &str/1))},
      {"Hash", uint(Map.get(point, :hash, 0))},
      {"ParentHash", uint(Map.get(point, :parent_hash, 0))},
      {"PathwayLatency", bin(Map.get(point, :pathway_latency, ""))},
      {"EdgeLatency", bin(Map.get(point, :edge_latency, ""))},
      {"PayloadSize", bin(Map.get(point, :payload_size, ""))},
      {"TimestampType", str(point |> Map.get(:timestamp_type, :current) |> to_string())}
    ])
  end

  # fixmap, then map16 and map32.
  defp map(pairs) when length(pairs) < 16 do
    [<<0x80 ||| length(pairs)>> | Enum.map(pairs, fn {key, value} -> [str(key), value] end)]
  end

  defp map(pairs) when length(pairs) < 65_536 do
    [<<0xDE, length(pairs)::16>> | Enum.map(pairs, fn {key, value} -> [str(key), value] end)]
  end

  defp map(pairs) do
    [<<0xDF, length(pairs)::32>> | Enum.map(pairs, fn {key, value} -> [str(key), value] end)]
  end

  # fixarray, array16, array32.
  defp array(items) when length(items) < 16, do: [<<0x90 ||| length(items)>> | items]
  defp array(items) when length(items) < 65_536, do: [<<0xDC, length(items)::16>> | items]
  defp array(items), do: [<<0xDD, length(items)::32>> | items]

  # fixstr, str8, str16, str32.
  defp str(string) when byte_size(string) < 32, do: <<0xA0 ||| byte_size(string), string::binary>>

  defp str(string) when byte_size(string) < 0x100,
    do: <<0xD9, byte_size(string), string::binary>>

  defp str(string) when byte_size(string) < 0x1_0000,
    do: <<0xDA, byte_size(string)::16, string::binary>>

  defp str(string), do: <<0xDB, byte_size(string)::32, string::binary>>

  # bin8, bin16, bin32. The sketches must be bin, never str.
  defp bin(bytes) when byte_size(bytes) < 0x100, do: <<0xC4, byte_size(bytes), bytes::binary>>

  defp bin(bytes) when byte_size(bytes) < 0x1_0000,
    do: <<0xC5, byte_size(bytes)::16, bytes::binary>>

  defp bin(bytes), do: <<0xC6, byte_size(bytes)::32, bytes::binary>>

  # positive fixint, uint8, uint16, uint32, uint64.
  defp uint(number) when number < 0x80, do: <<number>>
  defp uint(number) when number < 0x100, do: <<0xCC, number>>
  defp uint(number) when number < 0x1_0000, do: <<0xCD, number::16>>
  defp uint(number) when number < 0x1_0000_0000, do: <<0xCE, number::32>>
  defp uint(number), do: <<0xCF, number::64>>
end
