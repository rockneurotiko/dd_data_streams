defmodule DdDataStreams.Sketch do
  @moduledoc """
  A DDSketch distribution: values are counted in logarithmically sized bins so
  that a whole distribution serialises into a compact protobuf message.

  Only what the datastreams wire format needs is implemented: `new/0`, `add/2`
  and `to_proto/1`. The mapping is the one dd-trace-go v2.11.2 and
  dd-trace-java 1.67.1 emit, gamma 1.015625 with index offset
  1.8761281912861705; the mapping is self-describing in the payload (gamma and
  offset travel with it). dd-trace-py uses offset 1338.5, so its bins do not
  line up with Go/Java. The store is dense, so the encoded bin counts are
  contiguous with empty bins written as zeros.

  Values whose magnitude is too small to map count as zero, and negative values
  are counted in a second store, indexed by their magnitude.
  """

  @gamma 1.015625
  @index_offset 1.8761281912861705

  @multiplier 1 / :math.log(@gamma)

  # Magnitudes at or below this are counted as zero rather than indexed. It is
  # the larger of the two bounds sketches-go derives: the value that maps back
  # to the smallest 32-bit index, and the smallest normal double scaled by
  # gamma. The first term underflows to zero here, so the second one wins.
  @min_indexable max(
                   :math.exp((-2_147_483_648 - @index_offset) / @multiplier + 1),
                   2.2250738585072014e-308 * @gamma
                 )

  @type t :: %__MODULE__{
          positive: %{optional(integer()) => float()},
          negative: %{optional(integer()) => float()},
          zero_count: float()
        }

  defstruct positive: %{}, negative: %{}, zero_count: 0.0

  @doc """
  Returns an empty distribution.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Adds `value` to the distribution and returns the updated distribution.
  """
  @spec add(t(), number()) :: t()
  def add(%__MODULE__{} = sketch, value) when is_number(value) do
    cond do
      value > @min_indexable -> increment(sketch, :positive, index(value))
      value < -@min_indexable -> increment(sketch, :negative, index(-value))
      true -> %{sketch | zero_count: sketch.zero_count + 1.0}
    end
  end

  @doc """
  Encodes the distribution as a protobuf `DDSketch` message.
  """
  @spec to_proto(t()) :: binary()
  def to_proto(%__MODULE__{} = sketch) do
    [
      # mapping (field 1): gamma (field 1, fixed64) and the index offset
      # (field 2, fixed64), 18 bytes of payload. The interpolation is NONE,
      # which proto3 omits.
      <<0x0A, 0x12, 0x09, @gamma::float-64-little, 0x11, @index_offset::float-64-little>>,
      store(0x12, sketch.positive),
      store(0x1A, sketch.negative),
      if(sketch.zero_count == 0,
        do: [],
        else: <<0x21, sketch.zero_count::float-64-little>>
      )
    ]
    |> IO.iodata_to_binary()
  end

  # protobuf-encoded Store message; the bin counts are emitted contiguously from
  # the lowest occupied bin, with a zero for every bin in between.
  defp store(tag, bins) when map_size(bins) == 0, do: <<tag, 0x00>>

  defp store(tag, bins) do
    lowest = Enum.min(Map.keys(bins))
    highest = Enum.max(Map.keys(bins))

    counts =
      for index <- lowest..highest, into: <<>> do
        <<Map.get(bins, index, 0.0)::float-64-little>>
      end

    inner =
      [
        # contiguousBinCounts (field 2), packed fixed64.
        0x12,
        varint(byte_size(counts)),
        counts,
        offset_field(lowest)
      ]

    [tag, varint(IO.iodata_length(inner)), inner]
  end

  defp offset_field(0), do: []
  defp offset_field(lowest), do: [0x18, varint(zigzag(lowest))]

  # The bin of a positive value: floor(ln(value) * multiplier + index offset).
  # Truncation already floors non-negative results, negative ones need the
  # extra step.
  defp index(value) do
    index = :math.log(value) * @multiplier + @index_offset
    if index >= 0, do: trunc(index), else: trunc(index) - 1
  end

  defp increment(sketch, store, index) do
    Map.update!(sketch, store, &Map.update(&1, index, 1.0, fn count -> count + 1.0 end))
  end

  # sint32 fields are zigzag encoded.
  defp zigzag(index) when index >= 0, do: index * 2
  defp zigzag(index), do: -index * 2 - 1

  defp varint(value) when value < 0x80, do: <<value>>
  defp varint(value), do: <<rem(value, 0x80) + 0x80, varint(div(value, 0x80))::binary>>
end
