defmodule DdDataStreams.Propagator do
  @moduledoc """
  The pathway header codec.

  The convenience arities, `put/2` and `get/1`, use the configured key
  `DdDataStreams.Config.header/0`, whose default value is
  `dd-pathway-ctx-base64`, the name the official Datadog tracers use; the
  explicit-key arities, `put/3` and `get/2`, use the key they are given. For
  backwards compatibility with older producers,
  `get/2` also falls back to the legacy binary `dd-pathway-ctx` key, decoding
  its raw (non-base64) bytes, when the configured key is absent.

  The payload is the 64-bit unsigned hash in
  little-endian byte order followed by
  the pathway start and the edge start as zigzag-encoded varints of Unix
  milliseconds, with no separator or length prefix, and the whole thing is
  standard base64. The timestamps are length-parsed rather than assumed to be a
  fixed width, so a header written by an official tracer decodes here and a
  header written here decodes there for every timestamp a real clock produces;
  the widest varint the format allows diverges, and that divergence is recorded
  in the interoperability fixtures. Verified against dd-trace-go v2.11.2,
  dd-trace-java 1.67.1 and dd-trace-py 3.17.0.

  A malformed or missing header is not an error condition for a pipeline:
  `get/1` returns `nil` and the consumer starts a new pathway. `fetch/1` tells
  the two apart, so a consumer that wants to report a header it could not read
  can.

  Next to the pathway header, a producer writes the outgoing topic under
  `dd-topic` (`topic_header/0`) whenever the extractor named one. It is not part
  of the pathway codec: it is the tag a consumer whose metadata names only a
  subscription reads to report the queue the pathway crossed.
  """

  alias DdDataStreams.Config
  alias DdDataStreams.Pathway

  import Bitwise

  @default_header "dd-pathway-ctx-base64"

  @legacy_header "dd-pathway-ctx"

  @topic_header "dd-topic"

  # Group index of the last varint byte: a 64-bit unsigned integer takes at
  # most nine varint bytes, so the ninth (index 8) is taken whole.
  @max_varint_bytes 8

  # A start is a Unix time in milliseconds the reported latency is measured
  # back from, so a start before 2000-01-01 is not a clock a tracer could have
  # stamped: believing it would report a latency of decades.
  @epoch_floor_ms 946_684_800_000

  # How far a start may sit in the future before the header is dropped rather
  # than believed: a minute is past any clock skew between two nodes.
  @future_horizon_ms 60_000

  # An entry is a tuple of any arity above one whose first element is the key,
  # whatever else the broker puts in it: its value is the last element.
  defguardp entry(candidate, key)
            when is_tuple(candidate) and tuple_size(candidate) >= 2 and elem(candidate, 0) == key

  @doc """
  Puts the pathway into a message's headers under `key`. `put/2` defaults to
  the configured key `DdDataStreams.Config.header/0`, whose default value is
  `#{@default_header}`.

  The headers are either an attribute map or a list of tuples whose first
  element is the header key, as `BroadwayKafka` exposes them; an existing entry
  under the same key is replaced in both, keeping whatever other elements that
  entry carries. A pathway that has not been checkpointed (hash zero) carries
  no information and is not written. The headers are returned unchanged so a
  produce path can call this unconditionally.
  """
  @spec put(map() | [tuple()], Pathway.t()) :: map() | [tuple()]
  @spec put(map() | [tuple()], Pathway.t(), String.t()) :: map() | [tuple()]
  def put(attributes, pathway), do: put(attributes, pathway, Config.header())

  def put(attributes, %Pathway{hash: 0}, _key), do: attributes

  def put(attributes, %Pathway{} = pathway, key) when is_list(attributes) do
    value = encode_str(pathway)

    case Enum.split_while(attributes, &(not entry(&1, key))) do
      {_before, []} ->
        [{key, value} | attributes]

      {before, [entry | rest]} ->
        before ++
          [put_elem(entry, tuple_size(entry) - 1, value) | Enum.reject(rest, &entry(&1, key))]
    end
  end

  def put(attributes, %Pathway{} = pathway, key) do
    Map.put(attributes, key, encode_str(pathway))
  end

  @doc """
  Reads the pathway out of a message's headers under `key`. `get/1` defaults
  to the configured key `DdDataStreams.Config.header/0`, whose default value is
  `#{@default_header}`.

  The headers are an attribute map or a list of tuples whose first element is
  the header key and whose value is its last element. Returns `nil` when the
  header is absent, is not a valid base64 header, does not contain a hash and
  two timestamps, carries a start outside the window `decode/1` believes, or is
  carried in a shape no entry can be read from.

  When `key` is absent from the carrier, a legacy binary header stored under
  `#{@legacy_header}` is tried instead and decoded as raw bytes via `decode/1`.
  The configured key always wins when it is present.
  """
  @spec get(map() | [tuple()] | nil) :: Pathway.t() | nil
  @spec get(map() | [tuple()] | nil, String.t()) :: Pathway.t() | nil
  def get(attributes), do: get(attributes, Config.header())

  def get(attributes, key) do
    case fetch(attributes, key) do
      {:ok, pathway} -> pathway
      _absent_or_malformed -> nil
    end
  end

  @doc """
  Reads the pathway out of a message's headers under `key`, like `get/2`, but
  tells a header that is absent from one that is present and cannot be read.

  Answers `{:ok, pathway}`, `:absent` when neither the configured key nor the
  legacy `#{@legacy_header}` one is present, or `{:error, :malformed}` when a
  header is present but is not a usable base64 pathway: not decodable,
  missing the hash and two timestamps, or carrying a start outside the window
  `decode/1` believes. A consumer starts a new pathway either way, but a
  producer writing a header this consumer cannot read would otherwise be
  indistinguishable from one that sent no header at all. A header under any
  other key is indistinguishable from an absent one.
  """
  @spec fetch(map() | [tuple()] | nil) :: {:ok, Pathway.t()} | :absent | {:error, :malformed}
  @spec fetch(map() | [tuple()] | nil, String.t()) ::
          {:ok, Pathway.t()} | :absent | {:error, :malformed}
  def fetch(attributes), do: fetch(attributes, Config.header())

  def fetch(attributes, key) when is_map(attributes) do
    case Map.fetch(attributes, key) do
      {:ok, value} ->
        decode_value(value)

      :error ->
        case Map.fetch(attributes, @legacy_header) do
          {:ok, value} -> decode_raw_value(value)
          :error -> :absent
        end
    end
  end

  def fetch(attributes, key) when is_list(attributes) do
    case find(attributes, key) do
      {:ok, value} ->
        decode_value(value)

      :error ->
        case find(attributes, @legacy_header) do
          {:ok, value} -> decode_raw_value(value)
          :error -> :absent
        end
    end
  end

  def fetch(_attributes, _key), do: :absent

  defp find([entry | _rest], key) when entry(entry, key),
    do: {:ok, elem(entry, tuple_size(entry) - 1)}

  defp find([_entry | rest], key), do: find(rest, key)
  defp find(_carrier, _key), do: :error

  defp decode_value(value) when is_binary(value) do
    case decode_str(value) do
      {:ok, pathway} -> {:ok, pathway}
      :error -> {:error, :malformed}
    end
  end

  defp decode_value(_value), do: {:error, :malformed}

  defp decode_raw_value(value) when is_binary(value) do
    case decode(value) do
      {:ok, pathway} -> {:ok, pathway}
      :error -> {:error, :malformed}
    end
  end

  defp decode_raw_value(_value), do: {:error, :malformed}

  @doc """
  The legacy binary header key, `"#{@legacy_header}"`.
  """
  @spec legacy_header() :: String.t()
  def legacy_header, do: @legacy_header

  @doc """
  The attribute or header key the producer writes the outgoing topic under,
  `"#{@topic_header}"`.

  It travels next to the pathway header, so a consumer whose broker metadata
  names only the subscription can still report the topic the message crossed.
  """
  @spec topic_header() :: String.t()
  def topic_header, do: @topic_header

  @doc """
  Encodes a pathway as the raw header bytes.
  """
  @spec encode(Pathway.t()) :: binary()
  def encode(%Pathway{hash: hash, pathway_start: pathway_start, edge_start: edge_start}) do
    <<hash::little-64, varint(zigzag(pathway_start))::binary, varint(zigzag(edge_start))::binary>>
  end

  @doc """
  Decodes a raw header.

  Bytes after the two timestamps are ignored, and a varint need not be encoded
  in its shortest form. The ninth byte of a varint ends it even when its
  continuation bit is set.

  A start outside the plausible window makes the whole header unusable rather
  than trustworthy: `get/1` then yields `nil` and the consumer starts a new
  pathway, as it does for every other malformed header.
  """
  @spec decode(binary()) :: {:ok, Pathway.t()} | :error
  def decode(binary) when is_binary(binary) do
    with <<hash::little-64, rest::binary>> <- binary,
         {:ok, pathway_start_zz, rest} <- take_varint(rest, 0, 0, 0),
         {:ok, edge_start_zz, _rest} <- take_varint(rest, 0, 0, 0),
         pathway_start <- unzigzag(pathway_start_zz),
         edge_start <- unzigzag(edge_start_zz),
         true <- plausible_start?(pathway_start),
         true <- plausible_start?(edge_start) do
      {:ok,
       %Pathway{
         hash: hash,
         pathway_start: pathway_start,
         edge_start: edge_start,
         previous_direction: nil,
         closest_opposite_direction_hash: 0,
         closest_opposite_direction_edge_start: 0
       }}
    else
      _ -> :error
    end
  end

  def decode(_binary), do: :error

  @doc """
  Encodes a pathway as a base64 header string.
  """
  @spec encode_str(Pathway.t()) :: String.t()
  def encode_str(%Pathway{} = pathway) do
    pathway |> encode() |> Base.encode64()
  end

  @doc """
  Decodes a base64 header string.
  """
  @spec decode_str(binary()) :: {:ok, Pathway.t()} | :error
  def decode_str(encoded) when is_binary(encoded) do
    case Base.decode64(encoded, padding: false) do
      {:ok, binary} -> decode(binary)
      :error -> :error
    end
  end

  def decode_str(_encoded), do: :error

  # Zero is the start a pathway carries until its first checkpoint, so it is
  # overwritten rather than believed; any other start has to be a time this
  # node could have shared the pathway with.
  defp plausible_start?(0), do: true

  defp plausible_start?(start),
    do:
      start >= @epoch_floor_ms and start <= System.system_time(:millisecond) + @future_horizon_ms

  defp zigzag(value) when value >= 0, do: value * 2
  defp zigzag(value), do: -value * 2 - 1

  defp unzigzag(value) do
    Bitwise.bxor(value >>> 1, -(value &&& 1))
  end

  defp varint(value), do: varint(value, 0)

  defp varint(value, @max_varint_bytes), do: <<value>>

  defp varint(value, _groups) when value < 0x80, do: <<value>>

  defp varint(value, groups) do
    <<(value &&& 0x7F) ||| 0x80, varint(value >>> 7, groups + 1)::binary>>
  end

  # The final byte is taken whole, so a varint whose ninth byte still has the
  # continuation bit set terminates there instead of running off the end.
  defp take_varint(<<byte, rest::binary>>, shift, acc, @max_varint_bytes) do
    {:ok, acc ||| byte <<< shift, rest}
  end

  defp take_varint(<<byte, rest::binary>>, shift, acc, _groups) when byte < 0x80 do
    {:ok, acc ||| byte <<< shift, rest}
  end

  defp take_varint(<<byte, rest::binary>>, shift, acc, groups) do
    take_varint(rest, shift + 7, acc ||| (byte &&& 0x7F) <<< shift, groups + 1)
  end

  defp take_varint(<<>>, _shift, _acc, _groups), do: :error
end
