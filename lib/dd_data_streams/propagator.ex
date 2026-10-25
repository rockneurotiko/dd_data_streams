defmodule DdDataStreams.Propagator do
  @moduledoc """
  The pathway header codec.

  The default header key is `dd-pathway-ctx-base64`, the name the official
  Datadog tracers use; `DdDataStreams.Config.header/0` is the configured key the
  library reads and writes. The payload is the 64-bit unsigned hash in
  little-endian byte order followed by
  the pathway start and the edge start as zigzag-encoded varints of Unix
  milliseconds, with no separator or length prefix, and the whole thing is
  standard base64. The timestamps are length-parsed rather than assumed to be a
  fixed width, so a header written by any official tracer decodes here and a
  header written here decodes there.

  A malformed or missing header is not an error condition for a pipeline:
  `get/1` returns `nil` and the consumer starts a new pathway.
  """

  alias DdDataStreams.Pathway

  import Bitwise

  @default_header "dd-pathway-ctx-base64"

  # A 64-bit unsigned integer takes at most nine varint bytes: eight groups of
  # seven bits plus a final byte carrying the remaining eight.
  @max_varint_bytes 8

  @doc """
  Puts the pathway into a message's headers under `key`, which defaults to
  `#{@default_header}`.

  The headers are either an attribute map or a list of `{key, value}` pairs, as
  `BroadwayKafka` exposes them; an existing entry under the same key is
  replaced in both. A pathway that has not been checkpointed (hash zero) carries
  no information and is not written. The headers are returned unchanged so a
  produce path can call this unconditionally.
  """
  @spec put(map() | [{binary(), binary()}], Pathway.t()) :: map() | [{binary(), binary()}]
  @spec put(map() | [{binary(), binary()}], Pathway.t(), String.t()) ::
          map() | [{binary(), binary()}]
  def put(attributes, pathway), do: put(attributes, pathway, @default_header)

  def put(attributes, %Pathway{hash: 0}, _key), do: attributes

  def put(attributes, %Pathway{} = pathway, key) when is_list(attributes) do
    [
      {key, encode_str(pathway)}
      | Enum.reject(attributes, fn
          {existing, _value} -> existing == key
          _other -> false
        end)
    ]
  end

  def put(attributes, %Pathway{} = pathway, key) do
    Map.put(attributes, key, encode_str(pathway))
  end

  @doc """
  Reads the pathway out of a message's headers under `key`, which defaults to
  `#{@default_header}`.

  The headers are an attribute map or a list of `{key, value}` pairs. Returns
  `nil` when the header is absent, is not a valid base64 header, or does not
  contain a hash and two timestamps.
  """
  @spec get(map() | [{binary(), binary()}] | nil) :: Pathway.t() | nil
  @spec get(map() | [{binary(), binary()}] | nil, String.t()) :: Pathway.t() | nil
  def get(attributes), do: get(attributes, @default_header)

  def get(attributes, key) when is_map(attributes) do
    attributes |> Map.get(key) |> decode_value()
  end

  def get(attributes, key) when is_list(attributes) do
    case List.keyfind(attributes, key, 0) do
      {^key, value} -> decode_value(value)
      nil -> nil
    end
  end

  def get(_attributes, _key), do: nil

  defp decode_value(value) when is_binary(value) do
    case decode_str(value) do
      {:ok, pathway} -> pathway
      :error -> nil
    end
  end

  defp decode_value(_value), do: nil

  @doc """
  Encodes a pathway as the raw header bytes.
  """
  @spec encode(Pathway.t()) :: binary()
  def encode(%Pathway{hash: hash, pathway_start: pathway_start, edge_start: edge_start}) do
    <<hash::little-64, varint(zigzag(pathway_start))::binary, varint(zigzag(edge_start))::binary>>
  end

  @doc """
  Decodes a raw header.

  Bytes after the two timestamps are ignored, and a varint need not be encoded in
  its shortest form. The ninth byte of a varint ends it even when its
  continuation bit is set.
  """
  @spec decode(binary()) :: {:ok, Pathway.t()} | :error
  def decode(binary) when is_binary(binary) do
    with <<hash::little-64, rest::binary>> <- binary,
         {:ok, pathway_start, rest} <- take_varint(rest, 0, 0, 0),
         {:ok, edge_start, _rest} <- take_varint(rest, 0, 0, 0) do
      {:ok,
       %Pathway{
         hash: hash,
         pathway_start: unzigzag(pathway_start),
         edge_start: unzigzag(edge_start)
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
