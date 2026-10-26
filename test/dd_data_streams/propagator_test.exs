defmodule DdDataStreams.PropagatorTest do
  # Not async: a test sets the configured header key, which every test in the
  # suite reads through `DdDataStreams.Config.header/0`.
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.Pathway
  alias DdDataStreams.Propagator

  @key "dd-pathway-ctx-base64"

  # The floor of the window the decoder believes, 2000-01-01 in milliseconds.
  @epoch_floor_ms 946_684_800_000

  @headers Path.expand("../fixtures/propagation/headers.txt", __DIR__)
           |> File.read!()
           |> Code.eval_string()
           |> elem(0)

  defp plausible_start?(0), do: true

  defp plausible_start?(start) do
    start >= @epoch_floor_ms and start <= System.system_time(:millisecond) + 60_000
  end

  test "put/1 and get/1 use the default key under default configuration" do
    pathway = %Pathway{hash: 1, pathway_start: 0, edge_start: 0}

    attributes = Propagator.put(%{}, pathway)

    assert Map.has_key?(attributes, "dd-pathway-ctx-base64")
    assert Propagator.get(attributes) == pathway
  end

  test "put/1 and get/1 use the configured key end to end" do
    Application.put_env(:dd_data_streams, :header, "x-pathway")
    on_exit(fn -> Application.delete_env(:dd_data_streams, :header) end)

    pathway = %Pathway{hash: 1, pathway_start: 0, edge_start: 0}

    attributes = Propagator.put(%{}, pathway)

    refute Map.has_key?(attributes, "dd-pathway-ctx-base64")
    assert Map.has_key?(attributes, "x-pathway")
    assert Propagator.get(attributes) == pathway

    list = Propagator.put([], pathway)
    assert Propagator.get(list) == pathway
  end

  test "decodes every header from the official tracers" do
    for header <- @headers do
      if plausible_start?(header.pathway_start) do
        assert {:ok, pathway} = Propagator.decode_str(header.base64),
               "could not decode #{header.provenance}"

        assert pathway.hash == header.hash,
               "hash of #{header.provenance}"

        assert pathway.pathway_start == header.pathway_start,
               "pathway start of #{header.provenance}"

        assert pathway.edge_start == header.edge_start,
               "edge start of #{header.provenance}"
      else
        # A tracer emits it, but the start is a clock error rather than a time
        # this node shared the pathway with, so the header is unusable.
        assert Propagator.decode_str(header.base64) == :error, header.provenance
      end
    end
  end

  test "re-encodes every reference header to the same bytes" do
    for header <- @headers do
      pathway =
        Pathway.new(
          hash: header.hash,
          pathway_start: header.pathway_start,
          edge_start: header.edge_start
        )

      assert Propagator.encode(pathway) == Base.decode64!(header.base64),
             "raw bytes of #{header.provenance}"

      assert Propagator.encode_str(pathway) == header.base64,
             "base64 of #{header.provenance}"
    end
  end

  test "encodes the hash little-endian, then two zigzag varints of milliseconds" do
    # Raw bytes of a dd-trace-go header for hash 0x0123456789abcdef and both
    # starts at 1700000000000 ms: the eight hash bytes reversed, then
    # zigzag(1700000000000) = 3400000000000 = 0x03179FCAD000, little-endian
    # seven-bit groups 0x80 0xa0 0xab 0xfe 0xf9 0x62, twice.
    pathway = %Pathway{
      hash: 0x0123_4567_89AB_CDEF,
      pathway_start: 1_700_000_000_000,
      edge_start: 1_700_000_000_000
    }

    assert Propagator.encode(pathway) ==
             Base.decode16!("EFCDAB896745230180A0ABFEF96280A0ABFEF962", case: :mixed)

    assert Propagator.encode_str(pathway) == "782riWdFIwGAoKv++WKAoKv++WI="
  end

  test "positive timestamps put a zero low bit in the first varint byte" do
    pathway = %Pathway{hash: 1, pathway_start: 1_700_000_000_000, edge_start: 1_700_000_000_000}

    <<_hash::binary-8, first_pathway_byte, _rest::binary>> = Propagator.encode(pathway)

    assert Bitwise.band(first_pathway_byte, 1) == 0
  end

  test "timestamps whose varints are one to nine bytes round-trip" do
    # zigzag(2^(7n-8)) = 2^(7n-7), which is the smallest value that needs n
    # bytes for n > 1: below 2^56 a varint carries seven bits per byte, and the
    # ninth byte carries the final eight.
    for {varint_bytes, timestamp} <- [
          {1, 0},
          {2, 64},
          {3, 8_192},
          {4, 1_048_576},
          {5, 134_217_728},
          {6, 17_179_869_184},
          {7, 2_199_023_255_552},
          {8, 281_474_976_710_656},
          {9, 36_028_797_018_963_968}
        ] do
      pathway = %Pathway{hash: 42, pathway_start: timestamp, edge_start: timestamp}
      raw = Propagator.encode(pathway)

      assert byte_size(raw) == 8 + 2 * varint_bytes

      # Every start that needs several bytes is outside the window the decoder
      # believes, so only the one-byte start survives the round trip.
      if plausible_start?(timestamp) do
        assert {:ok, ^pathway} = Propagator.decode(raw)
        assert {:ok, ^pathway} = Propagator.decode_str(Propagator.encode_str(pathway))
      else
        assert Propagator.decode(raw) == :error
      end
    end
  end

  test "negative timestamps use the zigzag sign path" do
    pathway = %Pathway{hash: 1, pathway_start: -100_000, edge_start: -50_000}

    assert Propagator.encode(pathway) ==
             Base.decode16!("0100000000000000BF9A0C9F8D06", case: :mixed)

    assert Propagator.decode(Propagator.encode(pathway)) == :error
  end

  test "the ninth byte of a varint ends it even with the continuation bit set" do
    # Eight continuation bytes, then one byte carrying the top eight bits
    # (0xff): the ninth ends the varint instead of reading the 0x00 after it,
    # and the start it decodes to is no time a tracer could have stamped.
    raw = <<0::little-64, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xFF, 0x00>>

    assert Propagator.decode(raw) == :error
  end

  test "bytes after the two timestamps are ignored" do
    raw = Base.decode64!("6gAAAAAAAACAoKv++WKA2uKB+mI=")
    raw = raw <> <<0xDE, 0xAD, 0xBE, 0xEF>>

    assert {:ok, pathway} = Propagator.decode(raw)
    assert pathway.hash == 234
    assert pathway.pathway_start == 1_700_000_000_000
    assert pathway.edge_start == 1_700_003_600_000
  end

  test "non-minimal varints are accepted" do
    raw = <<0::little-64, 0x80, 0x00, 0x80, 0x00>>

    assert {:ok, pathway} = Propagator.decode(raw)
    assert pathway.hash == 0
    assert pathway.pathway_start == 0
    assert pathway.edge_start == 0
  end

  test "malformed input is rejected without raising" do
    assert Propagator.decode_str("") == :error
    assert Propagator.decode_str("AAAA") == :error
    assert Propagator.decode_str("not-base64!!") == :error
    assert Propagator.decode(nil) == :error
  end

  test "a truncated header is rejected" do
    raw = Base.decode64!("6gAAAAAAAACAoKv++WKA2uKB+mI=")

    for length <- 0..(byte_size(raw) - 1) do
      assert Propagator.decode(binary_part(raw, 0, length)) == :error
    end
  end

  test "put/3 and get/2 use the given key" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}

    attributes = Propagator.put(%{"origin" => "eu"}, pathway, "x-pathway")

    assert attributes["origin"] == "eu"
    assert attributes["x-pathway"] == "6gAAAAAAAACAoKv++WKA2uKB+mI="
    assert Propagator.get(attributes, "x-pathway") == pathway
    assert Propagator.get(attributes) == nil
  end

  test "put/3 replaces an existing entry under the given key in a header list" do
    pathway = %Pathway{hash: 1}
    headers = [{"x-pathway", "stale"}, {"traceparent", "00-abc"}]

    assert [{"x-pathway", _value}, {"traceparent", "00-abc"}] =
             Propagator.put(headers, pathway, "x-pathway")
  end

  test "get/2 reads a header list entry of any arity above one" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
    encoded = "6gAAAAAAAACAoKv++WKA2uKB+mI="

    for headers <- [
          [{@key, encoded}],
          [{@key, :longstr, encoded}],
          [{@key, :longstr, :extra, encoded}],
          [{"traceparent", "00-abc"}, {@key, :longstr, encoded}]
        ] do
      assert Propagator.get(headers) == pathway,
             "could not read the pathway out of #{inspect(headers)}"
    end
  end

  test "get/2 returns nil for a carrier no entry can be read from, without raising" do
    assert Propagator.get(["dd-pathway-ctx-base64"], @key) == nil
    assert Propagator.get([{"dd-pathway-ctx-base64"}], @key) == nil
    assert Propagator.get([{"traceparent", "00-abc"}], @key) == nil
    assert Propagator.get([{@key, :longstr, nil}], @key) == nil
    assert Propagator.get([@key | "tail"], @key) == nil
    assert Propagator.get("dd-pathway-ctx-base64", @key) == nil
  end

  test "an entry before an improper tail is still read" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}

    assert Propagator.get([{@key, "6gAAAAAAAACAoKv++WKA2uKB+mI="} | "tail"], @key) == pathway
  end

  test "put/3 rewrites the last element of the first matching entry and drops later ones" do
    pathway = %Pathway{hash: 1}
    encoded = Propagator.encode_str(pathway)
    headers = [{"traceparent", "00-abc"}, {@key, :longstr, "stale"}, {@key, "staler"}]

    assert [{"traceparent", "00-abc"}, {@key, :longstr, ^encoded}] =
             Propagator.put(headers, pathway, @key)

    assert [{@key, ^encoded}] = Propagator.put([{@key, "stale"}], pathway, @key)
  end

  test "put/2 injects the header and keeps the other attributes" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
    attributes = Propagator.put(%{"ordering_key" => "k"}, pathway)

    assert attributes["ordering_key"] == "k"
    assert attributes["dd-pathway-ctx-base64"] == "6gAAAAAAAACAoKv++WKA2uKB+mI="
    assert Propagator.get(attributes) == pathway
  end

  test "put/2 does not write a pathway that was never checkpointed" do
    assert Propagator.put(%{}, %Pathway{}) == %{}
  end

  test "get/1 returns nil when the header is absent or unusable" do
    assert Propagator.get(%{}) == nil
    assert Propagator.get(%{"dd-pathway-ctx-base64" => "AAAA"}) == nil
    assert Propagator.get(%{"dd-pathway-ctx-base64" => nil}) == nil
    assert Propagator.get(nil) == nil
  end

  test "fetch/1 and fetch/2 tell a malformed header apart from an absent one" do
    assert Propagator.fetch(%{@key => "AAAA"}) == {:error, :malformed}
    assert Propagator.fetch(%{@key => nil}) == {:error, :malformed}
    assert Propagator.fetch(%{@key => "AAAA", "dd-pathway-ctx" => <<>>}) == {:error, :malformed}
    assert Propagator.fetch(%{"dd-pathway-ctx" => "AAAA"}) == {:error, :malformed}
    assert Propagator.fetch([{@key, "AAAA"}]) == {:error, :malformed}

    assert Propagator.fetch(%{}) == :absent
    assert Propagator.fetch(%{"other" => "AAAA"}, @key) == :absent
    assert Propagator.fetch(nil) == :absent

    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
    assert Propagator.fetch(Propagator.put(%{}, pathway)) == {:ok, pathway}
  end

  test "a legacy binary dd-pathway-ctx header decodes" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
    raw = Propagator.encode(pathway)

    assert Propagator.get(%{"dd-pathway-ctx" => raw}) == pathway
    assert Propagator.get([{"dd-pathway-ctx", raw}]) == pathway
    assert Propagator.get([{"dd-pathway-ctx", :binary, raw}]) == pathway

    assert Propagator.get([{"traceparent", "00-abc"}, {"dd-pathway-ctx", :longstr, raw}]) ==
             pathway
  end

  test "configured base64 header wins over legacy binary header" do
    configured = %Pathway{
      hash: 234,
      pathway_start: 1_700_000_000_000,
      edge_start: 1_700_003_600_000
    }

    legacy = %Pathway{hash: 999, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}

    attributes = %{
      "dd-pathway-ctx-base64" => Propagator.encode_str(configured),
      "dd-pathway-ctx" => Propagator.encode(legacy)
    }

    assert Propagator.get(attributes) == configured
    assert Propagator.get(attributes, "dd-pathway-ctx-base64") == configured

    headers = [
      {"dd-pathway-ctx-base64", Propagator.encode_str(configured)},
      {"dd-pathway-ctx", Propagator.encode(legacy)}
    ]

    assert Propagator.get(headers) == configured
  end

  test "malformed legacy binary header yields nil" do
    pathway = %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}

    assert Propagator.get(%{"dd-pathway-ctx" => <<>>}) == nil
    assert Propagator.get(%{"dd-pathway-ctx" => "AAAA"}) == nil
    assert Propagator.get([{"dd-pathway-ctx", "AAAA"}]) == nil

    # A present configured key is not rescued by a valid legacy value.
    assert Propagator.get(%{
             "dd-pathway-ctx-base64" => "AAAA",
             "dd-pathway-ctx" => Propagator.encode(pathway)
           }) == nil
  end
end
