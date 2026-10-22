defmodule DdDataStreams.PropagatorTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Pathway
  alias DdDataStreams.Propagator

  @headers Path.expand("../fixtures/propagation/headers.txt", __DIR__)
           |> File.read!()
           |> Code.eval_string()
           |> elem(0)

  test "the wire attribute name is dd-pathway-ctx-base64" do
    pathway = %Pathway{hash: 1, pathway_start: 0, edge_start: 0}

    assert Map.has_key?(Propagator.put(%{}, pathway), "dd-pathway-ctx-base64")
  end

  test "decodes every header from the official tracers" do
    for header <- @headers do
      assert {:ok, pathway} = Propagator.decode_str(header.base64),
             "could not decode #{header.provenance}"

      assert pathway.hash == header.hash,
             "hash of #{header.provenance}"

      assert pathway.pathway_start == header.pathway_start,
             "pathway start of #{header.provenance}"

      assert pathway.edge_start == header.edge_start,
             "edge start of #{header.provenance}"
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
      assert {:ok, ^pathway} = Propagator.decode(raw)
      assert {:ok, ^pathway} = Propagator.decode_str(Propagator.encode_str(pathway))
    end
  end

  test "negative timestamps use the zigzag sign path" do
    pathway = %Pathway{hash: 1, pathway_start: -100_000, edge_start: -50_000}

    assert Propagator.encode(pathway) ==
             Base.decode16!("0100000000000000BF9A0C9F8D06", case: :mixed)

    assert {:ok, ^pathway} = Propagator.decode(Propagator.encode(pathway))
  end

  test "the ninth byte of a varint ends it even with the continuation bit set" do
    # Eight continuation bytes carrying zero bits, then one byte carrying the
    # top eight bits (0xff); the following 0x00 is the second timestamp.
    raw = <<0::little-64, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xFF, 0x00>>

    assert {:ok, pathway} = Propagator.decode(raw)
    assert pathway.pathway_start == 0x7F80_0000_0000_0000
    assert pathway.edge_start == 0
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
end
