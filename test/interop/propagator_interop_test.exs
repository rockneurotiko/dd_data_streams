defmodule DdDataStreams.PropagatorInteropTest do
  # Interoperability of the `dd-pathway-ctx-base64` codec with the official
  # tracers. These tests do not run dd-trace-go, dd-trace-py or dd-trace-java:
  # the foreign values in the fixtures are recorded evidence of what those
  # implementations emitted or read, with the commit and version each came from
  # stated in the fixture headers (test/fixtures/propagation/headers.txt and
  # test/fixtures/interop/reference_decoders.txt).
  #
  # So the executable checks are only ever this library against a recording:
  #
  #   * headers captured from the official encoders decode back to the pathway
  #     they were recorded for;
  #   * the base64 those implementations read and re-encoded is what this
  #     library's encoder emits, and their own re-encodings of it are identical.
  #
  # The recordings are not a specification this library gets to adjust: if a
  # change here fails these assertions, the wire format moved and that is the
  # finding.
  #
  # The one recorded disagreement, the widest varint that dd-trace-go truncates
  # and dd-trace-java clamps, is asserted below against the recorded fields: it
  # is a real difference between the tracers worth pinning, not a property of
  # this library.
  use ExUnit.Case, async: true

  alias DdDataStreams.Pathway
  alias DdDataStreams.Propagator

  @tracer_headers Path.expand("../fixtures/propagation/headers.txt", __DIR__)
                  |> File.read!()
                  |> Code.eval_string()
                  |> elem(0)

  @reference Path.expand("../fixtures/interop/reference_decoders.txt", __DIR__)
             |> File.read!()
             |> Code.eval_string()
             |> elem(0)

  # dd-trace-go's decoder stops reading the nine-byte varint and dd-trace-java
  # clamps it, so that one entry is asserted separately rather than mixed into
  # the agreeing set.
  @wide_start 72_057_594_037_927_936

  @epoch_floor_ms 946_684_800_000

  defp plausible_start?(0), do: true

  defp plausible_start?(start) do
    start >= @epoch_floor_ms and start <= System.system_time(:millisecond) + 60_000
  end

  test "decodes the headers the official tracers emitted" do
    for header <- @tracer_headers do
      if plausible_start?(header.pathway_start) and plausible_start?(header.edge_start) do
        assert {:ok, pathway} = Propagator.decode_str(header.base64), header.provenance
        assert pathway.hash == header.hash, header.provenance
        assert pathway.pathway_start == header.pathway_start, header.provenance
        assert pathway.edge_start == header.edge_start, header.provenance
      else
        assert Propagator.decode_str(header.base64) == :error, header.provenance
      end
    end
  end

  test "emits the header the official decoders read back" do
    for entry <- Enum.reject(@reference, &(&1.pathway_start == @wide_start)) do
      encoded = Propagator.encode_str(Pathway.new(entry))

      assert encoded == entry.base64, entry.provenance

      # Each decoder read those bytes and re-encoded what it read; all three
      # came back with the same header this library emits.
      assert encoded == entry.go.reencoded, "dd-trace-go round trip of #{entry.provenance}"
      assert encoded == entry.python.reencoded, "dd-trace-py round trip of #{entry.provenance}"

      assert encoded == entry.java.reencoded,
             "dd-trace-java round trip of #{entry.provenance}"
    end
  end

  test "the widest varint is read by dd-trace-py and this library, and not by the others" do
    wide = Enum.find(@reference, &(&1.pathway_start == @wide_start))

    encoded = Propagator.encode_str(Pathway.new(wide))
    assert encoded == wide.base64

    # The start is far outside any plausible window, so the header is rejected
    # rather than believed; the varint itself is still parsed in full, which is
    # what distinguishes this library from dd-trace-go's truncation.
    assert Propagator.decode_str(wide.base64) == :error

    # dd-trace-py 3.17.0 parses the full varint, so it reads the header this
    # library emits and re-encodes it unchanged.
    assert wide.python.pathway_start == @wide_start
    assert wide.python.reencoded == encoded

    # dd-trace-go v2 (`DecodeBase64`) stops after seven groups and keeps
    # part of the timestamp; its encoder writes the truncated value back.
    assert wide.go.pathway_start == 4_611_686_018_427
    assert wide.go.reencoded != encoded

    # dd-trace-java 1.67.1 (`DefaultPathwayContext.decode`) clamps the start to
    # the largest signed long, so its re-encoded header differs too.
    assert wide.java.start_nanos == 9_223_372_036_854_775_807
    assert wide.java.reencoded != encoded
  end
end
