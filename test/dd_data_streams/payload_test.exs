defmodule DdDataStreams.PayloadTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias DdDataStreams.Config
  alias DdDataStreams.Payload
  alias DdDataStreams.Sketch
  alias DdDataStreams.Testing

  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]

  @hash 304_706_334_078_420_845
  @parent_hash 2_140_133_010_089_266_238

  # Both fixtures were built field by field and encoded with dd-trace-go
  # v2.11.2's generated msgp encoder (internal/datastreams/payload_msgp.go at
  # commit c7dcfe8940aad88f4b779a3e4ad75c478397872d,
  # https://github.com/DataDog/dd-trace-go/tree/c7dcfe8940aad88f4b779a3e4ad75c478397872d/internal/datastreams),
  # with every field hand-pinned to this library's values. Lang and
  # TracerVersion intentionally differ from what Go would emit; ProductMask
  # matches Go's APM|DSM value. They are therefore a foreign encoder's bytes,
  # not this library's: field names, field order, wire types and integer widths
  # all have to agree for a comparison to pass.
  @fixture_current """
  88a3456e76a470726f64a753657276696365a8636865636b6f7574a553746174739186a55374
  617274cf17979cfe362a0000a84475726174696f6ecf00000002540be400a553746174739187
  a8456467655461677393ac646972656374696f6e3a696eb7737562736372697074696f6e3a6f
  72646572732d737562b2747970653a676f6f676c652d707562737562a448617368cf043a88cb
  e38e476daa506172656e7448617368cf1db347a23e754c3eae506174687761794c6174656e63
  79c4250a1209000000000040f03f1100d88afe9e04fe3f120d1208000000000000f03f18af01
  1a00ab456467654c6174656e6379c4250a1209000000000040f03f1100d88afe9e04fe3f120d
  1208000000000000f03f1889021a00ab5061796c6f616453697a65c4250a1209000000000040
  f03f1100d88afe9e04fe3f120d1208000000000000f03f1898071a00ad54696d657374616d70
  54797065a763757272656e74a84261636b6c6f677390ac5472616e73616374696f6e73c400b8
  5472616e73616374696f6e436865636b706f696e74496473c400ad5472616365725665727369
  6f6ea5302e312e30a44c616e67a6656c69786972a756657273696f6ea5312e322e33ab50726f
  636573735461677390ab50726f647563744d61736b03
  """

  @fixture_origin """
  88a3456e76a470726f64a753657276696365a8636865636b6f7574a553746174739186a55374
  617274cf17979cfbe21e1c00a84475726174696f6ecf00000002540be400a553746174739187
  a8456467655461677393ad646972656374696f6e3a6f7574ac746f7069633a6f7264657273b2
  747970653a676f6f676c652d707562737562a448617368cf1db347a23e754c3eaa506172656e
  744861736800ae506174687761794c6174656e6379c4240a1209000000000040f03f1100d88a
  fe9e04fe3f120c1208000000000000f03f18551a00ab456467654c6174656e6379c4240a1209
  000000000040f03f1100d88afe9e04fe3f120c1208000000000000f03f18551a00ab5061796c
  6f616453697a65c4250a1209000000000040f03f1100d88afe9e04fe3f120d12080000000000
  00f03f18fe061a00ad54696d657374616d7054797065a66f726967696ea84261636b6c6f6773
  90ac5472616e73616374696f6e73c400b85472616e73616374696f6e436865636b706f696e74
  496473c400ad54726163657256657273696f6ea5302e312e30a44c616e67a6656c69786972a7
  56657273696f6ea5312e322e33ab50726f636573735461677390ab50726f647563744d61736b
  03
  """

  # Encoded with python3's msgpack 1.1.2,
  # `msgpack.packb(fields, use_bin_type=True)`, from the same field map as the
  # fixtures; the sketch blobs are the dd-trace-go captures. The empty payload
  # shows that every top-level key is written even when its value is empty.
  @empty_payload """
  88a3456e76a0a753657276696365a0a5537461747390ad54726163657256657273696f6ea530
  2e312e30a44c616e67a6656c69786972a756657273696f6ea5312e322e33ab50726f63657373
  5461677390ab50726f647563744d61736b03
  """

  # Same provenance as @empty_payload: the integer widths are fixint, uint8,
  # uint16, uint32 and uint64, and the 32-byte values need str8.
  @widths_payload """
  88a3456e76d92065656565656565656565656565656565656565656565656565656565656565
  65a753657276696365a8636865636b6f7574a553746174739286a55374617274cf17979cfe36
  2a0000a84475726174696f6ecf00000002540be400a553746174739587a84564676554616773
  91ac646972656374696f6e3a696ea4486173687faa506172656e744861736800ae5061746877
  61794c6174656e6379c400ab456467654c6174656e6379c400ab5061796c6f616453697a65c4
  00ad54696d657374616d7054797065a763757272656e7487a8456467655461677391ac646972
  656374696f6e3a696ea448617368cc80aa506172656e744861736800ae506174687761794c61
  74656e6379c400ab456467654c6174656e6379c400ab5061796c6f616453697a65c400ad5469
  6d657374616d7054797065a763757272656e7487a8456467655461677391ac64697265637469
  6f6e3a696ea448617368cdffffaa506172656e744861736800ae506174687761794c6174656e
  6379c400ab456467654c6174656e6379c400ab5061796c6f616453697a65c400ad54696d6573
  74616d7054797065a763757272656e7487a8456467655461677391ac646972656374696f6e3a
  696ea448617368ce00010000aa506172656e744861736800ae506174687761794c6174656e63
  79c400ab456467654c6174656e6379c400ab5061796c6f616453697a65c400ad54696d657374
  616d7054797065a763757272656e7487a8456467655461677391ac646972656374696f6e3a69
  6ea448617368cf0000000100000000aa506172656e744861736800ae506174687761794c6174
  656e6379c400ab456467654c6174656e6379c400ab5061796c6f616453697a65c400ad54696d
  657374616d7054797065a763757272656e74a84261636b6c6f677390ac5472616e7361637469
  6f6e73c400b85472616e73616374696f6e436865636b706f696e74496473c40086a553746172
  74cf17979cfbe21e1c00a84475726174696f6ecf00000002540be400a553746174739187a845
  6467655461677391d92a646972656374696f6e3a656565656565656565656565656565656565
  6565656565656565656565656565a448617368cf043a88cbe38e476daa506172656e74486173
  68cf1db347a23e754c3eae506174687761794c6174656e6379c4250a1209000000000040f03f
  1100d88afe9e04fe3f120d1208000000000000f03f18af011a00ab456467654c6174656e6379
  c4250a1209000000000040f03f1100d88afe9e04fe3f120d1208000000000000f03f1889021a
  00ab5061796c6f616453697a65c4250a1209000000000040f03f1100d88afe9e04fe3f120d12
  08000000000000f03f1898071a00ad54696d657374616d7054797065a66f726967696ea84261
  636b6c6f677390ac5472616e73616374696f6e73c400b85472616e73616374696f6e43686563
  6b706f696e74496473c400ad54726163657256657273696f6ea5302e312e30a44c616e67a665
  6c69786972a756657273696f6ea5312e322e33ab50726f636573735461677390ab50726f6475
  63744d61736b03
  """

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_VERSION DD_PRIMARY_TAG DD_DATA_STREAMS_ENABLED)
    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    on_exit(fn ->
      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)
  end

  defp sketch(value), do: Sketch.new() |> Sketch.add(value) |> Sketch.to_proto()

  defp encoded(payload), do: payload |> Payload.encode() |> IO.iodata_to_binary()

  defp hex(string), do: Base.decode16!(String.replace(string, ~r/\s/, ""), case: :mixed)

  defp fixture_payload(tags, hash, parent_hash, start, pathway_latency, edge_latency, size, type) do
    payload = %Payload{
      env: "prod",
      service: "checkout",
      tracer_version: "0.1.0",
      lang: "elixir",
      version: "1.2.3"
    }

    Payload.add_bucket(payload, %{
      start: start,
      duration: 10_000_000_000,
      stats: [
        %{
          edge_tags: tags,
          hash: hash,
          parent_hash: parent_hash,
          pathway_latency: sketch(pathway_latency),
          edge_latency: sketch(edge_latency),
          payload_size: sketch(size),
          timestamp_type: type
        }
      ]
    })
  end

  test "the current bucket matches the hand-pinned msgp body byte for byte" do
    payload =
      fixture_payload(
        @consume_tags,
        @hash,
        @parent_hash,
        1_700_000_000_000_000_000,
        0.25,
        0.125,
        1234,
        "current"
      )

    assert encoded(payload) == hex(@fixture_current)
  end

  test "the origin bucket matches the hand-pinned msgp body byte for byte" do
    payload =
      fixture_payload(
        @produce_tags,
        @parent_hash,
        0,
        1_699_999_990_000_000_000,
        0.5,
        0.5,
        999,
        "origin"
      )

    assert encoded(payload) == hex(@fixture_origin)
  end

  test "an empty payload still carries every top-level key" do
    payload = %Payload{env: "", service: "", tracer_version: "0.1.0", version: "1.2.3"}

    assert encoded(payload) == hex(@empty_payload)
  end

  test "integers use the smallest unsigned form and long strings the next string form" do
    payload = %Payload{
      env: String.duplicate("e", 32),
      service: "checkout",
      tracer_version: "0.1.0",
      version: "1.2.3"
    }

    payload =
      Payload.add_bucket(payload, %{
        start: 1_700_000_000_000_000_000,
        stats:
          for(width <- [127, 128, 65_535, 65_536, 4_294_967_296]) do
            %{
              edge_tags: ["direction:in"],
              hash: width,
              parent_hash: 0,
              timestamp_type: "current"
            }
          end
      })

    payload =
      Payload.add_bucket(payload, %{
        start: 1_699_999_990_000_000_000,
        stats: [
          %{
            edge_tags: ["direction:" <> String.duplicate("e", 32)],
            hash: @hash,
            parent_hash: @parent_hash,
            pathway_latency: sketch(0.25),
            edge_latency: sketch(0.125),
            payload_size: sketch(1234),
            timestamp_type: "origin"
          }
        ]
      })

    assert encoded(payload) == hex(@widths_payload)
  end

  test "add_bucket/2 appends buckets in the order they are added" do
    first = %{start: 1, stats: []}
    second = %{start: 2, stats: []}

    assert %Payload{stats: [^first, ^second]} =
             Payload.new() |> Payload.add_bucket(first) |> Payload.add_bucket(second)
  end

  # The decoder the testing API hands a test reads the same bytes these
  # fixtures pin, so decoding them is the round trip back to the fields the
  # encoder was given.
  describe "decode_payload/1" do
    test "reads the hand-pinned current fixture back field for field" do
      decoded = @fixture_current |> hex() |> :zlib.gzip() |> Testing.decode_payload()

      assert decoded.env == "prod"
      assert decoded.service == "checkout"
      assert decoded.version == "1.2.3"
      assert decoded.lang == "elixir"
      assert decoded.tracer_version == "0.1.0"
      assert decoded.process_tags == []
      assert decoded.product_mask == 3

      assert [bucket] = decoded.stats
      assert bucket.start == 1_700_000_000_000_000_000
      assert bucket.duration == 10_000_000_000
      assert bucket.backlogs == []
      assert bucket.transactions == ""
      assert bucket.transaction_checkpoint_ids == ""

      assert [point] = bucket.stats
      assert point.edge_tags == @consume_tags
      assert point.hash == @hash
      assert point.parent_hash == @parent_hash
      assert point.timestamp_type == "current"
      assert point.pathway_latency == sketch(0.25)
      assert point.edge_latency == sketch(0.125)
      assert point.payload_size == sketch(1234)
    end

    test "reads the hand-pinned origin fixture back" do
      decoded = @fixture_origin |> hex() |> :zlib.gzip() |> Testing.decode_payload()

      assert [bucket] = decoded.stats
      assert bucket.start == 1_699_999_990_000_000_000

      assert [point] = bucket.stats
      assert point.edge_tags == @produce_tags
      assert point.hash == @parent_hash
      assert point.parent_hash == 0
      assert point.timestamp_type == "origin"
      assert point.pathway_latency == sketch(0.5)
    end

    test "every integer width and string form reads back as what it was written from" do
      decoded = @widths_payload |> hex() |> :zlib.gzip() |> Testing.decode_payload()

      assert decoded.env == String.duplicate("e", 32)
      assert decoded.service == "checkout"
      assert decoded.version == "1.2.3"

      assert [current, origin] = decoded.stats

      assert Enum.map(current.stats, & &1.hash) ==
               [127, 128, 65_535, 65_536, 4_294_967_296]

      assert [origin_point] = origin.stats
      assert origin_point.edge_tags == ["direction:" <> String.duplicate("e", 32)]
      assert origin_point.hash == @hash
      assert origin_point.pathway_latency == sketch(0.25)
    end

    test "an empty payload decodes to every top-level key and no points" do
      decoded = @empty_payload |> hex() |> :zlib.gzip() |> Testing.decode_payload()

      assert decoded.env == ""
      assert decoded.service == ""
      assert decoded.stats == []
      assert decoded.process_tags == []
      assert decoded.product_mask == 3
    end

    test "a payload this library did not write says so instead of decoding half" do
      assert_raise ArgumentError, ~r/unsupported MsgPack byte/, fn ->
        Testing.decode_payload(:zlib.gzip(<<0xC0, 0xC0>>))
      end

      assert_raise ArgumentError, ~r/ends early/, fn ->
        Testing.decode_payload(:zlib.gzip(<<0x82>>))
      end
    end
  end

  test "new/0 takes the payload identity from the environment" do
    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_VERSION", "1.2.3")

    assert %Payload{
             service: "checkout",
             env: "prod",
             version: "1.2.3",
             lang: "elixir",
             process_tags: [],
             product_mask: 3,
             stats: []
           } = payload = Payload.new()

    assert payload.tracer_version == Mix.Project.config()[:version]
  end

  describe "config" do
    test "the identity defaults to an unnamed service, no env and no version" do
      assert Config.service() == "unnamed-elixir-service"
      assert Config.env() == ""
      assert Config.version() == ""
      assert Config.primary_tag() == ""
    end

    test "the identity is read from the DD_ variables" do
      System.put_env("DD_SERVICE", "checkout")
      System.put_env("DD_ENV", "prod")
      System.put_env("DD_VERSION", "1.2.3")
      System.put_env("DD_PRIMARY_TAG", "region:eu")

      assert Config.service() == "checkout"
      assert Config.env() == "prod"
      assert Config.version() == "1.2.3"
      assert Config.primary_tag() == "region:eu"
    end

    test "the identity is read from the configured keys, which win over the DD_ variables" do
      on_exit(fn ->
        for key <- [:service, :env, :version, :primary_tag] do
          Application.delete_env(:dd_data_streams, key)
        end
      end)

      System.put_env("DD_SERVICE", "from-env")
      System.put_env("DD_ENV", "from-env")
      System.put_env("DD_VERSION", "from-env")
      System.put_env("DD_PRIMARY_TAG", "from-env")

      for {key, value} <- [
            service: "checkout",
            env: "prod",
            version: "1.2.3",
            primary_tag: "region:eu"
          ] do
        Application.put_env(:dd_data_streams, key, value)
      end

      assert Config.service() == "checkout"
      assert Config.env() == "prod"
      assert Config.version() == "1.2.3"
      assert Config.primary_tag() == "region:eu"
    end

    test "the library is enabled unless DD_DATA_STREAMS_ENABLED turns it off" do
      System.delete_env("DD_DATA_STREAMS_ENABLED")
      assert Config.enabled?()

      for value <- ["", "true", "TRUE", "1", "yes"] do
        System.put_env("DD_DATA_STREAMS_ENABLED", value)
        assert Config.enabled?(), "expected #{inspect(value)} to leave the library enabled"
      end

      for value <- ["false", "FALSE", "0", "no", "off", " false ", "false\n", "OFF\r\n"] do
        System.put_env("DD_DATA_STREAMS_ENABLED", value)
        refute Config.enabled?(), "expected #{inspect(value)} to disable the library"
      end

      # An explicit configuration key decides, whatever the environment says.
      for {enabled, variable} <- [{false, nil}, {false, "true"}, {true, "false"}] do
        Application.put_env(:dd_data_streams, :enabled, enabled)

        if variable,
          do: System.put_env("DD_DATA_STREAMS_ENABLED", variable),
          else: System.delete_env("DD_DATA_STREAMS_ENABLED")

        assert Config.enabled?() == enabled,
               "expected enabled: #{inspect(enabled)} to win over #{inspect(variable)}"
      end
    after
      Application.delete_env(:dd_data_streams, :enabled)
    end

    test "a configured string is read the way the variable is" do
      System.put_env("DD_DATA_STREAMS_ENABLED", "true")

      for value <- ["false", " FALSE ", "0", "no", "off\n", " Off "] do
        Application.put_env(:dd_data_streams, :enabled, value)
        refute Config.enabled?(), "expected #{inspect(value)} to disable the library"
      end

      for value <- ["true", "TRUE", "yes"] do
        Application.put_env(:dd_data_streams, :enabled, value)
        assert Config.enabled?(), "expected #{inspect(value)} to leave the library enabled"
      end
    after
      Application.delete_env(:dd_data_streams, :enabled)
    end

    test "a nil or blank configuration lets the variable decide" do
      for value <- [nil, "", "  "], variable <- ["false", "true"] do
        Application.put_env(:dd_data_streams, :enabled, value)
        System.put_env("DD_DATA_STREAMS_ENABLED", variable)

        assert Config.enabled?() == (variable == "true"),
               "expected #{inspect(value)} to leave #{inspect(variable)} to decide"
      end
    after
      Application.delete_env(:dd_data_streams, :enabled)
    end

    test "an unrecognised value leaves the library on and says so once" do
      Application.put_env(:dd_data_streams, :enabled, ["x"])

      log =
        capture_log(fn ->
          assert Config.enabled?()
          assert Config.enabled?()
        end)

      assert [_] =
               Regex.scan(~r/\["x"\] is neither a boolean nor one of false, 0, no, off/, log)
    after
      Application.delete_env(:dd_data_streams, :enabled)
    end
  end
end
