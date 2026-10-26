defmodule DdDataStreams.CarrierTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.{Carrier, Carrier.Flat, Carrier.Sqs, Pathway, Propagator}

  @pathway %Pathway{hash: 234, pathway_start: 1_700_000_000_000, edge_start: 1_700_003_600_000}
  @key "dd-pathway-ctx-base64"

  test "adapter selects flat for plain maps and lists" do
    assert Carrier.adapter(%{"a" => "b"}) == Flat
    assert Carrier.adapter([{"a", "b"}]) == Flat
    assert Carrier.adapter(nil) == Flat
  end

  test "adapter selects sqs for _datadog carriers" do
    assert Carrier.adapter(%{"_datadog" => %{"string_value" => "{}"}}) == Sqs
    assert Carrier.adapter(%{"_datadog" => "{}"}) == Sqs
  end

  test "flat get/put/size round-trip" do
    carrier = %{@key => Propagator.encode_str(@pathway)}
    assert Carrier.get(carrier, @key) == @pathway
    assert Carrier.get(carrier) == @pathway

    updated = Carrier.put(carrier, @pathway, @key)
    assert Carrier.get(updated, @key) == @pathway

    updated2 = Carrier.put(%{}, @pathway)
    assert Carrier.get(updated2, @key) == @pathway

    assert Carrier.size(%{@key => "abc"}) == byte_size(@key) + 3
    assert Carrier.size([{@key, "abc"}]) == byte_size(@key) + 3
    assert Carrier.size(nil) == 0
  end

  test "flat put via adapter module" do
    assert Flat.put(%{}, @pathway, @key) |> Flat.get(@key) == @pathway
    assert Flat.put(%{}, @pathway) |> Flat.get(@key) == @pathway
  end

  test "fetch/2 tells a malformed header apart from an absent one in both adapters" do
    assert Carrier.fetch(%{@key => Propagator.encode_str(@pathway)}, @key) == {:ok, @pathway}
    assert Carrier.fetch(%{@key => "AAAA"}, @key) == {:error, :malformed}
    assert Carrier.fetch(%{}, @key) == :absent

    sqs = %{"_datadog" => Jason.encode!(%{@key => Propagator.encode_str(@pathway)})}
    assert Carrier.fetch(sqs, @key) == {:ok, @pathway}
    assert Carrier.fetch(%{"_datadog" => "{}"}, @key) == :absent

    assert Carrier.fetch(%{"_datadog" => Jason.encode!(%{@key => 42})}, @key) ==
             {:error, :malformed}
  end

  test "sqs get/put/size round-trip with string_value" do
    carrier = %{
      "_datadog" => %{
        name: "_datadog",
        data_type: "String",
        string_value: Jason.encode!(%{@key => Propagator.encode_str(@pathway)}),
        binary_value: "",
        value: Jason.encode!(%{@key => Propagator.encode_str(@pathway)})
      }
    }

    assert Carrier.adapter(carrier) == Sqs
    assert Carrier.get(carrier, @key) == @pathway
    assert Carrier.get(carrier) == @pathway

    updated = Carrier.put(carrier, @pathway, @key)
    assert Sqs.get(updated, @key) == @pathway
    assert Carrier.size(updated) > 0

    empty = %{"_datadog" => %{"string_value" => "{}"}}
    assert Sqs.get(empty, @key) == nil
    assert Sqs.size(empty) == 2

    assert Sqs.put(%{}, @pathway, @key) |> Sqs.get(@key) == @pathway
  end

  test "sqs handles binary _datadog and hash-zero pathway" do
    json = Jason.encode!(%{@key => Propagator.encode_str(@pathway)})
    assert Sqs.get(%{"_datadog" => json}, @key) == @pathway
    assert Sqs.size(%{"_datadog" => json}) == byte_size(json)

    carrier = %{"_datadog" => %{"string_value" => "{}"}}
    assert Sqs.put(carrier, %Pathway{hash: 0}, @key) == carrier
    assert Sqs.put("not-a-map", @pathway, @key) == "not-a-map"
    assert Sqs.size("not-a-map") == 0
  end

  test "carrier get/put with explicit adapter" do
    assert Carrier.get(Flat, %{@key => Propagator.encode_str(@pathway)}, @key) == @pathway
    assert Carrier.put(Flat, %{}, @pathway, @key) |> Flat.get(@key) == @pathway
  end

  test "sqs update_entry handles entries without string keys" do
    carrier = %{"_datadog" => %{"DataType" => "String", "StringValue" => "{}"}}
    updated = Sqs.put(carrier, @pathway, @key)
    assert Sqs.get(updated, @key) == @pathway
  end
end
