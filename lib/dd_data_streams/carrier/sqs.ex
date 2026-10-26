defmodule DdDataStreams.Carrier.Sqs do
  @moduledoc """
  The SQS carrier: a BroadwaySQS `message_attributes` map whose `_datadog`
  attribute carries a JSON object mapping header keys to their values.

  Reading unwraps that JSON; writing merges the pathway header into it; the size
  is the byte size of the `_datadog` JSON string.
  """

  @behaviour DdDataStreams.Carrier

  alias DdDataStreams.{Config, Pathway, Propagator}

  @datadog "_datadog"

  @impl true
  def get(carrier, key) do
    case fetch(carrier, key) do
      {:ok, pathway} -> pathway
      _absent_or_malformed -> nil
    end
  end

  @impl true
  def fetch(carrier, key) do
    case datadog_json(carrier) do
      json when is_map(json) -> fetch_value(json, key)
      _ -> :absent
    end
  end

  defp fetch_value(json, key) do
    case Map.fetch(json, key) do
      {:ok, value} when is_binary(value) ->
        case Propagator.decode_str(value) do
          {:ok, pathway} -> {:ok, pathway}
          :error -> {:error, :malformed}
        end

      {:ok, _other} ->
        {:error, :malformed}

      :error ->
        :absent
    end
  end

  def put(carrier, pathway), do: put(carrier, pathway, Config.header())

  @impl true
  def put(carrier, %Pathway{hash: 0}, _key), do: carrier

  @impl true
  def put(carrier, pathway, key) when is_map(carrier) do
    existing = datadog_json(carrier) || %{}
    updated = Map.put(existing, key, Propagator.encode_str(pathway))
    encoded = IO.iodata_to_binary(:json.encode(updated))

    put_datadog(carrier, encoded)
  end

  def put(carrier, _pathway, _key), do: carrier

  @impl true
  def size(carrier) when is_map(carrier) do
    case datadog_entry(carrier) do
      value when is_binary(value) ->
        byte_size(value)

      value when is_map(value) ->
        case string_value(value) do
          string when is_binary(string) -> byte_size(string)
          _ -> 0
        end

      _ ->
        0
    end
  end

  def size(_carrier), do: 0

  defp datadog_json(carrier) do
    case datadog_entry(carrier) do
      value when is_binary(value) ->
        decode_json(value)

      value when is_map(value) ->
        case string_value(value) do
          string when is_binary(string) -> decode_json(string)
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp datadog_entry(carrier) when is_map(carrier) do
    Map.get(carrier, @datadog) || Map.get(carrier, :_datadog)
  end

  defp datadog_entry(_carrier), do: nil

  defp string_value(entry) when is_map(entry) do
    Enum.find_value(
      [
        :string_value,
        "string_value",
        :value,
        "value",
        :StringValue,
        "StringValue",
        :Value,
        "Value"
      ],
      fn key ->
        case Map.get(entry, key) do
          value when is_binary(value) -> value
          _ -> nil
        end
      end
    )
  end

  defp decode_json(value) when is_binary(value) do
    case :json.decode(value) do
      map when is_map(map) -> map
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp put_datadog(carrier, encoded) do
    outer_key = if Map.has_key?(carrier, :_datadog), do: :_datadog, else: @datadog
    existing = datadog_entry(carrier)

    new_entry =
      case existing do
        value when is_map(value) ->
          update_entry(value, encoded)

        _ ->
          %{
            name: @datadog,
            data_type: "String",
            string_value: encoded,
            binary_value: "",
            value: encoded
          }
      end

    Map.put(carrier, outer_key, new_entry)
  end

  defp update_entry(entry, encoded) do
    keys =
      Enum.filter(
        [
          :string_value,
          "string_value",
          :value,
          "value",
          :StringValue,
          "StringValue",
          :Value,
          "Value"
        ],
        &Map.has_key?(entry, &1)
      )

    if keys == [] do
      Map.merge(entry, %{string_value: encoded, value: encoded})
    else
      Enum.reduce(keys, entry, fn key, acc -> Map.put(acc, key, encoded) end)
    end
  end
end
