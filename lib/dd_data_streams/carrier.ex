defmodule DdDataStreams.Carrier do
  @moduledoc """
  Broker-specific carriers for the pathway header.

  A carrier is the part of an inbound or outbound message that holds the
  pathway header: a flat attribute map or header list for Pub/Sub, Kafka and
  RabbitMQ, or the `_datadog` JSON message attribute for SQS. `Flat` and
  `Sqs` implement the behaviour; `adapter/1` selects one from the carrier's
  shape, so `DdDataStreams.Checkpoints` stays free of broker conditionals.
  """

  alias DdDataStreams.Config
  alias DdDataStreams.Pathway

  @callback get(carrier :: term(), key :: String.t()) :: Pathway.t() | nil
  @callback put(carrier :: term(), pathway :: Pathway.t(), key :: String.t()) :: term()
  @callback size(carrier :: term()) :: non_neg_integer()

  @doc """
  Selects the adapter that understands `carrier`'s shape.
  """
  @spec adapter(term()) :: module()
  def adapter(carrier), do: do_adapter(carrier)

  @doc false
  @spec select(term()) :: module()
  def select(carrier), do: do_adapter(carrier)

  @doc false
  @spec for(term()) :: module()
  def for(carrier), do: do_adapter(carrier)

  defp do_adapter(carrier) when is_map(carrier) do
    if sqs?(carrier), do: DdDataStreams.Carrier.Sqs, else: DdDataStreams.Carrier.Flat
  end

  defp do_adapter(_carrier), do: DdDataStreams.Carrier.Flat

  defp sqs?(carrier) do
    case datadog_value(carrier) do
      nil ->
        Enum.any?(carrier, fn {_key, value} ->
          is_map(value) and
            (Map.has_key?(value, :string_value) or Map.has_key?(value, "string_value") or
               Map.has_key?(value, :StringValue) or Map.has_key?(value, "StringValue") or
               Map.has_key?(value, :value) or Map.has_key?(value, "value"))
        end)

      _ ->
        true
    end
  end

  defp datadog_value(carrier) when is_map(carrier) do
    Map.get(carrier, "_datadog") || Map.get(carrier, :_datadog)
  end

  defp datadog_value(_carrier), do: nil

  @doc """
  Reads the pathway out of `carrier` under `key`, using the adapter selected by
  its shape.
  """
  @spec get(term(), String.t()) :: Pathway.t() | nil
  def get(carrier, key) when is_binary(key) do
    {adapter, c} = unwrap(carrier)
    adapter.get(c, key)
  end

  @doc false
  @spec get(module(), term(), String.t()) :: Pathway.t() | nil
  def get(adapter, carrier, key) when is_atom(adapter) and is_binary(key) do
    adapter.get(carrier, key)
  end

  @doc false
  @spec get(term()) :: Pathway.t() | nil
  def get(carrier), do: get(carrier, Config.header())

  @doc """
  Writes `pathway` into `carrier` under `key`, using the adapter selected by
  its shape.
  """
  @spec put(term(), Pathway.t(), String.t()) :: term()
  def put(carrier, pathway, key) when is_binary(key) do
    {adapter, c} = unwrap(carrier)
    adapter.put(c, pathway, key)
  end

  @spec put(term(), Pathway.t()) :: term()
  def put(carrier, pathway) do
    {adapter, c} = unwrap(carrier)
    adapter.put(c, pathway, Config.header())
  end

  @doc false
  @spec put(module(), term(), Pathway.t(), String.t()) :: term()
  def put(adapter, carrier, pathway, key) when is_atom(adapter) and is_binary(key) do
    adapter.put(carrier, pathway, key)
  end

  @doc """
  Measures the size in bytes of the carrier entries.
  """
  @spec size(term()) :: non_neg_integer()
  def size(carrier) do
    {adapter, c} = unwrap(carrier)
    adapter.size(c)
  end

  defp unwrap({adapter, carrier}) when is_atom(adapter), do: {adapter, carrier}
  defp unwrap(carrier), do: {adapter(carrier), carrier}
end
