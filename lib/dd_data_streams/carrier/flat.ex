defmodule DdDataStreams.Carrier.Flat do
  @moduledoc """
  The flat carrier: a Pub/Sub attribute map or a Kafka/RabbitMQ header list.

  Reading and writing go through `DdDataStreams.Propagator`; the size is the sum
  of the keys and values of every entry, as the consume side has always measured.
  """

  @behaviour DdDataStreams.Carrier

  alias DdDataStreams.Propagator

  @impl true
  def get(carrier, key), do: Propagator.get(carrier, key)

  def put(carrier, pathway), do: Propagator.put(carrier, pathway)

  @impl true
  def put(carrier, pathway, key), do: Propagator.put(carrier, pathway, key)

  @impl true
  def size(carrier) when is_map(carrier) or is_list(carrier) do
    Enum.reduce(carrier, 0, fn entry, size -> size + entry_size(entry) end)
  end

  def size(_carrier), do: 0

  defp entry_size(entry) when is_tuple(entry) and tuple_size(entry) >= 2 do
    entry |> Tuple.to_list() |> Enum.reduce(0, fn element, size -> size + bytes(element) end)
  end

  defp entry_size(_entry), do: 0

  defp bytes(value) when is_binary(value), do: byte_size(value)
  defp bytes(_value), do: 0
end
