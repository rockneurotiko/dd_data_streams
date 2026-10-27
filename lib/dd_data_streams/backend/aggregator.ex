defmodule DdDataStreams.Backend.Aggregator do
  @moduledoc """
  The default backend: the library's enabled flag and its shared aggregator.

  This is the production behaviour. It reads the same configuration and hands
  checkpoints to the same process as the library always has, so leaving
  `:backend` unset changes nothing.
  """

  @behaviour DdDataStreams.Backend

  alias DdDataStreams.{Aggregator, Config}

  @impl true
  def enabled?, do: Config.enabled?()

  @impl true
  def record(checkpoint), do: Aggregator.add(checkpoint)
end
