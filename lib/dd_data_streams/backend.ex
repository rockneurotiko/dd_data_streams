defmodule DdDataStreams.Backend do
  @moduledoc """
  The seam where checkpoints are recorded.

  Production never resolves a test owner: it only asks the configured backend
  whether the calling process is recording and, if it is, hands the checkpoint
  over. The default backend, `DdDataStreams.Backend.Aggregator`, reads
  `DdDataStreams.Config.enabled?/0` and calls
  `DdDataStreams.Aggregator.add/1`, so an application that configures nothing
  behaves exactly as it did before this behaviour existed.

  A test suite can replace it with `DdDataStreams.Testing.Backend`:

      config :dd_data_streams, backend: DdDataStreams.Testing.Backend

  That backend answers per process, from `NimbleOwnership`, instead of from the
  application environment, so tests that opt in can run concurrently without
  seeing each other's checkpoints. An application that configures no backend
  never references the testing modules.
  """

  alias DdDataStreams.Pathway

  @default DdDataStreams.Backend.Aggregator

  @doc """
  Whether the calling process is recording checkpoints.
  """
  @callback enabled?() :: boolean()

  @doc """
  Records one checkpoint.

  Called only when `enabled?/0` answered `true` for the same process.
  """
  @callback record(Pathway.checkpoint()) :: :ok

  @doc """
  The configured backend, or the default one when `:backend` is not set.
  """
  @spec impl() :: module()
  def impl, do: Application.get_env(:dd_data_streams, :backend) || @default

  @doc """
  Asks the configured backend whether the calling process is recording.
  """
  @spec enabled?() :: boolean()
  def enabled?, do: impl().enabled?()

  @doc """
  Hands one checkpoint to the configured backend.
  """
  @spec record(Pathway.checkpoint()) :: :ok
  def record(checkpoint), do: impl().record(checkpoint)
end
