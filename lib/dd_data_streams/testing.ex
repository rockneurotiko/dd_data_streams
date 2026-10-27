defmodule DdDataStreams.Testing do
  @moduledoc """
  Turning the library on around a single test, without restarting anything.

  `DdDataStreams.Application` decides once, at boot, whether the aggregator
  runs, so a test that flips `enabled` by itself records into an aggregator
  that is not there. `enable!/1` performs the whole flip for the test: it sets
  the configuration, attaches the Broadway handlers and starts the aggregator
  when it is not already running, and returns the function that puts
  everything back:

      setup do
        on_exit(DdDataStreams.Testing.enable!(transport: DdDataStreams.Transport.Capture))
        :ok
      end

  A test using it runs with `async: false`: the aggregator, the application
  environment and the Broadway handler id are all shared by the whole VM.
  """

  alias DdDataStreams.Aggregator

  @config_keys [:enabled, :transport, :service, :env]
  @option_keys [:transport, :service, :env]

  @doc """
  Enables the library for one test and returns the function that undoes it.

  Sets `enabled: true` and any of the options `:transport`, `:service` and
  `:env:` that are given, attaches the handlers `DdDataStreams.attach/0`
  installs, and starts the aggregator if it is not already running, so
  checkpoints recorded during the test are aggregated instead of dropped. Any
  other option raises an `ArgumentError`, so a typo does not silently configure
  nothing.

  The returned zero-arity function stops the aggregator this call started,
  detaches the handlers this call attached and restores the application
  environment to what it was before the call; anything the call found already
  running or already attached it leaves alone. It is safe to call twice, and
  the aggregator is linked to the calling process, so a test that never runs
  the cleanup still cannot leak one. Pass it to `ExUnit.Callbacks.on_exit/1`:

      on_exit(DdDataStreams.Testing.enable!())
  """
  @spec enable!(keyword()) :: (-> :ok)
  def enable!(opts \\ []) when is_list(opts) do
    validate_options!(opts)

    previous = Map.new(@config_keys, &{&1, Application.fetch_env(:dd_data_streams, &1)})
    attached? = DdDataStreams.status().attached

    Application.put_env(:dd_data_streams, :enabled, true)

    for key <- @option_keys, Keyword.has_key?(opts, key) do
      Application.put_env(:dd_data_streams, key, Keyword.fetch!(opts, key))
    end

    :ok = DdDataStreams.attach()
    started = start_aggregator()

    fn -> restore(previous, started, attached?) end
  end

  # A silently ignored typo would show up much later as a flush going
  # nowhere, so it is rejected here, where the test still points at it.
  defp validate_options!(opts) do
    case Keyword.keys(opts) -- @option_keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "unknown enable! options: #{inspect(unknown)}; " <>
                "expected #{inspect(@option_keys)}"
    end
  end

  # nil when the aggregator was already running, so the cleanup only ever
  # stops what this call started.
  defp start_aggregator do
    case Aggregator.start_link([]) do
      {:ok, pid} -> pid
      {:error, {:already_started, _pid}} -> nil
    end
  end

  defp restore(previous, started, attached?) do
    stop_aggregator(started)
    unless attached?, do: DdDataStreams.detach()

    for {key, value} <- previous do
      case value do
        {:ok, configured} -> Application.put_env(:dd_data_streams, key, configured)
        :error -> Application.delete_env(:dd_data_streams, key)
      end
    end

    :ok
  end

  # The aggregator may already be on its way out - it is linked to the test
  # process, which an on_exit callback outlives - so a stop that races its
  # exit is not an error.
  defp stop_aggregator(nil), do: :ok

  defp stop_aggregator(pid) do
    if Process.whereis(Aggregator) == pid do
      try do
        GenServer.stop(pid)
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end
end
