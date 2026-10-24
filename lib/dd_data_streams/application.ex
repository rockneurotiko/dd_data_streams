defmodule DdDataStreams.Application do
  @moduledoc """
  Runs the aggregator for the lifetime of the application.

  The aggregator is started under a supervisor, so it is restarted if it dies;
  an enabled pipeline that only calls `DdDataStreams.attach/0` is therefore
  recorded without the host adding the aggregator to its own supervision tree.

  The transport is read from the application environment when the aggregator is
  started, so a host can reuse its own HTTP pool instead of the default Req
  implementation:

      config :dd_data_streams, transport: MyApp.AgentTransport

  Without that key the Req implementation is used when Req is available. When
  neither resolves, the start logs an error naming the key, because every flush
  would otherwise be dropped in silence.

  Whether it runs at all is decided once, when the application starts, from
  `DdDataStreams.Config.enabled?/0`, which is on unless it is turned off.
  Changing the flag while the application is running neither starts nor stops
  the aggregator: restart the application to apply it. An idle aggregator sends
  nothing, so a pipeline that has not attached costs one process and no
  requests.

  An aggregator that another application or a test suite already supervises
  under the registered name is left as it is instead of being started twice,
  which also means it keeps the transport it was started with: the configured
  transport applies only when this supervisor starts the aggregator.
  """

  use Application

  require Logger

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Config

  @transport_key :transport
  @default_transport DdDataStreams.Transport.Req

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one)
  end

  defp children do
    if Config.enabled?() and is_nil(Process.whereis(Aggregator)) do
      [{Aggregator, transport: transport()}]
    else
      []
    end
  end

  defp transport do
    case Application.get_env(:dd_data_streams, @transport_key) || default_transport() do
      nil ->
        Logger.error(
          "no agent transport is available: set `config :dd_data_streams, " <>
            "#{@transport_key}: MyApp.Transport` or add the optional :req dependency; " <>
            "every flush would be dropped"
        )

        nil

      transport ->
        transport
    end
  end

  defp default_transport do
    if Code.ensure_loaded?(@default_transport), do: @default_transport
  end
end
