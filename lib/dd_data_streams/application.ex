defmodule DdDataStreams.Application do
  @moduledoc """
  Runs the aggregator for the lifetime of the application.

  The aggregator is started under a supervisor, so it is restarted if it dies;
  an enabled pipeline that only calls `DdDataStreams.attach/0` is therefore
  recorded without the host adding the aggregator to its own supervision tree.

  The transport comes from `config :dd_data_streams, transport:`, so a host can
  reuse its own HTTP pool instead of the default Req implementation:

      config :dd_data_streams, transport: MyApp.AgentTransport

  The aggregator reads it on every flush rather than at start, so changing the
  key while it runs applies to the next payload. Without that key the Req
  implementation is used when Req is available. When neither resolves, the start
  logs an error naming the key, because every flush would otherwise be dropped
  in silence.

  Whether it runs at all is decided once, when the application starts, from
  `DdDataStreams.Config.enabled?/0`, which is on unless it is turned off.
  Changing the flag while the application is running neither starts nor stops
  the aggregator: restart the application to apply it, or in a test call
  `DdDataStreams.Testing.enable!/1`, which starts the aggregator for the
  duration of the test and stops it again. An idle aggregator sends
  nothing, so a pipeline that has not attached costs one process and no
  requests.

  An aggregator that another application or a test suite already supervises
  under the registered name is left as it is instead of being started twice; it
  reads the same configuration, so it posts where this one would have.
  """

  use Application

  require Logger

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Config

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one)
  end

  defp children do
    if Config.enabled?() and is_nil(Process.whereis(Aggregator)) do
      warn_if_no_transport()
      log_effective_configuration()

      [
        %{
          id: Aggregator,
          start: {Aggregator, :start_link, [[]]},
          # Above the transport's own timeout, so a flush that is still talking
          # to the agent is not cut short by the hidden five-second default.
          shutdown: 15_000
        }
      ]
    else
      []
    end
  end

  defp warn_if_no_transport do
    if is_nil(Config.transport()) do
      Logger.error(
        "no agent transport is available: set `config :dd_data_streams, " <>
          "transport: MyApp.Transport` or add the optional :req dependency; " <>
          "every flush would be dropped"
      )
    end
  end

  # One line naming what this aggregator is started with, so the log alone says
  # what is being recorded without inspecting the running system.
  defp log_effective_configuration do
    status = DdDataStreams.status()

    Logger.info(
      "data streams monitoring starts: transport=#{inspect(status.transport)} " <>
        "agent_url=#{status.agent_url} header=#{inspect(status.header)} " <>
        "service=#{inspect(status.service)} env=#{inspect(status.env)} " <>
        "extractors=#{inspect(status.extractors)}"
    )
  end
end
