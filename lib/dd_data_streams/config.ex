defmodule DdDataStreams.Config do
  @moduledoc """
  The environment variables that configure the library.

  `service/0`, `env/0`, `version/0`, `primary_tag/0` and `enabled?/0` read the
  process environment when they are asked for, so a variable set before the
  supervision tree starts applies from the first call. The names and defaults
  follow dd-trace-go v2.13 and dd-trace-java 1.x, with one deliberate exception:
  those libraries enable data streams monitoring only when told to, because they
  do many other things already, while this one does nothing else, so it is on
  once you depend on it and `DD_DATA_STREAMS_ENABLED` is a kill switch.

  Two decisions are taken once, when the application starts.
  `DdDataStreams.Application` starts the aggregator from
  `enabled?/0`, and takes the transport from
  `config :dd_data_streams, transport:` or, when that key is absent, from the
  Req transport when Req is installed. Req is an optional dependency, so an
  application that does not depend on it has no transport at all: the aggregator
  starts with none, the start logs an error naming the missing configuration,
  and every flush is dropped.

  The enable flag means something different in each place, and the difference
  matters. `DdDataStreams.attach/0` and `DdDataStreams.publish/3` ask
  `enabled?/0` on every call, so they follow the flag when it changes at
  runtime, while the aggregator keeps the answer it started with. Turning the
  flag on after boot installs handlers and injects headers with no aggregator to
  take the checkpoints, and `DdDataStreams.Aggregator.add/1` drops every one of
  them in silence. Turning it off after boot stops header injection and further
  handler installation, but handlers installed earlier stay attached and keep
  recording into the aggregator that is still running.

  The two values belong in different places. The enable flag is read from
  `config :dd_data_streams, enabled:` when that key holds a boolean, and from
  `DD_DATA_STREAMS_ENABLED` otherwise; `attach/0` and `publish/3` follow it
  immediately, so changing it at runtime changes injection and handler
  installation, but not the aggregator, which keeps the decision it started with
  and needs an application restart to change. The transport is a configuration
  value, read only when the aggregator starts, so a change to it has no effect
  until the application restarts.

  The extractors are the third decision, and the only one that is not taken
  once: `DdDataStreams` resolves `config :dd_data_streams, extractors:` on every
  call, so naming a different list applies without a restart.

  `agent_url/1` takes the environment as an argument because it is also asked
  for outside the VM's environment, from tests and from a custom transport.
  """

  @enabled_key :enabled
  @default_service "unnamed-elixir-service"
  @default_agent_url "http://localhost:8126"
  @default_agent_port "8126"

  @doc """
  The service the pipeline reports itself as, from `DD_SERVICE`.
  """
  @spec service() :: String.t()
  def service, do: env("DD_SERVICE", @default_service)

  @doc """
  The deployment environment, from `DD_ENV`; empty when unset.
  """
  @spec env() :: String.t()
  def env, do: env("DD_ENV", "")

  @doc """
  The application version, from `DD_VERSION`; empty when unset.
  """
  @spec version() :: String.t()
  def version, do: env("DD_VERSION", "")

  @doc """
  The primary tag, from `DD_PRIMARY_TAG`; empty when unset.
  """
  @spec primary_tag() :: String.t()
  def primary_tag, do: env("DD_PRIMARY_TAG", "")

  @doc """
  Whether the library is enabled. On by default: depending on the library and
  calling `DdDataStreams.attach/0` is the opt-in.

  `config :dd_data_streams, enabled: true | false` decides when it holds a
  boolean. Otherwise `DD_DATA_STREAMS_ENABLED` is read, and it is a kill switch:
  only a value that reads as false - `false`, `0`, `no`, `off`, case-insensitive
  - turns the library off. An unset, empty or unrecognised value leaves it on.

  `DdDataStreams.Application` reads this when the application starts, so
  changing it at runtime does not start or stop the aggregator.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    case Application.get_env(:dd_data_streams, @enabled_key) do
      enabled when is_boolean(enabled) -> enabled
      _configured_otherwise -> env_enabled?()
    end
  end

  @doc """
  The base URL of the Datadog Agent, read from the given environment map.

  `DD_TRACE_AGENT_URL` wins when it is set and non-empty; otherwise the URL is
  built from `DD_AGENT_HOST` and `DD_TRACE_AGENT_PORT`, defaulting to
  `#{@default_agent_url}`. Unix domain sockets are not supported.

      iex> DdDataStreams.Config.agent_url(%{})
      "http://localhost:8126"

      iex> DdDataStreams.Config.agent_url(%{"DD_AGENT_HOST" => "agent", "DD_TRACE_AGENT_PORT" => "8127"})
      "http://agent:8127"

      iex> DdDataStreams.Config.agent_url(%{"DD_TRACE_AGENT_URL" => "http://agent:8126"})
      "http://agent:8126"
  """
  @spec agent_url(map()) :: String.t()
  def agent_url(environment) when is_map(environment) do
    case configured(environment["DD_TRACE_AGENT_URL"]) do
      nil ->
        host = configured(environment["DD_AGENT_HOST"]) || "localhost"
        port = configured(environment["DD_TRACE_AGENT_PORT"]) || @default_agent_port
        "http://#{host}:#{port}"

      url ->
        url
    end
  end

  defp env(name, default), do: System.get_env(name, default)

  defp env_enabled? do
    case System.get_env("DD_DATA_STREAMS_ENABLED") do
      nil -> true
      value -> String.downcase(value) not in ~w(false 0 no off)
    end
  end

  # An unset, empty or blank variable means "not configured".
  defp configured(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      url -> String.trim_trailing(url, "/")
    end
  end

  defp configured(_value), do: nil
end
