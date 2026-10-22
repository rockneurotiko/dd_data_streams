defmodule DdDataStreams.Config do
  @moduledoc """
  The environment variables that configure the library.

  Every value is read from the process environment when it is asked for, so an
  application can set the variables before it starts its supervision tree and
  changing one at runtime takes effect on the next call. The names and defaults
  follow dd-trace-go v2.13 and dd-trace-java 1.x.

  `agent_url/1` takes the environment as an argument because it is also asked
  for outside the VM's environment, from tests and from a custom transport.
  """

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
  Whether the library is enabled, from `DD_DATA_STREAMS_ENABLED`. Off by
  default, so adding the dependency changes nothing until it is turned on.

  `DdDataStreams.Application` reads this when the application starts, so
  changing the variable at runtime does not start or stop the aggregator.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    System.get_env("DD_DATA_STREAMS_ENABLED", "false")
    |> String.downcase()
    |> then(&(&1 in ["true", "1"]))
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

  # An unset, empty or blank variable means "not configured".
  defp configured(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      url -> String.trim_trailing(url, "/")
    end
  end

  defp configured(_value), do: nil
end
