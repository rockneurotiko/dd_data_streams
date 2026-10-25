defmodule DdDataStreams.Config do
  @moduledoc """
  The settings that configure the library.

  Every setting is read when it is asked for, never at compile time: from
  `config :dd_data_streams, ...` when that key is set, and from the process
  environment otherwise. Application configuration wins over the environment for
  every key, as it does for `enabled/0`, and an unset, empty or blank value
  counts as unset, so one key can be set in either place. A value changed at
  runtime - `Application.put_env/3` or `System.put_env/2` - is picked up by the
  next checkpoint or flush that reads it.

  The names and defaults follow dd-trace-go v2.13 and dd-trace-java 1.x, with
  one deliberate exception: those libraries enable data streams monitoring only
  when told to, because they do many other things already, while this one does
  nothing else, so it is on once you depend on it and `DD_DATA_STREAMS_ENABLED`
  is a kill switch.

  That the value is read late does not mean every use of it changes at runtime;
  some are decided once per process:

    * `service/0`, `env/0`, `version/0`, `primary_tag/0`, `header/0` and
      `enabled?/0` are read per call, so `DdDataStreams.attach/0`,
      `DdDataStreams.produce/3` and `DdDataStreams.checkpoint/2` follow them.
      The aggregator is started by
      `DdDataStreams.Application` from `enabled?/0` when the application starts
      and keeps that answer: turning the flag on afterwards installs handlers
      that record checkpoints into an aggregator that is not running, and
      `DdDataStreams.Aggregator.add/1` drops every one in silence; turning it
      off afterwards stops header injection and further handler installation,
      but handlers installed earlier stay attached and keep recording into the
      aggregator that is still running. Restart the application to change
      whether the aggregator runs.
    * `agent_url/1` and `transport/0` are read on every flush, so changing
      either at runtime moves the next payload - including for an aggregator
      that another application supervises.
    * an application that does not depend on the optional Req brings no default
      transport: the aggregator starts with none, the start logs an error naming
      the missing configuration, and every flush is dropped.

  `agent_url/1` takes the environment as an argument because it is also asked
  for outside the VM's environment, from tests and from a custom transport; the
  application configuration is still read from the VM.
  """

  require Logger

  @enabled_key :enabled
  @transport_key :transport
  @default_service "unnamed-elixir-service"
  @default_agent_url "http://localhost:8126"
  @default_agent_port "8126"
  @header_key :header
  @default_header "dd-pathway-ctx-base64"
  @default_transport DdDataStreams.Transport.Req
  @false_words ~w(false 0 no off)
  @warned_key {__MODULE__, :unrecognised_enabled}

  @doc """
  The service the pipeline reports itself as: `config :dd_data_streams, service:`
  when set, `DD_SERVICE` otherwise.

  Defaults to `"#{@default_service}"`.
  """
  @spec service() :: String.t()
  def service, do: setting(:service, System.get_env("DD_SERVICE")) || @default_service

  @doc """
  The deployment environment: `config :dd_data_streams, env:` when set, `DD_ENV`
  otherwise. Empty when neither is.
  """
  @spec env() :: String.t()
  def env, do: setting(:env, System.get_env("DD_ENV")) || ""

  @doc """
  The application version: `config :dd_data_streams, version:` when set,
  `DD_VERSION` otherwise. Empty when neither is.
  """
  @spec version() :: String.t()
  def version, do: setting(:version, System.get_env("DD_VERSION")) || ""

  @doc """
  The primary tag: `config :dd_data_streams, primary_tag:` when set,
  `DD_PRIMARY_TAG` otherwise. Empty when neither is.
  """
  @spec primary_tag() :: String.t()
  def primary_tag, do: setting(:primary_tag, System.get_env("DD_PRIMARY_TAG")) || ""

  @doc """
  The attribute or header key the pathway is written under: `config
  :dd_data_streams, header:` when set, `DD_DATA_STREAMS_HEADER` otherwise.

  Defaults to `"#{@default_header}"`. `DdDataStreams.produce/3` returns this key
  and the consume checkpoint reads it, so both sides of an edge must use the same
  value. Changing it also stops interoperability with the official Datadog
  tracers, which use the default.

  Read per produce and consume call, so a runtime change applies to the next one.
  """
  @spec header() :: String.t()
  def header,
    do: setting(@header_key, System.get_env("DD_DATA_STREAMS_HEADER")) || @default_header

  @doc """
  Whether the library is enabled. On by default: depending on the library and
  calling `DdDataStreams.attach/0` is the opt-in.

  `config :dd_data_streams, enabled: true | false` decides when it holds a
  boolean. A configured string is read the way `DD_DATA_STREAMS_ENABLED` is,
  trimmed and case-insensitive, so
  `enabled: System.get_env("DD_DATA_STREAMS_ENABLED")` needs no conversion at
  the call site. Otherwise `DD_DATA_STREAMS_ENABLED` is read, and it is a kill
  switch: only a value that reads as false - `false`, `0`, `no`, `off` - turns
  the library off. An unset, empty or unrecognised value leaves it on; an
  unrecognised configuration is also reported once per process, since the flag
  is read per produce and a mistake there is otherwise invisible.

  `DdDataStreams.Application` reads this when the application starts, so
  changing it at runtime does not start or stop the aggregator.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    case Application.get_env(:dd_data_streams, @enabled_key) do
      enabled when is_boolean(enabled) -> enabled
      configured -> configured_enabled?(configured)
    end
  end

  # An unset, empty or blank configuration falls through to the environment, as
  # `enabled: System.get_env(...)` does when the variable is not set. Anything
  # else that is neither a boolean nor a recognised string keeps the library on
  # and says so once per process: enabled?/0 is read per produce, so reporting
  # per call would bury the log.
  defp configured_enabled?(value) when is_binary(value) do
    case String.trim(value) do
      "" -> env_enabled?()
      _trimmed -> string_enabled?(value)
    end
  end

  defp configured_enabled?(nil), do: env_enabled?()

  defp configured_enabled?(value) do
    unless Process.get(@warned_key) == value do
      Process.put(@warned_key, value)
      Logger.warning(unrecognised_enabled_message(value))
    end

    true
  end

  defp unrecognised_enabled_message(value) do
    "data streams monitoring stays enabled: configured :enabled " <>
      "#{inspect(value)} is neither a boolean nor one of " <>
      Enum.join(@false_words, ", ")
  end

  @doc """
  The base URL of the Datadog Agent, read on every flush: `config
  :dd_data_streams, agent_url:` when set, then `DD_TRACE_AGENT_URL`; otherwise
  the URL is built from `agent_host:`/`DD_AGENT_HOST` and
  `agent_port:`/`DD_TRACE_AGENT_PORT`, defaulting to `#{@default_agent_url}`.
  Unix domain sockets are not supported.

  The environment is taken from the VM. `agent_url/1` is the same resolution
  over an environment passed in.

      iex> DdDataStreams.Config.agent_url(%{})
      "http://localhost:8126"

      iex> DdDataStreams.Config.agent_url(%{"DD_AGENT_HOST" => "agent", "DD_TRACE_AGENT_PORT" => "8127"})
      "http://agent:8127"

      iex> DdDataStreams.Config.agent_url(%{"DD_TRACE_AGENT_URL" => "http://agent:8126"})
      "http://agent:8126"
  """
  @spec agent_url() :: String.t()
  def agent_url, do: agent_url(System.get_env())

  @doc """
  The same as `agent_url/0`, resolved over the given environment and still
  letting the application configuration win.
  """
  @spec agent_url(map()) :: String.t()
  def agent_url(environment) when is_map(environment) do
    case setting(:agent_url, environment["DD_TRACE_AGENT_URL"]) do
      nil ->
        host = setting(:agent_host, environment["DD_AGENT_HOST"]) || "localhost"
        port = setting(:agent_port, environment["DD_TRACE_AGENT_PORT"]) || @default_agent_port
        "http://#{host}:#{port}"

      url ->
        String.trim_trailing(url, "/")
    end
  end

  @doc """
  The module the payloads are posted with, read on every flush: `config
  :dd_data_streams, transport:`, otherwise `DdDataStreams.Transport.Req` when
  the optional Req dependency is available, otherwise `nil` and every flush is
  dropped.
  """
  @spec transport() :: module() | nil
  def transport do
    Application.get_env(:dd_data_streams, @transport_key) || default_transport()
  end

  defp default_transport do
    if Code.ensure_loaded?(@default_transport), do: @default_transport
  end

  # Application configuration wins over the environment, as it does for
  # `enabled/0`; an unset, empty or blank value counts as not configured.
  defp setting(key, env_value) do
    configured(Application.get_env(:dd_data_streams, key)) || configured(env_value)
  end

  defp env_enabled? do
    case System.get_env("DD_DATA_STREAMS_ENABLED") do
      nil -> true
      value -> string_enabled?(value)
    end
  end

  defp string_enabled?(value), do: String.downcase(String.trim(value)) not in @false_words

  # An unset, empty or blank variable means "not configured".
  defp configured(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  # A port is naturally written as an integer in configuration.
  defp configured(value) when is_integer(value), do: Integer.to_string(value)

  defp configured(_value), do: nil
end
