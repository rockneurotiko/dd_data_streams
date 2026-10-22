defmodule DdDataStreams.MixProject do
  use Mix.Project

  def project do
    [
      app: :dd_data_streams,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {DdDataStreams.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Every checkpoint is a telemetry event, and attach/1 installs the
      # Broadway handlers on them, so telemetry is always needed.
      {:telemetry, "~> 1.0"},
      # The default agent transport talks HTTP through Req. It is optional so an
      # application that passes its own transport does not pull in an HTTP client.
      {:req, "~> 0.5", optional: true}
    ]
  end
end
