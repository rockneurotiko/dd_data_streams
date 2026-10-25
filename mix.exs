defmodule DdDataStreams.MixProject do
  use Mix.Project

  @source_url "https://github.com/rockneurotiko/dd_data_streams"

  def project do
    [
      app: :dd_data_streams,
      name: "DD Data Streams",
      version: "0.1.0",
      elixir: "~> 1.17",
      description: "Datadog Data Streams Monitoring for Broadway pipelines",
      source_url: @source_url,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md", "LICENSE"]],
      package: [
        licenses: ["Beerware"],
        links: %{"GitHub" => @source_url},
        files: ~w(lib mix.exs .formatter.exs README.md LICENSE)
      ],
      aliases: aliases(),
      test_coverage: [summary: [threshold: 90]]
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
      # Every checkpoint is a telemetry event, and attach/0 installs the
      # Broadway handlers on them, so telemetry is always needed.
      {:telemetry, "~> 1.0"},
      # The default agent transport talks HTTP through Req. It is optional so an
      # application that passes its own transport does not pull in an HTTP client.
      {:req, "~> 0.5", optional: true},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:mimic, "~> 1.7", only: :test}
    ]
  end

  # What CI runs, so a contributor can run the same thing locally.
  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp aliases do
    [
      ci: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --strict",
        "test --cover"
      ]
    ]
  end
end
