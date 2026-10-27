defmodule DdDataStreams.MixProject do
  use Mix.Project

  @source_url "https://github.com/rockneurotiko/dd_data_streams"

  def project do
    [
      app: :dd_data_streams,
      name: "DD Data Streams",
      version: "0.2.0",
      elixir: "~> 1.17",
      description: "Datadog Data Streams Monitoring for Broadway pipelines",
      source_url: @source_url,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md", "LICENSE"]],
      package: [
        licenses: ["Beerware"],
        links: %{"GitHub" => @source_url},
        files: ~w(lib mix.exs .formatter.exs README.md CHANGELOG.md LICENSE)
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
      # The owner-scoped test backend tracks checkpoints with NimbleOwnership.
      # It is optional so an application that does not use that backend, or
      # that never tests with this library, does not pull it in.
      {:nimble_ownership, "~> 1.0", optional: true},
      # The default agent transport talks HTTP through Req. It is optional so an
      # application that passes its own transport does not pull in an HTTP client.
      {:req, "~> 0.5", optional: true},
      # The Broadway integration is telemetry only, so a real pipeline is a test
      # dependency and never shipped.
      {:broadway, "~> 1.0", only: :test},
      # The broker tiers run a real Broadway pipeline against a real broker, so
      # each client is a test dependency. The library still ships none.
      {:broadway_kafka, "~> 0.7.1", only: :test},
      {:broadway_rabbitmq, "~> 0.8.2", only: :test},
      # `runtime: false` keeps `:aws_credentials` from starting: its provider
      # chain probes the EC2 metadata endpoint at boot, which costs a few
      # seconds on every suite run. The tier passes credentials to the client
      # explicitly, so nothing reads them from the environment.
      {:broadway_sqs, "~> 1.0.1", only: :test, runtime: false},
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
