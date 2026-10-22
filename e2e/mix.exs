defmodule E2e.MixProject do
  use Mix.Project

  def project do
    [
      app: :e2e,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  # A separate project, so the library under test stays free of Broadway, the
  # Pub/Sub client and any network service.
  defp deps do
    [
      {:dd_data_streams, path: ".."},
      {:broadway_cloud_pub_sub, "~> 2.0.0-rc.2"},
      {:broadway, "~> 1.0"},
      # The harness talks to the emulator's REST API and reads JSON from it.
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:finch, "~> 0.19"},
      # The pull producer uses the client's gRPC path.
      {:protobuf, "~> 0.17"},
      {:grpc, "~> 1.0"},
      # The script attaches a flush-error handler.
      {:telemetry, "~> 1.0"}
    ]
  end
end
