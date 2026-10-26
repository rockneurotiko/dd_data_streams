defmodule DdDataStreams.LifecycleTest do
  # The lifecycle surface: attach/0 in every state it can be called in,
  # detach/0 removing what attach/0 installed, and status/0 reporting both
  # without a Datadog Agent.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @consume_event [:broadway, :processor, :message, :start]

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED DD_DATA_STREAMS_HEADER)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    detach_handlers()
    Application.delete_env(:dd_data_streams, :extractors)

    on_exit(fn ->
      detach_handlers()
      Application.delete_env(:dd_data_streams, :extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "attach/0 returns :ok when the handlers are already attached" do
    assert :ok = DdDataStreams.attach()
    assert :ok = DdDataStreams.attach()

    assert length(:telemetry.list_handlers(@consume_event)) == 1
  end

  test "attach/0 returns :ok and attaches nothing while the library is disabled" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "false")

    assert :ok = DdDataStreams.attach()
    assert :telemetry.list_handlers(@consume_event) == []

    refute DdDataStreams.status().enabled
  end

  test "attach/0 validates the extractors even while the library is disabled" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "false")
    Application.put_env(:dd_data_streams, :extractors, [Enum])

    assert_raise ArgumentError, ~r/is not a list of DdDataStreams.Extractor/, fn ->
      DdDataStreams.attach()
    end

    assert :telemetry.list_handlers(@consume_event) == []
  end

  test "detach/0 removes the handlers attach/0 installed" do
    assert :ok = DdDataStreams.attach()
    assert length(:telemetry.list_handlers(@consume_event)) == 1

    assert :ok = DdDataStreams.detach()
    assert :telemetry.list_handlers(@consume_event) == []

    # Nothing attached is not an error, so a test can end with it always.
    assert :ok = DdDataStreams.detach()
  end

  test "status/0 reports the effective configuration and what is running" do
    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")

    status = DdDataStreams.status()

    assert status.enabled
    refute status.attached
    refute status.aggregator_running
    assert status.service == "checkout"
    assert status.env == "prod"
    assert status.header == "dd-pathway-ctx-base64"
    assert status.extractors == [DdDataStreams.Extractor.GooglePubSub]
    assert status.agent_url == DdDataStreams.Config.agent_url()
    assert status.transport == DdDataStreams.Config.transport()

    aggregator = start_supervised!({DdDataStreams.Aggregator, []})
    assert DdDataStreams.status().aggregator_running
    assert Process.whereis(DdDataStreams.Aggregator) == aggregator

    assert :ok = DdDataStreams.attach()
    assert DdDataStreams.status().attached

    assert :ok = DdDataStreams.detach()
    refute DdDataStreams.status().attached
  end

  defp detach_handlers do
    for %{id: id} <- :telemetry.list_handlers(@consume_event), do: :telemetry.detach(id)
  end
end
