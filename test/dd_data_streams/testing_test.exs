defmodule DdDataStreams.TestingTest do
  # enable!/1 mutates the application environment and starts the aggregator,
  # both global to the VM, so nothing here can run async.
  use ExUnit.Case, async: false

  doctest DdDataStreams.Testing

  @moduletag :capture_log

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Testing

  @consume_event [:broadway, :processor, :message, :start]
  @flush_event [:dd_data_streams, :flush, :stop]
  @config_keys [:enabled, :transport, :service, :env]

  setup do
    variables = ~w(DD_DATA_STREAMS_ENABLED DD_SERVICE DD_ENV)
    previous_env = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    previous_config = Map.new(@config_keys, &{&1, Application.fetch_env(:dd_data_streams, &1)})

    for key <- @config_keys, do: Application.delete_env(:dd_data_streams, key)
    # The state a suite that boots with the library off starts from.
    Application.put_env(:dd_data_streams, :enabled, false)
    DdDataStreams.detach()

    on_exit(fn ->
      DdDataStreams.detach()

      for {name, value} <- previous_env do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end

      for {key, value} <- previous_config do
        case value do
          {:ok, configured} -> Application.put_env(:dd_data_streams, key, configured)
          :error -> Application.delete_env(:dd_data_streams, key)
        end
      end
    end)

    :ok
  end

  test "enable! turns the library on and the cleanup puts everything back" do
    Application.put_env(:dd_data_streams, :service, "boot-service")

    refute DdDataStreams.status().enabled
    refute Process.whereis(Aggregator)

    cleanup =
      Testing.enable!(service: "test-service", transport: DdDataStreams.Transport.Capture)

    status = DdDataStreams.status()
    assert status.enabled
    assert status.attached
    assert status.aggregator_running
    assert status.transport == DdDataStreams.Transport.Capture
    assert DdDataStreams.Config.service() == "test-service"

    assert :ok = cleanup.()
    # Calling the cleanup twice is harmless, so a test that runs it both
    # directly and through on_exit cannot fail on the second call.
    assert :ok = cleanup.()

    status = DdDataStreams.status()
    refute status.enabled
    refute status.attached
    refute status.aggregator_running
    assert status.transport == DdDataStreams.Transport.Req
    assert DdDataStreams.Config.service() == "boot-service"
  end

  test "checkpoints recorded while enabled reach the aggregator instead of being dropped" do
    handler = "testing-flush-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      @flush_event,
      fn _event, measurements, metadata, _config ->
        send(test, {:flushed, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    cleanup = Testing.enable!(transport: DdDataStreams.Transport.Capture)

    [{_key, value}] = DdDataStreams.produce("orders-out", "payload")
    assert is_binary(value)

    assert :ok = Aggregator.flush(:all)
    assert_receive {:flushed, %{points: points}, %{reason: nil}}
    assert points >= 1

    cleanup.()
  end

  test "the cleanup leaves alone what enable! did not start or attach" do
    # The boot state of a suite that runs with the library on: aggregator
    # running, handlers attached, transport configured.
    Application.put_env(:dd_data_streams, :enabled, true)
    Application.put_env(:dd_data_streams, :transport, DdDataStreams.Transport.Capture)
    pid = start_supervised!({Aggregator, []})
    assert :ok = DdDataStreams.attach()

    cleanup = Testing.enable!()
    assert :ok = cleanup.()

    assert Process.whereis(Aggregator) == pid
    assert Process.alive?(pid)
    assert length(:telemetry.list_handlers(@consume_event)) == 1
    assert DdDataStreams.status().transport == DdDataStreams.Transport.Capture
    assert DdDataStreams.status().enabled
  end

  test "an unknown option raises instead of silently configuring nothing" do
    assert_raise ArgumentError, ~r/unknown enable! options/, fn ->
      Testing.enable!(trasnport: DdDataStreams.Transport.Capture)
    end

    refute DdDataStreams.status().enabled
    refute Process.whereis(Aggregator)
  end

  test "capture/1 returns the points flushed while fun ran, decoded" do
    cleanup = Testing.enable!(transport: DdDataStreams.Transport.Capture)

    points =
      Testing.capture(fn ->
        [{_key, value}] = DdDataStreams.produce("orders-out", "payload")

        DdDataStreams.consume(%{
          data: "payload",
          metadata: %{
            subscription: "projects/demo/subscriptions/orders-sub",
            attributes: %{DdDataStreams.Config.header() => value}
          }
        })
      end)

    cleanup.()

    produce = Enum.find(points, &("topic:orders-out" in &1.edge_tags))
    consume = Enum.find(points, &("subscription:orders-sub" in &1.edge_tags))

    assert produce
    assert consume

    # The consume continues the pathway the produce wrote into the header, so
    # its parent is the produce node, and both are points of one flush.
    assert consume.parent_hash == produce.hash
    assert consume.parent_hash != 0
    assert is_integer(produce.hash)
    assert is_binary(produce.pathway_latency)
    assert produce.timestamp_type in ["current", "origin"]
  end

  test "capture/1 returns [] when there is nothing to flush" do
    assert Testing.capture(fn -> :ok end) == []
  end
end
