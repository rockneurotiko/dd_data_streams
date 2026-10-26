defmodule DdDataStreams.AsyncSafetyTest do
  # The unit-test recipe from the README, and the claim that makes it work:
  # with the library disabled, host code that calls produce/3 shares nothing
  # with any other test - no telemetry handler id, no aggregator, no transport
  # - so these tests run async, and so can any number of tests over the same
  # pipeline code at once.
  use ExUnit.Case, async: true

  @consume_event [:broadway, :processor, :message, :start]

  setup_all do
    # One flip for the whole module: put_env in each setup would let one
    # test's cleanup re-enable the library under a test still running.
    Application.put_env(:dd_data_streams, :enabled, false)
    on_exit(fn -> Application.delete_env(:dd_data_streams, :enabled) end)

    :ok
  end

  test "produce/3 returns [] and attach/0 installs no handler while disabled" do
    assert DdDataStreams.produce("orders-out", "payload") == []

    assert :ok = DdDataStreams.attach()
    assert :telemetry.list_handlers(@consume_event) == []

    assert :ok = DdDataStreams.detach()
  end

  test "concurrent tests over the same pipeline code never reach the handler id" do
    results =
      1..8
      |> Task.async_stream(fn _ -> DdDataStreams.produce("orders-out", "payload") end)
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &(&1 == []))
    assert :telemetry.list_handlers(@consume_event) == []
  end
end
