defmodule DdDataStreams.ApplicationTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Transport

  setup do
    previous = System.get_env("DD_DATA_STREAMS_ENABLED")

    on_exit(fn ->
      if previous do
        System.put_env("DD_DATA_STREAMS_ENABLED", previous)
      else
        System.delete_env("DD_DATA_STREAMS_ENABLED")
      end
    end)

    :ok
  end

  test "the library starts the aggregator itself" do
    assert {DdDataStreams.Application, []} = Application.spec(:dd_data_streams, :mod)
  end

  test "an enabled application starts one supervised aggregator" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    supervisor = start_application()

    assert aggregator = Process.whereis(Aggregator)

    assert [{Aggregator, ^aggregator, :worker, [Aggregator]}] =
             Supervisor.which_children(supervisor)

    # Losing the aggregator must not stop the aggregation: the supervisor
    # replaces it and the replacement takes over the registered name.
    Process.exit(aggregator, :kill)

    assert replacement = await_replacement(aggregator)
    assert Process.alive?(replacement)
  end

  test "the aggregator posts through the transport module configured for it" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    # The behaviour has no post/3 of its own, so naming it as the transport makes
    # the flush fail in a way that says which module the aggregator really used.
    Application.put_env(:dd_data_streams, :transport, Transport)
    on_exit(fn -> Application.delete_env(:dd_data_streams, :transport) end)

    test = self()

    :telemetry.attach(
      "application-transport",
      [:dd_data_streams, :flush, :error],
      fn _event, _measurements, metadata, _config -> send(test, {:flush_error, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("application-transport") end)

    supervisor = start_application()

    assert [{Aggregator, aggregator, :worker, [Aggregator]}] =
             Supervisor.which_children(supervisor)

    assert :sys.get_state(aggregator).transport == Transport

    # Complete bucket, so the flush has something to post.
    Aggregator.add(%{
      hash: 1,
      parent_hash: 0,
      tags: ["direction:in"],
      timestamp: System.system_time(:millisecond) - 15_000,
      pathway_latency: 1,
      edge_latency: 1,
      payload_size: 1
    })

    assert :ok = Aggregator.flush()

    assert_receive {:flush_error,
                    %{reason: %UndefinedFunctionError{module: Transport, function: :post}}}
  end

  test "a disabled application starts no aggregator at all" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "false")

    supervisor = start_application()

    refute Process.whereis(Aggregator)
    assert Supervisor.which_children(supervisor) == []
  end

  test "an aggregator that is already running is left alone" do
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")
    existing = start_supervised!({Aggregator, []})

    supervisor = start_application()

    assert Process.whereis(Aggregator) == existing
    assert Supervisor.which_children(supervisor) == []
  end

  defp start_application do
    {:ok, supervisor} = DdDataStreams.Application.start(:normal, [])

    # start/0 links the supervisor to this test process, but the on_exit
    # callback below runs after that process has exited, so the link would kill
    # the supervisor and racing it with the stop used to fail with :noproc.
    Process.unlink(supervisor)
    on_exit(fn -> stop(supervisor) end)
    supervisor
  end

  defp stop(supervisor) do
    Supervisor.stop(supervisor)
  catch
    :exit, _reason -> :ok
  end

  defp await_replacement(previous, attempts \\ 100) do
    case Process.whereis(Aggregator) do
      pid when is_pid(pid) and pid != previous ->
        pid

      _ when attempts > 0 ->
        Process.sleep(10)
        await_replacement(previous, attempts - 1)

      _ ->
        nil
    end
  end
end
