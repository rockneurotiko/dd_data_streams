defmodule DdDataStreams.OwnerScopedTest do
  # The owner-scoped testing backend: recording is per test, so the module runs
  # async and its checkpoints never reach another test.
  use ExUnit.Case, async: true

  use DdDataStreams.Testing

  alias DdDataStreams.Testing

  @backend DdDataStreams.Testing.Backend
  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]

  setup_all do
    previous = Application.fetch_env(:dd_data_streams, :backend)
    Application.put_env(:dd_data_streams, :backend, @backend)

    on_exit(fn ->
      case previous do
        {:ok, configured} -> Application.put_env(:dd_data_streams, :backend, configured)
        :error -> Application.delete_env(:dd_data_streams, :backend)
      end
    end)

    :ok
  end

  test "a test that never opts in returns [] and records nothing" do
    assert DdDataStreams.produce("orders-out", "payload") == []
    assert Testing.checkpoints() == []
  end

  test "start/1 records the produce and consume checkpoints in the owner" do
    assert :ok = Testing.start()
    assert :ok = Testing.start()

    [{key, value} | _topic] = DdDataStreams.produce("orders-out", "payload")
    assert key == DdDataStreams.Config.header()

    DdDataStreams.consume(%{
      data: "payload",
      metadata: %{
        subscription: "projects/demo/subscriptions/orders-sub",
        attributes: %{DdDataStreams.Config.header() => value}
      }
    })

    produce = Testing.assert_produced("orders-out")
    consume = Testing.assert_consumed("orders-sub")

    assert consume.edge_tags == @consume_tags
    assert is_integer(produce.pathway_latency)
    assert produce.timestamp_type == "current"

    Testing.assert_chained(produce, consume)
  end

  test "an allowed process records into the owner's checkpoints" do
    Testing.start()
    parent = self()

    pid =
      spawn_link(fn ->
        receive do
          :go -> send(parent, {:produced, DdDataStreams.produce("orders-allowed", "payload")})
        end
      end)

    assert :ok = Testing.allow(self(), pid)
    send(pid, :go)

    assert_receive {:produced, [{_key, _value} | _topic]}
    assert [%{edge_tags: tags}] = Testing.checkpoints()
    assert "topic:orders-allowed" in tags
  end

  test "allow/2 resolves a registered name" do
    Testing.start()
    parent = self()
    name = :"owner_scoped_allowed_#{System.unique_integer([:positive])}"

    pid =
      spawn_link(fn ->
        Process.register(self(), name)
        send(parent, :registered)

        receive do
          :go -> send(parent, {:produced, DdDataStreams.produce("orders-named", "payload")})
        end
      end)

    assert_receive :registered
    assert :ok = Testing.allow(self(), name)
    send(pid, :go)

    assert_receive {:produced, [{_key, _value} | _topic]}
    assert [%{edge_tags: tags}] = Testing.checkpoints()
    assert "topic:orders-named" in tags
  end

  test "two owners record in parallel and never see each other's checkpoints" do
    parent = self()

    tasks =
      for topic <- ["orders-a", "orders-b"] do
        Task.async(fn ->
          :ok = Testing.start()
          [{_key, _value} | _topic] = DdDataStreams.produce(topic, "payload")
          send(parent, {:ready, self(), topic})

          receive do
            :release -> Testing.checkpoints()
          end
        end)
      end

    owners =
      for _ <- tasks do
        assert_receive {:ready, owner, topic}
        {owner, topic}
      end

    assert length(owners) == 2

    for {owner, topic} <- owners do
      points = Testing.checkpoints(owner)
      assert points != []
      assert Enum.all?(points, &("topic:#{topic}" in &1.edge_tags))
    end

    for {owner, _topic} <- owners, do: send(owner, :release)
    Enum.each(tasks, &Task.await/1)
  end

  test "the owner exiting drops its checkpoints" do
    parent = self()

    pid =
      spawn(fn ->
        Testing.start()
        DdDataStreams.produce("orders-dropped", "payload")
        send(parent, :ready)

        receive do
          :stop -> :ok
        end
      end)

    monitor = Process.monitor(pid)

    assert_receive :ready
    assert [_point] = Testing.checkpoints(pid)

    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    assert_eventually(fn -> NimbleOwnership.get_owned(@backend, pid) == nil end)
  end

  test "subscribe/0 sends each recorded point to the owner" do
    Testing.start()
    assert :ok = Testing.subscribe()

    [{_key, _value} | _topic] = DdDataStreams.produce("orders-subscribed", "payload")

    assert_receive {DdDataStreams.Testing, :checkpoint, point}
    assert "topic:orders-subscribed" in point.edge_tags
  end

  test "set_global/1 refuses an async case and set_from_context/1 picks private" do
    assert_raise RuntimeError, ~r/cannot be global/, fn ->
      Testing.set_global(%{async: true})
    end

    assert :ok = Testing.set_from_context(%{async: true})
  end

  test "a Broadway pipeline records a consume point chained to the produce point" do
    Testing.start()

    pipeline = :"owner_scoped_pipeline_#{System.unique_integer([:positive])}"
    start_supervised!({DdDataStreams.OwnerScopedTest.Pipeline, name: pipeline})

    assert :ok = Testing.allow(self(), :"#{pipeline}.Broadway.Processor_default_0")

    [{_key, value} | _topic] = DdDataStreams.produce("orders-out", "payload")

    assert :ok = DdDataStreams.attach()
    on_exit(fn -> DdDataStreams.detach() end)

    ack =
      Broadway.test_message(pipeline, "payload",
        metadata: %{
          subscription: "orders-sub",
          attributes: %{DdDataStreams.Config.header() => value}
        }
      )

    assert_receive {:ack, ^ack, [%{data: "payload"}], []}, 1_000

    # The handlers are global to the VM: detach as soon as the checkpoints are
    # recorded, so another async test never sees them attached.
    assert :ok = DdDataStreams.detach()

    produce = Testing.assert_produced("orders-out")
    consume = Testing.assert_consumed("orders-sub")

    Testing.assert_chained(produce, consume)
  end

  defmodule Pipeline do
    @moduledoc false
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: Keyword.fetch!(opts, :name),
        producer: [module: {Broadway.DummyProducer, []}],
        processors: [default: [concurrency: 1]]
      )
    end

    @impl true
    def handle_message(_processor, message, _context), do: message
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
