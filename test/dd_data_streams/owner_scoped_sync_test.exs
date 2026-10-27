defmodule DdDataStreams.OwnerScopedSyncTest do
  # Shared mode is one owner for the whole test, so this module is the
  # async: false counterpart of OwnerScopedTest.
  use ExUnit.Case, async: false

  alias DdDataStreams.Testing

  @backend DdDataStreams.Testing.Backend

  setup do
    previous = Application.fetch_env(:dd_data_streams, :backend)
    Application.put_env(:dd_data_streams, :backend, @backend)

    on_exit(fn ->
      case previous do
        {:ok, configured} -> Application.put_env(:dd_data_streams, :backend, configured)
        :error -> Application.delete_env(:dd_data_streams, :backend)
      end
    end)

    Testing.set_private()
    :ok
  end

  test "set_from_context/1 picks shared mode for a sync case" do
    assert :ok = Testing.set_from_context(%{async: false})

    # No start/1 and no allow/2: in shared mode every process records into the
    # shared owner, which is this test process.
    parent = self()

    spawn_link(fn ->
      send(parent, {:produced, DdDataStreams.produce("orders-shared", "payload")})
    end)

    assert_receive {:produced, [{_key, _value}]}
    assert [%{edge_tags: tags}] = Testing.checkpoints()
    assert "topic:orders-shared" in tags
  end

  test "set_global/1 is accepted by a sync case" do
    assert :ok = Testing.set_global(%{async: false})

    [{_key, _value}] = DdDataStreams.produce("orders-global", "payload")
    assert [%{edge_tags: tags}] = Testing.checkpoints()
    assert "topic:orders-global" in tags
  end

  test "a shared owner also owns a process that calls start/1" do
    assert :ok = Testing.set_global(%{async: false})
    parent = self()

    spawn_link(fn -> send(parent, {:started, Testing.start()}) end)
    assert_receive {:started, :ok}
  end

  test "allow/2 reports the shared mode refusal" do
    assert :ok = Testing.set_global(%{async: false})

    assert {:error, %NimbleOwnership.Error{reason: :cant_allow_in_shared_mode}} =
             Testing.allow(self(), self())
  end

  test "the backend exposes a child specification for a supervision tree" do
    assert %{
             id: DdDataStreams.Testing.Backend,
             start: {DdDataStreams.Testing.Backend, :start_link, [[]]}
           } =
             DdDataStreams.Testing.Backend.child_spec([])
  end

  test "an unowned private test cannot subscribe or start a missing name" do
    assert_raise RuntimeError, ~r/no owner is recording/, fn -> Testing.subscribe() end

    assert_raise ArgumentError, ~r/no process is registered/, fn ->
      Testing.start(:owner_scoped_missing_name)
    end
  end
end
