defmodule DdDataStreams.HandlerSafetyTest do
  # Telemetry detaches a handler that raises, for every event it is attached to
  # and every pipeline in the VM, and never reattaches it: one bad message
  # would stop Data Streams Monitoring for the whole application. These tests
  # pin that the handlers survive everything a message or an extractor can hand
  # them.
  use ExUnit.Case, async: false

  use Mimic

  @moduletag :capture_log

  alias DdDataStreams.{Aggregator, Context}
  alias DdDataStreams.HandlerSafetyTest.Configurable

  @consume_event [:broadway, :processor, :message, :start]
  @fan_in_event [:broadway, :batch_processor, :start]
  @events [@consume_event, @fan_in_event]
  @error_event [:dd_data_streams, :handler, :error]

  @tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    detach_all()
    Context.clear()
    Application.delete_env(:dd_data_streams, :extractors)

    test = self()

    stub(Aggregator, :add, fn checkpoint -> send(test, {:checkpoint, checkpoint}) end)

    :telemetry.attach(
      "handler-safety-probe",
      @error_event,
      &__MODULE__.probe/4,
      %{pid: test}
    )

    on_exit(fn ->
      detach_all()
      Context.clear()
      Application.delete_env(:dd_data_streams, :extractors)
      :telemetry.detach("handler-safety-probe")

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "a raising extractor does not detach the handlers" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:raise, "extractor boom"})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    assert length(:telemetry.list_handlers(@consume_event)) == 1
    assert length(:telemetry.list_handlers(@fan_in_event)) == 1
    refute_received {:checkpoint, _checkpoint}
  end

  test "the reason a message was skipped is reported" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:raise, "extractor boom"})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    assert_receive {:skipped,
                    %{
                      event: nil,
                      extractor: Configurable,
                      reason: %RuntimeError{},
                      stacktrace: stacktrace
                    }}

    assert is_list(stacktrace)
  end

  test "the pathway of the previous message does not survive a failing one" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:ok, @tags, %{}})

    assert :ok = DdDataStreams.attach()

    consume_event(message())
    assert_receive {:checkpoint, _checkpoint}
    assert Context.get()

    Process.put(:extractor_result, {:raise, "extractor boom"})
    consume_event(message())

    assert Context.get() == nil
    assert Context.get_extractor() == nil
  end

  test "a message after a failing one is still recorded" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:raise, "extractor boom"})

    assert :ok = DdDataStreams.attach()

    consume_event(message())
    refute_received {:checkpoint, _checkpoint}

    Process.put(:extractor_result, {:ok, @tags, %{}})
    consume_event(message())

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.tags == @tags
    assert length(:telemetry.list_handlers(@consume_event)) == 1
  end

  test "a failure inside the handler does not detach the handlers" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:ok, @tags, %{}})

    assert :ok = DdDataStreams.attach()

    consume_event(message())
    assert_receive {:checkpoint, _checkpoint}
    assert Context.get()

    stub(Aggregator, :add, fn _checkpoint -> raise "aggregator boom" end)

    consume_event(message())

    assert length(:telemetry.list_handlers(@consume_event)) == 1
    assert length(:telemetry.list_handlers(@fan_in_event)) == 1
    assert Context.get() == nil
    assert Context.get_extractor() == nil

    assert_receive {:skipped, %{event: @consume_event, reason: %RuntimeError{}}}
  end

  test "a tag that is not a string is dropped and the checkpoint is recorded" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:ok, ["direction:in", :not_a_string, "subscription:s"], %{}})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.tags == ["direction:in", "subscription:s"]
    assert length(:telemetry.list_handlers(@consume_event)) == 1
  end

  test "a three-tuple header is ignored and the checkpoint is recorded" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])

    Process.put(:extractor_result, {
      :ok,
      @tags,
      %{attributes: [{"dd-pathway-ctx-base64", "AAAABBBB", :long}]}
    })

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.tags == @tags
    assert length(:telemetry.list_handlers(@consume_event)) == 1
  end

  test "attributes that are not a map are ignored and the checkpoint is recorded" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:ok, @tags, %{attributes: "not-a-map"}})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    assert_receive {:checkpoint, checkpoint}
    assert checkpoint.tags == @tags
    assert length(:telemetry.list_handlers(@consume_event)) == 1
  end

  test "an extractor that hands back an unusable shape names no edge" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:ok, "direction:in", %{}})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    refute_received {:checkpoint, _checkpoint}
    assert Context.get() == nil
    assert length(:telemetry.list_handlers(@consume_event)) == 1

    assert_receive {:skipped,
                    %{
                      event: nil,
                      extractor: Configurable,
                      reason: :unusable_edge,
                      stacktrace: nil
                    }}
  end

  test "an extractor that hands back an unknown shape names no edge" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:nope, 1})

    assert :ok = DdDataStreams.attach()

    consume_event(message())

    refute_received {:checkpoint, _checkpoint}
    assert Context.get() == nil
    assert length(:telemetry.list_handlers(@consume_event)) == 1
    assert_receive {:skipped, %{extractor: Configurable, reason: :unusable_edge}}
  end

  test "the batch handler survives a failing extractor" do
    Application.put_env(:dd_data_streams, :extractors, [Configurable])
    Process.put(:extractor_result, {:raise, "extractor boom"})

    assert :ok = DdDataStreams.attach()

    batch_event([message()])

    assert length(:telemetry.list_handlers(@consume_event)) == 1
    assert length(:telemetry.list_handlers(@fan_in_event)) == 1
    assert_receive {:skipped, %{extractor: Configurable}}
  end

  def probe(_event, _measurements, metadata, %{pid: pid}), do: send(pid, {:skipped, metadata})

  defp message do
    %{data: "hello", metadata: %{subscription: "orders-sub"}}
  end

  defp consume_event(message) do
    :telemetry.execute(
      @consume_event,
      %{monotonic_time: 0, system_time: 0},
      %{processor_key: :default, topology_name: "TestPipeline", message: message, context: %{}}
    )
  end

  defp batch_event(messages) do
    :telemetry.execute(
      @fan_in_event,
      %{monotonic_time: 0, system_time: 0},
      %{topology_name: "TestPipeline", messages: messages, context: %{}}
    )
  end

  defp detach_all do
    Enum.each(@events, fn event ->
      for %{id: id} <- :telemetry.list_handlers(event), do: :telemetry.detach(id)
    end)
  end
end

defmodule DdDataStreams.HandlerSafetyTest.Configurable do
  # An extractor whose answer the test sets, so one module can raise, hand back
  # a malformed shape, or name an edge.
  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(_metadata) do
    case Process.get(:extractor_result) do
      {:raise, reason} -> raise reason
      result -> result
    end
  end

  @impl true
  def produce(topic), do: ["direction:out", "topic:" <> topic, "type:google-pubsub"]
end
