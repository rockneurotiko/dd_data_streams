defmodule DdDataStreams.PipelineTest do
  # The library meets Broadway only through telemetry, so the same two events
  # the other Broadway test executes by hand are emitted here by a real
  # topology: the consume handler runs in the processor process, and the fan-in
  # handler in the batch processor's, which is the part a hand-emitted event in
  # the emitting process can never cover.
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.{Aggregator, Context, Pathway, Propagator}

  @events [[:broadway, :processor, :message, :start], [:broadway, :batch_processor, :start]]

  @subscription "orders-sub"
  @consume_tags ["direction:in", "subscription:" <> @subscription, "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]
  @metadata %{subscription: @subscription, orderingKey: "customer-1"}

  setup do
    detach_all()
    Context.clear()

    # Checkpoints are recorded by Broadway's own processes, not by the test's,
    # so every process has to see the stub.
    set_mimic_global()
    test = self()
    stub(Aggregator, :add, fn checkpoint -> send(test, {:checkpoint, checkpoint}) end)

    pipeline = :"dd_data_streams_pipeline_#{System.unique_integer([:positive])}"
    start_supervised!({DdDataStreams.PipelineTest.Pipeline, name: pipeline, test: test})

    on_exit(fn ->
      detach_all()
      Context.clear()
      set_mimic_private()
    end)

    {:ok, pipeline: pipeline}
  end

  test "a message answered from both stages records one consume and two produces", %{
    pipeline: pipeline
  } do
    assert :ok = DdDataStreams.attach()

    ack = Broadway.test_message(pipeline, "hello", metadata: @metadata)
    assert_receive {:ack, ^ack, [%{data: "hello"}], []}

    recorded = checkpoints(3)
    [consumed] = Enum.filter(recorded, &(&1.tags == @consume_tags))
    produces = Enum.filter(recorded, &(&1.tags == @produce_tags))
    assert length(produces) == 2
    assert Enum.all?(produces, &(&1.parent_hash == consumed.hash))

    # The consume checkpoint is the one the processor stage recorded for this
    # message, and the pathway it left there is the one the stage answers from.
    assert_receive {:processor, processor, %Pathway{} = pathway, [{key, value}]}
    assert pathway.hash == consumed.hash
    assert key == "dd-pathway-ctx-base64"
    assert {:ok, %Pathway{hash: hash}} = Propagator.decode_str(value)
    assert Enum.all?(produces, &(&1.hash == hash))

    # Fan-in: the batch stage is another process, so the pathway it was handed
    # is the only one it can answer the batch from.
    assert_receive {:batcher, batcher, %Pathway{} = merged, [{^key, batch_value}]}
    assert merged.hash == consumed.hash
    refute batcher == processor
    assert {:ok, %Pathway{hash: ^hash}} = Propagator.decode_str(batch_value)
  end

  test "a batch records one consume per message and fans them in", %{pipeline: pipeline} do
    assert :ok = DdDataStreams.attach()

    ack = Broadway.test_batch(pipeline, ["one", "two", "three"], metadata: @metadata)
    assert_receive {:ack, ^ack, messages, []}
    assert length(messages) == 3

    recorded = checkpoints(9)
    consumed = Enum.filter(recorded, &(&1.tags == @consume_tags))
    assert length(consumed) == 3

    # Fan-in keeps one pathway of the batch's, so the whole batch is published
    # onto it.
    assert_receive {:batcher, _batcher, %Pathway{} = merged, [{_key, value}]}
    assert merged.hash in Enum.map(consumed, & &1.hash)
    assert {:ok, %Pathway{hash: hash}} = Propagator.decode_str(value)

    produces = Enum.filter(recorded, &(&1.tags == @produce_tags))
    assert length(produces) == 6
    assert Enum.all?(produces, &(&1.parent_hash == merged.hash))
    assert Enum.all?(produces, &(&1.hash == hash))
  end

  defmodule Pipeline do
    @moduledoc false
    # Only what the library attaches to: one processor, so a message's whole
    # path is easy to follow, and one batcher, so a batch is handled by another
    # process than the one that consumed its messages.
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: Keyword.fetch!(opts, :name),
        context: %{test: Keyword.fetch!(opts, :test)},
        producer: [module: {Broadway.DummyProducer, []}],
        processors: [default: [concurrency: 1]],
        batchers: [default: [concurrency: 1, batch_size: 3, batch_timeout: 20]]
      )
    end

    @impl true
    def handle_message(_processor, message, %{test: test}) do
      send(
        test,
        {:processor, self(), Context.get(), DdDataStreams.produce("orders", message.data)}
      )

      message
    end

    @impl true
    def handle_batch(_batcher, messages, _info, %{test: test}) do
      send(
        test,
        {:batcher, self(), Context.get(),
         DdDataStreams.produce_batch("orders", Enum.map(messages, & &1.data))}
      )

      messages
    end
  end

  defp checkpoints(count) do
    for _ <- 1..count do
      assert_receive {:checkpoint, checkpoint}, 1_000
      checkpoint
    end
  end

  defp detach_all do
    Enum.each(@events, fn event ->
      for %{id: id} <- :telemetry.list_handlers(event), do: :telemetry.detach(id)
    end)
  end
end
