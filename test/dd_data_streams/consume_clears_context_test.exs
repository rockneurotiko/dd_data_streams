defmodule DdDataStreams.ConsumeClearsContextTest do
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.{Aggregator, Context, Pathway}

  @extractor DdDataStreams.ConsumeClearsContextTest.Extractor

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    Context.clear()
    Application.put_env(:dd_data_streams, :extractors, [@extractor])

    test = self()
    stub(Aggregator, :add, fn checkpoint -> send(test, {:checkpoint, checkpoint}) end)

    on_exit(fn ->
      Context.clear()
      Application.delete_env(:dd_data_streams, :extractors)

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "a non-matching consume clears the context so produce starts a new pathway" do
    match = %{data: "hello", metadata: %{match: true}}
    no_match = %{data: "bye", metadata: %{match: false}}

    assert {pathway, _checkpoint} = DdDataStreams.consume(match)
    assert %Pathway{} = pathway
    assert pathway.hash != 0

    # Drain the consume checkpoint.
    assert_receive {:checkpoint, _}

    assert DdDataStreams.consume(no_match) == nil
    assert Context.get() == nil

    assert {key, _value} = DdDataStreams.produce("orders", "payload")
    assert key == "dd-pathway-ctx-base64"

    assert_receive {:checkpoint, produce_checkpoint}
    # A cleared context makes produce chain onto a fresh (hash 0) pathway.
    assert produce_checkpoint.parent_hash == 0
  end
end

defmodule DdDataStreams.ConsumeClearsContextTest.Extractor do
  @behaviour DdDataStreams.Extractor

  @impl true
  def consume(%{match: true}) do
    {:ok, ["direction:in", "subscription:orders-sub", "type:google-pubsub"], %{}}
  end

  def consume(_metadata), do: :error

  @impl true
  def produce(topic), do: ["direction:out", "topic:" <> topic, "type:google-pubsub"]
end
