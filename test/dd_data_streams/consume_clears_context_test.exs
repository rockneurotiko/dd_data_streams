defmodule DdDataStreams.ConsumeClearsContextTest do
  use ExUnit.Case, async: false

  alias DdDataStreams.{Context, Pathway, Testing}

  @backend DdDataStreams.Testing.Backend
  @extractor DdDataStreams.ConsumeClearsContextTest.Extractor

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED)

    previous = Map.new(variables, &{&1, System.get_env(&1)})
    previous_backend = Application.fetch_env(:dd_data_streams, :backend)
    Enum.each(variables, &System.delete_env/1)

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    Application.put_env(:dd_data_streams, :backend, @backend)
    Context.clear()
    Application.put_env(:dd_data_streams, :extractors, [@extractor])

    Testing.set_private()
    Testing.start()

    on_exit(fn ->
      Context.clear()
      Application.delete_env(:dd_data_streams, :extractors)

      case previous_backend do
        {:ok, configured} -> Application.put_env(:dd_data_streams, :backend, configured)
        :error -> Application.delete_env(:dd_data_streams, :backend)
      end

      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "a non-matching consume clears the context so produce starts a new pathway" do
    match = %{data: "hello", metadata: %{match: true}}
    no_match = %{data: "bye", metadata: %{match: false}}

    assert %Pathway{} = pathway = DdDataStreams.consume(match)
    assert pathway.hash != 0

    # The consume checkpoint is already stored, not waiting for a flush.
    assert [%{edge_tags: ["direction:in" | _]}] = Testing.checkpoints()

    assert DdDataStreams.consume(no_match) == nil
    assert Context.get() == nil

    assert [{key, _value}] = DdDataStreams.produce("orders", "payload")
    assert key == "dd-pathway-ctx-base64"

    # A cleared context makes produce chain onto a fresh (hash 0) pathway.
    assert produce = Testing.assert_produced("orders")
    assert produce.parent_hash == 0
    assert length(Testing.checkpoints()) == 2
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
  def produce(topic, _producer_info),
    do: ["direction:out", "topic:" <> topic, "type:google-pubsub"]
end
