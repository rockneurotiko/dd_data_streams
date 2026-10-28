defmodule DdDataStreams.ServiceNameTest do
  # The configured namer is application environment, and the payload test uses
  # the shared aggregator, so nothing here runs async.
  use ExUnit.Case, async: false

  alias DdDataStreams.{Aggregator, Config, Context, Pathway, Testing}

  @moduletag :capture_log

  @backend DdDataStreams.Testing.Backend

  defmodule Topical do
    @behaviour DdDataStreams.ServiceName

    @impl true
    def call(%{direction: :out, topic: topic}), do: "my_service." <> topic

    def call(%{direction: :in, metadata: %{subscription: subscription}}),
      do: "my_service." <> subscription
  end

  defmodule Parent do
    @behaviour DdDataStreams.ServiceName

    @impl true
    def call(%{direction: :out, pathway: %Pathway{tags: tags}}) do
      case DdDataStreams.Tags.get(tags, "subscription") do
        nil -> nil
        parent -> "from_" <> parent
      end
    end

    def call(_context), do: nil
  end

  defmodule Silent do
    @behaviour DdDataStreams.ServiceName

    @impl true
    def call(_context), do: nil
  end

  defmodule Broken do
    @behaviour DdDataStreams.ServiceName

    @impl true
    def call(_context), do: raise("no name today")
  end

  setup do
    previous_backend = Application.fetch_env(:dd_data_streams, :backend)
    previous_name = Application.fetch_env(:dd_data_streams, :service_name)

    Application.put_env(:dd_data_streams, :backend, @backend)
    Application.delete_env(:dd_data_streams, :service_name)

    Testing.set_private()
    Testing.start()
    Context.clear()

    on_exit(fn ->
      Context.clear()
      restore(:backend, previous_backend)
      restore(:service_name, previous_name)
    end)

    :ok
  end

  defp restore(key, {:ok, value}), do: Application.put_env(:dd_data_streams, key, value)
  defp restore(key, :error), do: Application.delete_env(:dd_data_streams, key)

  defp node_hash(service, tags, parent \\ 0) do
    Pathway.pathway_hash(
      Pathway.node_hash(service, Config.env(), Config.primary_tag(), tags),
      parent
    )
  end

  test "the configured namer names a produce from its topic" do
    Application.put_env(:dd_data_streams, :service_name, Topical)

    assert [{_key, _value} | _topic] = DdDataStreams.produce("orders", "hello")

    [produce] = Testing.checkpoints()
    assert produce.edge_tags == ["direction:out", "topic:orders", "type:google-pubsub"]
    assert produce.hash == node_hash("my_service.orders", produce.edge_tags)
  end

  test "the configured namer names a consume from the message metadata" do
    Application.put_env(:dd_data_streams, :service_name, Topical)

    message = %{data: "hello", metadata: %{subscription: "orders-sub", attributes: %{}}}
    assert %Pathway{} = DdDataStreams.consume(message)

    [consume] = Testing.checkpoints()
    assert consume.edge_tags == ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
    assert consume.hash == node_hash("my_service.orders-sub", consume.edge_tags)
  end

  test "an out namer reads the parent edge from the pathway" do
    Application.put_env(:dd_data_streams, :service_name, Parent)

    # The consume leaves the edge it named on the pathway in the context, so
    # the produce in the same stage can name itself after what it answers.
    assert %Pathway{hash: consumed} =
             DdDataStreams.consume(%{
               data: "hello",
               metadata: %{subscription: "orders-sub", attributes: %{}}
             })

    DdDataStreams.produce("orders-out", "hello")

    [produce] =
      Enum.filter(Testing.checkpoints(), &("direction:out" in &1.edge_tags))

    assert produce.hash == node_hash("from_orders-sub", produce.edge_tags, consumed)
  end

  test "an out namer that reads the pathway names nothing without one" do
    Application.put_env(:dd_data_streams, :service_name, Parent)

    DdDataStreams.produce("orders-out", "hello")

    [produce] = Testing.checkpoints()
    assert produce.hash == node_hash(Config.service(), produce.edge_tags)
  end

  test "the service option wins over the configured namer" do
    Application.put_env(:dd_data_streams, :service_name, Topical)

    DdDataStreams.produce("orders", "hello", service: "explicit")

    [produce] = Testing.checkpoints()
    assert produce.hash == node_hash("explicit", produce.edge_tags)
  end

  test "a namer that names nothing leaves the configured service" do
    Application.put_env(:dd_data_streams, :service_name, Silent)

    DdDataStreams.produce("orders", "hello")

    [produce] = Testing.checkpoints()
    assert produce.hash == node_hash(Config.service(), produce.edge_tags)
  end

  test "a namer that raises is reported and the checkpoint is still recorded" do
    handler = {__MODULE__, make_ref()}
    test_pid = self()

    :telemetry.attach(
      handler,
      [:dd_data_streams, :handler, :error],
      fn event, measurements, metadata, _config ->
        send(test_pid, {event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    Application.put_env(:dd_data_streams, :service_name, Broken)

    assert [{_key, _value} | _topic] = DdDataStreams.produce("orders", "hello")

    [produce] = Testing.checkpoints()
    assert produce.hash == node_hash(Config.service(), produce.edge_tags)

    assert_receive {[:dd_data_streams, :handler, :error], %{count: 1},
                    %{service_name: Broken, reason: %RuntimeError{}}}
  end

  test "an option that is not a service name is rejected at the call site" do
    assert_raise ArgumentError, ~r/:service must be a string/, fn ->
      DdDataStreams.produce("orders", "hello", service: fn -> "orders" end)
    end
  end

  test "attach rejects a namer that cannot be called" do
    Application.put_env(:dd_data_streams, :service_name, self())

    assert_raise ArgumentError, ~r/is not a DdDataStreams.ServiceName module/, fn ->
      DdDataStreams.attach()
    end
  end

  test "one payload per service reaches the agent" do
    Application.put_env(:dd_data_streams, :service_name, Topical)
    # The payload path is the aggregator's, not the owner-scoped backend's.
    Application.delete_env(:dd_data_streams, :backend)

    test_pid = self()
    handler = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler,
      [:dd_data_streams, :transport, :captured],
      fn _event, _measurements, %{body: body}, _config -> send(test_pid, {:body, body}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    on_exit(Testing.enable!(transport: DdDataStreams.Transport.Capture))

    DdDataStreams.produce("orders", "hello")
    DdDataStreams.produce("payments", "hello")
    :ok = Aggregator.flush(:all)

    services =
      collect_bodies()
      |> Enum.map(&Testing.decode_payload(&1).service)
      |> Enum.sort()

    assert services == ["my_service.orders", "my_service.payments"]
  end

  defp collect_bodies(acc \\ []) do
    receive do
      {:body, body} -> collect_bodies([body | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
