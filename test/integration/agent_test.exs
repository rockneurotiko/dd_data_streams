defmodule DdDataStreams.AgentTest do
  # Posts a flushed payload to a real Datadog Agent, which is the only check that
  # the pipeline stats schema is accepted by something other than this library.
  # No agent runs in the default suite, so this file skips itself unless
  # DD_TRACE_AGENT_URL names one (for example http://127.0.0.1:8126).
  #
  # The transport is the library's own Req implementation, wrapped only to report
  # the status the agent answered with. The agent accepts every status below 400
  # for /v0.1/pipeline_stats; 202 Accepted is what a working intake answers, and
  # that is what is asserted here.
  use ExUnit.Case, async: false

  alias DdDataStreams.Aggregator

  @agent System.get_env("DD_TRACE_AGENT_URL")

  if is_nil(@agent) do
    @moduletag skip:
                 "set DD_TRACE_AGENT_URL (for example http://127.0.0.1:8126) to post to an agent"
  end

  defmodule Reporting do
    @behaviour DdDataStreams.Transport

    @impl true
    def post(url, headers, body) do
      result = DdDataStreams.Transport.Req.post(url, headers, body)
      send(:persistent_term.get({__MODULE__, :recipient}), {:agent_response, result})
      result
    end
  end

  setup_all do
    {:ok, _apps} = Application.ensure_all_started(:req)
    :ok
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED)
    previous = Map.new(variables, &{&1, System.get_env(&1)})

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    :persistent_term.put({Reporting, :recipient}, self())
    start_supervised!({Aggregator, transport: Reporting})

    on_exit(fn ->
      for {name, value} <- previous do
        if value, do: System.put_env(name, value), else: System.delete_env(name)
      end
    end)

    :ok
  end

  test "an agent accepts a flushed payload" do
    # flush/0 only sends buckets whose start is at least ten seconds old, so the
    # checkpoint is placed fifteen seconds in the past.
    timestamp = System.system_time(:millisecond) - 15_000

    assert :ok =
             Aggregator.add(%{
               hash: 304_706_334_078_420_845,
               parent_hash: 2_140_133_010_089_266_238,
               tags: ["direction:in", "subscription:orders-sub", "type:google-pubsub"],
               timestamp: timestamp,
               pathway_latency: 15_000,
               edge_latency: 15_000,
               payload_size: 74
             })

    assert :ok = Aggregator.flush()
    assert_receive {:agent_response, {:ok, 202}}, 10_000
  end
end
