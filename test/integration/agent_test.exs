defmodule DdDataStreams.AgentTest do
  # Posts a flushed payload to a Datadog Agent. The agent does not decode the
  # body: /v0.1/pipeline_stats is a reverse proxy to the pipeline stats intake on
  # DD_SITE, so the status this test sees is the intake's own answer. That is
  # what makes it worth running: a 202 means the backend accepted the schema, a
  # 500 means the agent has no API key to forward with, and a 4xx is the intake
  # rejecting the payload.
  #
  # This is the `agent` tier: excluded from `mix test`, run with
  #
  #   DD_TRACE_AGENT_URL=http://127.0.0.1:8126 mix test --only agent
  #
  # e2e/stub_agent.exs answers 202 for a machine with no agent, which proves the
  # transport but not that the schema is accepted.
  use ExUnit.Case, async: false

  use Mimic

  alias DdDataStreams.Aggregator
  alias DdDataStreams.Transport

  @moduletag :agent

  setup_all do
    if System.get_env("DD_TRACE_AGENT_URL") in [nil, ""] do
      raise """
      DD_TRACE_AGENT_URL is not set, so there is no agent to post to.

      Start an agent (a real one, or e2e/stub_agent.exs) and run:

          DD_TRACE_AGENT_URL=http://127.0.0.1:8126 mix test --only agent
      """
    end

    {:ok, _apps} = Application.ensure_all_started(:req)
    :ok
  end

  setup do
    variables = ~w(DD_SERVICE DD_ENV DD_DATA_STREAMS_ENABLED)
    previous = Map.new(variables, &{&1, System.get_env(&1)})

    System.put_env("DD_SERVICE", "checkout")
    System.put_env("DD_ENV", "prod")
    System.put_env("DD_DATA_STREAMS_ENABLED", "true")

    test = self()

    # The real transport is the point of this tier, so the expectation calls the
    # original: it only reports the status the agent answered with.
    expect(Transport.Req, :post, fn url, headers, body ->
      result = call_original(Transport.Req, :post, [url, headers, body])
      send(test, {:agent_response, result})
      result
    end)

    aggregator = start_supervised!({Aggregator, []})
    allow(Transport.Req, self(), aggregator)

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
