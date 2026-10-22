defmodule DdDataStreams.PathwayTest do
  use ExUnit.Case, async: true

  alias DdDataStreams.Pathway

  doctest DdDataStreams.Pathway

  # Node hashes are FNV-1 64-bit over service, env, primary tag and the hashable
  # edge tags in byte order. The consume/produce vectors were reproduced with
  # dd-trace-py's fnv1_64 over "checkout" <> "prod" <> the tags sorted, which is
  # its DataStreamsCtx._compute_hash; the base hash alone is the value
  # dd-trace-go's BaseHash("checkout", "prod", nil, "") returns.
  @consume_node_hash 7_951_410_199_073_889_140
  @consume_pathway_hash 11_571_056_430_286_957_182
  @produce_node_hash 14_383_890_804_426_121_796
  @produce_pathway_hash 17_914_629_665_428_218_669
  @chained_produce_hash 16_190_897_810_891_563_068

  @consume_tags ["direction:in", "subscription:orders-sub", "type:google-pubsub"]
  @produce_tags ["direction:out", "topic:orders", "type:google-pubsub"]

  test "the base hash of service, env and primary tag is FNV-1 over their concatenation" do
    assert Pathway.node_hash("checkout", "prod", nil, []) == 5_452_023_167_263_768_086
    assert Pathway.node_hash("checkout", "prod", "", []) == 5_452_023_167_263_768_086
  end

  test "the node hash includes the hashable tags in byte order" do
    assert Pathway.node_hash("checkout", "prod", nil, @consume_tags) == @consume_node_hash
    assert Pathway.node_hash("checkout", "prod", nil, @produce_tags) == @produce_node_hash
  end

  test "the node hash does not depend on the order the tags arrive in" do
    assert Pathway.node_hash("checkout", "prod", nil, Enum.reverse(@consume_tags)) ==
             @consume_node_hash
  end

  test "the primary tag identifies the node too" do
    # Same inputs as @consume_node_hash with "region:eu" between env and tags.
    assert Pathway.node_hash("checkout", "prod", "region:eu", @consume_tags) ==
             7_822_085_030_007_189_004
  end

  test "partition is never included in a node hash" do
    assert Pathway.node_hash("checkout", "prod", nil, @consume_tags ++ ["partition:0"]) ==
             @consume_node_hash

    assert Pathway.node_hash("checkout", "prod", nil, @consume_tags ++ ["partition:1"]) ==
             Pathway.node_hash("checkout", "prod", nil, @consume_tags ++ ["partition:0"])
  end

  test "per-message and aggregation tags do not identify the node" do
    extra = ["partition:0", "has_routing_key:true", "group:workers", "ds.name:orders"]

    assert Pathway.node_hash("checkout", "prod", nil, @consume_tags ++ extra) ==
             @consume_node_hash
  end

  test "the pathway hash chains the node hash onto its parent" do
    assert Pathway.pathway_hash(@produce_node_hash, 0) == @produce_pathway_hash
    assert Pathway.pathway_hash(@consume_node_hash, 0) == @consume_pathway_hash

    assert Pathway.pathway_hash(@produce_node_hash, @consume_pathway_hash) ==
             @chained_produce_hash
  end

  test "new/1 fills the fields it is given and zeroes the rest" do
    assert %Pathway{hash: 7, pathway_start: 11, edge_start: 13} =
             Pathway.new(%{hash: 7, pathway_start: 11, edge_start: 13})

    assert %Pathway{hash: 0, pathway_start: 11, edge_start: 0} = Pathway.new(pathway_start: 11)
  end

  test "merge/1 samples one of the given pathways and handles an empty fan-in" do
    pathways = [
      Pathway.new(hash: 1),
      Pathway.new(hash: 2),
      Pathway.new(hash: 3)
    ]

    assert Pathway.merge(pathways) in pathways
    assert Pathway.merge([]) == %Pathway{}
  end

  test "the first checkpoint starts the pathway and chains onto zero" do
    {pathway, checkpoint} =
      Pathway.set_checkpoint(%Pathway{}, @consume_tags,
        service: "checkout",
        env: "prod",
        now: 1_000
      )

    assert pathway == %Pathway{
             hash: @consume_pathway_hash,
             pathway_start: 1_000,
             edge_start: 1_000
           }

    assert checkpoint == %{
             hash: @consume_pathway_hash,
             parent_hash: 0,
             tags: @consume_tags,
             timestamp: 1_000,
             pathway_latency: 0,
             edge_latency: 0
           }
  end

  test "a later checkpoint chains onto the previous node and measures both latencies" do
    {consumed, _checkpoint} =
      Pathway.set_checkpoint(%Pathway{}, @consume_tags,
        service: "checkout",
        env: "prod",
        now: 1_000
      )

    {produced, checkpoint} =
      Pathway.set_checkpoint(consumed, @produce_tags,
        service: "checkout",
        env: "prod",
        now: 1_500
      )

    assert produced == %Pathway{
             hash: @chained_produce_hash,
             pathway_start: 1_000,
             edge_start: 1_500
           }

    assert checkpoint == %{
             hash: @chained_produce_hash,
             parent_hash: @consume_pathway_hash,
             tags: @produce_tags,
             timestamp: 1_500,
             pathway_latency: 500,
             edge_latency: 500
           }
  end

  test "start overrides measure from the broker time rather than the local clock" do
    {pathway, checkpoint} =
      Pathway.set_checkpoint(%Pathway{}, @consume_tags,
        service: "checkout",
        env: "prod",
        now: 2_000,
        pathway_start: 1_300,
        edge_start: 1_700
      )

    assert pathway.pathway_start == 1_300
    assert pathway.edge_start == 2_000
    assert checkpoint.pathway_latency == 700
    assert checkpoint.edge_latency == 300
  end

  test "clock skew never produces a negative latency" do
    {_pathway, checkpoint} =
      Pathway.set_checkpoint(%Pathway{pathway_start: 5_000, edge_start: 5_000}, @consume_tags,
        service: "checkout",
        env: "prod",
        now: 4_000
      )

    assert checkpoint.pathway_latency == 0
    assert checkpoint.edge_latency == 0
  end
end
