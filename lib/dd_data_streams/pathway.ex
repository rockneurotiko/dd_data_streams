defmodule DdDataStreams.Pathway do
  @moduledoc """
  A pathway is the chain of nodes a payload passes through: the producer, the
  broker edge, the consumer, and so on.

  `hash` identifies the current node in that chain. It is a 64-bit FNV-1 hash of
  the node's own identity (`node_hash/4`) chained onto the hash of the node that
  sent the payload (`pathway_hash/2`), so the payload carries the same value on
  to the next service. `pathway_start` and `edge_start` are the Unix times in
  milliseconds at which the pathway and the current edge started; the
  difference between them and the time of a checkpoint is the latency reported
  for the edge.

  Only tags that identify a node take part in the hash. Tags that vary per
  message, such as `partition`, are reported but never hashed, because hashing
  them would split one node into a node per partition and stop the downstream
  produce edges from joining up.
  """

  alias DdDataStreams.Tags

  # FNV-1 (multiply, then xor), 64-bit.
  @fnv_offset_basis 0xCBF2_9CE4_8422_2325
  @fnv_prime 0x0000_0100_0000_01B3
  @mask 0xFFFF_FFFF_FFFF_FFFF

  @type t :: %__MODULE__{
          hash: non_neg_integer(),
          pathway_start: non_neg_integer(),
          edge_start: non_neg_integer()
        }

  @typedoc """
  The measurement a checkpoint produces. `pathway_latency` and `edge_latency`
  are in milliseconds and measure back from the checkpoint's `timestamp`.
  """
  @type checkpoint :: %{
          hash: non_neg_integer(),
          parent_hash: non_neg_integer(),
          tags: [String.t()],
          timestamp: integer(),
          pathway_latency: non_neg_integer(),
          edge_latency: non_neg_integer()
        }

  defstruct hash: 0, pathway_start: 0, edge_start: 0

  @doc """
  Builds a pathway from the given fields, defaulting missing ones to zero.

      iex> DdDataStreams.Pathway.new(hash: 234)
      %DdDataStreams.Pathway{hash: 234, pathway_start: 0, edge_start: 0}

      iex> DdDataStreams.Pathway.new(edge_start: 1700003600000)
      %DdDataStreams.Pathway{hash: 0, pathway_start: 0, edge_start: 1700003600000}
  """
  @spec new(keyword() | map()) :: t()
  def new(fields) do
    fields = Map.new(fields)

    %__MODULE__{
      hash: Map.get(fields, :hash, 0),
      pathway_start: Map.get(fields, :pathway_start, 0),
      edge_start: Map.get(fields, :edge_start, 0)
    }
  end

  @doc """
  Returns the hash of the node identified by `service`, `env`, `primary_tag` and
  `tags`.

  The fields are concatenated without a separator: the base hash over service,
  environment and primary tag, followed by the hashable edge tags in byte order.
  Tags outside the hashable vocabulary are dropped; a tag repeated in `tags`
  takes part as many times as it appears.
  """
  @spec node_hash(String.t(), String.t(), String.t() | nil, [String.t()]) :: non_neg_integer()
  def node_hash(service, env, primary_tag, tags) do
    [service, env, primary_tag || "", tags |> Tags.filter(:hash) |> Enum.sort()]
    |> IO.iodata_to_binary()
    |> fnv1()
  end

  @doc """
  Chains a node hash onto the hash of the node that sent the payload.

  The two hashes are hashed as 64-bit unsigned little-endian integers, node
  first, so a downstream node can be computed from the parent it received.
  """
  @spec pathway_hash(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def pathway_hash(node_hash, parent_hash) do
    fnv1(<<node_hash::little-64, parent_hash::little-64>>)
  end

  @doc """
  Samples one pathway from a fan-in, so a batch that consumed several pathways
  continues exactly one of them. An empty list yields a fresh pathway.
  """
  @spec merge([t()]) :: t()
  def merge([]), do: %__MODULE__{}
  def merge(pathways), do: Enum.random(pathways)

  @doc """
  Sets a checkpoint for the node reached by `tags` at `:now` (milliseconds) and
  returns the checkpointed pathway together with the measurement.

  The first checkpoint of a pathway starts it at `:now`, or at the `:pathway_start`
  override. Both starts can be overridden per checkpoint: the consume side uses
  that to measure from the time the broker received the message rather than the
  time the consumer picked it up.

  Options:

    * `:now` - checkpoint time in milliseconds, defaults to the current time
    * `:service`, `:env`, `:primary_tag` - identity of the node; see `node_hash/4`
    * `:pathway_start`, `:edge_start` - override the respective start time
  """
  @spec set_checkpoint(t(), [String.t()], keyword()) :: {t(), checkpoint()}
  def set_checkpoint(%__MODULE__{} = pathway, tags, opts \\ []) do
    now = Keyword.get_lazy(opts, :now, &now_ms/0)

    pathway =
      if pathway.pathway_start == 0 do
        start = Keyword.get(opts, :pathway_start, now)
        %{pathway | pathway_start: start, edge_start: start}
      else
        pathway
      end

    pathway =
      case Keyword.fetch(opts, :edge_start) do
        {:ok, edge_start} -> %{pathway | edge_start: edge_start}
        :error -> pathway
      end

    pathway =
      case Keyword.fetch(opts, :pathway_start) do
        {:ok, pathway_start} -> %{pathway | pathway_start: pathway_start}
        :error -> pathway
      end

    parent_hash = pathway.hash

    hash =
      node_hash(
        Keyword.get(opts, :service, ""),
        Keyword.get(opts, :env, ""),
        Keyword.get(opts, :primary_tag),
        tags
      )
      |> pathway_hash(parent_hash)

    checkpoint = %{
      hash: hash,
      parent_hash: parent_hash,
      tags: tags,
      timestamp: now,
      pathway_latency: max(now - pathway.pathway_start, 0),
      edge_latency: max(now - pathway.edge_start, 0)
    }

    {%{pathway | hash: hash, edge_start: now}, checkpoint}
  end

  defp now_ms, do: System.system_time(:millisecond)

  defp fnv1(binary), do: fnv1(binary, @fnv_offset_basis)

  defp fnv1(<<>>, hash), do: hash

  defp fnv1(<<byte, rest::binary>>, hash) do
    # BEAM integers are unbounded, so mask the product back to 64 bits.
    fnv1(rest, Bitwise.bxor(Bitwise.band(hash * @fnv_prime, @mask), byte))
  end
end
