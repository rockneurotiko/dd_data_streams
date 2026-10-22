defmodule DdDataStreams.Context do
  @moduledoc """
  The pathway the current process is producing from.

  A produce checkpoint needs the pathway the message being answered was
  consumed on, and passing it through every function signature of a pipeline is
  noise. Instead the stage that consumed the message leaves the pathway here,
  and the produce call in the same process picks it up. The value lives in the
  process dictionary, so it belongs to one process: a processor stage, a batch
  processor and a task spawned from either do not see each other's pathway.

  `DdDataStreams.attach/1` writes this context for both kinds of Broadway
  stage - the pathway of the message a `handle_message/3` call is about to
  receive, and the fan-in pathway of a `handle_batch/4` call - so an application
  that only publishes from those callbacks never touches this module. Set it
  yourself in a stage the library does not know about:

      pathway = my_stage_consume_pathway
      :ok = DdDataStreams.Context.put(pathway)
      DdDataStreams.publish(topic, message, &MyApp.Publisher.publish/1)

  The context is per stage process and is overwritten by each message, so it is
  not cleared when a message is done: until the next message arrives it holds
  the pathway of the last one, which is why `clear/0` exists for a stage that
  produces outside the message it consumed.
  """

  alias DdDataStreams.Pathway

  @key :dd_data_streams_pathway

  @doc """
  Stores `pathway` as the process's current pathway.

      iex> DdDataStreams.Context.put(%DdDataStreams.Pathway{hash: 7})
      :ok

  """
  @spec put(Pathway.t()) :: :ok
  def put(%Pathway{} = pathway) do
    Process.put(@key, pathway)
    :ok
  end

  @doc """
  Returns the process's current pathway, or `nil` when it has none.

      iex> DdDataStreams.Context.put(%DdDataStreams.Pathway{hash: 7})
      iex> DdDataStreams.Context.get()
      %DdDataStreams.Pathway{hash: 7, pathway_start: 0, edge_start: 0}

  """
  @spec get() :: Pathway.t() | nil
  def get, do: Process.get(@key)

  @doc """
  Removes the process's current pathway.

      iex> DdDataStreams.Context.put(%DdDataStreams.Pathway{hash: 7})
      iex> DdDataStreams.Context.clear()
      :ok
      iex> DdDataStreams.Context.get()
      nil

  """
  @spec clear() :: :ok
  def clear do
    Process.delete(@key)
    :ok
  end
end
