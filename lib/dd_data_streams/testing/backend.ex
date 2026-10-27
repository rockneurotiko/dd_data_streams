if Code.ensure_loaded?(NimbleOwnership) do
  defmodule DdDataStreams.Testing.Backend do
    @moduledoc """
    An owner-scoped backend that records checkpoints per test, with
    `NimbleOwnership`.

    Configure it in the test environment and start its ownership server from
    `test_helper.exs`:

        # config/test.exs
        config :dd_data_streams, backend: DdDataStreams.Testing.Backend

        # test/test_helper.exs
        {:ok, _} = DdDataStreams.Testing.Backend.start_link()

    A process records only when it owns the checkpoints or was allowed to use
    them, so a test that never opts in sees the library exactly as if it were
    disabled. `DdDataStreams.Testing` is the public interface; this module is
    the seam it installs.

    The ownership server is private by default, which is what makes concurrent
    tests independent. `DdDataStreams.Testing.set_global/1` switches it to the
    shared, single-owner mode for an `async: false` case. Production code never
    starts this server and never depends on it running: without it `enabled?/0`
    is false and `record/1` drops the checkpoint.
    """

    @behaviour DdDataStreams.Backend

    alias NimbleOwnership

    @server __MODULE__
    @key :checkpoints
    @timeout 5_000

    @doc """
    Starts the ownership server under this module's name.

    Accepts the options `NimbleOwnership.start_link/1` accepts. Calling it when
    the server already runs returns `{:error, {:already_started, pid}}`, as any
    named process does; a test helper usually starts it once.
    """
    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts \\ []) do
      NimbleOwnership.start_link(Keyword.put_new(opts, :name, @server))
    end

    @doc """
    A child specification for starting the ownership server in a supervision
    tree, instead of from `test_helper.exs`.
    """
    @spec child_spec(keyword()) :: Supervisor.child_spec()
    def child_spec(opts) do
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [opts]},
        type: :worker,
        restart: :permanent,
        shutdown: 500
      }
    end

    @impl true
    def enabled? do
      case resolved_owner(callers()) do
        {:ok, _owner} -> true
        :error -> false
      end
    end

    @impl true
    def record(checkpoint) do
      point = point(checkpoint)

      with {:ok, owner} <- resolved_owner(callers()),
           {:ok, subscribed?} <- append(owner, point) do
        if subscribed?, do: send(owner, {DdDataStreams.Testing, :checkpoint, point})
        :ok
      else
        _unowned -> :ok
      end
    end

    @doc """
    Makes `owner` an owner of the checkpoints, so its own calls and its
    `$callers` record into it.
    """
    @spec start(pid() | atom()) :: :ok
    def start(owner \\ self()) do
      owner = find_pid(owner)
      ensure_owner(owner)
    end

    @doc """
    Allows `allowed` to record into `owner`.
    """
    @spec allow(pid() | atom(), pid() | atom()) :: :ok | {:error, NimbleOwnership.Error.t()}
    def allow(owner, allowed) do
      owner = find_pid(owner)
      allowed = find_pid(allowed)

      NimbleOwnership.allow(server!(), owner, allowed, @key, @timeout)
    end

    @doc """
    The checkpoints recorded for `owner`, oldest first.
    """
    @spec checkpoints(pid() | atom()) :: [map()]
    def checkpoints(owner \\ self()) do
      owner = find_pid(owner)

      case Process.whereis(@server) do
        nil ->
          []

        _server ->
          case NimbleOwnership.get_owned(@server, data_owner(owner), %{}, @timeout) do
            %{} = owned -> owned |> Map.get(@key, %{}) |> Map.get(:points, []) |> Enum.reverse()
            _none -> []
          end
      end
    end

    @doc """
    Subscribes the calling owner to `{DdDataStreams.Testing, :checkpoint, point}`
    messages, in addition to the pull API in `checkpoints/1`.
    """
    @spec subscribe() :: :ok
    def subscribe do
      case resolved_owner(callers()) do
        {:ok, owner} ->
          case NimbleOwnership.get_and_update(server!(), owner, @key, &subscribe_one/1, @timeout) do
            {:ok, _} -> :ok
            {:error, error} -> raise_error(error)
          end

        :error ->
          raise "no owner is recording for #{inspect(self())}; call DdDataStreams.Testing.start/1 first"
      end
    end

    @doc """
    Switches the ownership server to private mode, the default.
    """
    @spec set_private() :: :ok
    @spec set_private(map()) :: :ok
    def set_private(_context \\ %{}) do
      NimbleOwnership.set_mode_to_private(server!())
    end

    @doc """
    Switches the ownership server to shared mode, with the calling process as
    the owner every caller records into.
    """
    @spec set_global(map()) :: :ok
    def set_global(%{async: true}) do
      raise "DdDataStreams.Testing cannot be global when the ExUnit case is async; " <>
              "remove \"async: true\" to use shared mode"
    end

    def set_global(_context) do
      NimbleOwnership.set_mode_to_shared(server!(), self())
    end

    @doc """
    Picks private mode for an async case and shared mode otherwise.
    """
    @spec set_from_context(map()) :: :ok
    def set_from_context(%{async: true}), do: set_private()
    def set_from_context(context), do: set_global(context)

    ## Ownership

    defp ensure_owner(owner) do
      case NimbleOwnership.get_and_update(server!(), owner, @key, &ensure_owner_meta/1, @timeout) do
        {:ok, _} ->
          :ok

        {:error, %NimbleOwnership.Error{reason: {:not_shared_owner, shared_owner}}} ->
          ensure_owner(shared_owner)

        # Already recording through another owner: nothing to initialise.
        {:error, %NimbleOwnership.Error{reason: {:already_allowed, _owner}}} ->
          :ok

        {:error, error} ->
          raise_error(error)
      end
    end

    defp ensure_owner_meta(nil), do: {nil, %{points: [], subscribe: false}}
    defp ensure_owner_meta(state), do: {nil, state}

    defp subscribe_one(nil), do: {nil, %{points: [], subscribe: true}}
    defp subscribe_one(state), do: {nil, %{state | subscribe: true}}

    defp append(owner, point) do
      NimbleOwnership.get_and_update(
        server!(),
        owner,
        @key,
        fn
          nil ->
            {false, %{points: [point], subscribe: false}}

          %{subscribe: subscribed?} = state ->
            {subscribed?, %{state | points: [point | state.points]}}
        end,
        @timeout
      )
    end

    defp resolved_owner(callers) do
      case fetch_owner(callers) do
        {:ok, owner} -> {:ok, owner}
        {:shared_owner, owner} -> {:ok, owner}
        :error -> :error
      end
    end

    defp fetch_owner(callers) do
      case Process.whereis(@server) do
        nil -> :error
        _server -> NimbleOwnership.fetch_owner(@server, callers, @key, @timeout)
      end
    end

    defp data_owner(owner) do
      case fetch_owner([owner]) do
        {:shared_owner, shared_owner} -> shared_owner
        {:ok, resolved} -> resolved
        :error -> owner
      end
    end

    # A Task and a process started by OTP both carry the pids that started them,
    # so a child records into the owner that spawned it without an explicit
    # allow/2.
    defp callers do
      [self() | List.wrap(Process.get(:"$callers"))]
    end

    defp point(checkpoint) do
      %{
        edge_tags: Map.get(checkpoint, :tags, []),
        hash: Map.get(checkpoint, :hash),
        parent_hash: Map.get(checkpoint, :parent_hash),
        pathway_latency: Map.get(checkpoint, :pathway_latency, 0),
        edge_latency: Map.get(checkpoint, :edge_latency, 0),
        payload_size: Map.get(checkpoint, :payload_size, 0),
        timestamp_type: "current"
      }
    end

    defp find_pid(pid) when is_pid(pid), do: pid

    defp find_pid(name) when is_atom(name) do
      Process.whereis(name) ||
        raise ArgumentError, "no process is registered as #{inspect(name)}"
    end

    defp server! do
      Process.whereis(@server) ||
        raise "the DdDataStreams test ownership server is not running; " <>
                "start DdDataStreams.Testing.Backend from test_helper.exs"
    end

    defp raise_error(%NimbleOwnership.Error{} = error) do
      raise "DdDataStreams.Testing ownership failed: #{inspect(error.reason)}"
    end
  end
end
