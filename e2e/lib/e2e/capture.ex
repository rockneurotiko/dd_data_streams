defmodule E2e.Capture do
  @moduledoc """
  A transport that writes the flushed body to disk instead of posting it.

  The aggregator reads the transport once, when it starts, so the script sets
  this one before it starts the application. The files it writes are what the
  decoder reads, and they are the exact bytes the agent would have received.
  """

  @behaviour DdDataStreams.Transport

  @impl true
  def post(url, headers, body) do
    dir = System.get_env("CAPTURE_DIR", "capture")
    File.mkdir_p!(dir)
    stamp = System.system_time(:microsecond)

    File.write!(Path.join(dir, "#{stamp}.bin"), IO.iodata_to_binary(body))
    File.write!(Path.join(dir, "#{stamp}.meta"), inspect(%{url: url, headers: headers}))

    {:ok, 202}
  end
end
