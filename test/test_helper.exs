# Modules whose behaviour a test may replace. Copying one changes nothing on
# its own: Mimic only takes over for the tests that set an expectation, and only
# for the processes those expectations are allowed into.
# The library is on by default, so the application starts its aggregator at
# boot. Each test starts the one it needs, under the same registered name, so
# the application's is stopped here rather than left running for the suite.
Application.stop(:dd_data_streams)

Mimic.copy(DdDataStreams.Aggregator)
Mimic.copy(DdDataStreams.Transport.Req)

# The default suite is the protocol core: it needs nothing but Elixir, so it
# runs anywhere and stays fast. The tiers that talk to a broker or an agent are
# tagged instead of skipped, so a plain `mix test` cannot report them as passing
# while nothing was listening. Run them with `--only emulator` and
# `--only agent`, or both with `--include emulator --include agent`.
ExUnit.start(exclude: [:emulator, :agent])
