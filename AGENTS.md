# DD Data Streams

Elixir library for data streams over Broadway pub/sub.

## Design motto: generic by default

The library is generic, so anything broker-shaped lives in exactly one of two
places: the generic core, or an adapter the host application configures.
Per-broker branches inside the core are not an option.

The pathway header rule is the pattern to copy: an entry is a tuple whose first
element is the key and whose last element is the value, whatever arity the
broker uses, so Kafka `{key, value}` pairs and AMQP `{key, type, value}`
triples both follow from one rule with no broker named anywhere. Whatever
cannot be said generically becomes a configured adapter instead of a special
case.

## Tests

Start the Pub/Sub emulator in Docker on port 8085 before running the suite. The
`emulator` and `agent` tiers are excluded from `mix test` by default, so only a
running emulator lets them reach a real broker; use the command under `###
emulator` in the README with project `dsm-test`, then run that tier with
`PUBSUB_EMULATOR_HOST=127.0.0.1:8085 mix test --only emulator`.

## Agent skills

### Issue tracker

Issues and specs are tracked with org-tracker. See
`docs/agents/issue-tracker.md`.

### Triage labels

Triage labels are documented in `docs/agents/triage-labels.md`.


## Subagents

When spawning subagents for work in this repository:

- Restrict all file searches and commands to this repository root (`/home/rock/Git/dd_data_streams`). Never run `find`, `grep`, `rg`, or `sed` from `/` or another project.
- Always set an explicit timeout on shell commands; avoid unbounded or recursive searches.
- Use targeted paths and file names; prefer `rg`/`grep` within `lib/`, `test/`, or `docs/`.
- Do not run commands that can block indefinitely (for example `sed` over large trees) without a timeout.
