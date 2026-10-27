# Triage label vocabulary

org-tracker's own state tags already match the five canonical triage roles
one-for-one - no relabeling needed, only the write commands that set them.

| Canonical role | Tag or close written by `org-tracker` |
|---|---|
| `needs-triage` | `:NeedsTriage:` state tag |
| `needs-info` | `:NeedsInfo:` state tag |
| `ready-for-agent` | `:ReadyForAgent:` state tag |
| `ready-for-human` | `:ReadyForHuman:` state tag |
| `wontfix` | `:WontFix:` state tag, plus closing as `CANCELED` - never `DONE` |

Set a state tag with `org-tracker ticket tags`. `wontfix` is the one role
that is not a bare tag write: record the reason with `org-tracker rejection
add` when it's a new rejected concept, then close the ticket with
`org-tracker ticket status --keyword CANCELED`. A request that matches a
concept already on record does not go through `rejection add` again - see
the **org-tracker** skill's `TRACKER.md`, under "Per-skill operations" for
`triage`, for that case and the full label vocabulary, including the
category tags this axis is kept separate from.
