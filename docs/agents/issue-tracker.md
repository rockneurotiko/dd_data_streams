# Issue tracker: org-tracker

Issues and specs for this repo live as org-roam feature nodes, read and
written through the `org-tracker` CLI. Load the **org-tracker** skill before
touching any of it - its `SKILL.md` and `TRACKER.md` document the entity
model, exit codes and write-refusal codes this file assumes, and are the
source of truth if anything here goes stale.

## Conventions

- **Publish a new spec**: `org-tracker feature new --title "<Feature>"
  --spec-file spec.org`.
- **Publish tickets against an existing spec**: `org-tracker ticket new`, one
  per ticket or batched with `--batch-file`.
- **Fetch the relevant ticket**: `org-tracker ticket show --ticket NN`.
- **List / survey open work**: `org-tracker ticket list`, `--frontier` for
  what's actually pickable right now.
- **Comment**: `org-tracker ticket comment --heading "..."`.
- **Apply state**: two separate axes - `org-tracker ticket status
  --keyword ...` for where the work is, `org-tracker ticket tags ...` for
  whether it's specified well enough to hand off. See the label vocabulary in
  `docs/agents/triage-labels.md` when present.
- **Close**: `org-tracker ticket status --keyword DONE`, or `CANCELED`
  alongside `org-tracker rejection add` for a rejection.

The full operation-by-operation mapping - what each engineering skill's own
steps mean in terms of these commands - already lives in the org-tracker
skill's `TRACKER.md`, under "Per-skill operations". Read that instead of
duplicating it here; this file only says which tracker is configured, not how
every skill uses it.

## Wayfinding operations

Where a wayfinding-style skill is installed, the map is the feature node
itself, its child tickets are decision tickets under it, and
blocking/frontier/claim/resolve are the same `org-tracker` operations as any
other ticket. See TRACKER.md's "Wayfinding operations" table for the exact
mapping. Nothing here is required if no such skill is present.
