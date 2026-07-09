# Weft

*Chat, threads, and work tracking, woven into one self-hosted fabric.*

Weft fuses what Slack, Zulip, and Jira do into a single open-source,
self-hostable platform: one discussion primitive (the thread), one work
primitive (the work item — which owns a thread), one event log underneath
everything, and AI agents as first-class, permission-checked members.

**Status: M0 (skeleton).** The design lives in a sibling repo of ADRs
(ADR-001..014 + red-team review + milestones); the code is catching up to it.

## Layout

- `cmd/weftd/` — the server binary
- `internal/brand/` — the product name lives here and only here
  (see `docs/BRANDING.md`)
- `migrations/` — Postgres schema; `0001_event_log.sql` is the spine

## Build

```sh
go build ./...
./weftd version
```

## License

Server: AGPL-3.0-only (LICENSE file pending; SPDX headers to follow).
Clients will be MIT.
