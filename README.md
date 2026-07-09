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

**TBD — deliberately undecided** while the commercial model is chosen
(dual-license w/ CLA vs open-core vs BSL). No LICENSE file is committed on
purpose; all rights reserved by the sole author until then. **No external
contributions are accepted before the licensing decision** — outside code
without a CLA would foreclose the proprietary/dual-license options.

## Development workflow

- `main` — stable; only receives merges from `dev` at milestone cuts.
- `dev` — integration branch and the repo default; all work lands here.
- **One branch per unit of work**, PR into `dev`, squash-merge, delete the
  branch. No direct pushes to `dev` or `main`.

### Branch naming: `<category>/<module>-<slug>`

Machine-relatable by construction: the category says the KIND of change, the
module says WHERE (the ARCHITECTURE.md module map), the slug says WHAT.
Lowercase, hyphens, no milestone numbers (milestones live in MILESTONES.md).

Categories (fixed vocabulary):
`feat` new functionality · `fix` bug fix · `perf` performance ·
`refactor` structure, no behavior change · `schema` migrations ·
`test` test-only · `docs` documentation · `ci` CI/tooling ·
`sec` security · `chore` deps/housekeeping

Modules: the ARCHITECTURE.md map (`eventlog`, `gateway`, `rest`, `identity`,
`perms`, `messaging`, `content`, `worktrack`, `files`, `notify`, `importer`,
`automation`, `compliance`, `platform`) plus `schema`, `repo` for repo-wide.

Examples:
- `feat/messaging-thread-endpoints`
- `feat/messaging-read-watermarks`
- `perf/gateway-org-multicast`
- `schema/notify-deliverability-set`
- `fix/content-mention-escaping`
- `feat/importer-zulip-adapter`

The triangle stays consistent: branch `feat/messaging-thread-endpoints` →
commit/PR title `messaging: Add thread endpoints.` → REALITY.md row
`messaging` updated in the same PR.

**Machine-enforced** (not just documented): the `conventions` CI job rejects
malformed branch names and PR titles and fails any PR that touches
`internal/` without updating `docs/REALITY.md`; `dev` is branch-protected
(required checks: `test` + `conventions`, admins included, no force pushes);
merges are auto-merge-on-green; merged branches auto-delete.
