# Reality ledger — what's proven vs what's scaffolding

*The anti-self-deception document. Every subsystem is rated honestly; a claim
without a cited test is not "done." Update this in the SAME PR that changes a
subsystem's reality. Ratings:*

- **PROVEN** — behavior demonstrated by a cited test against real Postgres
- **WORKS-THIN** — runs end-to-end, known gaps listed
- **PLACEHOLDER** — deliberately fake stand-in; replacement designed
- **DORMANT** — schema/design exists, zero code paths exercise it

| Subsystem | Status | Evidence / gaps |
|---|---|---|
| Event log write+consume | **PROVEN** | `TestCommitOrderGate` (commit-order race defused), `TestCursorAdvance` (monotone cursors, backdated imports) |
| Gateway resume (F-2) | **PROVEN** | `TestMessageEndToEnd`: disconnect → send-while-away → reconnect(last_id) → gap replays; checkpoint heartbeats |
| Outbox rule (write+event, one tx) | **PROVEN** | send path in `server.sendMessage`, asserted via live delivery in e2e |
| Schema (all 7 migrations) | **PROVEN to apply** / **DORMANT in bulk** | applies clean on PG17 + constraint smoke tests; but work-tracking, automations, files, notifications, compliance tables have ZERO code exercising them — shape bugs there are undiscovered by definition |
| Auth | WORKS-THIN | bcrypt + hashed bearer sessions, 401 negative tested; **login brute-force rate-limited (PROVEN: `TestAuthRateLimit`)**. Gaps: session revocation endpoint, 2FA (M4+) |
| Transport quality (errors, limits, middleware) | **PROVEN** | error taxonomy via one mapper (`TestErrorTaxonomy`), per-IP pre-auth + per-user API limiters with bounded memory (janitor), request-id/logging/panic-recovery middleware, 1 MiB body caps |
| Permission model | **PROVEN (core)** | (verb,scope)→group resolver with closure lookup, live in the send path. `TestResolverCore` (nested-group transitivity, deny-by-default, allow-after-join), `TestMostSpecificWins` (channel override beats org default). Gaps: ~7 of ~40 verbs registered; profiles/custom groups have no API; grant∩permission for agents (CC-6) is a hook; read path (Get) still membership-join; item-scope VisibilityScope unenforced |
| Message rendering | **PROVEN (core)** | Portable AST engine live in the send path: CommonMark+GFM (tables/strike/tasklists/autolinks), chat hard-breaks, typed @**name** mentions resolved in-tx (ids in render + event payload), :emoji:, XSS-safe by construction (no raw HTML path; scheme allowlist) — `TestCommonMarkAndGFM`, `TestXSSNeutralized`, `TestMentions`, `TestRichContentEndToEnd`. Gaps: full emoji data table (20 starter codes), spoiler/math/quote-reply syntax, ADF/mrkdwn import converters (importer milestone), re-render-on-version-bump job |
| Gateway ACL filter | WORKS-THIN | membership-set filter + refresh on membership events. Missing: history_mode/protected floor, VisibilityScope, the client-side snapshot-refetch rule (no client exists) |
| Threads | **PROVEN (core)** | create (titled/untitled, root message in one tx), retitle, resolve/reopen (idempotent), keyset-paginated list (root excluded), thread messages with before-cursor; F-15 root protections and the create_thread/edit_thread_title/resolve_threads verbs proven end-to-end (`TestThreadLifecycle`, incl. channel-scope verb override). Gaps: move/propagate between channels, DM/Space-governed threads, thread subscriptions (follow/mute), thread events not yet consumed by any UI |
| Read state | **PROVEN (durable core)** | F-7 watermark live: monotone clamped mark-read (`POST /threads/{id}/read`), per-channel unreads excluding own messages (`GET /unreads`) — `TestReadWatermarks`. Deliberately NOT event-logged (highest-volume action; scale contract). Gaps: live multi-device sync rides the ephemeral WS plane (next PR), per-thread unread breakdown, mention badges, F-17 materialized counters at scale |
| Search / notifications / files / work items / automations / compliance | DORMANT | schema + ADRs only |
| Importers (founding requirement) | DORMANT | zero code; Zulip importer is the M1 showcase |
| Load generator | **PROVEN** | `cmd/loadgen` + `internal/loadtest`: many-tenant HTTP+WS load, id-correlated delivery latency, histogram percentiles. Unit-tested (`histogram_test.go`); used for the numbers below |
| Scale — code path | **MEASURED** | uncontended full send = ~3.8 ms (auth+perms+AST+insert+event+notify+commit). Stable under 150 tenants / 450 concurrent WS with **zero errors**. See docs/PERF.md |
| Scale — per-node capacity | **NOT YET MEASURED (rig-bound)** | laptop rig conflates loadgen+server+Docker-VM Postgres on 8 cores (VM at 228% CPU was the ceiling, not weftd at 63%). A true per-cell number needs dedicated hosts (PERF.md has the procedure). No CI perf floor yet — would be environment-bound on the shared runner |
| Clients | ABSENT | curl + tests are the only consumers; every UI claim is future |

## Standing rules
1. A subsystem may not be marked done anywhere (README, PRs, timeline) at a
   rating higher than this ledger's.
2. Every PR that touches a subsystem updates its row — reviewers check the row
   against the diff.
3. "DESIGNED" is never evidence. Tests against real infrastructure are.
