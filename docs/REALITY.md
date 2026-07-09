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
| Message rendering | PLACEHOLDER | HTML-escaped paragraph; Portable AST engine (ADR-007) not started. `message.ast` content is a stub shape |
| Gateway ACL filter | WORKS-THIN | membership-set filter + refresh on membership events. Missing: history_mode/protected floor, VisibilityScope, the client-side snapshot-refetch rule (no client exists) |
| Threads | WORKS-THIN | channel-root flow only (F-15 counter rule honored). No create-titled-thread, resolve, move, or subscription endpoints |
| Read state | DORMANT | watermark tables exist; no API reads/writes them |
| Search / notifications / files / work items / automations / compliance | DORMANT | schema + ADRs only |
| Importers (founding requirement) | DORMANT | zero code; Zulip importer is the M1 showcase |
| Scale claims | **DESIGNED, NOT MEASURED** | scale contract documented; no load test, no benchmark. Connection/node and msg/s numbers are targets until a perf harness exists (M1 exit criterion: CI perf floor per blueprint §3.6) |
| Clients | ABSENT | curl + tests are the only consumers; every UI claim is future |

## Standing rules
1. A subsystem may not be marked done anywhere (README, PRs, timeline) at a
   rating higher than this ledger's.
2. Every PR that touches a subsystem updates its row — reviewers check the row
   against the diff.
3. "DESIGNED" is never evidence. Tests against real infrastructure are.
