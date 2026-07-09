# Backend architecture — the LLD

*How the server is actually structured and how features get built. The HLD
lives in the design repo (blueprint + ADR-001..014); storage design in
SCHEMA.md; honesty ratings in REALITY.md. This document is the contract for
code structure — PRs that violate a rule here need a written reason.*

## 1. Shape: modular monolith with extraction seams

One Go binary. Modules are the future service boundaries; the discipline that
keeps extraction possible is ownership + no cross-module table access.

```
cmd/weftd/                  wiring only: config → pool → modules → transports
internal/
  platform/                 cross-cutting, imported by anyone
    (db, eventlog, config, brand, enum — today at internal/*, moving here
     opportunistically; auth token/session mechanics live here too)
  domain/                   the modules — business logic + their own SQL
    identity/               org, workspace, user, group, session      (M1)
    perms/                  the (verb,scope)→group resolver + closure (M1)
    messaging/              channel, thread, message, reactions, read (M1)
    content/                Portable AST: parse/render/convert        (M1)
    worktrack/              space, item, board, sprint                (M2)
    files/                  file, reference, storage backends         (M2)
    notify/                 resolution pipeline, schemes, delivery    (M2)
    importer/               source adapters → IR → domain writers     (M1 Zulip)
    automation/             rules, runs, agents                       (M3)
    compliance/             retention, holds, export, scrub           (M2+)
  transport/
    rest/                   HTTP handlers — thin translation only
    gateway/                WS fan-out (exists)
```

**Rules**
- `transport → domain → platform`. Never sideways at the store level: a module
  may not touch another module's tables; cross-module reads go through the
  other module's service API, cross-module reactions go through the event log.
- **The cell invariant (SCHEMA.md scale contract): no cross-org global state,
  sequences, transactions, or coordination — ever.** Capacity scales by adding
  cells; any feature needing cross-org anything uses projection (ADR-004
  style) or the routing directory, never shared state.
- Table ownership is written in each module's doc.go and mirrors SCHEMA.md.
- A module = `service.go` (exported API, takes ctx + domain types),
  `store.go` (all SQL — pgx directly, no ORM), `types.go`, `*_test.go`
  (integration, real PG — the established pattern).

## 2. The write path (the one pattern everything follows)

```
handler(rest) : decode → auth.Identity → call service → map error → encode
service(domain): db.WithTx(ctx, pool, func(tx):
                   1. permission check (perms.Require(tx, actor, verb, scope))
                   2. domain writes (own tables only)
                   3. eventlog.Append(tx, …)        ← same tx, always (outbox)
                 )
consumers      : eventlog.Consumer loops per concern (gateway hub today;
                 search indexer, notify producer, automation engine later) —
                 in-process goroutines now, extractable because they only
                 speak (event log, own tables).
```

`db.WithTx` (to add in M1) owns Begin/Commit/Rollback; services never hold a
transaction across network calls; transactions stay short (scale contract —
the xmin gate).

## 3. Error taxonomy

Domain returns typed sentinel errors only: `ErrNotFound`, `ErrForbidden`,
`ErrInvalid(msg)`, `ErrConflict(msg)`, `ErrUnauthorized`. One mapper in
transport/rest converts to status + `{"error": …}` envelope. SQL errors never
cross a service boundary unwrapped (`fmt.Errorf("…: %w")` internally; the
transport logs, the client gets the taxonomy).

## 4. Concurrency model

- Request goroutines: share nothing; state lives in Postgres.
- Singletons per process, started in cmd wiring, stopped by root ctx:
  gateway Hub dispatcher (exists), event consumers, schedulers (scheduled
  messages/reminders — M1). Each singleton: one owner goroutine, channels or
  small mutexed maps, no locks held across I/O.
- Context discipline: every blocking call takes ctx; write deadlines on WS
  sends (exists); no bare `go func` without an owner and a stop path.

## 5. API conventions

`/api/v1`; JSON snake_case (matches DB and wire events); errors are the
envelope above; ids are int64-as-JSON-number; timestamps RFC3339 UTC.
Writes are POST/PATCH/DELETE over REST (ADR-002 P3) and will take an
`Idempotency-Key` header once the helper lands (M1 — dedupe table keyed
(org, key), replays the stored response). List endpoints: cursor pagination
(`?after=<id>&limit=`), never offset.

## 6. Observability (M1 minimum)

`slog` everywhere with request-scoped attrs (request id, org, user via
middleware); one Prometheus registry (`/metrics`): HTTP latency/status,
gateway connections + queue depth, consumer lag per (consumer, org) — lag is
THE health signal of the whole design. pprof behind a debug flag.

## 7. Testing strategy (established, now written)

Integration-first against real Postgres (`TEST_DATABASE_URL`, CI service,
`-p 1` — shared DB). Unit tests only for pure logic (AST transforms, rank
math). Every subsystem's "done" = its REALITY.md row cites a test. Perf: a
load harness with a CI floor is an M1 exit criterion (blueprint §3.6).

## 8. Known debt against this document

M0 predates the LLD, deliberately small: `internal/server` mixes transport
and messaging domain (sendMessage + SQL in the handler package), and
platform packages sit directly under `internal/`. First M1 PR extracts
`domain/messaging` + `transport/rest` and introduces `db.WithTx` +
`perms.Require`; the move is mechanical and the e2e test pins behavior.

## 9. How a feature gets built (the loop)

ADR (design repo) → schema migration (if new tables: append-only files) →
module service + store + integration test → transport handlers → REALITY.md
row update → PR with scale review → squash into dev. Nothing skips the ledger.
