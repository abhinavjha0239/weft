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
- **Infra seams (standing directive): every infrastructure choice hides
  behind a platform interface chosen by config, so operators swap backends
  without touching core code.** First instance: `platform/blob.Store`
  (`WEFT_BLOB_DRIVER`) — `fs` for bare metal/any mounted volume ships
  first; S3/GCS/Azure are one file implementing the interface plus a
  factory case. The same pattern applies to any future cache, queue, or
  media dependency (the LiveKit seam is the founding example).

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

`consumer_lag{consumer,org}` counts what is COMMITTED and not yet acked, and
is measured WITHOUT the driver's delivery gate — deliberately. Measuring it
through `pg_snapshot_xmin` (as the xmin driver did until the S4 review) made
it read 0 during the exact stall it exists to surface, because the horizon
that stops delivery also stops the max id the query can see. A stopped
delivery mechanism must SHOW as rising lag under either driver, and
committed-but-not-yet-deliverable events count as lag, because that is what
they are. The unit is a driver detail (xmin: an id delta against
`event_consumer_cursor.last_id`; logical: an exact entry count against its
`lsn`); the contract is "0 means caught up, and it rises when a consumer
falls behind".

### 6.1 Runbook — the logical-decoding event feed (S4)

The event feed is a seam with two drivers, picked by
`WEFT_EVENT_FEED_DRIVER`:

- **`xmin` (default).** The polling consumer gated on
  `pg_snapshot_xmin(pg_current_snapshot())`. Nothing to provision — a small
  install and CI need no replication slot. The gate is DATABASE-GLOBAL: one
  long write transaction anywhere stalls delivery for every org, and the
  txid/id crossing described in `internal/eventlog/logical.go` can drop an
  event undetectably.
- **`logical`.** A logical-decoding reader. WAL order is commit order, so
  there is no gate and no crossing. It costs a replication slot, which
  **retains WAL** — the operator obligations below are not optional.

**Enable (operator steps, in order).**

1. Start Postgres with `wal_level = logical` (needs a restart) and leave
   room in `max_replication_slots` / `max_wal_senders`.
2. Create the publication and the slot ONCE per cell. This is deliberately
   not a migration: migrations run on every install, including the ones
   that never enable the feed, and a slot silently pinning WAL is not an
   acceptable side effect of `migrate`.

   ```sql
   CREATE PUBLICATION eventlog_pub FOR TABLE event_log
       WITH (publish_via_partition_root = true);
   SELECT pg_create_logical_replication_slot('eventlog_feed', 'pgoutput');
   ```

   (`eventlog.ProvisionLogical` performs exactly these two statements for
   an operator tool; `weftd serve` never calls it. Names are overridable
   with `WEFT_EVENT_FEED_SLOT` / `WEFT_EVENT_FEED_PUBLICATION`.)
3. Set `WEFT_EVENT_FEED_DRIVER=logical` and restart. A missing slot fails
   the reader loudly with the SQL above; it never degrades silently back to
   the poller.

**Single reader per cell.** A replication slot allows ONE active
connection, so exactly one process streams the feed. Other nodes retry and
their consumers report `ErrFeedNotReady` until they win the slot, which
makes the slot a crude but real takeover lease. Plan the feed onto one
node, or accept that consumers only run wherever the slot is held.

**Watch.** `eventlog_slot_lag_bytes{slot}` is the disk-fill risk in bytes:
the WAL the slot is retaining because some consumer has not acked. Alarm
well below free disk (a few GB on a small cell). `consumer_lag{consumer,
org}` shows WHICH consumer is behind, and
`eventlog_feed_evicted_total{org}` fires when a consumer fell further
behind than the in-memory commit-order window and its `Poll` starts
returning `ErrCursorTooOld`.

**Stuck slot — drop and resync.** A consumer that dies (or a node that is
never coming back holding the slot) makes `eventlog_slot_lag_bytes` climb
without bound. The remedy, in order of preference:

1. Fix or restart the consumer; the slot advances on its own.
2. If it cannot be recovered, DROP AND RESYNC — trade replay for disk:

   ```sql
   -- 1. stop the feed process (or set the driver back to xmin and restart)
   SELECT pg_drop_replication_slot('eventlog_feed');   -- frees the WAL now
   -- 2. reset the stuck consumer so it replays from scratch
   UPDATE event_consumer_cursor SET last_id = 0, lsn = NULL
    WHERE consumer = '<name>';
   -- 3. recreate the slot (step 2 of Enable) and start the feed again
   ```

   Resetting the cursor is the supported replay path (ADR-003 E2): a NULL
   `lsn` sends the consumer through the bootstrap lane, which walks the
   table in id order and then splices back onto the live feed. Consumers
   are idempotent (the outbox rule), so replay is safe — it re-does work,
   it does not double-apply it.

**Roll back** at any time by setting `WEFT_EVENT_FEED_DRIVER=xmin` and
dropping the slot: cursors keep their `last_id`, which is all the xmin
driver reads.

## 7. Testing strategy (established, now written)

Integration-first against real Postgres (`TEST_DATABASE_URL`, CI service,
`-p 1` — shared DB). Unit tests only for pure logic (AST transforms, rank
math). Every subsystem's "done" = its REALITY.md row cites a test. Perf: a
load harness with a CI floor is an M1 exit criterion (blueprint §3.6).

**Quality gates (CI):** gofmt, go vet, staticcheck, govulncheck (gates on
vulnerabilities our code *calls*, not mere go.sum presence), the brand-token
greps, and a cross-package statement-coverage floor (72% at introduction,
measured with `-coverpkg=./internal/...` because domain modules are
exercised through the rest-layer integration tests; the floor ratchets up,
never down). CodeQL and Codecov are the go-public upgrades — both
paid/limited for private repos; revisit at the licensing decision.

## 8. Known debt against this document

~~M0's `internal/server` mixed transport and domain~~ — resolved by the M1
opener: `domain/identity` + `domain/messaging` + `transport/rest` with
`db.WithTx`, the apperr taxonomy, and rate-limited middleware. Remaining
debt: `perms.Require` is still the membership placeholder (next PR), and
legacy platform packages (db, eventlog, auth, config, enum, brand) sit at
`internal/*` rather than `internal/platform/*` — move opportunistically,
never in a feature PR.

## 9. How a feature gets built (the loop)

ADR (design repo) → schema migration (if new tables: append-only files) →
module service + store + integration test → transport handlers → REALITY.md
row update → PR with scale review → squash into dev. Nothing skips the ledger.
