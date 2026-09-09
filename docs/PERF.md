# Performance — methodology and measured numbers

*Honest measurements, not marketing. The load generator (`cmd/loadgen`,
`internal/loadtest`) provisions many tenants via the service layer, then drives
real HTTP sends and real WebSocket fan-out against a running server, correlating
end-to-end delivery latency by message id. Re-run any claim here yourself.*

## What is measured vs setup

- **Setup (not measured):** tenant provisioning goes straight through
  `identity.Service` against the DB — fast, and deliberately outside the timing
  window.
- **Measured hot path (100% real):** `POST message → one transaction (auth +
  permission resolve + AST parse + insert + event append + NOTIFY) → gateway
  fan-out → subscriber receipt`. Over real HTTP + WebSocket + Postgres.

## The number that matters: uncontended send latency

A single send, full path (auth token lookup, `(verb,scope)→group` resolve, AST
parse+render, message insert, event append, `pg_notify`, commit):

```
~3.8 ms  (localhost, warm)
```

That is the quality signal: the write path is cheap. Everything below is about
how many of these run concurrently before a *given environment* saturates.

## Rig #1 — laptop, everything co-located (a smoke test, NOT a capacity number)

macOS (8 cores) · Postgres 17 in Docker · **server + Postgres-VM + load
generator all on the same 8 cores** · 150 tenants · 450 concurrent WebSocket
connections · 20 s window.

| Metric | Result |
|---|---|
| Send throughput | ~1,000 msg/s |
| Fan-out delivery | ~2,800 deliveries/s |
| Concurrent WS connections | 450, stable |
| Errors / 429s | **0 / 0** |
| Send latency p50 / p99 | ~135 ms / ~550 ms |
| Delivery latency p50 / p99 | ~136 ms / ~690 ms |

**Why the latency balloons from 3.8 ms to 135 ms here, and why it is NOT Weft:**
process CPU sampled mid-run showed the **Docker/macOS virtualization layer at
228%** (Postgres inside the Linux VM) plus Docker backend at ~124%, while
`weftd` used only ~63% of one core and `loadgen` ~16%. The ceiling on this rig
is the VM tax and three processes fighting 8 cores — not the server. On native
Linux with a dedicated database host these numbers are expected to be far
higher; this rig cannot prove a per-node capacity figure, only that the system
is correct and stable under concurrency with zero errors.

## Real findings the load test surfaced (about Weft, fixed/documented)

1. **pgx pool default is far too low.** Default `MaxConns = max(4, numCPU)` (≈8)
   made pool-wait dominate latency. Fixed: `db.Connect` now floors MaxConns at
   25; operators raise it per cell via `?pool_max_conns=N`.
2. **Pool size must respect `max_connections`.** An 80-conn server pool + a
   10-conn loadgen pool against Postgres's default `max_connections=100` produced
   `FATAL: sorry, too many clients` — exactly the "one Postgres tops out in the
   low hundreds of connections" note in SCHEMA.md, now empirically confirmed.
   A cell sizes its pool under its Postgres ceiling; beyond that it adds
   PgBouncer or splits.

## Reconnect storms: the resume lane's read is shared per cohort

S3 made a message cost ONE `event_log` read per org however many connections are
live. The RESUME lane stayed per connection, which is right for an occasional
reconnect and wrong for a node restart: every connection that node held comes
back at once, so the spike S3 removed returns at every deploy.

The premise was measured before the fix was built, and the obvious reason for it
is not the real one. Connections that die together do NOT automatically resume
together: the server-side cursor is identical for every live connection (the fan
advances past ACL-filtered rows), but a CLIENT resumes from the last seq it was
DELIVERED, which is per visibility class. On a 12-connection, four-channel,
skewed-traffic fixture:

```
org head 34 — busy channel (6 conns) resumed at 28, medium (3) at 33,
              quiet (2) at 34, SILENT-channel member at 0
              → spread 34, four distinct cursors for twelve connections
same fixture, checkpoint interval shortened:
              → every one of the twelve at 34.  Spread 0.
```

What converges them is the **F-2 checkpoint**, which hands every connection the
org-wide cursor (ACL gaps included) every `checkpointInterval`. So a cohort's
cursor spread is bounded by one checkpoint interval of org events, and one read
serves it: the shard keeps the last resume read as a shared block, single-flighted,
usable only by connections that arrived before it was read (so it can never be
staler than a connection's own read would have been) and only where it both
starts at or below their cursor and reaches past it (so a far-behind resumer
reads alone instead of widening anyone's read). `gateway_resume_reads_total` is
the series; measured on the pin, 8 connections cost 3 reads and 24 cost 2, where
the per-connection lane cost 9 and 25.

Not free, and worth stating: a reconnect still costs its own `Tail.Head` plus two
membership queries before it reaches the resume lane. This removes the heaviest
of the four, not all four.

## Known scale-tier optimizations (not yet needed, documented for when they are)

- **Gateway per-org multicast** (`perf/gateway-org-multicast`, future): today
  each subscriber connection runs its own catch-up query on wake. Verified NOT
  the current ceiling (send-only throughput matched fan-out throughput), but at
  high fan-out the dispatcher should query once per org and fan rows to that
  org's connections in memory.
- ~~**Event-log consumer via logical decoding**~~ — BUILT (S4), opt-in behind
  the `eventlog.Feed` seam: `WEFT_EVENT_FEED_DRIVER=logical` streams a
  replication slot so WAL order (= commit order) drives delivery and the
  DB-global xmin gate disappears. The poller stays the default (no slot, no
  `wal_level=logical`); the slot's WAL-retention risk has metrics and a
  drop-and-resync runbook (ARCHITECTURE.md §6.1). Measured on the pins, not
  the rig: with a long write transaction open in org A, an unrelated org B
  sends and consumes with lag 0 under the logical feed and is stalled to 0
  rows under the poller.

## How to run a real capacity benchmark

The laptop rig conflates load generator, server, and database. For a true
per-cell number, put each on its own host:

```
# database host: native Postgres, max_connections tuned
# server host:
WEFT_DATABASE_URL='postgres://…/weft?pool_max_conns=150' WEFT_LISTEN_ADDR=:8080 weftd serve
# one or more load-agent hosts:
loadgen -db 'postgres://…/weft' -url http://SERVER:8080 -orgs 500 -subs 5 -rate 40 -duration 60s
```

Fleet capacity (the "×100" target) = per-cell capacity × cell count; the cell
invariant (no cross-org coordination) is what makes that multiplication honest.
