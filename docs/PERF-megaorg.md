# Mega-org proof — the 100k operator procedure

*This is the environment-bound sibling of PERF.md. It documents how to drive a
single 100k-member org and read the scale signals off the running server. It is
NOT a CI gate — the shared runner cannot host a real mega-org (the PERF.md VM-tax
lesson). CI ships the bounded smoke instead: `TestMegaOrgHarnessSmoke`
(internal/loadtest) provisions a 2k-member channel with 200 connections and one
send, and pins that the pump-query counter rose by ~connection-count. The real
number is what an operator produces with the procedure below.*

## What it proves

The cluster's north star is that NO per-message cost may scale with membership
or connection count. Two costs must be watched as an org grows huge:

1. **Gateway fan-out** — today each connection runs its OWN catch-up query on
   every wake, so one message to a channel with N connections costs ~N queries
   (`gateway_pump_queries_total`). This is the S3 blowup; the harness makes it a
   number BEFORE S3's per-org multicast fixes it.
2. **Closure rebuild** — a full-org permission-closure rebuild is O(org group
   graph) and runs on every group edit (`closure_rebuild_seconds`). The harness
   times one injected edit against the mega-org.

## Setup (separate hosts — do not co-locate)

The laptop rig conflates loadgen + server + database and tops out on the VM tax,
not the server (PERF.md rig #1). For a real number put each on its own host, and
run the server with the expvar metrics driver so the counters are readable:

```
# database host: native Postgres, max_connections tuned well above the
# server pool + a headroom for provisioning.

# server host — expvar exposes the counters at /debug/vars:
WEFT_DATABASE_URL='postgres://…/weft?pool_max_conns=200' \
WEFT_LISTEN_ADDR=:8080 \
WEFT_METRICS_DRIVER=expvar \
  weftd serve

# one or more load-agent hosts (loadgen provisions via the DB, drives over HTTP):
loadgen -mega \
  -db 'postgres://…/weft' -url http://SERVER:8080 \
  -members 100000 -conns 100000 -sends 50 -rate 40
```

Provisioning 100k members through the service layer is slow and is deliberately
OUTSIDE the timing window (PERF.md "what is measured vs setup"). It is the run's
dominant wall-time; **cache a provisioned org and re-point runs at it** rather
than re-provisioning each time (drop `-members` below the existing count to skip
the bulk insert, or keep the DB between runs).

## Reading the result

`loadgen -mega` prints, and the same numbers are live at
`http://SERVER:8080/debug/vars`:

- **pump queries `+X (Y per message)`** — Y is the headline. Today Y ≈ connection
  count: the O(N) fan-out. After S3, Y should be ~O(1). Watch
  `gateway_pump_queries_total` and `gateway_connections{org}`.
- **delivery p50/p99** — id-correlated end-to-end latency, the same histogram the
  many-tenant load test uses.
- **closure rebuild `Z s`** — wall-time of one injected group edit on the
  mega-org; also published as `closure_rebuild_seconds`. This is the number the
  perms scale-tier (incremental / async rebuild) must beat.
- **consumer lag** — `consumer_lag{consumer,org}` at /debug/vars is THE health
  signal: max committed event id minus each consumer's cursor. It should hover
  near 0; a rising lag under load is a consumer that cannot keep up.

## Why this stays deferred as a CI floor

A true per-cell capacity figure is environment-bound (PERF.md); a shared CI
runner would measure the runner, not the server. So the 100k floor is an operator
procedure, and CI guards only that the instrument WORKS (the bounded smoke) — the
proving ground, shipped first, so every later scale slice attaches its red/green
pin to these real numbers.
