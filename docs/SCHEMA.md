# Schema design — performance & future-proofing rationale

*The migrations in `migrations/` materialize ADR-001..014 (design repo) plus
every red-team amendment. This file records WHY the schema is shaped this way,
so future changes don't accidentally undo a load-bearing decision.*

## Keys & types

- **BIGINT identity keys everywhere; no UUIDs on hot paths.** Half the index
  width of UUIDs, sequential insert locality (no B-tree fragmentation), cheap
  joins. External identity needs are served by provenance columns and signed
  API tokens, not by making every PK random.
- **SMALLINT type codes, never PG enums.** `ALTER TYPE ... ADD VALUE` can't run
  in a transaction and enums can't be removed; SMALLINT + a Go constant
  registry gives the same integrity via domain validation and free evolution.
- **TEXT everywhere (no VARCHAR(n))** — identical performance in Postgres;
  length rules that matter are CHECKs or domain validation.
- **TIMESTAMPTZ only.** Date predicates evaluate in the requester's timezone at
  the query layer (ADR-010 S-6, one rule).

## The five deliberate denormalizations

1. **`org_id` on every tenant-scoped row** — tenant isolation, per-org
   consumers (ADR-003 E2), and index locality for org-scoped scans.
2. **`channel_id`/`dm_space_id` copied onto `message`** — message fetch and
   search never join through `thread` on the hot path; `thread` stays the
   source of truth.
3. **`has_*` flags + `search_tsv` on `message`** — `has:link` / FTS predicates
   are index-only (ADR-010 S-3, Zulip's proven pattern).
4. **`thread.last_activity_at`/`message_count`** — thread lists sort without
   aggregation; deliberately NOT maintained for channel-root threads (F-15:
   no per-message hot row on the root).
5. **`work_item.resolved_at`** — derived from status category at transition
   (W-3); denormalized so "open items" is a partial-index scan, and the
   resolution trap stays structurally impossible.

## Read state: O(threads), never O(messages) (F-7)

`thread_read_watermark` (user, thread → last read id) + `message_user_flag`
(sparse exceptions: starred/mentioned/marked-unread/alert-word). Zulip's dense
per-(user,message) UserMessage table is their documented scaling ceiling — a
10k-member channel means 10k rows *per message sent*. Ours writes one watermark
row per reader per thread, ever. The channel-root thread makes the flat-channel
case a single watermark. `fillfactor=85/90` on the two hot-update tables keeps
updates HOT (heap-only tuples — no index churn).

## Permission checks: one join, not a recursion (F-16)

`user_group_closure` flattens nested group membership on group edits (a
background rebuild on org-wide edits), so every ACL check — search, files,
gateway fan-out — is one indexed lookup. `channel_member.history_from` is the
protected-history floor evaluated as a plain column predicate (F-16b).
`capability_grant` narrows (never extends) group permissions — CC-6.

## The event log (F-1/F-4)

Partitioned by id range: retention/compaction = partition drop (ADR-003 E6);
the DEFAULT partition means inserts never fail if partition maintenance lags.
`txid` gates consumers behind `pg_snapshot_xmin(pg_current_snapshot())` so the
commit-order race cannot skip events. Payload holds structural deltas +
revision references + content hash — never the only copy of content — which is
what makes GDPR erasure a revision-row delete (`scrub` cascade) instead of a
log rewrite, and keeps hash-chains valid across purges.

## Deletion & history (F-8)

Delete is revision-append: `message_revision` captures prior content with
`kind=delete`; live fields are cleared. Compliance export reads revisions;
retention/scrub purges them; the log row (structural) remains.

## Ephemeral state

`presence` is UNLOGGED — rebuildable from live connections, never in the event
log (ADR-002 P5), no WAL cost for heartbeats. Everything else durable.

## Future-proofing seams (deliberate)

- **Partitioning:** only `event_log` is partitioned now. `message` and
  `work_item` carry `org_id` in every query path, so hash-partitioning by org
  later is a table rewrite but no query change. Don't add partition keys to
  PKs prematurely — it taxes every FK today for a maybe-someday.
- **JSONB long tail** (`settings`, `fields`, `policy`, `definition`,
  `origin_meta`): vendor-specific and rare attributes evolve without
  migrations (ADR-001 D7); anything that becomes hot gets promoted to a
  column + backfill migration.
- **`applies_to BIGINT[]`** on field_def and `labels TEXT[]` on work_item:
  arrays + GIN beat join tables at this cardinality; promote to join tables
  only if per-element metadata appears.
- **Search:** `search_tsv` is backend-neutral (ADR-010 S-3) — PGroonga or an
  external engine can replace it behind the same query semantics; pgvector
  embeddings live in their own table when the AI lane lands (S-7), never
  inline (row width).
- **No FKs into `event_log`** — partitions get dropped; references are by id
  with no integrity dependency (e.g. `automation_run.trigger_event_id`).

## WorkItem description = the thread's root message

Jira's description field gets no column: an item's description IS the root
message of its owned thread (ADR-001 D2 taken to its conclusion). One content
system means descriptions get AST, edit revisions, search, and mentions for
free; Jira import writes description → root message and comments → replies.
The item view renders the root message as the description block.

## Scale contract (target: no architectural ceiling — "Slack × 100")

Standing rule: every PR gets an explicit scale review. The target is not a
number, it's the absence of a ceiling: capacity must scale by ADDING CELLS,
where a cell = one full deployment of this design (app nodes + Postgres)
owning a set of orgs. This is how Slack itself scales (cell/shard by team);
billions of actives = thousands of cells, each running the schema below.

**The cell invariant (the one rule that makes ×100 possible):**
NO cross-org global state or coordination may ever enter the design — no
global sequences, no cross-org transactions, no global uniqueness except at
the routing layer (org slug → cell, a directory service when multi-cell
arrives). Everything already complies: the event log orders per-org, every
row carries org_id, and cross-org sharing (ADR-004) works by PROJECTION
between orgs — peer-style, never shared state — which is exactly
cell-compatible. A PR that introduces cross-org coordination fails review
regardless of how convenient it is.

Within a cell, the schema's sharding seam is `org_id` on every row; a single
Postgres serves the small case, org-hash sharding inside a cell is the
intermediate step, cells are the endgame. Known accepted-for-now items, each
with its scale-tier replacement designed:

- **xmin gate is DB-global** (`eventlog.Consumer`): one long transaction
  anywhere stalls all delivery. Contract: short write transactions +
  `idle_in_transaction_session_timeout` + analytics on replicas. Scale tier:
  logical-decoding feed (WAL order = commit order) behind the same interface.
- **Per-org consumption** must be NOTIFY-scheduled at runtime — idle orgs
  cost zero; a dispatcher polls only signaled orgs. Never one loop per org.
- **NOTIFY per append** is fine to ~10k events/s; beyond that, coalesce
  notifications per (tx, org) — payloads are already just the org id.
- **Fan-out ceilings** are designed away up front: read state is O(threads)
  (F-7), notification candidates come from the materialized deliverability
  set (F-17), ACL checks are one closure join (F-16) — these are the three
  places 10k-member channels kill naive designs.

## What is intentionally absent (each is a named deferral, not an oversight)

- Interactive-block state, canvas, MCP registries, DLP rules — their ADR hooks
  are JSONB fields or future tables; adding tables later is cheap, removing
  wrong ones is not.
- Dense per-user-per-message anything. If a feature seems to need it, it needs
  a design pass first (see F-7).
- **App platform tables** (manifest registry, event-subscription endpoints with
  delivery-health state, slash-command registry) — M3 with the automations
  engine; incoming write-only webhooks already work via `capability_grant`.
- **Peer-instance registry** (allowlist, keys, per-peer sync cursors) — lands
  with cross-instance sharing T2; `shared_channel.peer_instance` is the seam.
- **Hash-chain head + SIEM stream config** (ADR-013 AD-2, v1-hook) — one tiny
  table each when the compliance surfaces land.
- **Search-side stores** (pgvector embeddings, cross-entity search index,
  materialized notification deliverability sets — F-16/F-17) — implementation-
  stage caches/indexes owned by their milestones; the source columns exist.
- **SSO/SCIM/2FA tables** — M4+ per MILESTONES.md; `user_credential` +
  `auth_session` are deliberately minimal for M0.
