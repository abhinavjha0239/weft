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

## What is intentionally absent

- Interactive-block state, canvas, MCP registries, DLP rules — their ADR hooks
  are JSONB fields or future tables; adding tables later is cheap, removing
  wrong ones is not.
- Dense per-user-per-message anything. If a feature seems to need it, it needs
  a design pass first (see F-7).
