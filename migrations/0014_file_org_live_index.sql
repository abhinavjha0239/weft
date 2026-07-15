-- P-19: the per-org storage-quota check sums an org's LIVE file sizes on every
-- upload. This partial covering index makes that an index-only scan — the
-- WHERE deleted_at IS NULL predicate confines it to live rows and INCLUDE
-- (size_bytes) carries the summed column in the leaf, so neither the heap nor
-- purged rows are touched. Self-host scale is fine on this; a per-org counter
-- table is the recorded mitigation if a tenant's uploads go hot.
--
-- Filename number is pinned by the batch plan (P-35 took 0013); db.Migrate is
-- filename-keyed and gap-tolerant.
CREATE INDEX file_org_live_idx ON file (org_id) INCLUDE (size_bytes)
    WHERE deleted_at IS NULL;
