-- F-17 (ADR-011 amendment, red-team 2026-07-09): the materialized per-channel
-- deliverability set + batch coalescing. The materializer's per-message
-- candidate scan must be O(actual reasons), never O(members) — a 100k-member
-- channel where nobody opted into level=all yields ZERO rows here, so a
-- normal message resolves only its mentions. The O(members) work moves to
-- RARE events (level/follow/alert-word edits, joins/leaves, the lazy first
-- build), which patch or rebuild this set.

-- One row per (channel, user, reason, medium) — a row EXISTS only for a
-- NON-DEFAULT delivery reason. reason: 1 follows-a-thread-here (the
-- per-thread state itself rides thread_subscription; this row only marks
-- "has at least one follow in this channel") · 2 level=all ·
-- 3 alert-word-holder. medium: notification_medium_pref vocabulary
-- (1 in_app — the only medium materialized today; email/push lanes read
-- notification rows downstream). The set is a CACHE of derivable truth
-- (channel_member/thread_subscription/alert_word remain the source);
-- consumers re-verify live settings per candidate, so a stale-extra row
-- costs one wasted scan and only a stale-missing row can drop work — the
-- reconciliation sweep repairs and logs those.
CREATE TABLE channel_deliverability (
    channel_id BIGINT NOT NULL REFERENCES channel (id),
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    reason     SMALLINT NOT NULL,
    medium     SMALLINT NOT NULL,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    PRIMARY KEY (channel_id, user_id, reason, medium)
) WITH (fillfactor = 90); -- churned in place like channel_member

-- The per-message candidate scan: all rows for a channel with an
-- activity-capable reason.
CREATE INDEX channel_deliverability_reason_idx
    ON channel_deliverability (channel_id, reason);

-- Lazy backfill marker (F-17, decided): NULL = set not yet derived. The
-- first message processed after deploy builds the channel's set (the LAST
-- O(members) pass) and stamps this; invalidation patches maintain it from
-- then on. No big-bang migration over a live org.
ALTER TABLE channel ADD COLUMN deliverability_built_at TIMESTAMPTZ;

-- F-17b: batch coalescing. Bulk producers (sprint close, automation sweeps,
-- retention vacuum) stamp a batch_id in the event hint; the materializer
-- folds a member's N per-item notifications into ONE digest row per
-- (user, kind, batch) — the 0010 dedupe key with batch identity in place of
-- entity identity, so a 100k-item bulk event cannot mint 100k rows per user.
ALTER TABLE notification ADD COLUMN batch_id BIGINT;

-- The batch-aware dedupe key: partial, so the unbatched hot path pays
-- nothing for it.
CREATE UNIQUE INDEX notification_batch_dedupe_key
    ON notification (user_id, kind, batch_id) WHERE batch_id IS NOT NULL;
