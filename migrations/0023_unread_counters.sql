-- S6 (F-17 twin): the maintained O(1) unread counter that readstate.go's
-- scale note reserved. messaging.Unreads was a per-request aggregate over the
-- user's channels × messages-since-watermark (readstate.go:126-129: "a
-- maintained per-(user,channel) counter updated on the notification path
-- (F-17 deliverability sets) — same result, O(1) read. Documented, not
-- built."). This builds it: the counter rides S1's EXISTING notification
-- consumer pass (never the O(1) send path), and the unread-count READ becomes
-- a plain index scan of this table — O(user's containers), never a
-- re-aggregation over messages in a high-volume channel. The live aggregate
-- survives ONLY as the reconciliation recompute (the counter is a CACHE; the
-- thread_read_watermark stays the source of truth).

-- One row per (user, container). A container is a channel OR a dm_space — the
-- DM plane gets the same counter for symmetry (DMUnreads reads the dm_space
-- leg), so the num_nonnulls CHECK is the structural rule that exactly one of
-- the two container columns is set (the invite-role-ceiling column-CHECK
-- pattern). unread_count/mention_count are the maintained aggregates;
-- mention_count wakes ChannelUnread.Mentioned (declared-but-unpopulated at
-- readstate.go:83). last_event_id is the idempotency high-water: the consumer
-- delivers at-least-once (a crash between processing and cursor-ack replays a
-- batch), so an increment applies only when the event id exceeds this
-- watermark, making replay a no-op. fillfactor mirrors thread_read_watermark
-- (85): every write here is an in-place counter bump, so leaving free space in
-- each page keeps the updates HOT (no index churn) on the hottest read-state
-- table.
CREATE TABLE container_unread_counter (
    user_id       BIGINT NOT NULL REFERENCES user_account (id),
    channel_id    BIGINT REFERENCES channel (id),
    dm_space_id   BIGINT REFERENCES dm_space (id),
    unread_count  INT NOT NULL DEFAULT 0,
    mention_count INT NOT NULL DEFAULT 0,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    CHECK (num_nonnulls(channel_id, dm_space_id) = 1)
) WITH (fillfactor = 85);

-- The read/maintenance probes are all keyed by (user, container); a partial
-- unique index per leg is both the upsert conflict target and the O(1) scan
-- the Unreads/DMUnreads reads ride (WHERE user_id = $1 returns the user's
-- containers directly, no message touch).
CREATE UNIQUE INDEX container_unread_counter_channel_key
    ON container_unread_counter (user_id, channel_id) WHERE channel_id IS NOT NULL;
CREATE UNIQUE INDEX container_unread_counter_dm_key
    ON container_unread_counter (user_id, dm_space_id) WHERE dm_space_id IS NOT NULL;
