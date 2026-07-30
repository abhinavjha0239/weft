package messaging

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/eventlog"
)

// S6 (F-17 twin): maintenance of the O(1) unread counter (container_unread_counter,
// migration 0023). The design keeps the READ — messaging.Unreads/DMUnreads — a
// plain index scan of this table, while the counter is maintained OFF the hot
// send path:
//
//   - INCREMENT rides S1's notification consumer pass (ApplyMessageUnread, called
//     by notification.Runner via the UnreadMaintainer seam once per created
//     message). It is ONE set-based statement per message — a bulk UPSERT over the
//     container's live members, O(members) ROWS touched but never a per-member Go
//     loop — because unread is "every live container member except the author",
//     which is NOT the O(reasons) notification deliverability set. That cost lands
//     on the async consumer, never messaging.Send, and turns the READ into O(1).
//   - DECREMENT-ON-READ rides MarkRead (applyMarkReadDelta, same tx as the
//     watermark write): the counter drops by exactly the marked thread's
//     messages between the old and new watermark — O(the slice just read),
//     never a container-wide recompute (mark-read is the highest-volume user
//     action; an O(container-history) aggregate there would hand the removed
//     read-path O(N) back on the write path).
//   - DECREMENT rides DeleteMessage (decrementUnreadOnDelete, same tx as the
//     scrub): a deleted message stops counting, so members who had not read it
//     lose one unread.
//   - RECONCILE (ReconcileUnreadOnce, the notification runner's slow ticker)
//     recomputes every counter row from the live aggregate and repairs +
//     Warn-logs divergence. The counter is a CACHE; thread_read_watermark is the
//     source of truth. This is the backstop for the bounded drift the design
//     tolerates: at-least-once consumer replay (guarded by last_event_id, so
//     replay is normally a no-op), an intra-channel P-04 move (the container is
//     unchanged, only a per-thread watermark relationship shifts), and a
//     delete/create commit race.
//
// Idempotency: the consumer delivers at-least-once (a crash between processing
// and cursor-ack replays a batch). Each counter row carries last_event_id, and
// an increment applies only WHERE last_event_id < the event's id, so a replay of
// an already-counted event is a no-op. A message the user marks read BEFORE the
// consumer counts it is subtracted first and incremented later (+1 drift for
// one consumer-lag window, floored at 0, healed by the sweep) — the bounded
// price of keeping mark-read O(delta); see applyMarkReadDelta.
//
// mention_count wakes ChannelUnread.Mentioned. It is incremented for mentioned
// members on the consumer pass; MarkRead/delete/reconcile hold the invariant
// mention_count <= unread_count via a LEAST clamp, so a full read (unread → 0)
// always clears the badge. There is no per-message mention TRUTH to reconcile
// against (message_user_flag is dormant, out of this slice), so the badge is a
// best-effort signal: it can linger after a partial read while unread non-mention
// messages remain, but it never MISSES a live mention and always clears on a full
// read.

// queryRower is satisfied by both *pgxpool.Pool and pgx.Tx, so the recompute
// runs identically inside MarkRead's transaction and in the standalone sweep.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// recomputeContainerUnread is THE live aggregate for ONE (user, container),
// mirroring Unreads/DMUnreads exactly (an unsubscribed member / non-participant
// yields 0, matching the reads that drop them): unread = messages in the
// container after the user's per-thread watermarks, authored by someone else,
// still visible. It is the reconciliation truth and MarkRead's reset value —
// shared here so the counter and the aggregate can never drift apart in
// definition.
func recomputeContainerUnread(ctx context.Context, q queryRower, userID, channelID, dmSpaceID int64) (int, error) {
	var unread int
	var err error
	if channelID != 0 {
		err = q.QueryRow(ctx, `
			SELECT count(*)
			FROM channel_member cm
			JOIN message m
			  ON m.channel_id = cm.channel_id
			 AND m.author_id <> cm.user_id
			 AND m.deleted_at IS NULL
			LEFT JOIN thread_read_watermark w
			  ON w.user_id = cm.user_id AND w.thread_id = m.thread_id
			WHERE cm.user_id = $1 AND cm.channel_id = $2 AND cm.unsubscribed_at IS NULL
			  AND m.id > COALESCE(w.last_read_message_id, 0)`,
			userID, channelID).Scan(&unread)
	} else {
		err = q.QueryRow(ctx, `
			SELECT count(*)
			FROM dm_participant dp
			JOIN message m
			  ON m.dm_space_id = dp.dm_space_id
			 AND m.author_id <> dp.user_id
			 AND m.deleted_at IS NULL
			LEFT JOIN thread_read_watermark w
			  ON w.user_id = dp.user_id AND w.thread_id = m.thread_id
			WHERE dp.user_id = $1 AND dp.dm_space_id = $2
			  AND m.id > COALESCE(w.last_read_message_id, 0)`,
			userID, dmSpaceID).Scan(&unread)
	}
	if err != nil {
		return 0, fmt.Errorf("unread: recompute: %w", err)
	}
	return unread, nil
}

// ApplyMessageUnread increments the counter for a newly-created message,
// off the notification consumer pass (the UnreadMaintainer seam). Exactly one
// of channelID/dmSpaceID is non-zero. It is one set-based UPSERT over the
// container's live members except the author: +1 unread each, +1 mention for a
// mentioned member. The last_event_id guard makes an at-least-once replay a
// no-op, and the EXISTS(message live) guard skips a message already deleted
// before the consumer reached it (so it never over-counts against a delete).
func (s *Service) ApplyMessageUnread(ctx context.Context, orgID, channelID, dmSpaceID, messageID, authorID, eventID int64, mentions []int64) error {
	if mentions == nil {
		mentions = []int64{} // an explicit NULL array would make = ANY() null; keep it empty
	}
	var err error
	if channelID != 0 {
		_, err = s.pool.Exec(ctx, `
			INSERT INTO container_unread_counter
				(user_id, channel_id, org_id, unread_count, mention_count, last_event_id)
			SELECT cm.user_id, $1, $2, 1,
			       (CASE WHEN cm.user_id = ANY($5::bigint[]) THEN 1 ELSE 0 END),
			       $4
			FROM channel_member cm
			WHERE cm.channel_id = $1 AND cm.unsubscribed_at IS NULL AND cm.user_id <> $3
			  AND EXISTS (SELECT 1 FROM message WHERE id = $6 AND deleted_at IS NULL)
			ON CONFLICT (user_id, channel_id) WHERE channel_id IS NOT NULL
			DO UPDATE SET
			    unread_count  = container_unread_counter.unread_count  + EXCLUDED.unread_count,
			    mention_count = container_unread_counter.mention_count + EXCLUDED.mention_count,
			    last_event_id = EXCLUDED.last_event_id
			WHERE container_unread_counter.last_event_id < EXCLUDED.last_event_id`,
			channelID, orgID, authorID, eventID, mentions, messageID)
	} else {
		_, err = s.pool.Exec(ctx, `
			INSERT INTO container_unread_counter
				(user_id, dm_space_id, org_id, unread_count, mention_count, last_event_id)
			SELECT dp.user_id, $1, $2, 1,
			       (CASE WHEN dp.user_id = ANY($5::bigint[]) THEN 1 ELSE 0 END),
			       $4
			FROM dm_participant dp
			WHERE dp.dm_space_id = $1 AND dp.user_id <> $3
			  AND EXISTS (SELECT 1 FROM message WHERE id = $6 AND deleted_at IS NULL)
			ON CONFLICT (user_id, dm_space_id) WHERE dm_space_id IS NOT NULL
			DO UPDATE SET
			    unread_count  = container_unread_counter.unread_count  + EXCLUDED.unread_count,
			    mention_count = container_unread_counter.mention_count + EXCLUDED.mention_count,
			    last_event_id = EXCLUDED.last_event_id
			WHERE container_unread_counter.last_event_id < EXCLUDED.last_event_id`,
			dmSpaceID, orgID, authorID, eventID, mentions, messageID)
	}
	if err != nil {
		return fmt.Errorf("unread: apply message: %w", err)
	}
	return nil
}

// applyMarkReadDelta decrements ONE (user, container)'s unread by exactly the
// messages the mark just covered — this thread's rows in (oldWM, applied],
// authored by someone else, still visible — inside MarkRead's transaction.
// Cost is O(the marked slice), never O(container history): mark-read is among
// the highest-volume user actions, so a container-wide recompute here would
// hand back on the write path the O(N) the S6 counter removed from the read
// path. mention_count rides the LEAST clamp (unread→0 always clears the
// badge). A user with no counter row (pre-counter history, nothing new since)
// has nothing to decrement — the row appears with the next counted message.
//
// Bounded drift, healed by the hourly reconcile sweep (the counter is a
// CACHE): (1) a message the user read BEFORE the consumer counted it is
// subtracted here (floored at 0) and incremented later — +1 until the sweep;
// the window is the consumer lag, which S0 makes observable. (2) two
// concurrent FIRST-EVER marks of one thread can both see oldWM=0 (no row to
// FOR-UPDATE yet) and overlap their deltas — floored at 0, likewise swept.
func (s *Service) applyMarkReadDelta(ctx context.Context, tx pgx.Tx, userID, channelID, dmSpaceID, threadID, oldWM, applied int64) error {
	if applied <= oldWM {
		return nil // monotone clamp: nothing newly read
	}
	var delta int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM message
		WHERE thread_id = $1 AND id > $2 AND id <= $3
		  AND author_id <> $4 AND deleted_at IS NULL`,
		threadID, oldWM, applied, userID).Scan(&delta); err != nil {
		return fmt.Errorf("unread: mark-read delta: %w", err)
	}
	if delta == 0 {
		return nil
	}
	col, containerID := "channel_id", channelID
	if channelID == 0 {
		col, containerID = "dm_space_id", dmSpaceID
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE container_unread_counter
		SET unread_count  = GREATEST(unread_count - $3, 0),
		    mention_count = LEAST(mention_count, GREATEST(unread_count - $3, 0))
		WHERE user_id = $1 AND %s = $2`, col),
		userID, containerID, delta)
	if err != nil {
		return fmt.Errorf("unread: mark-read decrement: %w", err)
	}
	return nil
}

// decrementUnreadOnDelete drops one unread from every member who had NOT read
// the deleted message (their thread watermark is below it), inside
// DeleteMessage's transaction. GREATEST(...,0) floors a counter that a
// delete-before-consume race left short, and the mention clamp holds
// mention_count <= unread_count.
func (s *Service) decrementUnreadOnDelete(ctx context.Context, tx pgx.Tx, channelID, dmSpaceID, threadID, messageID, authorID int64) error {
	var err error
	if channelID != 0 {
		_, err = tx.Exec(ctx, `
			UPDATE container_unread_counter c
			SET unread_count  = GREATEST(c.unread_count - 1, 0),
			    mention_count = LEAST(c.mention_count, GREATEST(c.unread_count - 1, 0))
			FROM channel_member cm
			LEFT JOIN thread_read_watermark w
			  ON w.user_id = cm.user_id AND w.thread_id = $3
			WHERE c.user_id = cm.user_id AND c.channel_id = $1
			  AND cm.channel_id = $1 AND cm.unsubscribed_at IS NULL AND cm.user_id <> $4
			  AND $2 > COALESCE(w.last_read_message_id, 0)`,
			channelID, messageID, threadID, authorID)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE container_unread_counter c
			SET unread_count  = GREATEST(c.unread_count - 1, 0),
			    mention_count = LEAST(c.mention_count, GREATEST(c.unread_count - 1, 0))
			FROM dm_participant dp
			LEFT JOIN thread_read_watermark w
			  ON w.user_id = dp.user_id AND w.thread_id = $3
			WHERE c.user_id = dp.user_id AND c.dm_space_id = $1
			  AND dp.dm_space_id = $1 AND dp.user_id <> $4
			  AND $2 > COALESCE(w.last_read_message_id, 0)`,
			dmSpaceID, messageID, threadID, authorID)
	}
	if err != nil {
		return fmt.Errorf("unread: decrement on delete: %w", err)
	}
	return nil
}

// UnreadCounterSweep is the durable identity of the S6 counter reconcile
// sweep's cell-wide claim and per-org settled markers. Exported for the same
// reason a consumer name is operator-visible (correlating pg_locks.objid or a
// sweep_org_state row with "who is sweeping"), and append-only: changing a
// field is a rename, never a fix.
//
// The counter's increments ride the notifications consumer, so that consumer's
// progress is what gates settling an org as verified. Its name is a literal
// here because messaging must stay free of a notification import (the
// UnreadMaintainer seam rule); a mismatch would degrade safely rather than
// corrupt — an unknown consumer reads cursor 0, every org looks permanently
// behind, nothing ever settles, and the sweep simply keeps its pre-skip
// behaviour.
var UnreadCounterSweep = eventlog.SweepID{
	Name: "unread_counter_reconcile", Key: 2, Consumer: "notifications",
}

// ReconcileUnreadOnce sweeps every counter row, recomputes its unread from the
// live aggregate, and repairs + Warn-logs divergence (the F-17-style safety
// net: the counter is a cache, so a nonzero repair means a maintenance miss).
// One short recompute per row on the runner's slow ticker; the mention clamp
// travels with the repair. Exposed as the UnreadMaintainer seam.
//
// The pass is claimed cell-wide first (eventlog.Sweeper): the notification
// runner drives this ticker on EVERY node, so without a claim an N-node cell
// ran N redundant full passes per window, each issuing a live recompute per
// counter row. A node that loses the claim returns nil and waits for its own
// next tick — at-most-once per window (see the Sweeper doc for why that is
// the right side of the trade for a convergence sweep).
//
// Idle orgs then cost approximately nothing (docs/SCHEMA.md's scale
// contract). An org is skipped only while its event-log high-water mark still
// equals the mark a previous pass recorded after verifying it CLEAN, so drift
// is never what gets skipped: a pass that repaired anything, hit any error,
// or ran ahead of the consumer whose increments it is checking refuses to
// settle, and the org is walked again next window. The skip is a LEASE, never
// a permanent excuse: a settled marker older than eventlog.SettleTTL stops
// suppressing work, so every org is fully verified at least once per
// SettleTTL however quiet it is. That deadline is what bounds this counter's
// one eventless drift class — the concurrent first-ever mark-read window
// (#118 drift 2), which appends nothing and could otherwise sit in a quiet
// org forever. sweep_org_state (0025) carries the whole argument.
func (s *Service) ReconcileUnreadOnce(ctx context.Context) error {
	release, ok, err := s.unreadSweep.Claim(ctx)
	if err != nil || !ok {
		return err
	}
	defer release()
	orgs, err := s.unreadSweep.Orgs(ctx)
	if err != nil {
		return err
	}
	for _, o := range orgs {
		if o.Settled {
			continue
		}
		clean, err := s.reconcileUnreadOrg(ctx, o.OrgID)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			s.log.Warn("messaging: unread reconcile org", "org", o.OrgID, "err", err)
			continue
		}
		if !clean || o.Lagging {
			continue // a repaired or unverifiable org stays unsettled
		}
		if err := s.unreadSweep.Settle(ctx, o.OrgID, o.HighWater); err != nil {
			if ctx.Err() != nil {
				return err
			}
			s.log.Warn("messaging: settle unread sweep", "org", o.OrgID, "err", err)
		}
	}
	return nil
}

// reconcileUnreadOrg recomputes one org's counter rows and reports whether
// the org came back CLEAN — every row visited, none repaired, none errored.
// Only that verdict may settle the org: a repair means maintenance missed,
// which is the last moment to stop checking.
func (s *Service) reconcileUnreadOrg(ctx context.Context, orgID int64) (bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, COALESCE(channel_id, 0), COALESCE(dm_space_id, 0),
		       unread_count
		FROM container_unread_counter
		WHERE org_id = $1
		ORDER BY user_id, channel_id, dm_space_id`, orgID)
	if err != nil {
		return false, fmt.Errorf("unread: reconcile list: %w", err)
	}
	type counter struct {
		userID, channelID, dmSpaceID int64
		stored                       int
	}
	var counters []counter
	for rows.Next() {
		var c counter
		if err := rows.Scan(&c.userID, &c.channelID, &c.dmSpaceID, &c.stored); err != nil {
			rows.Close()
			return false, fmt.Errorf("unread: reconcile scan: %w", err)
		}
		counters = append(counters, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("unread: reconcile list: %w", err)
	}
	clean := true
	for _, c := range counters {
		recomputed, err := recomputeContainerUnread(ctx, s.pool, c.userID, c.channelID, c.dmSpaceID)
		if err != nil {
			if ctx.Err() != nil {
				return false, err
			}
			s.log.Warn("messaging: unread reconcile recompute", "user", c.userID, "err", err)
			clean = false
			continue
		}
		if recomputed == c.stored {
			continue
		}
		clean = false
		if err := s.repairUnread(ctx, c.userID, c.channelID, c.dmSpaceID, recomputed); err != nil {
			if ctx.Err() != nil {
				return false, err
			}
			s.log.Warn("messaging: unread reconcile repair", "user", c.userID, "err", err)
			continue
		}
		s.log.Warn("messaging: unread counter diverged (repaired)",
			"user", c.userID, "channel", c.channelID, "dm_space", c.dmSpaceID,
			"stored", c.stored, "recomputed", recomputed)
	}
	return clean, nil
}

// repairUnread SETs one counter row to the recomputed unread and clamps its
// mention to match (the reconcile write half).
func (s *Service) repairUnread(ctx context.Context, userID, channelID, dmSpaceID int64, unread int) error {
	col, containerID := "channel_id", channelID
	if channelID == 0 {
		col, containerID = "dm_space_id", dmSpaceID
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE container_unread_counter
		SET unread_count  = $3,
		    mention_count = LEAST(mention_count, $3)
		WHERE user_id = $1 AND %s = $2`, col), userID, containerID, unread)
	if err != nil {
		return fmt.Errorf("unread: repair: %w", err)
	}
	return nil
}

// SeedUnreadCounters materializes an org's counter rows from the live
// aggregate — the bulk backfill for read state that never flowed through the
// consumer path. The Zulip importer is the lane that NEEDS it: imported
// messages carry importer-actor events the consumer deliberately skips, so
// without a seed an imported org's real unread state (watermarks below live
// messages) would be invisible to the O(1) read — an import-fidelity
// regression. Two set-based statements (one per container leg) in the
// caller's transaction; last_event_id lands on the org's current max event
// id so later consumption never re-counts what the aggregate already
// reflects. mention_count seeds 0 (no per-message mention truth exists — the
// badge is creation-time best-effort, see the package note). Package-level
// like the importer's other cross-module calls; also the operator remedy for
// a deploy of 0023 over a live pre-counter org.
func SeedUnreadCounters(ctx context.Context, tx pgx.Tx, orgID int64) error {
	if _, err := tx.Exec(ctx, `
		WITH maxev AS (
		    SELECT COALESCE(MAX(id), 0) AS id FROM event_log WHERE org_id = $1
		)
		INSERT INTO container_unread_counter
			(user_id, channel_id, org_id, unread_count, mention_count, last_event_id)
		SELECT cm.user_id, cm.channel_id, $1, count(*), 0, (SELECT id FROM maxev)
		FROM channel_member cm
		JOIN channel c ON c.id = cm.channel_id AND c.org_id = $1
		JOIN message m
		  ON m.channel_id = cm.channel_id
		 AND m.author_id <> cm.user_id
		 AND m.deleted_at IS NULL
		LEFT JOIN thread_read_watermark w
		  ON w.user_id = cm.user_id AND w.thread_id = m.thread_id
		WHERE cm.unsubscribed_at IS NULL
		  AND m.id > COALESCE(w.last_read_message_id, 0)
		GROUP BY cm.user_id, cm.channel_id
		ON CONFLICT (user_id, channel_id) WHERE channel_id IS NOT NULL
		DO UPDATE SET
		    unread_count  = EXCLUDED.unread_count,
		    mention_count = LEAST(container_unread_counter.mention_count, EXCLUDED.unread_count),
		    last_event_id = GREATEST(container_unread_counter.last_event_id, EXCLUDED.last_event_id)`,
		orgID); err != nil {
		return fmt.Errorf("unread: seed channels: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		WITH maxev AS (
		    SELECT COALESCE(MAX(id), 0) AS id FROM event_log WHERE org_id = $1
		)
		INSERT INTO container_unread_counter
			(user_id, dm_space_id, org_id, unread_count, mention_count, last_event_id)
		SELECT dp.user_id, dp.dm_space_id, $1, count(*), 0, (SELECT id FROM maxev)
		FROM dm_participant dp
		JOIN dm_space ds ON ds.id = dp.dm_space_id AND ds.org_id = $1
		JOIN message m
		  ON m.dm_space_id = dp.dm_space_id
		 AND m.author_id <> dp.user_id
		 AND m.deleted_at IS NULL
		LEFT JOIN thread_read_watermark w
		  ON w.user_id = dp.user_id AND w.thread_id = m.thread_id
		WHERE m.id > COALESCE(w.last_read_message_id, 0)
		GROUP BY dp.user_id, dp.dm_space_id
		ON CONFLICT (user_id, dm_space_id) WHERE dm_space_id IS NOT NULL
		DO UPDATE SET
		    unread_count  = EXCLUDED.unread_count,
		    mention_count = LEAST(container_unread_counter.mention_count, EXCLUDED.unread_count),
		    last_event_id = GREATEST(container_unread_counter.last_event_id, EXCLUDED.last_event_id)`,
		orgID); err != nil {
		return fmt.Errorf("unread: seed dms: %w", err)
	}
	return nil
}

// compile-time guard that the pool type carries QueryRow (documents the
// queryRower contract the recompute relies on for both pool and tx).
var _ queryRower = (*pgxpool.Pool)(nil)
