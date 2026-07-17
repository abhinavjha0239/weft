package notification

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
)

// Deliverability maintains the F-17 materialized candidate set
// (channel_deliverability, ADR-011 amendment): one row per (channel, user,
// reason, medium) held ONLY for a non-default delivery reason — 1 the user
// follows at least one thread here (the per-thread state itself stays in
// thread_subscription), 2 level=all, 3 alert-word-holder. The materializer
// scans this set per message instead of joining every channel_member row,
// so per-message cost is O(actual reasons), never O(members).
//
// The set is a CACHE of derivable truth — channel_member,
// thread_subscription, and alert_word remain the source, and consumers
// re-verify live settings per candidate. It is (re)computed ONLY on rare
// events: the lazy first build, level changes, follow-state changes,
// alert-word edits, and member.joined/left (the O(N)→rare-event push).
// This component is THE shared invalidation dispatch: messaging reaches
// the settings patches through its DeliverabilityPatcher seam, the
// alert-word service calls in-package, and the runner feeds membership
// events plus the lazy build. Later slices (S2) reuse the same dispatch.
//
// Concurrency: every writer — build, patch, reconcile — takes the channel
// row lock FIRST (multi-channel patches lock in ascending id order), so a
// patch committing alongside a first build can never be derived over and
// lost. Patches run in the SAME tx as their source write; a channel not
// yet built skips the patch under the lock, because the build that follows
// derives from post-commit truth anyway.
type Deliverability struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewDeliverability(pool *pgxpool.Pool, log *slog.Logger) *Deliverability {
	if log == nil {
		log = slog.Default()
	}
	return &Deliverability{pool: pool, log: log}
}

// derivedReasonsSQL is THE derivation of who holds a non-default delivery
// reason in channel $1 — the truth the cache mirrors. $2 pins one user
// (NULL = the whole channel); build, patch, and reconcile all share this
// one string so their notions of truth can never drift apart. Root
// threads carry no follow state (F-15), matching the thread index.
const derivedReasonsSQL = `
	SELECT ts.user_id, 1::smallint AS reason
	FROM thread t
	JOIN thread_subscription ts ON ts.thread_id = t.id AND ts.state = 1
	JOIN channel_member cm ON cm.channel_id = $1 AND cm.user_id = ts.user_id
	     AND cm.unsubscribed_at IS NULL
	WHERE t.channel_id = $1 AND t.kind = 1
	  AND ($2::bigint IS NULL OR ts.user_id = $2)
	GROUP BY ts.user_id
	UNION ALL
	SELECT cm.user_id, 2::smallint
	FROM channel_member cm
	WHERE cm.channel_id = $1 AND cm.unsubscribed_at IS NULL AND cm.level = 1
	  AND ($2::bigint IS NULL OR cm.user_id = $2)
	UNION ALL
	SELECT cm.user_id, 3::smallint
	FROM channel_member cm
	WHERE cm.channel_id = $1 AND cm.unsubscribed_at IS NULL
	  AND EXISTS (SELECT 1 FROM alert_word aw WHERE aw.user_id = cm.user_id)
	  AND ($2::bigint IS NULL OR cm.user_id = $2)`

// lockChannel serializes set writers on the channel row and reports the
// built marker as of the lock grant. ok=false means the channel does not
// exist in this org — nothing to maintain.
func (d *Deliverability) lockChannel(ctx context.Context, tx pgx.Tx, orgID, channelID int64) (built, ok bool, err error) {
	err = tx.QueryRow(ctx, `
		SELECT deliverability_built_at IS NOT NULL FROM channel
		WHERE id = $1 AND org_id = $2
		FOR UPDATE`, channelID, orgID).Scan(&built)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("deliverability: lock channel: %w", err)
	}
	return built, true, nil
}

// resync is the single maintenance primitive: make the stored medium-1
// rows for channel (and optionally one user) match the derivation.
// Callers hold the channel lock. Returns repaired (missing, extra) counts
// so the reconciler can report divergence.
func (d *Deliverability) resync(ctx context.Context, tx pgx.Tx, orgID, channelID int64, userID *int64) (missing, extra int64, err error) {
	ct, err := tx.Exec(ctx, `
		DELETE FROM channel_deliverability cd
		WHERE cd.channel_id = $1 AND cd.medium = 1
		  AND ($2::bigint IS NULL OR cd.user_id = $2)
		  AND (cd.user_id, cd.reason) NOT IN (
		      SELECT d.user_id, d.reason FROM (`+derivedReasonsSQL+`) d)`,
		channelID, userID)
	if err != nil {
		return 0, 0, fmt.Errorf("deliverability: resync delete: %w", err)
	}
	extra = ct.RowsAffected()
	ct, err = tx.Exec(ctx, `
		INSERT INTO channel_deliverability (channel_id, user_id, reason, medium, org_id)
		SELECT $1, d.user_id, d.reason, 1, $3
		FROM (`+derivedReasonsSQL+`) d
		ON CONFLICT DO NOTHING`,
		channelID, userID, orgID)
	if err != nil {
		return 0, 0, fmt.Errorf("deliverability: resync insert: %w", err)
	}
	return ct.RowsAffected(), extra, nil
}

// PatchChannelUser resyncs ONE user's rows in ONE channel inside the
// caller's transaction — the invalidation entry point for every per-user
// settings change (channel level, thread follow state, one channel of an
// alert-word edit, membership). It satisfies messaging's
// DeliverabilityPatcher seam. An unbuilt channel skips under the lock:
// its eventual first build derives from committed truth.
func (d *Deliverability) PatchChannelUser(ctx context.Context, tx pgx.Tx, orgID, channelID, userID int64) error {
	built, ok, err := d.lockChannel(ctx, tx, orgID, channelID)
	if err != nil || !ok || !built {
		return err
	}
	_, _, err = d.resync(ctx, tx, orgID, channelID, &userID)
	return err
}

// PatchAlertWords resyncs the alert-word reason across every channel the
// user is a live member of, inside the SAME tx as the word-list write.
// Channels are locked one at a time in ascending id order (deadlock-free
// against other multi-channel patchers), unbuilt ones skipping under the
// lock like PatchChannelUser.
func (d *Deliverability) PatchAlertWords(ctx context.Context, tx pgx.Tx, orgID, userID int64) error {
	rows, err := tx.Query(ctx, `
		SELECT cm.channel_id FROM channel_member cm
		JOIN channel c ON c.id = cm.channel_id AND c.org_id = $1
		WHERE cm.user_id = $2 AND cm.unsubscribed_at IS NULL
		ORDER BY cm.channel_id`, orgID, userID)
	if err != nil {
		return fmt.Errorf("deliverability: list channels: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("deliverability: scan channel: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("deliverability: list channels: %w", err)
	}
	for _, id := range ids {
		if err := d.PatchChannelUser(ctx, tx, orgID, id, userID); err != nil {
			return err
		}
	}
	return nil
}

// PatchMembership resyncs one user after a member.joined/left event, in
// its own short transaction (the runner consumes these off the event log,
// which keeps messaging and identity free of a notification import). A
// join is O(1) — one user's rows, never a channel rebuild; a leave
// derives to zero rows and clears them.
func (d *Deliverability) PatchMembership(ctx context.Context, orgID, channelID, userID int64) error {
	return db.WithTx(ctx, d.pool, func(tx pgx.Tx) error {
		return d.PatchChannelUser(ctx, tx, orgID, channelID, userID)
	})
}

// EnsureBuilt lazily materializes a channel's set on first use (the F-17
// backfill decision: no big-bang migration over a live org). The fast
// path is one PK read; the build itself — the LAST O(members) pass this
// channel ever pays — runs once, serialized by the channel lock.
func (d *Deliverability) EnsureBuilt(ctx context.Context, orgID, channelID int64) error {
	var built bool
	err := d.pool.QueryRow(ctx, `
		SELECT deliverability_built_at IS NOT NULL FROM channel
		WHERE id = $1 AND org_id = $2`, channelID, orgID).Scan(&built)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // channel gone or foreign: nothing to build
	}
	if err != nil {
		return fmt.Errorf("deliverability: built check: %w", err)
	}
	if built {
		return nil
	}
	return db.WithTx(ctx, d.pool, func(tx pgx.Tx) error {
		built, ok, err := d.lockChannel(ctx, tx, orgID, channelID)
		if err != nil || !ok || built {
			return err // a concurrent build won the lock: done
		}
		if _, _, err := d.resync(ctx, tx, orgID, channelID, nil); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE channel SET deliverability_built_at = now()
			WHERE id = $1`, channelID); err != nil {
			return fmt.Errorf("deliverability: stamp built: %w", err)
		}
		return nil
	})
}

// ReconcileChannel recomputes one built channel's set from the derivation
// and repairs any divergence (pre-mortem F-17 risk: a missed invalidation
// would silently DROP notifications — the sweep is the safety net, and
// it reports loudly because a nonzero repair means an invalidation bug).
// The channel lock is held for the recompute, so patches serialize behind
// it; low frequency keeps that acceptable.
func (d *Deliverability) ReconcileChannel(ctx context.Context, orgID, channelID int64) (missing, extra int64, err error) {
	err = db.WithTx(ctx, d.pool, func(tx pgx.Tx) error {
		built, ok, err := d.lockChannel(ctx, tx, orgID, channelID)
		if err != nil || !ok || !built {
			return err
		}
		missing, extra, err = d.resync(ctx, tx, orgID, channelID, nil)
		return err
	})
	if err == nil && missing+extra > 0 {
		d.log.Warn("notification: deliverability set diverged (repaired)",
			"channel", channelID, "missing", missing, "extra", extra)
	}
	return missing, extra, err
}

// ReconcileOnce sweeps every built channel, one short transaction each
// (the janitor cadence — the runner drives this on a slow ticker).
func (d *Deliverability) ReconcileOnce(ctx context.Context) error {
	rows, err := d.pool.Query(ctx, `
		SELECT id, org_id FROM channel
		WHERE deliverability_built_at IS NOT NULL ORDER BY id`)
	if err != nil {
		return fmt.Errorf("deliverability: list built channels: %w", err)
	}
	type ch struct{ id, org int64 }
	var chans []ch
	for rows.Next() {
		var c ch
		if err := rows.Scan(&c.id, &c.org); err != nil {
			rows.Close()
			return fmt.Errorf("deliverability: scan channel: %w", err)
		}
		chans = append(chans, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("deliverability: list built channels: %w", err)
	}
	for _, c := range chans {
		if _, _, err := d.ReconcileChannel(ctx, c.org, c.id); err != nil {
			if ctx.Err() != nil {
				return err
			}
			d.log.Warn("notification: reconcile channel", "channel", c.id, "err", err)
		}
	}
	return nil
}
