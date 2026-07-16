package compliance

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// Janitor is the retention-enforcement lane (ADR-012 F-1 + ADR-013): files
// are purged from storage only when references hit zero AND the grace/
// restore window elapsed AND no active legal hold covers them. Rows are
// soft-deleted (the audit trail keeps them); only blob BYTES are reclaimed,
// through the same storage seam every backend implements.
type Janitor struct {
	pool  *pgxpool.Pool
	store blob.Store
	log   *slog.Logger
	// UnclaimedGrace: how long a never-referenced upload survives — long,
	// because drafts and scheduled sends hold uploads silently (Zulip ships
	// 5 weeks). DeadRefWindow: how long a file outlives the deletion of its
	// last referencing message (Zulip's 30-day archive vacuum delay).
	UnclaimedGrace time.Duration
	DeadRefWindow  time.Duration
	// VacuumRestoreWindow: how long a retention-vacuumed message stays
	// recoverable (soft-tombstoned) before the purge lane permanently
	// removes it (P-17). Mirrors DeadRefWindow's role for files.
	VacuumRestoreWindow time.Duration
	Interval            time.Duration
}

func NewJanitor(pool *pgxpool.Pool, store blob.Store, log *slog.Logger) *Janitor {
	return &Janitor{
		pool: pool, store: store, log: log,
		UnclaimedGrace:      35 * 24 * time.Hour,
		DeadRefWindow:       30 * 24 * time.Hour,
		VacuumRestoreWindow: 30 * 24 * time.Hour,
		Interval:            time.Hour,
	}
}

// Report counts one sweep's work.
type Report struct {
	UnclaimedPurged   int `json:"unclaimed_purged"`
	DeadRefPurged     int `json:"dead_ref_purged"`
	BlobsDeleted      int `json:"blobs_deleted"`
	RevisionsScrubbed int `json:"revisions_scrubbed"`
	MessagesVacuumed  int `json:"messages_vacuumed"`
	MessagesPurged    int `json:"messages_purged"`
}

// Run sweeps on a ticker until ctx ends.
func (j *Janitor) Run(ctx context.Context) {
	t := time.NewTicker(j.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rep, err := j.SweepOnce(ctx, time.Now())
			if err != nil {
				j.log.Warn("janitor: sweep failed", "err", err)
				continue
			}
			if rep != (Report{}) {
				j.log.Info("janitor: swept",
					"unclaimed", rep.UnclaimedPurged, "dead_ref", rep.DeadRefPurged,
					"blobs", rep.BlobsDeleted, "revisions", rep.RevisionsScrubbed,
					"messages_vacuumed", rep.MessagesVacuumed,
					"messages_purged", rep.MessagesPurged)
			}
		}
	}
}

// The two eligibility predicates, shared verbatim between the candidate scan
// and the in-transaction re-check (the recheck closes the race where a
// reference lands between scan and purge). $1 = file id (NULL in the scan,
// where the predicate ranges over all files), $2 = cutoff.
//
// Liveness is conservative: a reference of a type the janitor cannot
// evaluate (work items, threads, avatars-by-reference, …) counts as live,
// as does a dangling message reference — never delete on uncertainty.
// Avatar/emoji/export direct FKs are guarded the same way even though no
// code writes them yet.
const (
	fileGuards = `
	  AND NOT EXISTS (SELECT 1 FROM user_account u WHERE u.avatar_file_id = f.id)
	  AND NOT EXISTS (SELECT 1 FROM custom_emoji ce WHERE ce.file_id = f.id)
	  AND NOT EXISTS (SELECT 1 FROM export_job ej WHERE ej.result_file_id = f.id)`

	unclaimedPredicate = `
	f.kind = 1 AND f.deleted_at IS NULL AND ($1::bigint IS NULL OR f.id = $1)
	  AND f.created_at < $2
	  AND NOT EXISTS (SELECT 1 FROM file_reference fr WHERE fr.file_id = f.id)` +
		fileGuards + `
	  AND NOT EXISTS (
	      SELECT 1 FROM legal_hold h
	      WHERE h.org_id = f.org_id AND h.released_at IS NULL
	        AND h.custodian_user_id = f.uploader_id)`

	deadRefPredicate = `
	f.kind = 1 AND f.deleted_at IS NULL AND ($1::bigint IS NULL OR f.id = $1)
	  AND EXISTS (SELECT 1 FROM file_reference fr WHERE fr.file_id = f.id)
	  AND NOT EXISTS (
	      SELECT 1 FROM file_reference fr
	      LEFT JOIN message m ON fr.entity_type = 1 AND m.id = fr.entity_id
	      WHERE fr.file_id = f.id
	        AND (fr.entity_type <> 1 OR m.id IS NULL OR m.deleted_at IS NULL))
	  AND (SELECT max(m2.deleted_at) FROM file_reference fr2
	       JOIN message m2 ON fr2.entity_type = 1 AND m2.id = fr2.entity_id
	       WHERE fr2.file_id = f.id) < $2` +
		fileGuards + `
	  AND NOT EXISTS (
	      SELECT 1 FROM legal_hold h
	      WHERE h.org_id = f.org_id AND h.released_at IS NULL
	        AND (h.custodian_user_id = f.uploader_id
	          OR EXISTS (
	              SELECT 1 FROM file_reference fr3
	              JOIN message m3 ON fr3.entity_type = 1 AND m3.id = fr3.entity_id
	              WHERE fr3.file_id = f.id
	                AND (m3.author_id = h.custodian_user_id
	                  OR m3.channel_id = h.channel_id))))`
)

// messageVacuumPredicate selects live messages whose age has passed the
// EFFECTIVE retention policy for their scope (P-17). The scope ladder is
// nearest-wins channel(3)→org(1), matching scrubRevisions; workspace/space/dm
// rungs are the same recorded gap. duration_days = -1 (forever) is the
// COALESCE default and is excluded by the `<> -1` guard — a message with no
// policy, or a forever policy, is never vacuumed. The duration lookup is
// spelled twice (the `<> -1` guard and the age comparison) because the
// predicate text is shared verbatim between the batch scan and the in-tx
// recheck, which rules out a CTE/LATERAL binding. An active legal hold on the
// author or the channel freezes the message (the scrubRevisions guard).
// $1 = message id (NULL in the scan), $2 = the sweep clock.
const effectiveDuration = `
	COALESCE(
	  (SELECT rp.duration_days FROM retention_policy rp
	   WHERE rp.org_id = m.org_id AND rp.scope_type = 3 AND rp.scope_id = m.channel_id),
	  (SELECT rp.duration_days FROM retention_policy rp
	   WHERE rp.org_id = m.org_id AND rp.scope_type = 1 AND rp.scope_id = m.org_id),
	  -1)`

const messageVacuumPredicate = `
	m.deleted_at IS NULL AND ($1::bigint IS NULL OR m.id = $1)
	  AND (` + effectiveDuration + `) <> -1
	  AND m.created_at < $2::timestamptz - (` + effectiveDuration + `) * interval '1 day'
	  AND NOT EXISTS (
	      SELECT 1 FROM legal_hold h
	      WHERE h.org_id = m.org_id AND h.released_at IS NULL
	        AND (h.custodian_user_id = m.author_id
	          OR h.channel_id = m.channel_id))`

// SweepOnce runs every lane against the given clock and reports the work.
func (j *Janitor) SweepOnce(ctx context.Context, now time.Time) (Report, error) {
	var rep Report
	if err := j.sweepFiles(ctx, unclaimedPredicate, now.Add(-j.UnclaimedGrace),
		"unclaimed", &rep.UnclaimedPurged, &rep.BlobsDeleted); err != nil {
		return rep, err
	}
	if err := j.sweepFiles(ctx, deadRefPredicate, now.Add(-j.DeadRefWindow),
		"dead_references", &rep.DeadRefPurged, &rep.BlobsDeleted); err != nil {
		return rep, err
	}
	if err := j.scrubRevisions(ctx, &rep.RevisionsScrubbed); err != nil {
		return rep, err
	}
	if err := j.vacuumMessages(ctx, now, &rep.MessagesVacuumed); err != nil {
		return rep, err
	}
	if err := j.purgeVacuumedMessages(ctx, now.Add(-j.VacuumRestoreWindow),
		&rep.MessagesPurged); err != nil {
		return rep, err
	}
	return rep, nil
}

// purgeVacuumedPredicate selects retention-vacuumed messages whose restore
// window has elapsed and that are NOT under an active legal hold — a hold
// placed after the vacuum freezes permanent removal, so a held message stays
// tombstoned and recoverable. $1 = message id (NULL in the scan), $2 = the
// window cutoff (now - VacuumRestoreWindow).
const purgeVacuumedPredicate = `
	m.retention_vacuumed_at IS NOT NULL AND ($1::bigint IS NULL OR m.id = $1)
	  AND m.retention_vacuumed_at < $2::timestamptz
	  AND NOT EXISTS (
	      SELECT 1 FROM legal_hold h
	      WHERE h.org_id = m.org_id AND h.released_at IS NULL
	        AND (h.custodian_user_id = m.author_id
	          OR h.channel_id = m.channel_id))`

const sweepBatch = 200

func (j *Janitor) sweepFiles(ctx context.Context, predicate string, cutoff time.Time, reason string, purged, blobs *int) error {
	for {
		// Candidate scan: f.id = $1 is neutralized so the predicate text can
		// be shared with the per-file recheck.
		rows, err := j.pool.Query(ctx, `
			SELECT f.id FROM file f WHERE `+predicate+`
			ORDER BY f.id LIMIT `+fmt.Sprint(sweepBatch),
			nil, cutoff)
		if err != nil {
			return fmt.Errorf("janitor: scan %s: %w", reason, err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("janitor: scan %s: %w", reason, err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("janitor: scan %s: %w", reason, err)
		}
		for _, id := range ids {
			if err := j.purgeFile(ctx, id, predicate, cutoff, reason, purged, blobs); err != nil {
				return err
			}
		}
		if len(ids) < sweepBatch {
			return nil
		}
	}
}

// purgeFile soft-deletes one file row and reclaims its blob when no live
// twin shares the key (same content re-uploaded in the org). The row is
// locked and the FULL eligibility predicate re-checked inside the
// transaction: a reference committed before the lock saves the file. A
// reference committed after we commit can still slip in (its author linked
// a file that was already past the grace window) — that link 404s, the same
// benign race Zulip's claim path has. Bytes are deleted AFTER commit; a
// crash between leaves an orphaned blob, never a live row without bytes.
func (j *Janitor) purgeFile(ctx context.Context, fileID int64, predicate string, cutoff time.Time, reason string, purged, blobs *int) error {
	var key string
	var orgID int64
	var deleteBlob bool
	err := db.WithTx(ctx, j.pool, func(tx pgx.Tx) error {
		// Lock first (waits out any in-flight reference attach, whose FK
		// takes a KEY SHARE on this row), THEN re-evaluate eligibility.
		var locked int64
		err := tx.QueryRow(ctx, `
			SELECT id FROM file WHERE id = $1 AND deleted_at IS NULL FOR UPDATE`,
			fileID).Scan(&locked)
		if err == pgx.ErrNoRows {
			return nil // purged by a concurrent sweep
		}
		if err != nil {
			return fmt.Errorf("janitor: lock: %w", err)
		}
		var eligible bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM file f WHERE `+predicate+`)`,
			fileID, cutoff).Scan(&eligible); err != nil {
			return fmt.Errorf("janitor: recheck: %w", err)
		}
		if !eligible {
			return nil
		}
		if err := tx.QueryRow(ctx, `
			UPDATE file SET deleted_at = now()
			WHERE id = $1 RETURNING org_id, storage_key`,
			fileID).Scan(&orgID, &key); err != nil {
			return fmt.Errorf("janitor: purge: %w", err)
		}
		var twinLive bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM file
			  WHERE org_id = $1 AND storage_key = $2 AND kind = 1 AND deleted_at IS NULL)`,
			orgID, key).Scan(&twinLive); err != nil {
			return fmt.Errorf("janitor: twin check: %w", err)
		}
		deleteBlob = !twinLive
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityFile, EntityID: fileID, Verb: "file.purged",
			Payload: eventlog.MustPayload(map[string]any{
				"file_id": fileID, "reason": reason, "blob_deleted": deleteBlob}),
		}); err != nil {
			return fmt.Errorf("janitor: event: %w", err)
		}
		*purged++
		return nil
	})
	if err != nil {
		return err
	}
	if deleteBlob {
		if err := j.store.Delete(ctx, key); err != nil {
			// The row is already gone; an orphaned blob is recoverable by a
			// rerun of the backend's own tooling, never a correctness issue.
			j.log.Warn("janitor: blob delete failed", "key", key, "err", err)
			return nil
		}
		*blobs++
		// P-18: the thumbnail is a DERIVED blob (files.ThumbKey) under the
		// original's content-addressed key, never a file row — so it is
		// reclaimed HERE, gated by the SAME twin rule as the original (the
		// deleteBlob = !twinLive decision above): a live twin keeps the shared
		// original AND its shared thumb key. Best-effort like the original's
		// own delete; an orphaned thumb is recoverable, never a live-row issue.
		if err := j.store.Delete(ctx, files.ThumbKey(key)); err != nil {
			j.log.Warn("janitor: thumb delete failed", "key", key, "err", err)
		}
	}
	return nil
}

// vacuumMessages tombstones messages whose age has passed their scope's
// retention policy (P-17). It reuses deleted_at — every product read path
// already filters it, so a vacuumed message vanishes at once from the authed
// and anonymous surfaces alike — and additionally stamps
// retention_vacuumed_at so the purge lane can find it after the restore
// window. Content is left in place (hidden, not blanked), recoverable until
// the purge. Each row is locked and re-checked in its own transaction (the
// sweepFiles pattern): a legal hold or policy change committed between scan
// and lock spares the message.
func (j *Janitor) vacuumMessages(ctx context.Context, now time.Time, vacuumed *int) error {
	for {
		rows, err := j.pool.Query(ctx, `
			SELECT m.id FROM message m WHERE `+messageVacuumPredicate+`
			ORDER BY m.id LIMIT `+fmt.Sprint(sweepBatch),
			nil, now)
		if err != nil {
			return fmt.Errorf("janitor: vacuum scan: %w", err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("janitor: vacuum scan: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("janitor: vacuum scan: %w", err)
		}
		for _, id := range ids {
			if err := j.vacuumMessage(ctx, id, now, vacuumed); err != nil {
				return err
			}
		}
		if len(ids) < sweepBatch {
			return nil
		}
	}
}

// vacuumMessage tombstones one message in a transaction: lock the live row,
// re-check the full predicate, set deleted_at + retention_vacuumed_at to the
// sweep clock, then drop its pins and decrement its thread's count — the
// DeleteMessage side-effects that keep the per-channel pin cap and thread
// counters honest. Content is left in place for recovery; the purge lane
// removes it after the window. Emits retention.message_vacuumed (system
// actor).
func (j *Janitor) vacuumMessage(ctx context.Context, msgID int64, now time.Time, vacuumed *int) error {
	return db.WithTx(ctx, j.pool, func(tx pgx.Tx) error {
		var locked int64
		err := tx.QueryRow(ctx,
			`SELECT id FROM message WHERE id = $1 AND deleted_at IS NULL FOR UPDATE`,
			msgID).Scan(&locked)
		if err == pgx.ErrNoRows {
			return nil // deleted or vacuumed by a concurrent actor
		}
		if err != nil {
			return fmt.Errorf("janitor: vacuum lock: %w", err)
		}
		var eligible bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM message m WHERE `+messageVacuumPredicate+`)`,
			msgID, now).Scan(&eligible); err != nil {
			return fmt.Errorf("janitor: vacuum recheck: %w", err)
		}
		if !eligible {
			return nil
		}
		var orgID, threadID int64
		var channelID *int64
		if err := tx.QueryRow(ctx, `
			UPDATE message SET deleted_at = $2, retention_vacuumed_at = $2
			WHERE id = $1 RETURNING org_id, thread_id, channel_id`,
			msgID, now).Scan(&orgID, &threadID, &channelID); err != nil {
			return fmt.Errorf("janitor: vacuum: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM pin WHERE message_id = $1`, msgID); err != nil {
			return fmt.Errorf("janitor: vacuum pins: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET message_count = GREATEST(message_count - 1, 0)
			WHERE id = $1 AND kind = 1`, threadID); err != nil {
			return fmt.Errorf("janitor: vacuum thread count: %w", err)
		}
		payload := map[string]any{"message_id": msgID, "thread_id": threadID}
		if channelID != nil {
			payload["channel_id"] = *channelID
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "retention.message_vacuumed",
			Payload: eventlog.MustPayload(payload),
		}); err != nil {
			return fmt.Errorf("janitor: vacuum event: %w", err)
		}
		*vacuumed++
		return nil
	})
}

// purgeVacuumedMessages permanently removes messages whose restore window
// has elapsed (P-17 commit 2). The batch scan / per-row lock + recheck
// mirrors the other lanes.
func (j *Janitor) purgeVacuumedMessages(ctx context.Context, cutoff time.Time, purged *int) error {
	for {
		rows, err := j.pool.Query(ctx, `
			SELECT m.id FROM message m WHERE `+purgeVacuumedPredicate+`
			ORDER BY m.id LIMIT `+fmt.Sprint(sweepBatch),
			nil, cutoff)
		if err != nil {
			return fmt.Errorf("janitor: purge scan: %w", err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("janitor: purge scan: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("janitor: purge scan: %w", err)
		}
		for _, id := range ids {
			if err := j.purgeVacuumedMessage(ctx, id, cutoff, purged); err != nil {
				return err
			}
		}
		if len(ids) < sweepBatch {
			return nil
		}
	}
}

// purgeVacuumedMessage permanently removes one message and everything that
// references it, in a transaction after re-checking eligibility under a row
// lock. Child rows are deleted (reactions, pins, saved items, user flags,
// reports, link previews, revisions, reminders, and the scheduled_message
// send record — cleared outright rather than NULLed, which would re-arm the
// due index) and the no-FK back-references are nulled (thread.root_message_id,
// forwarded_from_message_id on any forward copy). The event_log has no FK to
// message, so the audit trail — including this message's own created/vacuumed
// entries — survives; a retention.message_purged entry is appended.
func (j *Janitor) purgeVacuumedMessage(ctx context.Context, msgID int64, cutoff time.Time, purged *int) error {
	return db.WithTx(ctx, j.pool, func(tx pgx.Tx) error {
		var orgID int64
		err := tx.QueryRow(ctx,
			`SELECT org_id FROM message WHERE id = $1 AND retention_vacuumed_at IS NOT NULL FOR UPDATE`,
			msgID).Scan(&orgID)
		if err == pgx.ErrNoRows {
			return nil // purged or restored by a concurrent actor
		}
		if err != nil {
			return fmt.Errorf("janitor: purge lock: %w", err)
		}
		var eligible bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM message m WHERE `+purgeVacuumedPredicate+`)`,
			msgID, cutoff).Scan(&eligible); err != nil {
			return fmt.Errorf("janitor: purge recheck: %w", err)
		}
		if !eligible {
			return nil
		}
		for _, stmt := range []string{
			`DELETE FROM reaction WHERE message_id = $1`,
			`DELETE FROM pin WHERE message_id = $1`,
			`DELETE FROM saved_item WHERE message_id = $1`,
			`DELETE FROM message_user_flag WHERE message_id = $1`,
			`DELETE FROM message_report WHERE message_id = $1`,
			`DELETE FROM message_link_preview WHERE message_id = $1`,
			`DELETE FROM message_revision WHERE message_id = $1`,
			`DELETE FROM reminder WHERE message_id = $1`,
			`DELETE FROM scheduled_message WHERE sent_message_id = $1`,
			`UPDATE thread SET root_message_id = NULL WHERE root_message_id = $1`,
			`UPDATE message SET forwarded_from_message_id = NULL WHERE forwarded_from_message_id = $1`,
		} {
			if _, err := tx.Exec(ctx, stmt, msgID); err != nil {
				return fmt.Errorf("janitor: purge cleanup: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM message WHERE id = $1`, msgID); err != nil {
			return fmt.Errorf("janitor: purge: %w", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityMessage, EntityID: msgID, Verb: "retention.message_purged",
			Payload: eventlog.MustPayload(map[string]any{"message_id": msgID}),
		}); err != nil {
			return fmt.Errorf("janitor: purge event: %w", err)
		}
		*purged++
		return nil
	})
}

// scrubRevisions purges prior message versions wherever the EFFECTIVE
// retention policy says keep_edits=false — the AD-3 "delete edits" toggle.
// The scope ladder is nearest-wins: channel override, then the org default;
// messages outside any channel (DMs, space threads) resolve to the org
// default until their own rungs land. Only content-bearing edit revisions
// (kind 1 content, 2 title) are scrubbed, and only their prev_source/
// prev_ast: the structural record (who edited, when) survives, matching
// AD-5 — purged versions are absent from exports by design. Kind 4 rows
// (deletion capture) are evidence for the message-retention lane, never
// touched here. Holds freeze matching content: a custodian's or held
// channel's revisions keep their originals (AD-4). New edits in a
// keep_edits=false scope are scrubbed on the next sweep, so prior versions
// linger at most one janitor interval.
func (j *Janitor) scrubRevisions(ctx context.Context, scrubbed *int) error {
	for {
		var batch int
		err := db.WithTx(ctx, j.pool, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `
				SELECT mr.id, m.org_id
				FROM message_revision mr
				JOIN message m ON m.id = mr.message_id
				WHERE mr.kind IN (1, 2)
				  AND (mr.prev_source IS NOT NULL OR mr.prev_ast IS NOT NULL)
				  AND COALESCE(
				      (SELECT rp.keep_edits FROM retention_policy rp
				       WHERE rp.org_id = m.org_id AND rp.scope_type = 3
				         AND rp.scope_id = m.channel_id),
				      (SELECT rp.keep_edits FROM retention_policy rp
				       WHERE rp.org_id = m.org_id AND rp.scope_type = 1
				         AND rp.scope_id = m.org_id),
				      true) = false
				  AND NOT EXISTS (
				      SELECT 1 FROM legal_hold h
				      WHERE h.org_id = m.org_id AND h.released_at IS NULL
				        AND (h.custodian_user_id = m.author_id
				          OR h.channel_id = m.channel_id))
				ORDER BY mr.id LIMIT `+fmt.Sprint(sweepBatch))
			if err != nil {
				return fmt.Errorf("janitor: scrub scan: %w", err)
			}
			var ids []int64
			perOrg := map[int64]int{}
			for rows.Next() {
				var id, orgID int64
				if err := rows.Scan(&id, &orgID); err != nil {
					rows.Close()
					return fmt.Errorf("janitor: scrub scan: %w", err)
				}
				ids = append(ids, id)
				perOrg[orgID]++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("janitor: scrub scan: %w", err)
			}
			if len(ids) == 0 {
				return nil
			}
			if _, err := tx.Exec(ctx, `
				UPDATE message_revision
				SET prev_source = NULL, prev_ast = NULL
				WHERE id = ANY($1)`, ids); err != nil {
				return fmt.Errorf("janitor: scrub: %w", err)
			}
			for orgID, n := range perOrg {
				if _, err := eventlog.Append(ctx, tx, eventlog.Event{
					OrgID: orgID, ActorKind: enum.ActorSystem,
					EntityType: enum.EntityOrg, EntityID: orgID,
					Verb:    "retention.revisions_scrubbed",
					Payload: eventlog.MustPayload(map[string]any{"count": n}),
				}); err != nil {
					return fmt.Errorf("janitor: scrub event: %w", err)
				}
			}
			batch = len(ids)
			return nil
		})
		if err != nil {
			return err
		}
		*scrubbed += batch
		if batch < sweepBatch {
			return nil
		}
	}
}
