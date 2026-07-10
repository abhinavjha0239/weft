package compliance

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
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
	Interval       time.Duration
}

func NewJanitor(pool *pgxpool.Pool, store blob.Store, log *slog.Logger) *Janitor {
	return &Janitor{
		pool: pool, store: store, log: log,
		UnclaimedGrace: 35 * 24 * time.Hour,
		DeadRefWindow:  30 * 24 * time.Hour,
		Interval:       time.Hour,
	}
}

// Report counts one sweep's work.
type Report struct {
	UnclaimedPurged   int `json:"unclaimed_purged"`
	DeadRefPurged     int `json:"dead_ref_purged"`
	BlobsDeleted      int `json:"blobs_deleted"`
	RevisionsScrubbed int `json:"revisions_scrubbed"`
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
					"blobs", rep.BlobsDeleted, "revisions", rep.RevisionsScrubbed)
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
	return rep, nil
}

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
	}
	return nil
}
