package perms

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// RebuildWorker drains closure_rebuild_job — the async lane for full-org
// closure recomputes (bulk import, big restructures) that must never run on
// a request path. One job per transaction: the FOR UPDATE SKIP LOCKED claim
// is multi-node safe; the rebuild runs in a savepoint so a poisoned org
// records its reason on the still-claimed row without stalling the lane; the
// done-mark commits atomically with the rebuild's version flip.
//
// Delivery semantics: AT-LEAST-ONCE across crashes (an unfinished claim
// rolls back to pending and is re-run) with idempotent effect — every
// attempt fills a fresh closure version and the atomic pointer flip is the
// only visible change, so a re-run can never double-apply.
type RebuildWorker struct {
	pool *pgxpool.Pool
	svc  *Service
	log  *slog.Logger
}

func NewRebuildWorker(pool *pgxpool.Pool, svc *Service, log *slog.Logger) *RebuildWorker {
	return &RebuildWorker{pool: pool, svc: svc, log: log}
}

// RunOnce drains every claimable pending job and reports how many it settled
// (done + failed). Tests and the import CLI drive this directly — no sleeps.
func (w *RebuildWorker) RunOnce(ctx context.Context) (int, error) {
	var settled int
	for {
		var advanced bool
		err := db.WithTx(ctx, w.pool, func(tx pgx.Tx) error {
			var jobID, orgID int64
			err := tx.QueryRow(ctx, `
				SELECT id, org_id FROM closure_rebuild_job
				WHERE status = 1
				ORDER BY id LIMIT 1
				FOR UPDATE SKIP LOCKED`).Scan(&jobID, &orgID)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return apperr.Internal("claim rebuild job", err)
			}
			advanced = true
			sp, err := tx.Begin(ctx)
			if err != nil {
				return apperr.Internal("savepoint", err)
			}
			if rebuildErr := w.svc.RebuildClosure(ctx, sp, orgID); rebuildErr != nil {
				_ = sp.Rollback(ctx)
				if _, err := tx.Exec(ctx, `
					UPDATE closure_rebuild_job
					SET status = 3, finished_at = now(), error = $2
					WHERE id = $1`, jobID, rebuildErr.Error()); err != nil {
					return apperr.Internal("mark rebuild failed", err)
				}
				w.log.Warn("perms: closure rebuild failed",
					"org", orgID, "job", jobID, "err", rebuildErr)
				return nil
			}
			if err := sp.Commit(ctx); err != nil {
				return apperr.Internal("commit rebuild", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE closure_rebuild_job SET status = 2, finished_at = now()
				WHERE id = $1`, jobID); err != nil {
				return apperr.Internal("mark rebuild done", err)
			}
			return nil
		})
		if err != nil || !advanced {
			return settled, err
		}
		settled++
	}
}

// Run drains on a timer until ctx ends (weftd's serve lane). Enqueues are
// rare bulk events; polling bounds post-import staleness without a dedicated
// wakeup channel. The import CLI additionally drains inline before exiting.
func (w *RebuildWorker) Run(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				w.log.Warn("perms: rebuild lane", "err", err)
			}
		}
	}
}
