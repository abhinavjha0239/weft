package unfurl

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const (
	consumerName  = "unfurl"
	sweepInterval = 5 * time.Second
	batchSize     = 500
)

// Runner is the unfurl lane: a named, cursor-tracked, txid-gated event-log
// consumer (the M0 Consumer pattern) on message.created. Fetches happen
// ONLY here — the send path never waits on a remote page.
type Runner struct {
	pool     *pgxpool.Pool
	svc      *Service
	consumer *eventlog.Consumer
	log      *slog.Logger
}

func NewRunner(pool *pgxpool.Pool, svc *Service, log *slog.Logger) *Runner {
	return &Runner{
		pool:     pool,
		svc:      svc,
		consumer: eventlog.NewConsumer(pool, consumerName, batchSize),
		log:      log,
	}
}

// Run blocks until ctx ends: LISTEN on the event channel and process the
// signalled org; a sweep catches anything a missed NOTIFY left behind.
func (r *Runner) Run(ctx context.Context) {
	go r.sweep(ctx)
	for ctx.Err() == nil {
		if err := r.listenLoop(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("unfurl: listen loop restarting", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (r *Runner) listenLoop(ctx context.Context) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN event_log`); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if orgID, err := strconv.ParseInt(n.Payload, 10, 64); err == nil {
			if err := r.ProcessOrg(ctx, orgID); err != nil && ctx.Err() == nil {
				r.log.Warn("unfurl: process", "org", orgID, "err", err)
			}
		}
	}
}

func (r *Runner) sweep(ctx context.Context) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rows, err := r.pool.Query(ctx, `SELECT id FROM org`)
			if err != nil {
				continue
			}
			var ids []int64
			for rows.Next() {
				var id int64
				if rows.Scan(&id) == nil {
					ids = append(ids, id)
				}
			}
			rows.Close()
			for _, id := range ids {
				_ = r.ProcessOrg(ctx, id)
			}
		}
	}
}

// ProcessOrg drains the org's cursor. Infra errors abort the batch unacked
// (at-least-once — the idempotent cache upsert and the association PK make
// the replay a no-op); a message that vanished or an org with the toggle
// off just moves on.
func (r *Runner) ProcessOrg(ctx context.Context, orgID int64) error {
	for {
		batch, err := r.consumer.Poll(ctx, orgID)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, ev := range batch {
			if ev.Verb != "message.created" {
				continue
			}
			if err := r.handle(ctx, ev); err != nil {
				return err
			}
		}
		if err := r.consumer.Ack(ctx, orgID, batch[len(batch)-1].ID); err != nil {
			return err
		}
		if len(batch) < batchSize {
			return nil
		}
	}
}

func (r *Runner) handle(ctx context.Context, ev eventlog.Row) error {
	enabled, err := r.svc.Enabled(ctx, ev.OrgID)
	if err != nil || !enabled {
		return err
	}
	var ast []byte
	var threadID int64
	var channelID, dmSpaceID *int64
	err = r.pool.QueryRow(ctx, `
		SELECT ast, thread_id, channel_id, dm_space_id FROM message
		WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
		ev.EntityID, ev.OrgID).Scan(&ast, &threadID, &channelID, &dmSpaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // deleted since; nothing to preview
	}
	if err != nil {
		return apperr.Internal("unfurl: load message", err)
	}
	var doc content.Node
	if err := json.Unmarshal(ast, &doc); err != nil {
		return nil // unparseable ast is not this lane's problem
	}
	links := r.svc.externalLinks(doc.Links())
	if len(links) == 0 {
		return nil
	}

	// Fetches run OUTSIDE any tx (seconds-slow remote pages must not hold
	// a connection's transaction open).
	type hit struct {
		previewID int64
		position  int16
	}
	var hits []hit
	for i, u := range links {
		id, ok, err := r.svc.previewFor(ctx, u)
		if err != nil {
			return err
		}
		if ok {
			hits = append(hits, hit{previewID: id, position: int16(i)})
		}
	}
	if len(hits) == 0 {
		return nil
	}

	return db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		inserted := int64(0)
		for _, h := range hits {
			ct, err := tx.Exec(ctx, `
				INSERT INTO message_link_preview (message_id, preview_id, position)
				VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				ev.EntityID, h.previewID, h.position)
			if err != nil {
				return apperr.Internal("unfurl: associate", err)
			}
			inserted += ct.RowsAffected()
		}
		if inserted == 0 {
			return nil // replay — associations already landed, no second event
		}
		// Mirrors message.created's container fields so the gateway routes
		// the refresh to exactly the message's audience.
		payload := map[string]any{"message_id": ev.EntityID, "thread_id": threadID}
		if channelID != nil {
			payload["channel_id"] = *channelID
		}
		if dmSpaceID != nil {
			payload["dm_space_id"] = *dmSpaceID
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: ev.OrgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityMessage, EntityID: ev.EntityID,
			Verb: "message.preview_added", Payload: eventlog.MustPayload(payload),
		}); err != nil {
			return apperr.Internal("unfurl: append event", err)
		}
		return nil
	})
}
