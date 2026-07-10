package notification

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
)

const (
	consumerName  = "notifications"
	sweepInterval = 5 * time.Second
	batchSize     = 500
)

// Fanout delivers a live in-app ping to one user's connections; the gateway
// implements it. Delivery is best-effort — the notification ROW is the
// truth an offline client reads on its next fetch.
type Fanout interface {
	NotifyUser(ctx context.Context, orgID, userID int64, payload json.RawMessage)
}

// Runner is the materializer: a named, cursor-tracked, txid-gated event-log
// consumer (the same Consumer the M0 spine shipped), NOTIFY-driven with a
// slow sweep. Processing is idempotent — at-least-once replays hit the
// dedupe key and vanish.
type Runner struct {
	pool     *pgxpool.Pool
	consumer *eventlog.Consumer
	fan      Fanout
	log      *slog.Logger
}

func NewRunner(pool *pgxpool.Pool, fan Fanout, log *slog.Logger) *Runner {
	return &Runner{
		pool:     pool,
		consumer: eventlog.NewConsumer(pool, consumerName, batchSize),
		fan:      fan,
		log:      log,
	}
}

// Run blocks until ctx ends: LISTEN on the event channel and process the
// signalled org; a sweep catches anything a missed NOTIFY left behind.
func (r *Runner) Run(ctx context.Context) {
	go r.sweep(ctx)
	for ctx.Err() == nil {
		if err := r.listenLoop(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("notification: listen loop restarting", "err", err)
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
				r.log.Warn("notification: process", "org", orgID, "err", err)
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

type messagePayload struct {
	MessageID int64   `json:"message_id"`
	ThreadID  int64   `json:"thread_id"`
	ChannelID int64   `json:"channel_id"`
	DMSpaceID int64   `json:"dm_space_id"`
	Mentions  []int64 `json:"mentions"`
}

// ProcessOrg drains the org's pending events into notification rows and
// acks the cursor. Exported for tests; production calls arrive via Run.
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
			if err := r.processEvent(ctx, ev); err != nil {
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

func (r *Runner) processEvent(ctx context.Context, ev eventlog.Row) error {
	// Backfill semantics (ADR-003 E4): imported history never notifies.
	if ev.Verb != "message.created" || ev.ActorKind == enum.ActorImporter {
		return nil
	}
	var p messagePayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return nil // malformed payloads are logged history, not a stall
	}
	author := int64(0)
	if ev.ActorID != nil {
		author = *ev.ActorID
	}

	mentioned := map[int64]bool{}
	for _, uid := range p.Mentions {
		if uid == author {
			continue // self-mentions never notify
		}
		mentioned[uid] = true
		if err := r.insert(ctx, ev, p, uid, KindMention, author); err != nil {
			return err
		}
	}
	// DM messages notify every OTHER participant; a mention in the same
	// message wins (more specific reason), so those users are skipped.
	if p.DMSpaceID != 0 {
		rows, err := r.pool.Query(ctx,
			`SELECT user_id FROM dm_participant WHERE dm_space_id = $1`, p.DMSpaceID)
		if err != nil {
			return err
		}
		var parts []int64
		for rows.Next() {
			var uid int64
			if rows.Scan(&uid) == nil {
				parts = append(parts, uid)
			}
		}
		rows.Close()
		for _, uid := range parts {
			if uid == author || mentioned[uid] {
				continue
			}
			if err := r.insert(ctx, ev, p, uid, KindDM, author); err != nil {
				return err
			}
		}
	}
	return nil
}

// insert materializes one notification, gated on the recipient's ability to
// SEE the message: channel mentions require membership (a private-channel
// mention must not leak the channel's existence), DM and space threads use
// their own containment (participants were just resolved; space threads are
// org-visible in the v1 slice).
func (r *Runner) insert(ctx context.Context, ev eventlog.Row, p messagePayload, userID int64, kind int16, author int64) error {
	var actorID *int64
	if author != 0 {
		actorID = &author
	}
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO notification (org_id, user_id, kind, entity_type, entity_id, actor_id, created_at)
		SELECT $1, $2, $3, $4, $5, $6, $7
		WHERE $8 = 0 OR EXISTS (
		    SELECT 1 FROM channel_member
		    WHERE channel_id = $8 AND user_id = $2 AND unsubscribed_at IS NULL)
		ON CONFLICT (user_id, kind, entity_type, entity_id) DO NOTHING`,
		ev.OrgID, userID, kind, int16(enum.EntityMessage), p.MessageID,
		actorID, ev.RecordedAt, p.ChannelID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() > 0 && r.fan != nil {
		payload, _ := json.Marshal(map[string]any{
			"kind": kind, "entity_type": int16(enum.EntityMessage),
			"entity_id": p.MessageID, "thread_id": p.ThreadID, "actor_id": author,
		})
		r.fan.NotifyUser(ctx, ev.OrgID, userID, payload)
	}
	return nil
}
