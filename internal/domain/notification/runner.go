package notification

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
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
// slow sweep. Resolution reads the CURRENT muting/level settings, so
// replays are no-ops only while decisions are stable — the dedupe key
// absorbs redelivery, not settings changes. The normal at-least-once window
// is seconds wide; a deliberate full cursor reset re-evaluates history
// under today's settings, which is exactly what "replay = reset cursor"
// means for this consumer.
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
	MessageID   int64   `json:"message_id"`
	ThreadID    int64   `json:"thread_id"`
	ChannelID   int64   `json:"channel_id"`
	DMSpaceID   int64   `json:"dm_space_id"`
	Mentions    []int64 `json:"mentions"`
	NewMentions []int64 `json:"new_mentions"`
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
	if (ev.Verb != "message.created" && ev.Verb != "message.edited") ||
		ev.ActorKind == enum.ActorImporter {
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

	// An edit pings only the NEWLY-added mentions; the dedupe key would
	// swallow repeats anyway, but the intent belongs in the code.
	mentionList := p.Mentions
	if ev.Verb == "message.edited" {
		mentionList = p.NewMentions
	}
	// Keyword candidates resolve BEFORE the passes so specificity can win:
	// followed (3) beats keyword (4) beats channel activity (5).
	keyworded, err := r.keywordMatches(ctx, ev, p, author)
	if err != nil {
		return err
	}

	mentioned := map[int64]bool{}
	for _, uid := range mentionList {
		if uid == author {
			continue // self-mentions never notify
		}
		mentioned[uid] = true
		delete(keyworded, uid)
		if err := r.insert(ctx, ev, p, uid, KindMention, author); err != nil {
			return err
		}
	}
	// Channel messages resolve N-1 steps 2–3 in one pass: followed threads
	// boost (and override the channel level); level=all members get
	// activity pings; the SEPARATE mute flag suppresses both — with an
	// unmuted thread reviving activity inside a muted channel — while
	// mentions and DMs break through mute (handled above/below, which is
	// why the notified-set skip matters). More specific reasons win.
	if p.ChannelID != 0 && p.ThreadID != 0 && ev.Verb == "message.created" {
		rows, err := r.pool.Query(ctx, `
			SELECT cm.user_id,
			       CASE WHEN COALESCE(ts.state, 0) = 1 THEN true ELSE false END
			FROM channel_member cm
			LEFT JOIN thread_subscription ts
			  ON ts.thread_id = $2 AND ts.user_id = cm.user_id
			WHERE cm.channel_id = $1 AND cm.unsubscribed_at IS NULL
			  AND cm.user_id <> $3
			  AND (
			    (COALESCE(ts.state, 0) = 1 AND NOT cm.muted)
			    OR (cm.level = 1 AND COALESCE(ts.state, 0) <> 2
			        AND (NOT cm.muted OR COALESCE(ts.state, 0) = 3))
			  )`, p.ChannelID, p.ThreadID, author)
		if err != nil {
			return err
		}
		type rec struct {
			uid      int64
			followed bool
		}
		var recs []rec
		for rows.Next() {
			var x rec
			if rows.Scan(&x.uid, &x.followed) == nil {
				recs = append(recs, x)
			}
		}
		rows.Close()
		for _, x := range recs {
			if mentioned[x.uid] {
				continue // the more specific reason already fired
			}
			kind := int16(KindChannelActivity)
			if x.followed {
				kind = KindFollowedThread
			} else if keyworded[x.uid] {
				kind = KindKeyword // a keyword upgrades plain activity
			}
			delete(keyworded, x.uid)
			if err := r.insert(ctx, ev, p, x.uid, kind, author); err != nil {
				return err
			}
		}
	}

	// Remaining keyword matches (members outside the level/follow pass, or
	// edits that newly introduced a word) get the kind-4 row.
	for uid := range keyworded {
		if err := r.insert(ctx, ev, p, uid, KindKeyword, author); err != nil {
			return err
		}
	}

	// DM messages notify every OTHER participant; a mention in the same
	// message wins (more specific reason), so those users are skipped.
	// Edits never re-ping the conversation.
	if p.DMSpaceID != 0 && ev.Verb == "message.created" {
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

// keywordMatches finds channel members whose alert words appear in the
// message (kind 4): a cheap substring prefilter in SQL over the channel's
// members' words, refined to WORD boundaries in Go. Mute-respecting like
// plain activity (a muted thread suppresses; an unmuted thread revives
// inside a muted channel) — keywords do not break through mute the way
// mentions do. Users already notified for this message are excluded (the
// message.edited path must not double-ping), and DMs are skipped: the DM
// row itself already pings every participant.
func (r *Runner) keywordMatches(ctx context.Context, ev eventlog.Row, p messagePayload, author int64) (map[int64]bool, error) {
	if p.ChannelID == 0 || p.ThreadID == 0 {
		return nil, nil
	}
	var source string
	if err := r.pool.QueryRow(ctx,
		`SELECT source FROM message WHERE id = $1 AND deleted_at IS NULL`,
		p.MessageID).Scan(&source); err != nil {
		return nil, nil // deleted or gone: nothing to match
	}
	rows, err := r.pool.Query(ctx, `
		SELECT aw.user_id, aw.word
		FROM alert_word aw
		JOIN channel_member cm ON cm.user_id = aw.user_id
		LEFT JOIN thread_subscription ts
		  ON ts.thread_id = $2 AND ts.user_id = aw.user_id
		WHERE cm.channel_id = $1 AND cm.unsubscribed_at IS NULL
		  AND aw.user_id <> $3
		  AND COALESCE(ts.state, 0) <> 2
		  AND (NOT cm.muted OR COALESCE(ts.state, 0) = 3)
		  AND position(aw.word IN lower($4)) > 0
		  AND NOT EXISTS (
		      SELECT 1 FROM notification n
		      WHERE n.user_id = aw.user_id AND n.entity_type = $5 AND n.entity_id = $6)`,
		p.ChannelID, p.ThreadID, author, source,
		int16(enum.EntityMessage), p.MessageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	matched := map[int64]bool{}
	for rows.Next() {
		var uid int64
		var word string
		if rows.Scan(&uid, &word) != nil {
			continue
		}
		if matched[uid] {
			continue
		}
		re, err := regexp.Compile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`)
		if err != nil {
			continue
		}
		if re.MatchString(source) {
			matched[uid] = true
		}
	}
	return matched, rows.Err()
}
