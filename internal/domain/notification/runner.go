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
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

const (
	consumerName  = "notifications"
	sweepInterval = 5 * time.Second
	batchSize     = 500
	// reconcileInterval paces the F-17 safety-net sweep (janitor cadence):
	// it recomputes each built channel's deliverability set and repairs +
	// reports divergence, because a missed invalidation would silently
	// DROP notifications.
	reconcileInterval = time.Hour
	// candidatesScannedMetric counts candidate rows the materializer
	// examines per message — the F-17 O(reasons)-not-O(members) invariant,
	// observable.
	candidatesScannedMetric = "notification_candidates_scanned_total"
)

// Fanout delivers a live in-app ping to one user's connections; the gateway
// implements it. Delivery is best-effort — the notification ROW is the
// truth an offline client reads on its next fetch.
type Fanout interface {
	NotifyUser(ctx context.Context, orgID, userID int64, payload json.RawMessage)
}

// UnreadMaintainer keeps the S6 O(1) unread counters current off THIS
// materializer's existing per-message pass (the F-17 twin): the counter rides
// the async consumer, never messaging's O(1) send path. messaging owns the
// container_unread_counter table and implements it; a nil maintainer (a
// composition without unread counters, e.g. a focused notification test) skips
// it. Signatures are primitive-only so the implementor needs no notification
// import — the Fanout seam pattern.
type UnreadMaintainer interface {
	ApplyMessageUnread(ctx context.Context, orgID, channelID, dmSpaceID, messageID, authorID, eventID int64, mentions []int64) error
	ReconcileUnreadOnce(ctx context.Context) error
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
	// scanned counts channel members walked by the activity/follow candidate
	// pass — the O(channel size) fan-out cost this consumer bears per message
	// (S0). Optional (default Nop). Set once at wiring.
	scanned metrics.Counter
	// deliv owns the F-17 candidate set: the runner lazily builds it per
	// channel and patches it from member.joined/left events.
	deliv *Deliverability
	// unread is the S6 O(1) unread-counter maintainer (messaging owns the
	// table). Optional (nil skips): the counter rides this consumer's
	// per-message pass and its slow reconcile ticker.
	unread UnreadMaintainer
}

func NewRunner(pool *pgxpool.Pool, fan Fanout, log *slog.Logger) *Runner {
	r := &Runner{
		pool:     pool,
		consumer: eventlog.NewConsumer(pool, consumerName, batchSize),
		fan:      fan,
		log:      log,
		deliv:    NewDeliverability(pool, log),
	}
	r.SetMetrics(metrics.Nop())
	return r
}

// SetMetrics wires an observability registry (S0), propagating it to the
// underlying event-log consumer (so fanout_events_total{consumer=notifications}
// and consumer_lag are published too). Optional — default Nop. Call once before
// Run/ProcessOrg.
func (r *Runner) SetMetrics(reg metrics.Registry) {
	r.scanned = reg.Counter(candidatesScannedMetric)
	r.consumer.SetMetrics(reg)
}

// SetUnread wires the S6 unread-counter maintainer (messaging). Optional — a
// nil maintainer leaves the counter unmaintained (the read falls back to
// nothing, so a composition that wants counters must wire this). Call once
// before Run/ProcessOrg (the Fanout seam pattern).
func (r *Runner) SetUnread(m UnreadMaintainer) { r.unread = m }

// Run blocks until ctx ends: LISTEN on the event channel and process the
// signalled org; a sweep catches anything a missed NOTIFY left behind.
func (r *Runner) Run(ctx context.Context) {
	go r.sweep(ctx)
	go r.reconcileLoop(ctx)
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

// reconcileLoop drives the F-17 divergence sweep on the slow ticker;
// ReconcileOnce does the per-channel work and logging.
func (r *Runner) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.deliv.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				r.log.Warn("notification: deliverability reconcile", "err", err)
			}
			// S6: the same slow ticker drives the O(1) unread-counter sweep
			// (recompute from the live aggregate, repair + Warn divergence).
			if r.unread != nil {
				if err := r.unread.ReconcileUnreadOnce(ctx); err != nil && ctx.Err() == nil {
					r.log.Warn("notification: unread reconcile", "err", err)
				}
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

// hintPayload reads the F-17b coalescing stamp bulk producers set on the
// event hint (the reserved field, ADR-002 P1): every item event of one
// bulk operation carries the same batch_id, and the materializer folds a
// recipient's N item rows into ONE digest row per (user, kind, batch).
type hintPayload struct {
	BatchID *int64 `json:"batch_id"`
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

// membershipPayload mirrors the member.joined/left event payload shape.
type membershipPayload struct {
	ChannelID int64 `json:"channel_id"`
	UserID    int64 `json:"user_id"`
}

func (r *Runner) processEvent(ctx context.Context, ev eventlog.Row) error {
	// F-17 invalidation: membership changes patch the deliverability set
	// off the event log (a single-user resync — O(1) per join, never a
	// channel rebuild; consuming events keeps messaging and identity free
	// of a notification import).
	if ev.Verb == "member.joined" || ev.Verb == "member.left" {
		var m membershipPayload
		if err := json.Unmarshal(ev.Payload, &m); err != nil ||
			m.ChannelID == 0 || m.UserID == 0 {
			return nil // malformed payloads are logged history, not a stall
		}
		return r.deliv.PatchMembership(ctx, ev.OrgID, m.ChannelID, m.UserID)
	}
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
	// F-17b: a batch-stamped hint switches every insert for this event
	// into digest mode. A malformed hint is ignored like a malformed
	// payload — never a stall.
	var batchID *int64
	if len(ev.Hint) > 0 {
		var h hintPayload
		if json.Unmarshal(ev.Hint, &h) == nil {
			batchID = h.BatchID
		}
	}
	// S6: a newly-created message increments the O(1) unread counter for every
	// live container member except the author (mentions bump mention_count).
	// This rides THIS consumer pass — one set-based statement, off messaging's
	// send path — and only for creates: edits add no unread, and the early
	// return above already excluded imports (backfills never notify or unread).
	if ev.Verb == "message.created" && r.unread != nil {
		if err := r.unread.ApplyMessageUnread(ctx, ev.OrgID, p.ChannelID, p.DMSpaceID,
			p.MessageID, author, ev.ID, p.Mentions); err != nil {
			return err
		}
	}
	// F-17 lazy backfill: the first channel message after deploy derives
	// the deliverability set (the last O(members) pass this channel pays).
	if p.ChannelID != 0 && p.ThreadID != 0 {
		if err := r.deliv.EnsureBuilt(ctx, ev.OrgID, p.ChannelID); err != nil {
			return err
		}
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
		if err := r.insert(ctx, ev, p, uid, KindMention, author, batchID); err != nil {
			return err
		}
	}
	// Channel messages resolve N-1 steps 2–3 in one pass over the F-17
	// deliverability set — O(actual reasons), never O(members). Candidates
	// are the channel's reason-1 (follows a thread here) and reason-2
	// (level=all) rows; their mute/level/follow SETTINGS are re-verified
	// LIVE per candidate below (the set caches candidacy, never settings,
	// so a stale-extra row costs one wasted scan and can never mint a
	// wrong notification). Semantics are unchanged: followed threads boost
	// (and override the channel level); level=all members get activity
	// pings; the SEPARATE mute flag suppresses both — with an unmuted
	// thread reviving activity inside a muted channel — while mentions and
	// DMs break through mute (handled above/below, which is why the
	// notified-set skip matters). More specific reasons win.
	if p.ChannelID != 0 && p.ThreadID != 0 && ev.Verb == "message.created" {
		rows, err := r.pool.Query(ctx, `
			SELECT DISTINCT cd.user_id, cm.muted, cm.level,
			       COALESCE(ts.state, 0::smallint)
			FROM channel_deliverability cd
			JOIN channel_member cm ON cm.channel_id = cd.channel_id
			     AND cm.user_id = cd.user_id AND cm.unsubscribed_at IS NULL
			LEFT JOIN thread_subscription ts
			  ON ts.thread_id = $2 AND ts.user_id = cd.user_id
			WHERE cd.channel_id = $1 AND cd.reason IN (1, 2) AND cd.medium = 1
			  AND cd.user_id <> $3`, p.ChannelID, p.ThreadID, author)
		if err != nil {
			return err
		}
		type rec struct {
			uid     int64
			muted   bool
			level   int16
			tsState int16
		}
		var recs []rec
		for rows.Next() {
			var x rec
			if rows.Scan(&x.uid, &x.muted, &x.level, &x.tsState) == nil {
				r.scanned.Add(1) // once per candidate row scanned
				recs = append(recs, x)
			}
		}
		rows.Close()
		for _, x := range recs {
			followed := x.tsState == 1
			// The live settings filter — exactly the decisions the
			// pre-F-17 channel_member join made in SQL.
			if !((followed && !x.muted) ||
				(x.level == 1 && x.tsState != 2 && (!x.muted || x.tsState == 3))) {
				continue
			}
			if mentioned[x.uid] {
				continue // the more specific reason already fired
			}
			kind := int16(KindChannelActivity)
			if followed {
				kind = KindFollowedThread
			} else if keyworded[x.uid] {
				kind = KindKeyword // a keyword upgrades plain activity
			}
			delete(keyworded, x.uid)
			if err := r.insert(ctx, ev, p, x.uid, kind, author, batchID); err != nil {
				return err
			}
		}
	}

	// Remaining keyword matches (members outside the level/follow pass, or
	// edits that newly introduced a word) get the kind-4 row.
	for uid := range keyworded {
		if err := r.insert(ctx, ev, p, uid, KindKeyword, author, batchID); err != nil {
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
			if err := r.insert(ctx, ev, p, uid, KindDM, author, batchID); err != nil {
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
//
// A non-nil batchID (F-17b) switches to digest mode: the row lands once
// per (user, kind, batch) — the batch dedupe key — so a bulk operation's
// N item events fold into ONE row per recipient. The conflict target is
// left open there because a batched insert may legitimately hit EITHER
// unique key (the entity dedupe on replay, the batch key on coalesce) and
// both mean "already recorded"; the unbatched path keeps the explicit
// 0010 target.
func (r *Runner) insert(ctx context.Context, ev eventlog.Row, p messagePayload, userID int64, kind int16, author int64, batchID *int64) error {
	var actorID *int64
	if author != 0 {
		actorID = &author
	}
	conflict := `ON CONFLICT (user_id, kind, entity_type, entity_id) DO NOTHING`
	if batchID != nil {
		conflict = `ON CONFLICT DO NOTHING`
	}
	ct, err := r.pool.Exec(ctx, `
		INSERT INTO notification (org_id, user_id, kind, entity_type, entity_id, actor_id, created_at, batch_id)
		SELECT $1, $2, $3, $4, $5, $6, $7, $9
		WHERE $8 = 0 OR EXISTS (
		    SELECT 1 FROM channel_member
		    WHERE channel_id = $8 AND user_id = $2 AND unsubscribed_at IS NULL)
		`+conflict,
		ev.OrgID, userID, kind, int16(enum.EntityMessage), p.MessageID,
		actorID, ev.RecordedAt, p.ChannelID, batchID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() > 0 && r.fan != nil {
		suppressed, err := dndSuppressed(ctx, r.pool, userID, author)
		if err != nil {
			return err
		}
		if !suppressed {
			payload, _ := json.Marshal(map[string]any{
				"kind": kind, "entity_type": int16(enum.EntityMessage),
				"entity_id": p.MessageID, "thread_id": p.ThreadID, "actor_id": author,
			})
			r.fan.NotifyUser(ctx, ev.OrgID, userID, payload)
		}
	}
	return nil
}

// dndSuppressed reports whether the live ping to userID should be withheld:
// the recipient is snoozed (dnd_setting.snoozed_until in the future) AND the
// message's actor is not on their priority-contact (VIP) list — the N-2 DND
// gate with the VIP pierce. Only the live DELIVERY is suppressed; the in-app
// row already landed (N-4: the badge accrues even when delivery is
// suppressed). Runs only when a fan would otherwise happen, one indexed
// lookup on the recipient's PK row.
//
// Package-level (not a Runner method) so both the materializer's insert and
// the Service's PingNotification gate on it. An author of 0 (system/automation
// alerts) matches no priority_contact, so a snoozed recipient is always
// suppressed — there is no VIP pierce for a system actor.
func dndSuppressed(ctx context.Context, pool *pgxpool.Pool, userID, author int64) (bool, error) {
	var suppressed bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM dnd_setting d
		    WHERE d.user_id = $1 AND d.snoozed_until > now()
		      AND NOT EXISTS (
		          SELECT 1 FROM priority_contact pc
		          WHERE pc.user_id = $1 AND pc.contact_id = $2))`,
		userID, author).Scan(&suppressed); err != nil {
		return false, err
	}
	return suppressed, nil
}

// keywordMatches finds alert-word pings (kind 4), candidates bounded by
// the F-17 set (reason 3 = alert-word holders in this channel, never a
// channel_member walk): a cheap substring prefilter in SQL over the
// candidates' words, refined to WORD boundaries in Go, with membership
// and mute state re-verified live. Mute-respecting like plain activity
// (a muted thread suppresses; an unmuted thread revives inside a muted
// channel) — keywords do not break through mute the way mentions do.
// Users already notified for this message are excluded (the
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
		FROM channel_deliverability cd
		JOIN alert_word aw ON aw.user_id = cd.user_id
		JOIN channel_member cm ON cm.channel_id = cd.channel_id
		     AND cm.user_id = cd.user_id AND cm.unsubscribed_at IS NULL
		LEFT JOIN thread_subscription ts
		  ON ts.thread_id = $2 AND ts.user_id = cd.user_id
		WHERE cd.channel_id = $1 AND cd.reason = 3 AND cd.medium = 1
		  AND cd.user_id <> $3
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
		r.scanned.Add(1) // once per candidate row scanned
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
