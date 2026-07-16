package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/content"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const maxTitleLen = 200

type CreateThreadParams struct {
	ChannelID int64
	Title     string // optional; "" = untitled thread (Slack style)
	Content   string // the root message
}

type CreateThreadResult struct {
	ThreadID      int64 `json:"thread_id"`
	RootMessageID int64 `json:"root_message_id"`
}

// CreateThread starts a first-class thread (ADR-001 D1: titled = Zulip topic,
// untitled = Slack thread) with its root message, in one transaction.
func (s *Service) CreateThread(ctx context.Context, actor auth.Identity, p CreateThreadParams) (CreateThreadResult, error) {
	if p.Content == "" {
		return CreateThreadResult{}, apperr.Invalid("content required")
	}
	p.Title = strings.TrimSpace(p.Title)
	if len(p.Title) > maxTitleLen {
		return CreateThreadResult{}, apperr.Invalid("title too long (max 200)")
	}
	var out CreateThreadResult
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, p.ChannelID)
		if err != nil {
			return err
		}
		if err := s.perms.Require(ctx, tx, actor, perms.VerbCreateThread, chain); err != nil {
			return err
		}
		if err := s.perms.Require(ctx, tx, actor, perms.VerbSendMessage, chain); err != nil {
			return err
		}
		if err := s.requireMember(ctx, tx, p.ChannelID, actor.UserID); err != nil {
			return err
		}
		if err := s.requireLiveChannel(ctx, tx, actor.OrgID, p.ChannelID); err != nil {
			return err
		}

		var title *string
		if p.Title != "" {
			title = &p.Title
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO thread (org_id, channel_id, kind, title,
				last_activity_at, message_count)
			VALUES ($1, $2, 1, $3, now(), 0) RETURNING id`,
			actor.OrgID, p.ChannelID, title).Scan(&out.ThreadID); err != nil {
			return apperr.Internal("create thread", err)
		}

		msgID, err := s.InsertThreadMessage(ctx, tx, actor, out.ThreadID, &p.ChannelID, nil, p.Content)
		if err != nil {
			return err
		}
		out.RootMessageID = msgID
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET root_message_id = $1,
			       message_count = 1, last_activity_at = now()
			WHERE id = $2`, msgID, out.ThreadID); err != nil {
			return apperr.Internal("bind root message", err)
		}

		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityThread, EntityID: out.ThreadID, Verb: "thread.created",
			Payload: eventlog.MustPayload(map[string]any{
				"thread_id": out.ThreadID, "channel_id": p.ChannelID,
				"title": p.Title, "root_message_id": msgID}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return CreateThreadResult{}, err
	}
	return out, nil
}

type UpdateThreadParams struct {
	Title    *string
	Resolved *bool
}

// UpdateThread retitles and/or resolves/reopens. Channel-root threads reject
// every mutation (F-15). Resolve/reopen is idempotent — repeating the current
// state succeeds without emitting an event.
func (s *Service) UpdateThread(ctx context.Context, actor auth.Identity, threadID int64, p UpdateThreadParams) error {
	if p.Title == nil && p.Resolved == nil {
		return apperr.Invalid("nothing to update")
	}
	if p.Title != nil {
		t := strings.TrimSpace(*p.Title)
		if t == "" {
			return apperr.Invalid("title cannot be empty")
		}
		if len(t) > maxTitleLen {
			return apperr.Invalid("title too long (max 200)")
		}
		p.Title = &t
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var channelID *int64
		var kind int16
		var resolvedAt *time.Time
		err := tx.QueryRow(ctx, `
			SELECT channel_id, kind, resolved_at FROM thread
			WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID, &kind, &resolvedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("thread not found")
		}
		if err != nil {
			return apperr.Internal("load thread", err)
		}
		if kind == 2 {
			return apperr.Invalid("the channel root thread cannot be retitled or resolved")
		}
		if channelID == nil {
			// DM/Space-governed threads get endpoints with their features.
			return apperr.Invalid("only channel threads are supported here")
		}
		chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *channelID)
		if err != nil {
			return err
		}
		if err := s.requireMember(ctx, tx, *channelID, actor.UserID); err != nil {
			return err
		}

		if p.Title != nil {
			if err := s.perms.Require(ctx, tx, actor, perms.VerbEditThreadTitle, chain); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE thread SET title = $1 WHERE id = $2`, *p.Title, threadID); err != nil {
				return apperr.Internal("retitle", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityThread, EntityID: threadID, Verb: "thread.titled",
				Payload: eventlog.MustPayload(map[string]any{
					"thread_id": threadID, "channel_id": *channelID, "title": *p.Title}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}

		if p.Resolved != nil && *p.Resolved != (resolvedAt != nil) {
			if err := s.perms.Require(ctx, tx, actor, perms.VerbResolveThreads, chain); err != nil {
				return err
			}
			verb := "thread.resolved"
			if *p.Resolved {
				_, err = tx.Exec(ctx, `
					UPDATE thread SET resolved_at = now(), resolved_by = $1
					WHERE id = $2`, actor.UserID, threadID)
			} else {
				verb = "thread.reopened"
				_, err = tx.Exec(ctx, `
					UPDATE thread SET resolved_at = NULL, resolved_by = NULL
					WHERE id = $1`, threadID)
			}
			if err != nil {
				return apperr.Internal("resolve", err)
			}
			if _, err := eventlog.Append(ctx, tx, eventlog.Event{
				OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
				EntityType: enum.EntityThread, EntityID: threadID, Verb: verb,
				Payload: eventlog.MustPayload(map[string]any{
					"thread_id": threadID, "channel_id": *channelID}),
			}); err != nil {
				return apperr.Internal("append event", err)
			}
		}
		return nil
	})
}

type ThreadSummary struct {
	ID             int64     `json:"id"`
	Title          string    `json:"title,omitempty"`
	RootMessageID  int64     `json:"root_message_id"`
	MessageCount   int       `json:"message_count"`
	Resolved       bool      `json:"resolved"`
	LastActivityAt time.Time `json:"last_activity_at"`
}

type ThreadPage struct {
	Threads    []ThreadSummary `json:"threads"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// ListThreads pages a channel's threads by recency (keyset on
// (last_activity_at, id); the channel root is excluded per F-15).
func (s *Service) ListThreads(ctx context.Context, actor auth.Identity, channelID int64, cursor string, limit int) (ThreadPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	curTS, curID, err := decodeCursor(cursor)
	if err != nil {
		return ThreadPage{}, err
	}
	var page ThreadPage
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.requireChannelRead(ctx, tx, channelID, actor.UserID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, COALESCE(title, ''), COALESCE(root_message_id, 0),
			       message_count, resolved_at IS NOT NULL, last_activity_at
			FROM thread
			WHERE org_id = $1 AND channel_id = $2 AND kind = 1
			  AND (last_activity_at, id) < ($3::timestamptz, $4::bigint)
			ORDER BY last_activity_at DESC, id DESC
			LIMIT $5`,
			actor.OrgID, channelID, curTS, curID, limit)
		if err != nil {
			return apperr.Internal("list threads", err)
		}
		defer rows.Close()
		for rows.Next() {
			var t ThreadSummary
			if err := rows.Scan(&t.ID, &t.Title, &t.RootMessageID,
				&t.MessageCount, &t.Resolved, &t.LastActivityAt); err != nil {
				return apperr.Internal("scan thread", err)
			}
			page.Threads = append(page.Threads, t)
		}
		return rows.Err()
	})
	if err != nil {
		return ThreadPage{}, err
	}
	if len(page.Threads) == limit {
		last := page.Threads[len(page.Threads)-1]
		page.NextCursor = encodeCursor(last.LastActivityAt, last.ID)
	}
	return page, nil
}

type MessagePage struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// ListMessages pages a thread newest-first with a before-id cursor (the chat
// catch-up shape: fetch latest, page backwards).
func (s *Service) ListMessages(ctx context.Context, actor auth.Identity, threadID int64, beforeID int64, limit int) (MessagePage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if beforeID <= 0 {
		beforeID = int64(1) << 62
	}
	var page MessagePage
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var channelID, dmSpaceID *int64
		err := tx.QueryRow(ctx,
			`SELECT channel_id, dm_space_id FROM thread WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID, &dmSpaceID)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("thread not found")
		}
		if err != nil {
			return apperr.Internal("load thread", err)
		}
		var historyFrom *time.Time
		if channelID != nil {
			if err := s.requireChannelRead(ctx, tx, *channelID, actor.UserID); err != nil {
				return err
			}
			// P-16: a protected channel (history_mode 2) bounds each member's
			// view to their join stamp. NULL — the creator, every member of a
			// shared channel, and web-public readers (never protected) — means
			// full history.
			if err := tx.QueryRow(ctx, `
				SELECT cm.history_from
				FROM channel_member cm
				JOIN channel c ON c.id = cm.channel_id AND c.history_mode = 2
				WHERE cm.channel_id = $1 AND cm.user_id = $2
				  AND cm.unsubscribed_at IS NULL`,
				*channelID, actor.UserID).Scan(&historyFrom); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return apperr.Internal("history boundary", err)
			}
		}
		if dmSpaceID != nil {
			if err := s.requireParticipant(ctx, tx, *dmSpaceID, actor.UserID); err != nil {
				return err
			}
		}
		// Space-governed threads (work items) are org-visible in the v1 slice.
		rows, err := tx.Query(ctx, `
			SELECT id, COALESCE(channel_id, 0), thread_id, author_id, source, rendered
			FROM message
			WHERE thread_id = $1 AND id < $2 AND deleted_at IS NULL
			  AND ($4::timestamptz IS NULL OR created_at >= $4)
			ORDER BY id DESC
			LIMIT $3`, threadID, beforeID, limit, historyFrom)
		if err != nil {
			return apperr.Internal("list messages", err)
		}
		defer rows.Close()
		for rows.Next() {
			var m Message
			if err := rows.Scan(&m.ID, &m.ChannelID, &m.ThreadID,
				&m.AuthorID, &m.Source, &m.Rendered); err != nil {
				return apperr.Internal("scan message", err)
			}
			page.Messages = append(page.Messages, m)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := attachReactions(ctx, tx, actor.UserID, page.Messages); err != nil {
			return err
		}
		return attachLinkPreviews(ctx, tx, page.Messages)
	})
	if err != nil {
		return MessagePage{}, err
	}
	if len(page.Messages) == limit {
		page.NextCursor = strconv.FormatInt(page.Messages[len(page.Messages)-1].ID, 10)
	}
	return page, nil
}

// requireMember is the visibility gate (ADR-008 C-2 read-model slice).
func (s *Service) requireMember(ctx context.Context, tx pgx.Tx, channelID, userID int64) error {
	var member bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM channel_member
		 WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL)`,
		channelID, userID).Scan(&member); err != nil {
		return apperr.Internal("membership check", err)
	}
	if !member {
		return apperr.Forbidden("not a channel member")
	}
	return nil
}

// requireChannelRead is the READ gate (P-16): live membership, or the channel
// is web-public AND live. Web-public is world-readable, member-writable — the
// WRITE paths (requireThreadSend, CreateThread, pins) must keep requireMember,
// or non-members could post. Archiving closes the web-public branch while
// members keep their history (the lifecycle contract).
func (s *Service) requireChannelRead(ctx context.Context, tx pgx.Tx, channelID, userID int64) error {
	var ok bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM channel_member
		 WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL)
		OR EXISTS (SELECT 1 FROM channel
		 WHERE id = $1 AND visibility = 3 AND archived_at IS NULL)`,
		channelID, userID).Scan(&ok); err != nil {
		return apperr.Internal("read-access check", err)
	}
	if !ok {
		return apperr.Forbidden("not a channel member")
	}
	return nil
}

// requireParticipant is the DM visibility gate: participation IS the
// permission (no verb chain — a DM has no admin surface in v1). A miss is an
// oracle-free NotFound, never a Forbidden: for a DM, participation IS
// visibility, so a non-participant must not be able to distinguish a
// conversation that is absent from one they are merely denied — the same
// contract single-message Get already honors (P-33). This one return covers
// every caller (ListMessages, the InsertThreadMessage send path, and the
// read-state mark-read), which all just propagate the error.
func (s *Service) requireParticipant(ctx context.Context, tx pgx.Tx, dmSpaceID, userID int64) error {
	var in bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM dm_participant
		 WHERE dm_space_id = $1 AND user_id = $2)`,
		dmSpaceID, userID).Scan(&in); err != nil {
		return apperr.Internal("participant check", err)
	}
	if !in {
		return apperr.NotFound("conversation not found")
	}
	return nil
}

// InsertThreadMessage is the shared in-transaction message write (Send,
// CreateThread, DM sends, and the worktrack module's item descriptions and
// comments): AST parse with in-tx mention resolution, insert,
// message.created event. At most one of channelID/dmSpaceID is set; both
// nil = a space-governed thread, whose event the gateway delivers org-wide
// (the v1 space-visibility slice). DM events carry dm_space_id so the
// gateway fans out to participants only.
func (s *Service) InsertThreadMessage(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64, channelID, dmSpaceID *int64, source string) (int64, error) {
	return s.insertThreadMessageAs(ctx, tx, actor, enum.ActorHuman, &actor.UserID, nil,
		threadID, channelID, dmSpaceID, source, true)
}

// insertThreadMessageAs is the one message-insert path with the event actor
// made explicit: automations post with ActorAutomation + the automation's id
// as the event actor (the loop guard reads it) and consumer metadata in the
// event hint (chain depth), while the message row's author stays a real
// user_account (the acting principal).
func (s *Service) insertThreadMessageAs(ctx context.Context, tx pgx.Tx, actor auth.Identity, actorKind enum.ActorKind, eventActorID *int64, hint json.RawMessage, threadID int64, channelID, dmSpaceID *int64, source string, attachFiles bool) (int64, error) {
	doc := content.Parse(source, func(label string) (int64, bool) {
		var uid int64
		err := tx.QueryRow(ctx, `
			SELECT id FROM user_account
			WHERE org_id = $1 AND full_name = $2 AND deactivated_at IS NULL
			ORDER BY id LIMIT 1`, actor.OrgID, label).Scan(&uid)
		return uid, err == nil
	})
	var msgID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO message (org_id, thread_id, channel_id, dm_space_id,
			author_id, source, ast, rendered, render_version, has_link)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
		actor.OrgID, threadID, channelID, dmSpaceID, actor.UserID,
		source, doc.JSON(), content.RenderHTML(doc), content.RenderVersion,
		doc.HasLink()).Scan(&msgID); err != nil {
		return 0, apperr.Internal("insert message", err)
	}
	// Attachment references: any /api/v1/files/{id} link in the content
	// becomes a file_reference (union-of-referencing-ACLs, ADR-012) —
	// unattachable ids are skipped by the files service, never an error.
	// Forwards pass attachFiles=false: the links quoted from the source stay
	// inert text, so a forward creates no new file_reference rows (P-03).
	if attachFiles && s.files != nil {
		if ids := fileIDsFromLinks(doc.Links()); len(ids) > 0 {
			attached, err := s.files.AttachMessageReferences(ctx, tx, actor, msgID, ids)
			if err != nil {
				return 0, err
			}
			if attached > 0 {
				if _, err := tx.Exec(ctx,
					`UPDATE message SET has_attachment = true WHERE id = $1`, msgID); err != nil {
					return 0, apperr.Internal("flag attachment", err)
				}
			}
		}
	}
	payload := map[string]any{
		"message_id": msgID, "thread_id": threadID, "mentions": doc.Mentions()}
	if channelID != nil {
		payload["channel_id"] = *channelID
	}
	if dmSpaceID != nil {
		payload["dm_space_id"] = *dmSpaceID
	}
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: actor.OrgID, ActorKind: actorKind, ActorID: eventActorID,
		EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
		Payload: eventlog.MustPayload(payload), Hint: hint,
	}); err != nil {
		return 0, apperr.Internal("append event", err)
	}
	return msgID, nil
}

// PostToChannelAsAutomation posts into a channel's root thread on behalf of
// an automation, inside the CALLER'S transaction — the runner commits the
// run row, the message, and its event atomically. No permission gate: the
// scope's admin authorized the rule at creation (AU-2); the org pin and the
// live-channel check still hold. The event carries ActorAutomation + the
// automation's id (the loop guard's signal) and the chain depth as a hint.
func (s *Service) PostToChannelAsAutomation(ctx context.Context, tx pgx.Tx, orgID, authorID, automationID, channelID int64, depth int, source string) (int64, error) {
	var rootThreadID int64
	err := tx.QueryRow(ctx, `
		SELECT root_thread_id FROM channel
		WHERE id = $1 AND org_id = $2 AND archived_at IS NULL`,
		channelID, orgID).Scan(&rootThreadID)
	if err != nil {
		return 0, apperr.NotFound("channel not found or archived")
	}
	hint := eventlog.MustPayload(map[string]any{"automation_depth": depth})
	return s.insertThreadMessageAs(ctx, tx,
		auth.Identity{UserID: authorID, OrgID: orgID},
		enum.ActorAutomation, &automationID, hint,
		rootThreadID, &channelID, nil, source, true)
}

// SendToThread posts into any thread by its governing container (F-5):
// channel threads require send_message + membership; DM threads require
// participation (participation IS the permission); space threads (work
// items) require edit_items (v1 slice — VisibilityScope refines later).
// Bumps the thread's activity counters (never on channel roots, F-15).
func (s *Service) SendToThread(ctx context.Context, actor auth.Identity, threadID int64, contentSrc string) (int64, error) {
	if contentSrc == "" {
		return 0, apperr.Invalid("content required")
	}
	var msgID int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		msgID, err = s.deliverToThread(ctx, tx, actor, threadID, contentSrc)
		return err
	})
	if err != nil {
		return 0, err
	}
	return msgID, nil
}

// RequireChannelSend is the channel-send gate: send_message on the channel's
// scope chain, live membership, and a non-archived channel — the exact
// authorization posting into a channel requires. It is the channel branch of
// requireThreadSend, exported so callers outside messaging (automation's
// slash-command invocation) run the SAME access control rather than
// duplicating it. Runs in the caller's transaction.
func (s *Service) RequireChannelSend(ctx context.Context, tx pgx.Tx, actor auth.Identity, channelID int64) error {
	chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, channelID)
	if err != nil {
		return err
	}
	if err := s.perms.Require(ctx, tx, actor, perms.VerbSendMessage, chain); err != nil {
		return err
	}
	if err := s.requireMember(ctx, tx, channelID, actor.UserID); err != nil {
		return err
	}
	return s.requireLiveChannel(ctx, tx, actor.OrgID, channelID)
}

// requireThreadSend runs the full container gate for posting into a thread
// and returns its containers — shared by the live send path and the
// scheduled-delivery runner (which re-checks at fire time: access revoked
// between scheduling and sending must fail the send).
func (s *Service) requireThreadSend(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64) (channelID, dmSpaceID *int64, kind int16, err error) {
	var spaceID *int64
	err = tx.QueryRow(ctx, `
		SELECT channel_id, dm_space_id, space_id, kind FROM thread
		WHERE id = $1 AND org_id = $2`, threadID, actor.OrgID).
		Scan(&channelID, &dmSpaceID, &spaceID, &kind)
	if err != nil {
		return nil, nil, 0, apperr.NotFound("thread not found")
	}
	switch {
	case channelID != nil:
		if err := s.RequireChannelSend(ctx, tx, actor, *channelID); err != nil {
			return nil, nil, 0, err
		}
	case dmSpaceID != nil:
		if err := s.requireParticipant(ctx, tx, *dmSpaceID, actor.UserID); err != nil {
			return nil, nil, 0, err
		}
	default:
		if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
			perms.OrgScope(actor.OrgID)); err != nil {
			return nil, nil, 0, err
		}
	}
	return channelID, dmSpaceID, kind, nil
}

// deliverToThread is the gated in-transaction delivery: container gate,
// insert, activity bump (never on channel roots, F-15).
func (s *Service) deliverToThread(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64, contentSrc string) (int64, error) {
	return s.deliverToThreadOpts(ctx, tx, actor, threadID, contentSrc, true)
}

// deliverToThreadOpts is deliverToThread with explicit control over file
// attachment. A normal send (attachFiles=true) records file_reference rows
// for the /api/v1/files/{id} links it contains; a forward passes false so the
// links quoted from the source remain inert text and no new references are
// created (P-03 / ADR-012). Both run the identical send gate — this is THE
// gated delivery path.
func (s *Service) deliverToThreadOpts(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64, contentSrc string, attachFiles bool) (int64, error) {
	channelID, dmSpaceID, kind, err := s.requireThreadSend(ctx, tx, actor, threadID)
	if err != nil {
		return 0, err
	}
	msgID, err := s.insertThreadMessageAs(ctx, tx, actor, enum.ActorHuman, &actor.UserID, nil,
		threadID, channelID, dmSpaceID, contentSrc, attachFiles)
	if err != nil {
		return 0, err
	}
	if kind == 1 {
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET last_activity_at = now(),
			       message_count = message_count + 1 WHERE id = $1`,
			threadID); err != nil {
			return 0, apperr.Internal("bump thread", err)
		}
	}
	return msgID, nil
}

const cursorSep = "|"

func encodeCursor(ts time.Time, id int64) string {
	return fmt.Sprintf("%d%s%d", ts.UnixNano(), cursorSep, id)
}

func decodeCursor(c string) (time.Time, int64, error) {
	if c == "" {
		return time.Now().Add(24 * time.Hour), int64(1) << 62, nil
	}
	parts := strings.SplitN(c, cursorSep, 2)
	if len(parts) != 2 {
		return time.Time{}, 0, apperr.Invalid("bad cursor")
	}
	ns, err1 := strconv.ParseInt(parts[0], 10, 64)
	id, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return time.Time{}, 0, apperr.Invalid("bad cursor")
	}
	return time.Unix(0, ns), id, nil
}

// fileIDsFromLinks extracts managed-file ids from link destinations —
// relative or absolute forms of /api/v1/files/{id}.
func fileIDsFromLinks(links []string) []int64 {
	var out []int64
	for _, l := range links {
		idx := strings.Index(l, "/api/v1/files/")
		if idx < 0 {
			continue
		}
		rest := l[idx+len("/api/v1/files/"):]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		if id, err := strconv.ParseInt(rest[:end], 10, 64); err == nil {
			out = append(out, id)
		}
	}
	return out
}
