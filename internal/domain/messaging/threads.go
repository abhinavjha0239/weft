package messaging

import (
	"context"
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

		msgID, err := s.InsertThreadMessage(ctx, tx, actor, out.ThreadID, &p.ChannelID, p.Content)
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
		if err := s.requireMember(ctx, tx, channelID, actor.UserID); err != nil {
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
		var channelID *int64
		err := tx.QueryRow(ctx,
			`SELECT channel_id FROM thread WHERE id = $1 AND org_id = $2`,
			threadID, actor.OrgID).Scan(&channelID)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("thread not found")
		}
		if err != nil {
			return apperr.Internal("load thread", err)
		}
		if channelID != nil {
			if err := s.requireMember(ctx, tx, *channelID, actor.UserID); err != nil {
				return err
			}
		}
		// Space-governed threads (work items) are org-visible in the v1 slice.
		rows, err := tx.Query(ctx, `
			SELECT id, COALESCE(channel_id, 0), thread_id, author_id, source, rendered
			FROM message
			WHERE thread_id = $1 AND id < $2 AND deleted_at IS NULL
			ORDER BY id DESC
			LIMIT $3`, threadID, beforeID, limit)
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
		return rows.Err()
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

// InsertThreadMessage is the shared in-transaction message write (Send,
// CreateThread, and the worktrack module's item descriptions/comments):
// AST parse with in-tx mention resolution, insert, message.created event.
// channelID is nil for space-governed threads; the event payload then omits
// a channel and the gateway delivers org-wide (the v1 space-visibility slice).
func (s *Service) InsertThreadMessage(ctx context.Context, tx pgx.Tx, actor auth.Identity, threadID int64, channelID *int64, source string) (int64, error) {
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
		INSERT INTO message (org_id, thread_id, channel_id, author_id,
			source, ast, rendered, render_version, has_link)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
		actor.OrgID, threadID, channelID, actor.UserID,
		source, doc.JSON(), content.RenderHTML(doc), content.RenderVersion,
		doc.HasLink()).Scan(&msgID); err != nil {
		return 0, apperr.Internal("insert message", err)
	}
	payload := map[string]any{
		"message_id": msgID, "thread_id": threadID, "mentions": doc.Mentions()}
	if channelID != nil {
		payload["channel_id"] = *channelID
	}
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
		EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
		Payload: eventlog.MustPayload(payload),
	}); err != nil {
		return 0, apperr.Internal("append event", err)
	}
	return msgID, nil
}

// SendToThread posts into any thread by its governing container (F-5): channel
// threads require send_message + membership; space threads (work items)
// require edit_items (v1 slice — VisibilityScope refines later). Bumps the
// thread's activity counters (never on channel roots, F-15).
func (s *Service) SendToThread(ctx context.Context, actor auth.Identity, threadID int64, contentSrc string) (int64, error) {
	if contentSrc == "" {
		return 0, apperr.Invalid("content required")
	}
	var msgID int64
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var channelID, spaceID *int64
		var kind int16
		err := tx.QueryRow(ctx, `
			SELECT channel_id, space_id, kind FROM thread
			WHERE id = $1 AND org_id = $2`, threadID, actor.OrgID).
			Scan(&channelID, &spaceID, &kind)
		if err != nil {
			return apperr.NotFound("thread not found")
		}
		if channelID != nil {
			chain, err := s.perms.ChannelScope(ctx, tx, actor.OrgID, *channelID)
			if err != nil {
				return err
			}
			if err := s.perms.Require(ctx, tx, actor, perms.VerbSendMessage, chain); err != nil {
				return err
			}
			if err := s.requireMember(ctx, tx, *channelID, actor.UserID); err != nil {
				return err
			}
		} else {
			if err := s.perms.Require(ctx, tx, actor, perms.VerbEditItems,
				perms.OrgScope(actor.OrgID)); err != nil {
				return err
			}
		}
		msgID, err = s.InsertThreadMessage(ctx, tx, actor, threadID, channelID, contentSrc)
		if err != nil {
			return err
		}
		if kind == 1 {
			if _, err := tx.Exec(ctx, `
				UPDATE thread SET last_activity_at = now(),
				       message_count = message_count + 1 WHERE id = $1`,
				threadID); err != nil {
				return apperr.Internal("bump thread", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
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
