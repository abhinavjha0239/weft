package messaging

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// The identity-free read surface for web-public channels (P-16). Every method
// pins `visibility = 3 AND archived_at IS NULL` in its SQL — defense in
// depth: even a mis-wired route cannot make a private row flow. Anything not
// live web-public is an oracle-free NotFound: absent, private, public-but-
// not-web, archived, DM, and space threads all read identically to the
// anonymous internet.

type PublicChannel struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	RootThreadID int64  `json:"root_thread_id"`
}

// PublicChannel returns a live web-public channel's metadata.
func (s *Service) PublicChannel(ctx context.Context, channelID int64) (PublicChannel, error) {
	var c PublicChannel
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, description, COALESCE(root_thread_id, 0)
		FROM channel
		WHERE id = $1 AND visibility = 3 AND archived_at IS NULL`,
		channelID).Scan(&c.ID, &c.Name, &c.Description, &c.RootThreadID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PublicChannel{}, apperr.NotFound("channel not found")
	}
	if err != nil {
		return PublicChannel{}, apperr.Internal("public channel", err)
	}
	return c, nil
}

// PublicChannelThreads pages a web-public channel's threads — the ListThreads
// shape without an identity. The metadata gate supplies the oracle-free 404
// (a JOIN alone cannot tell "not web-public" from "web-public but empty");
// the JOIN on the page query keeps private rows out even if that gate
// regressed.
func (s *Service) PublicChannelThreads(ctx context.Context, channelID int64, cursor string, limit int) (ThreadPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	curTS, curID, err := decodeCursor(cursor)
	if err != nil {
		return ThreadPage{}, err
	}
	if _, err := s.PublicChannel(ctx, channelID); err != nil {
		return ThreadPage{}, err
	}
	var page ThreadPage
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, COALESCE(t.title, ''), COALESCE(t.root_message_id, 0),
		       t.message_count, t.resolved_at IS NOT NULL, t.last_activity_at
		FROM thread t
		JOIN channel c ON c.id = t.channel_id
		  AND c.visibility = 3 AND c.archived_at IS NULL
		WHERE t.channel_id = $1 AND t.kind = 1
		  AND (t.last_activity_at, t.id) < ($2::timestamptz, $3::bigint)
		ORDER BY t.last_activity_at DESC, t.id DESC
		LIMIT $4`,
		channelID, curTS, curID, limit)
	if err != nil {
		return ThreadPage{}, apperr.Internal("public threads", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t ThreadSummary
		if err := rows.Scan(&t.ID, &t.Title, &t.RootMessageID,
			&t.MessageCount, &t.Resolved, &t.LastActivityAt); err != nil {
			return ThreadPage{}, apperr.Internal("scan public thread", err)
		}
		page.Threads = append(page.Threads, t)
	}
	if err := rows.Err(); err != nil {
		return ThreadPage{}, apperr.Internal("public threads", err)
	}
	if len(page.Threads) == limit {
		last := page.Threads[len(page.Threads)-1]
		page.NextCursor = encodeCursor(last.LastActivityAt, last.ID)
	}
	return page, nil
}

// PublicMessage is the anonymous projection: message content plus link
// previews (objective public content), and nothing that names org members
// beyond the message author — reactions are EXCLUDED because ReactionAgg
// carries reactor user ids (public reaction counts are a recorded gap).
type PublicMessage struct {
	ID           int64         `json:"id"`
	ThreadID     int64         `json:"thread_id"`
	AuthorID     int64         `json:"author_id"`
	Source       string        `json:"source"`
	Rendered     string        `json:"rendered"`
	CreatedAt    time.Time     `json:"created_at"`
	LinkPreviews []LinkPreview `json:"link_previews,omitempty"`
}

type PublicAuthor struct {
	FullName string `json:"full_name"`
}

type PublicMessagePage struct {
	Messages []PublicMessage `json:"messages"`
	// Authors names ONLY the authors of the returned messages (the Zulip
	// web-public model — bounded, never the org directory).
	Authors    map[int64]PublicAuthor `json:"authors"`
	NextCursor string                 `json:"next_cursor,omitempty"`
}

// PublicThreadMessages pages a web-public thread newest-first (the
// ListMessages shape without an identity).
func (s *Service) PublicThreadMessages(ctx context.Context, threadID, beforeID int64, limit int) (PublicMessagePage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if beforeID <= 0 {
		beforeID = int64(1) << 62
	}
	var ok bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM thread t
			JOIN channel c ON c.id = t.channel_id
			WHERE t.id = $1 AND c.visibility = 3 AND c.archived_at IS NULL)`,
		threadID).Scan(&ok); err != nil {
		return PublicMessagePage{}, apperr.Internal("public thread gate", err)
	}
	if !ok {
		return PublicMessagePage{}, apperr.NotFound("thread not found")
	}
	page := PublicMessagePage{Authors: map[int64]PublicAuthor{}}
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.thread_id, m.author_id, m.source, m.rendered, m.created_at
		FROM message m
		JOIN channel c ON c.id = m.channel_id
		  AND c.visibility = 3 AND c.archived_at IS NULL
		WHERE m.thread_id = $1 AND m.id < $2 AND m.deleted_at IS NULL
		ORDER BY m.id DESC
		LIMIT $3`, threadID, beforeID, limit)
	if err != nil {
		return PublicMessagePage{}, apperr.Internal("public messages", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m PublicMessage
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.AuthorID,
			&m.Source, &m.Rendered, &m.CreatedAt); err != nil {
			return PublicMessagePage{}, apperr.Internal("scan public message", err)
		}
		page.Messages = append(page.Messages, m)
	}
	if err := rows.Err(); err != nil {
		return PublicMessagePage{}, apperr.Internal("public messages", err)
	}
	if len(page.Messages) == 0 {
		return page, nil
	}
	msgIDs := make([]int64, len(page.Messages))
	authorSet := make(map[int64]struct{})
	for i, m := range page.Messages {
		msgIDs[i] = m.ID
		authorSet[m.AuthorID] = struct{}{}
	}
	previews, err := loadLinkPreviews(ctx, s.pool, msgIDs)
	if err != nil {
		return PublicMessagePage{}, err
	}
	for i := range page.Messages {
		page.Messages[i].LinkPreviews = previews[page.Messages[i].ID]
	}
	authorIDs := make([]int64, 0, len(authorSet))
	for id := range authorSet {
		authorIDs = append(authorIDs, id)
	}
	arows, err := s.pool.Query(ctx,
		`SELECT id, full_name FROM user_account WHERE id = ANY($1)`, authorIDs)
	if err != nil {
		return PublicMessagePage{}, apperr.Internal("public authors", err)
	}
	defer arows.Close()
	for arows.Next() {
		var id int64
		var name string
		if err := arows.Scan(&id, &name); err != nil {
			return PublicMessagePage{}, apperr.Internal("scan public author", err)
		}
		page.Authors[id] = PublicAuthor{FullName: name}
	}
	if err := arows.Err(); err != nil {
		return PublicMessagePage{}, apperr.Internal("public authors", err)
	}
	if len(page.Messages) == limit {
		page.NextCursor = strconv.FormatInt(page.Messages[len(page.Messages)-1].ID, 10)
	}
	return page, nil
}
