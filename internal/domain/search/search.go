package search

import (
	"context"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

type Result struct {
	MessageID int64 `json:"message_id"`
	ChannelID int64 `json:"channel_id"`
	ThreadID  int64 `json:"thread_id"`
	AuthorID  int64 `json:"author_id"`
	// ItemKey/SpaceID are set when the message's thread is a work item's
	// discussion (space-governed, or a promoted channel thread).
	ItemKey   string `json:"item_key,omitempty"`
	SpaceID   int64  `json:"space_id,omitempty"`
	Snippet   string `json:"snippet"`
	CreatedAt string `json:"created_at"`
}

// Search runs a parsed query under the read-model ACL — the same rule as
// message fetch: channel messages require membership of the governing
// channel; space-thread messages (item descriptions and comments) are
// org-visible in the v1 slice. Free text uses Postgres FTS with ts_rank
// ordering + a guillemet-highlighted snippet; operator-only queries order
// by recency.
func (s *Service) Search(ctx context.Context, actor auth.Identity, raw string, limit int) ([]Result, error) {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	q := Parse(raw)
	if q.Empty() {
		return nil, apperr.Invalid("empty search: provide text or a filter (from:, in:, has:link, is:resolved, before:, after:)")
	}

	var args []any
	add := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	orgP := add(actor.OrgID)
	actorP := add(actor.UserID)
	hasText := q.Text != ""

	var sb strings.Builder
	sb.WriteString("SELECT m.id, COALESCE(m.channel_id, 0), m.thread_id, m.author_id, ")
	// Item enrichment: work_item.thread_id is UNIQUE, so this is a per-row
	// index lookup that also tags promoted channel threads with their key.
	sb.WriteString("COALESCE(sp.key || '-' || w.key_no, ''), COALESCE(w.space_id, 0), ")
	if hasText {
		// Empty StartSel/StopSel via guillemets keeps the snippet PLAIN TEXT
		// (no HTML injected into a field the client shows).
		sb.WriteString("ts_headline('english', m.source, qq, " +
			"'MaxFragments=1,MaxWords=22,MinWords=6,StartSel=»,StopSel=«') AS snippet, ")
	} else {
		sb.WriteString("left(m.source, 160) AS snippet, ")
	}
	sb.WriteString("to_char(m.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSZ') AS created_at ")
	sb.WriteString("FROM message m ")
	sb.WriteString("LEFT JOIN work_item w ON w.thread_id = m.thread_id ")
	sb.WriteString("LEFT JOIN space sp ON sp.id = w.space_id ")
	if q.Resolved != nil {
		sb.WriteString("JOIN thread t ON t.id = m.thread_id ")
	}
	if hasText {
		sb.WriteString(", websearch_to_tsquery('english', " + add(q.Text) + ") qq ")
	}
	sb.WriteString("WHERE m.org_id = " + orgP + " AND m.deleted_at IS NULL ")
	// Read ACL (matches messaging.Get/ListMessages): membership for channel
	// messages, participation for DM messages, org-visible only for
	// space-thread messages (neither container).
	sb.WriteString("AND ((m.channel_id IS NOT NULL AND EXISTS (" +
		"SELECT 1 FROM channel_member cm WHERE cm.channel_id = m.channel_id " +
		"AND cm.user_id = " + actorP + " AND cm.unsubscribed_at IS NULL)) " +
		"OR (m.dm_space_id IS NOT NULL AND EXISTS (" +
		"SELECT 1 FROM dm_participant dp WHERE dp.dm_space_id = m.dm_space_id " +
		"AND dp.user_id = " + actorP + ")) " +
		"OR (m.channel_id IS NULL AND m.dm_space_id IS NULL)) ")
	if hasText {
		sb.WriteString("AND m.search_tsv @@ qq ")
	}
	if q.From != "" {
		fromP := add(q.From)
		sb.WriteString("AND m.author_id IN (SELECT id FROM user_account WHERE org_id = " + orgP +
			" AND (lower(full_name) = lower(" + fromP + ") OR lower(email) = lower(" + fromP + "))) ")
	}
	if q.InChannel != "" {
		sb.WriteString("AND m.channel_id = (SELECT id FROM channel WHERE org_id = " + orgP +
			" AND lower(name) = lower(" + add(q.InChannel) + ") AND archived_at IS NULL) ")
	}
	if q.HasLink {
		sb.WriteString("AND m.has_link ")
	}
	if q.Resolved != nil {
		if *q.Resolved {
			sb.WriteString("AND t.resolved_at IS NOT NULL ")
		} else {
			sb.WriteString("AND t.resolved_at IS NULL ")
		}
	}
	if q.After != nil {
		sb.WriteString("AND m.created_at >= " + add(*q.After) + " ")
	}
	if q.Before != nil {
		sb.WriteString("AND m.created_at < " + add(*q.Before) + " ")
	}
	if hasText {
		sb.WriteString("ORDER BY ts_rank(m.search_tsv, qq) DESC, m.id DESC ")
	} else {
		sb.WriteString("ORDER BY m.id DESC ")
	}
	sb.WriteString("LIMIT " + add(limit))

	rows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, apperr.Internal("search", err)
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		var r Result
		if err := rows.Scan(&r.MessageID, &r.ChannelID, &r.ThreadID,
			&r.AuthorID, &r.ItemKey, &r.SpaceID, &r.Snippet, &r.CreatedAt); err != nil {
			return nil, apperr.Internal("scan result", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
