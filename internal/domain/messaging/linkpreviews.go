package messaging

import (
	"context"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// LinkPreview is one unfurled link on a message (P-15), attached by the
// unfurl consumer and served under the message's own read ACL — previews add
// no visibility surface. image_url is the page's og:image STRING; the server
// never fetches or proxies it (client-era camo is the recorded gap).
type LinkPreview struct {
	URL         string `json:"url"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	ImageURL    string `json:"image_url,omitempty"`
	SiteName    string `json:"site_name,omitempty"`
}

// attachLinkPreviews fills the LinkPreviews aggregates for a fetched message
// page in one query (the attachReactions pattern), document-ordered. Only
// renderable rows (status 1) attach — failed/disallowed cache rows exist to
// suppress refetching and are never associated.
func attachLinkPreviews(ctx context.Context, q querier, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	ids := make([]int64, len(msgs))
	for i := range msgs {
		ids[i] = msgs[i].ID
	}
	previews, err := loadLinkPreviews(ctx, q, ids)
	if err != nil {
		return err
	}
	for i := range msgs {
		msgs[i].LinkPreviews = previews[msgs[i].ID]
	}
	return nil
}

// loadLinkPreviews returns the renderable (status 1) previews for a message
// id set, document-ordered within each message — shared by the authed
// aggregate and the anonymous web-public projection (P-16).
func loadLinkPreviews(ctx context.Context, q querier, ids []int64) (map[int64][]LinkPreview, error) {
	out := make(map[int64][]LinkPreview)
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT mlp.message_id, lp.url, lp.title, lp.description, lp.image_url, lp.site_name
		FROM message_link_preview mlp
		JOIN link_preview lp ON lp.id = mlp.preview_id AND lp.status = 1
		WHERE mlp.message_id = ANY($1)
		ORDER BY mlp.message_id, mlp.position`, ids)
	if err != nil {
		return nil, apperr.Internal("load link previews", err)
	}
	defer rows.Close()
	for rows.Next() {
		var msgID int64
		var p LinkPreview
		if err := rows.Scan(&msgID, &p.URL, &p.Title, &p.Description, &p.ImageURL, &p.SiteName); err != nil {
			return nil, apperr.Internal("scan link preview", err)
		}
		out[msgID] = append(out[msgID], p)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.Internal("load link previews", err)
	}
	return out, nil
}
