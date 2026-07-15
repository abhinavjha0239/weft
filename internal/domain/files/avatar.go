package files

import (
	"context"
	"io"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Avatar read path (P-06). Avatars ride the same content-addressed blob store
// as any upload, but their ACL is NOT the F-12 union rule: an avatar is
// org-public metadata. Any member may fetch any member's avatar; a GUEST may
// fetch only their own or a user sharing a live channel — mirroring identity's
// guestVisibleClause. The write side (user_account.avatar_file_id) lives in
// the identity module, which owns that table; this method only reads + streams.

// OpenAvatar resolves a user's current avatar and opens its bytes. A target
// with no avatar, a foreign-org id, or one a guest may not see is an
// oracle-free 404. The stored mime is authoritative — the bytes were
// magic-validated as an image at upload — so the handler may serve it inline.
func (s *Service) OpenAvatar(ctx context.Context, actor auth.Identity, userID int64) (Meta, io.ReadCloser, error) {
	var m Meta
	var key string
	// $3 = viewer id, $4 = viewer-is-guest: non-guests pass everything; a guest
	// sees only self or a live channel-mate (identity.guestVisibleClause shape).
	err := s.pool.QueryRow(ctx, `
		SELECT f.name, f.mime, f.size_bytes, f.storage_key
		FROM user_account u
		JOIN file f ON f.id = u.avatar_file_id AND f.org_id = $2
		             AND f.kind = 1 AND f.deleted_at IS NULL AND f.scan_status <> 2
		WHERE u.id = $1 AND u.org_id = $2
		  AND (NOT $4 OR u.id = $3 OR EXISTS (
		      SELECT 1 FROM channel_member me
		      JOIN channel_member them ON them.channel_id = me.channel_id
		      WHERE me.user_id = $3 AND me.unsubscribed_at IS NULL
		        AND them.user_id = u.id AND them.unsubscribed_at IS NULL))`,
		userID, actor.OrgID, actor.UserID, actor.IsGuest()).
		Scan(&m.Name, &m.Mime, &m.Size, &key)
	if err != nil {
		return Meta{}, nil, apperr.NotFound("avatar not found")
	}
	rc, err := s.store.Open(ctx, key)
	if err != nil {
		return Meta{}, nil, apperr.Internal("open blob", err)
	}
	return m, rc, nil
}
