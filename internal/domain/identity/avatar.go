package identity

import (
	"context"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Avatar pointer (P-06). identity owns user_account writes, so pointing an
// account at (or clearing) its avatar file lives here — the files module
// stored the bytes and the avatar_file_id FK guards the file from GC while it
// is pointed to. Clearing the pointer makes the old file GC-eligible again
// after the unclaimed grace (its designed lifecycle). Never event-logged:
// avatar is profile-adjacent per-user state (the status/read-state precedent);
// clients see it change via avatar_file_id on the next profile fetch.

// SetAvatar points the caller's account at fileID. The handler just uploaded
// the file through the org-scoped content-addressed path, so it is trusted
// org-local; the FK enforces its existence.
func (s *Service) SetAvatar(ctx context.Context, actor auth.Identity, fileID int64) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE user_account SET avatar_file_id = $1 WHERE id = $2 AND org_id = $3`,
		fileID, actor.UserID, actor.OrgID)
	if err != nil {
		return apperr.Internal("set avatar", err)
	}
	if ct.RowsAffected() == 0 {
		return apperr.NotFound("user not found")
	}
	return nil
}

// ClearAvatar drops the caller's avatar pointer. Idempotent — a user with no
// avatar clears to the same state. The file row is never deleted here; GC
// reaps it once nothing points to it.
func (s *Service) ClearAvatar(ctx context.Context, actor auth.Identity) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE user_account SET avatar_file_id = NULL WHERE id = $1 AND org_id = $2`,
		actor.UserID, actor.OrgID); err != nil {
		return apperr.Internal("clear avatar", err)
	}
	return nil
}
