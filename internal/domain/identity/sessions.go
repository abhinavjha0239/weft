package identity

import (
	"context"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Session management (P-29): a user lists and revokes their OWN sessions.
// Rows carry the ip/user-agent metadata recorded at creation; `current` marks
// the session the request itself rode in on (token-hash match — the raw token
// is never stored or echoed). Everything here is strictly self-scoped: the
// only session ids that resolve are the actor's own.
//
// KNOWN GAP (recorded): a websocket authenticated by a now-revoked session
// lives until it reconnects — the gateway authenticates at connect time.
// REST access dies immediately (FromToken checks revoked_at per request).
// A live kick is a queued slice.

type Session struct {
	ID        int64     `json:"session_id"`
	IP        string    `json:"ip"`
	UserAgent string    `json:"user_agent"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Current   bool      `json:"current"`
}

// Sessions lists the actor's LIVE sessions (unrevoked, unexpired), newest
// first. currentHash identifies the presenting session; rows created before
// the metadata slice read back "" ip/ua.
func (s *Service) Sessions(ctx context.Context, actor auth.Identity, currentHash string) ([]Session, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, COALESCE(ip, ''), COALESCE(user_agent, ''),
		       created_at, expires_at, token_hash = $2
		FROM auth_session
		WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
		ORDER BY id DESC`, actor.UserID, currentHash)
	if err != nil {
		return nil, apperr.Internal("list sessions", err)
	}
	defer rows.Close()
	out := []Session{}
	for rows.Next() {
		var se Session
		if err := rows.Scan(&se.ID, &se.IP, &se.UserAgent,
			&se.CreatedAt, &se.ExpiresAt, &se.Current); err != nil {
			return nil, apperr.Internal("scan session", err)
		}
		out = append(out, se)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.Internal("list sessions", err)
	}
	return out, nil
}

// RevokeSession revokes ONE of the actor's live sessions by id. The user_id
// pin inside the UPDATE is the whole ACL: a foreign, absent, already-revoked,
// or expired session all affect zero rows and return the same NotFound —
// oracle-free, a session id never confirms another user's session exists.
// Revoking the CURRENT session is allowed: that is logout.
func (s *Service) RevokeSession(ctx context.Context, actor auth.Identity, sessionID int64) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE auth_session SET revoked_at = now()
		WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL AND expires_at > now()`,
		sessionID, actor.UserID)
	if err != nil {
		return apperr.Internal("revoke session", err)
	}
	if ct.RowsAffected() == 0 {
		return apperr.NotFound("session not found")
	}
	return nil
}

// RevokeOtherSessions revokes every live session of the actor EXCEPT the one
// presenting currentHash ("sign out everywhere else"). Returns how many live
// sessions were revoked.
func (s *Service) RevokeOtherSessions(ctx context.Context, actor auth.Identity, currentHash string) (int64, error) {
	ct, err := s.pool.Exec(ctx, `
		UPDATE auth_session SET revoked_at = now()
		WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
		  AND token_hash <> $2`, actor.UserID, currentHash)
	if err != nil {
		return 0, apperr.Internal("revoke other sessions", err)
	}
	return ct.RowsAffected(), nil
}
