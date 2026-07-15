// Package auth: password credentials and bearer-token sessions (M0-minimal;
// SSO/2FA arrive per MILESTONES.md — the tables stay deliberately small).
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

const sessionTTL = 30 * 24 * time.Hour

var ErrUnauthorized = errors.New("auth: unauthorized")

func HashPassword(pw string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(h), err
}

func verifyPassword(hash, pw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

// TokenHash is the storage form of a bearer token (auth_session.token_hash):
// hex(sha256(token)). Exported so the session-management surface can match
// "the session this request rode in on" without ever storing the raw token.
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// CreateSession mints a bearer token; only its hash is stored. ip and
// userAgent are session METADATA for the owner's own device list (P-29) —
// display only, never consulted for authorization; empty values are allowed
// (stored as NULL, read back as "").
func CreateSession(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID int64, ip, userAgent string) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	var id int64
	err := q.QueryRow(ctx, `
		INSERT INTO auth_session (user_id, token_hash, ip, user_agent, expires_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5) RETURNING id`,
		userID, TokenHash(token), ip, userAgent, time.Now().Add(sessionTTL)).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("auth: create session: %w", err)
	}
	return token, nil
}

// Identity is the authenticated principal attached to a request.
type Identity struct {
	UserID int64
	OrgID  int64
	Kind   int16
	// Role is the preset pointer (10 owner … 50 guest). Synthetic
	// identities (runners) leave it 0 — never a guest.
	Role int16
}

// GuestRole is the P-5 visibility boundary: at or above it, read surfaces
// shrink to the user's own channels.
const GuestRole int16 = 50

// IsGuest reports whether the identity is visibility-restricted (P-5).
func (id Identity) IsGuest() bool { return id.Role >= GuestRole }

// Login verifies email+password and mints a session, recording the client's
// ip and user agent as session metadata.
func Login(ctx context.Context, pool *pgxpool.Pool, orgSlug, email, password, ip, userAgent string) (string, error) {
	var userID int64
	var hash string
	err := pool.QueryRow(ctx, `
		SELECT u.id, c.password_hash
		FROM user_account u
		JOIN org o ON o.id = u.org_id
		JOIN user_credential c ON c.user_id = u.id
		WHERE o.slug = $1 AND lower(u.email) = lower($2)
		  AND u.deactivated_at IS NULL`,
		orgSlug, email).Scan(&userID, &hash)
	if err != nil || !verifyPassword(hash, password) {
		return "", ErrUnauthorized
	}
	return CreateSession(ctx, pool, userID, ip, userAgent)
}

// FromToken resolves a bearer token to an identity.
func FromToken(ctx context.Context, pool *pgxpool.Pool, token string) (Identity, error) {
	var id Identity
	err := pool.QueryRow(ctx, `
		SELECT u.id, u.org_id, u.kind, u.role
		FROM auth_session s
		JOIN user_account u ON u.id = s.user_id
		WHERE s.token_hash = $1 AND s.revoked_at IS NULL
		  AND s.expires_at > now() AND u.deactivated_at IS NULL`,
		TokenHash(token)).Scan(&id.UserID, &id.OrgID, &id.Kind, &id.Role)
	if err != nil {
		return Identity{}, ErrUnauthorized
	}
	return id, nil
}

// BearerToken extracts the token from a request: Authorization header first,
// then the token query parameter (the WebSocket path).
func BearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return r.URL.Query().Get("token")
}

// ChangePassword verifies the caller's current password and replaces it,
// revoking every OTHER live session in the SAME transaction (currentHash —
// the presenting session's token hash — survives; a stolen old session dies
// with the old password). Returns how many sessions were revoked.
//
// A wrong current password — or a missing credential row, indistinguishable —
// is Forbidden, not Unauthorized: the caller's token IS valid, they just
// cannot prove the current password. The credential row is locked FOR UPDATE
// before verification, so concurrent changes serialize and the loser's
// "current" is checked against the winner's new hash and fails.
//
// New-password rules: Bootstrap's minimum (8 bytes) plus a 72-byte maximum —
// bcrypt silently truncates beyond 72 bytes, so accepting more would mint a
// password whose tail is ignored. Bootstrap does not enforce the 72-byte cap;
// it is enforced here only (recorded).
func ChangePassword(ctx context.Context, pool *pgxpool.Pool, userID int64, currentHash, currentPassword, newPassword string) (int64, error) {
	if len(newPassword) < 8 {
		return 0, apperr.Invalid("new password must be at least 8 characters")
	}
	if len(newPassword) > 72 {
		return 0, apperr.Invalid("new password must be at most 72 bytes")
	}
	newHash, err := HashPassword(newPassword)
	if err != nil {
		return 0, apperr.Internal("hash password", err)
	}
	var revoked int64
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var hash string
		err := tx.QueryRow(ctx,
			`SELECT password_hash FROM user_credential WHERE user_id = $1 FOR UPDATE`,
			userID).Scan(&hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.Forbidden("current password is incorrect")
		}
		if err != nil {
			return apperr.Internal("load credential", err)
		}
		if !verifyPassword(hash, currentPassword) {
			return apperr.Forbidden("current password is incorrect")
		}
		if _, err := tx.Exec(ctx, `
			UPDATE user_credential SET password_hash = $2, updated_at = now()
			WHERE user_id = $1`, userID, newHash); err != nil {
			return apperr.Internal("update credential", err)
		}
		ct, err := tx.Exec(ctx, `
			UPDATE auth_session SET revoked_at = now()
			WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
			  AND token_hash <> $2`, userID, currentHash)
		if err != nil {
			return apperr.Internal("revoke other sessions", err)
		}
		revoked = ct.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return revoked, nil
}
