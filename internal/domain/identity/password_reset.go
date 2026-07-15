package identity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/brand"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/mail"
)

// Password reset (P-35): an emailed single-use token, rooted in the
// password_reset table because both of its invariants need server state —
// single-use (the confirm CLAIMS the row) and revoke-on-change (a password
// change or a completed reset DELETEs the user's outstanding rows). The flow is
// enumeration-safe end to end: the request surface never varies with whether
// the (org, email) pair names a real account, and every confirm failure mode
// collapses to one indistinguishable 401.

// maxOutstandingResets caps a user's unused, unexpired tokens; past it, a
// request silently sends nothing (anti-abuse, no oracle, no burn).
const maxOutstandingResets = 3

// SetMailer wires the outbound-mail seam at composition (the SetSigningSecret
// pattern). Optional: with no mailer, RequestPasswordReset still succeeds and
// simply sends nothing, warning once.
func (s *Service) SetMailer(sender mail.Sender) { s.mailer = sender }

// RequestPasswordReset mints and emails a reset token for the LIVE, credentialed
// human matching (orgSlug, email). It is enumeration-safe: it returns nil — the
// handler always answers 200 {"ok":true} — whether or not the pair resolves,
// whether the account is a placeholder (kind != 1) or credential-less, and
// whether the per-user throttle is tripped. The ONLY differentiated result is a
// genuine infrastructure failure (apperr.Internal), which the handler logs but
// still answers 200 for: a DB/mail outage is independent of any single email, so
// it leaks no account-existence signal. A returned error never confirms a user.
func (s *Service) RequestPasswordReset(ctx context.Context, orgSlug, email string) error {
	var userID int64
	var toEmail string
	err := s.pool.QueryRow(ctx, `
		SELECT u.id, u.email
		FROM user_account u
		JOIN org o ON o.id = u.org_id
		JOIN user_credential c ON c.user_id = u.id
		WHERE o.slug = $1 AND lower(u.email) = lower($2)
		  AND u.deactivated_at IS NULL AND u.kind = 1`,
		orgSlug, email).Scan(&userID, &toEmail)
	if errors.Is(err, pgx.ErrNoRows) {
		// Unknown pair, placeholder, credential-less, or deactivated: silent.
		return nil
	}
	if err != nil {
		return apperr.Internal("password reset lookup", err)
	}

	var outstanding int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM password_reset
		WHERE user_id = $1 AND used_at IS NULL AND expires_at > now()`,
		userID).Scan(&outstanding); err != nil {
		return apperr.Internal("password reset throttle", err)
	}
	if outstanding >= maxOutstandingResets {
		return nil // silent — no oracle, use not burned
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return apperr.Internal("password reset token", err)
	}
	token := hex.EncodeToString(raw)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO password_reset (user_id, token_hash, expires_at)
		VALUES ($1, $2, now() + interval '1 hour')`,
		userID, auth.TokenHash(token)); err != nil {
		return apperr.Internal("password reset insert", err)
	}

	if s.mailer == nil {
		s.noMailerWarn.Do(func() {
			slog.Warn("password reset requested but no mailer configured; sending nothing")
		})
		return nil
	}
	// API-first: the mail carries the raw TOKEN, not a link — no reset web page
	// exists yet (the client-era link format is a recorded gap). who/where only,
	// never message content (privacy default).
	body := fmt.Sprintf(`A password reset was requested for your %s account in the "%s" workspace.

Your reset token (valid for 1 hour):

%s

Enter it in the password reset form to choose a new password. If you did not request this, you can safely ignore this email — your password will not change.
`, brand.Name, orgSlug, token)
	if err := s.mailer.Send(mail.Message{
		To:      toEmail,
		Subject: "[" + brand.Name + "] Password reset",
		Text:    body,
	}); err != nil {
		return apperr.Internal("password reset send", err)
	}
	return nil
}

// ConfirmPasswordReset claims a reset token and installs the new password. Every
// failure mode — unknown, expired, or already-used token, or a since-deactivated
// user — is one indistinguishable apperr.Unauthorized (oracle-free). New-password
// rules mirror auth.ChangePassword (min 8, max 72 bytes — the bcrypt bound).
//
// The whole effect runs in ONE tx: the claim UPDATE's `used_at IS NULL` clause
// is the race guard — a replayed token or a concurrent second confirm claims 0
// rows and 401s, so exactly one caller wins. On the win it upserts the
// credential, revokes ALL live sessions (mailbox control resets everything — no
// presenting-session exception, unlike ChangePassword), and deletes the user's
// other outstanding reset rows.
func (s *Service) ConfirmPasswordReset(ctx context.Context, token, newPassword string) error {
	if len(newPassword) < 8 {
		return apperr.Invalid("new password must be at least 8 characters")
	}
	if len(newPassword) > 72 {
		return apperr.Invalid("new password must be at most 72 bytes")
	}
	newHash, err := auth.HashPassword(newPassword)
	if err != nil {
		return apperr.Internal("hash password", err)
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var userID int64
		err := tx.QueryRow(ctx, `
			UPDATE password_reset pr
			SET used_at = now()
			FROM user_account u
			WHERE pr.token_hash = $1
			  AND pr.used_at IS NULL
			  AND pr.expires_at > now()
			  AND u.id = pr.user_id
			  AND u.deactivated_at IS NULL
			RETURNING pr.user_id`,
			auth.TokenHash(token)).Scan(&userID)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.Unauthorized("invalid or expired token")
		}
		if err != nil {
			return apperr.Internal("claim reset token", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_credential (user_id, password_hash)
			VALUES ($1, $2)
			ON CONFLICT (user_id) DO UPDATE
			  SET password_hash = EXCLUDED.password_hash, updated_at = now()`,
			userID, newHash); err != nil {
			return apperr.Internal("upsert credential", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE auth_session SET revoked_at = now()
			WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()`,
			userID); err != nil {
			return apperr.Internal("revoke sessions", err)
		}
		// The claimed row keeps used_at as an audit trace; its unused siblings
		// are now moot and are removed.
		if _, err := tx.Exec(ctx, `
			DELETE FROM password_reset WHERE user_id = $1 AND used_at IS NULL`,
			userID); err != nil {
			return apperr.Internal("clear other resets", err)
		}
		return nil
	})
}
