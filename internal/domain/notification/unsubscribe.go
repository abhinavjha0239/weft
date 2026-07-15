package notification

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// One-click unsubscribe (P-20). A digest carries a capability link that turns
// OFF every email medium for the recipient without any login — the mail
// client's native "Unsubscribe" affordance (RFC 8058). The link is a MAC over
// (org, user); the endpoints (registered OUTSIDE withAuth) verify it in
// constant time and consult no Authorization header, exactly like the signed
// download links (files/signed.go).
//
// The MAC is HMAC-SHA256 over "unsub|<org_id>|<user_id>". The literal "unsub|"
// prefix domain-separates it from the files signed-link MAC (which begins with
// a digit), so one WEFT_SIGNING_SECRET safely keys both. There is deliberately
// NO expiry baked into the MAC: an unsubscribe link must never rot in an old
// inbox. A secret rotation invalidating outstanding links is documented
// operator behavior.

func unsubMAC(secret string, orgID, userID int64) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "unsub|%d|%d", orgID, userID)
	return mac.Sum(nil)
}

// unsubPath is the relative one-click URL with a server-computed signature.
func unsubPath(secret string, orgID, userID int64) string {
	sig := hex.EncodeToString(unsubMAC(secret, orgID, userID))
	return fmt.Sprintf("/api/v1/unsubscribe?o=%d&u=%d&sig=%s", orgID, userID, sig)
}

// unsubscribeLink is the absolute link embedded in a digest, or "" when no
// secret is configured — the digest then degrades gracefully (no footer link,
// no List-Unsubscribe header), never emitting a broken URL.
func unsubscribeLink(baseURL, secret string, orgID, userID int64) string {
	if secret == "" {
		return ""
	}
	return baseURL + unsubPath(secret, orgID, userID)
}

// verifyUnsub checks a presented hex signature in constant time.
func verifyUnsub(secret string, orgID, userID int64, sigHex string) bool {
	if secret == "" || orgID <= 0 || userID <= 0 {
		return false
	}
	provided, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	return hmac.Equal(unsubMAC(secret, orgID, userID), provided)
}

// SetUnsubscribe wires the unsubscribe MAC secret at composition time (the
// files.SetSigningSecret pattern). An empty secret leaves the feature off.
func (s *Service) SetUnsubscribe(secret string) { s.unsubSecret = secret }

// UnsubscribeConfigured reports whether the secret is set; the endpoints 404
// when it is not (there is nothing to verify against).
func (s *Service) UnsubscribeConfigured() bool { return s.unsubSecret != "" }

// VerifyUnsub verifies a one-click link's signature over (org, user).
func (s *Service) VerifyUnsub(orgID, userID int64, sigHex string) bool {
	return verifyUnsub(s.unsubSecret, orgID, userID, sigHex)
}

// UnsubscribeFormAction is the POST target for the confirmation page — built
// from server-computed values (the parsed int64 ids + a server-recomputed
// signature), so the request's raw sig is never echoed back into HTML.
func (s *Service) UnsubscribeFormAction(orgID, userID int64) string {
	return unsubPath(s.unsubSecret, orgID, userID)
}

// Unsubscribe turns OFF the email medium for EVERY settable kind for the user
// (v1 is all-or-nothing — per-kind granularity is a later page). The user must
// exist in org o; a deactivated user MAY still unsubscribe (no deactivated
// filter). An unknown (org, user) affects zero rows → NotFound. Idempotent: a
// repeat re-sets the same rows to false. One statement via unnest, bounded by
// len(prefKinds).
func (s *Service) Unsubscribe(ctx context.Context, orgID, userID int64) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO notification_medium_pref (user_id, kind, medium, enabled)
		SELECT $1, k, $2, false FROM unnest($3::smallint[]) AS k
		WHERE EXISTS (SELECT 1 FROM user_account WHERE id = $1 AND org_id = $4)
		ON CONFLICT (user_id, kind, medium) DO UPDATE SET enabled = false`,
		userID, MediumEmail, prefKinds, orgID)
	if err != nil {
		return apperr.Internal("unsubscribe", err)
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("user not found")
	}
	return nil
}
