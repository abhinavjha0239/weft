package files

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Signed download links (P-07). A signed link is a capability URL that lets a
// client fetch a file WITHOUT a bearer header — the thing that makes
// <img src=…> work. It is NOT an S3 presigned URL (those bypass our ACL):
// minting runs the SAME union ACL as a download (authorizeDownload), then
// hands out a URL carrying an HMAC over the file id, an expiry, and the org.
// The download handler verifies the HMAC in constant time and skips the
// per-viewer ACL — the signature IS the capability — but still 404s a
// file that has been GC'd since the link was minted.
//
// The org is BOTH signed and required to scope the row load: a signature is
// bound to (file, exp, org), so a link cannot be replayed against a file in
// another org (the org-scoped load simply misses → 404). The signing secret
// is a single server-wide key (WEFT_SIGNING_SECRET); a leaked link is scoped
// to one file for ten minutes, not to the whole store.

const (
	signedLinkTTL    = 10 * time.Minute
	signedLinkLeeway = 30 * time.Second // tolerate modest clock skew past exp
)

// ErrNoSigningSecret is returned by SignedLink when WEFT_SIGNING_SECRET is
// unset. It is surfaced to the caller verbatim (a 500 with a clear message):
// it is an operator misconfiguration, not sensitive.
var ErrNoSigningSecret = errors.New("signing secret not configured")

// SetSigningSecret wires the HMAC key at composition time (like SetFiles).
func (s *Service) SetSigningSecret(secret string) { s.signingSecret = secret }

// signMAC is the raw HMAC-SHA256 over "file_id|exp|org_id".
func (s *Service) signMAC(fileID, exp, orgID int64) []byte {
	mac := hmac.New(sha256.New, []byte(s.signingSecret))
	fmt.Fprintf(mac, "%d|%d|%d", fileID, exp, orgID)
	return mac.Sum(nil)
}

// SignedLinkResult is the mint response: a relative URL plus its unix expiry.
type SignedLinkResult struct {
	URL       string `json:"url"`
	ExpiresAt int64  `json:"expires_at"`
}

// SignedLink authorizes the caller for the file (the union ACL, oracle-free
// 404) and mints a 10-minute capability URL. It refuses with ErrNoSigningSecret
// if the server has no signing key. The URL is org-scoped: only someone who
// could already download the file can mint a link, and the link is bound to
// this file in this org.
func (s *Service) SignedLink(ctx context.Context, actor auth.Identity, fileID int64) (SignedLinkResult, error) {
	if _, _, err := s.authorizeDownload(ctx, actor, fileID); err != nil {
		return SignedLinkResult{}, err
	}
	if s.signingSecret == "" {
		return SignedLinkResult{}, ErrNoSigningSecret
	}
	exp := time.Now().Add(signedLinkTTL).Unix()
	sig := hex.EncodeToString(s.signMAC(fileID, exp, actor.OrgID))
	return SignedLinkResult{
		URL:       fmt.Sprintf("/api/v1/files/%d?sig=%s&exp=%d&org=%d", fileID, sig, exp, actor.OrgID),
		ExpiresAt: exp,
	}, nil
}

// OpenSigned verifies a capability URL and opens the blob, bypassing the
// per-viewer ACL (the signature is the capability). It is unauthenticated —
// there is no bearer, so the org comes from the (signed) query. The row is
// loaded scoped to that org: a signature replayed against a file in another
// org simply misses the row → 404, indistinguishable from a GC'd file. A
// tampered or expired signature is a 401. Constant-time comparison throughout.
func (s *Service) OpenSigned(ctx context.Context, fileID, orgID int64, sigHex, expStr string) (Meta, io.ReadCloser, error) {
	if s.signingSecret == "" || orgID <= 0 {
		return Meta{}, nil, apperr.Unauthorized("invalid or expired link")
	}
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return Meta{}, nil, apperr.Unauthorized("invalid or expired link")
	}
	provided, err := hex.DecodeString(sigHex)
	if err != nil {
		return Meta{}, nil, apperr.Unauthorized("invalid or expired link")
	}
	if !hmac.Equal(s.signMAC(fileID, exp, orgID), provided) {
		return Meta{}, nil, apperr.Unauthorized("invalid or expired link")
	}
	if time.Now().After(time.Unix(exp, 0).Add(signedLinkLeeway)) {
		return Meta{}, nil, apperr.Unauthorized("invalid or expired link")
	}
	// The signature is valid; load the row (org-scoped so a cross-org replay
	// misses) — still 404 if the file was deleted/GC'd since minting.
	var m Meta
	var key string
	if err := s.pool.QueryRow(ctx, `
		SELECT name, mime, size_bytes, storage_key
		FROM file
		WHERE id = $1 AND org_id = $2 AND kind = 1 AND deleted_at IS NULL
		  AND scan_status <> 2`,
		fileID, orgID).Scan(&m.Name, &m.Mime, &m.Size, &key); err != nil {
		return Meta{}, nil, apperr.NotFound("file not found")
	}
	rc, err := s.store.Open(ctx, key)
	if err != nil {
		return Meta{}, nil, apperr.Internal("open blob", err)
	}
	return m, rc, nil
}
