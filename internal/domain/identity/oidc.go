package identity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/jackc/pgx/v5"
	"golang.org/x/oauth2"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// OIDC login (P-30). The library (github.com/coreos/go-oidc/v3 + x/oauth2) is a
// DELIBERATE exception to the codebase's no-dependency bias: ID-token
// verification — discovery, JWKS rotation, alg pinning, iss/aud/exp — is the
// one thing this project must NOT hand-roll, and go-oidc is the de-facto
// standard. Discovery, JWKS, and the token exchange all ride the SSRF-guarded
// egress client (injected via oidc.ClientContext, which sets the
// oauth2.HTTPClient context key), so even a token exchange dials only the
// pinned, address-vetted destinations.
//
// The invite remains the authorization (identity/invites.go): OIDC never
// PROVISIONS an account. It either recognizes an existing link, or links a
// verified email to one live human already invited into the org, or refuses.

// noAccountMsg is the one thing rule 3 ever says: no account exists for this
// identity and provisioning is not automatic — an admin must extend an invite.
const noAccountMsg = "no account for this identity — ask an admin for an invite"

// SetOIDC wires the SSRF-guarded egress client and public base URL the login
// flow needs (the SetMailer composition pattern). The egress client is the
// ONLY path the IdP endpoints are dialed; baseURL builds the absolute
// redirect_uri the IdP returns the browser to.
func (s *Service) SetOIDC(client *egress.Client, baseURL string) {
	s.oidcEgress = client
	s.oidcBaseURL = strings.TrimRight(baseURL, "/")
}

// providerRow is an enabled provider resolved for a login flow.
type providerRow struct {
	id       int64
	orgID    int64
	issuer   string
	clientID string
	secret   string
}

// resolveEnabledProvider looks up an ENABLED provider by (org slug, provider
// name). Every miss — unknown org, unknown provider, disabled provider,
// deactivated org — collapses to one NotFound: the pre-auth login surface must
// never confirm which orgs run which IdPs (the invite/session oracle-free
// discipline).
func (s *Service) resolveEnabledProvider(ctx context.Context, orgSlug, name string) (providerRow, error) {
	var pr providerRow
	err := s.pool.QueryRow(ctx, `
		SELECT p.id, p.org_id, p.issuer, p.client_id, p.client_secret
		FROM auth_provider p
		JOIN org o ON o.id = p.org_id
		WHERE o.slug = $1 AND p.name = $2 AND p.enabled = true
		  AND o.deactivated_at IS NULL`,
		orgSlug, name).Scan(&pr.id, &pr.orgID, &pr.issuer, &pr.clientID, &pr.secret)
	if errors.Is(err, pgx.ErrNoRows) {
		return providerRow{}, apperr.NotFound("provider not found")
	}
	if err != nil {
		return providerRow{}, apperr.Internal("resolve provider", err)
	}
	return pr, nil
}

// oidcRedirectURI is the absolute callback URL the IdP redirects to; it MUST be
// byte-identical at start (authorize request) and callback (token exchange), so
// both derive it from the configured base URL, never the request Host.
func (s *Service) oidcRedirectURI(orgSlug, provider string) string {
	return s.oidcBaseURL + "/api/v1/auth/oidc/" + orgSlug + "/" + provider + "/callback"
}

// discover builds a go-oidc Provider + oauth2 config for pr, fetching the
// discovery document through the egress client. The returned context carries
// that same client, so the caller's Exchange and VerifierContext (JWKS) ride
// the guard too.
func (s *Service) discover(ctx context.Context, pr providerRow, redirectURI string) (*oidc.Provider, oauth2.Config, context.Context, error) {
	if s.oidcEgress == nil {
		return nil, oauth2.Config{}, ctx, errors.New("oidc egress client not configured")
	}
	cctx := oidc.ClientContext(ctx, s.oidcEgress.HTTPClient())
	provider, err := oidc.NewProvider(cctx, pr.issuer)
	if err != nil {
		return nil, oauth2.Config{}, ctx, err
	}
	cfg := oauth2.Config{
		ClientID:     pr.clientID,
		ClientSecret: pr.secret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  redirectURI,
		Scopes:       []string{oidc.ScopeOpenID, "email"},
	}
	return provider, cfg, cctx, nil
}

// StartOIDC resolves the enabled provider, mints a single-use flow (sha256 of
// the state, a PKCE S256 verifier, and a nonce), and returns the IdP authorize
// URL to redirect the browser to. Only the state's hash is stored — the raw
// state travels to the IdP and back, the auth_session/password_reset precedent.
func (s *Service) StartOIDC(ctx context.Context, orgSlug, provider string) (string, error) {
	pr, err := s.resolveEnabledProvider(ctx, orgSlug, provider)
	if err != nil {
		return "", err
	}
	_, cfg, _, err := s.discover(ctx, pr, s.oidcRedirectURI(orgSlug, provider))
	if err != nil {
		return "", apperr.Internal("oidc discovery", err)
	}
	state, err := randToken()
	if err != nil {
		return "", apperr.Internal("oidc state", err)
	}
	nonce, err := randToken()
	if err != nil {
		return "", apperr.Internal("oidc nonce", err)
	}
	verifier := oauth2.GenerateVerifier()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO oidc_flow (state_hash, provider_id, pkce_verifier, nonce)
		VALUES ($1, $2, $3, $4)`,
		auth.TokenHash(state), pr.id, verifier, nonce); err != nil {
		return "", apperr.Internal("oidc flow insert", err)
	}
	return cfg.AuthCodeURL(state, oidc.Nonce(nonce),
		oauth2.S256ChallengeOption(verifier)), nil
}

// OIDCResult is the callback's answer: a native session token plus who it is
// for. The API-era austerity — no web page, no redirect handoff (a recorded
// gap) — mirrors the password-reset "the mail carries the token" precedent.
//
// When the redirect handoff IS built, the callback MUST be bound to the
// browser that initiated /start (a state cookie or double-submit pair set at
// /start and required at the callback). The state proves a flow EXISTS, not
// WHO is completing it: unbound, an attacker can start their own flow and
// drive a victim's browser to the callback URL, silently logging the victim
// into the attacker's account (login CSRF). Today the token is the response
// BODY to whoever presents code+state — the moment it becomes a cookie or
// fragment handoff, browser binding stops being optional.
type OIDCResult struct {
	Token  string `json:"token"`
	UserID int64  `json:"user_id"`
	OrgID  int64  `json:"org_id"`
}

// CallbackOIDC completes a login. It (1) re-resolves the enabled provider (a
// provider disabled mid-flow → 404), (2) CLAIMS the flow row in one tx — the
// password_reset single-use guard verbatim: `used_at IS NULL` + the 10-min TTL,
// bound to this provider, so a replay/expiry/unknown/wrong-provider state all
// claim zero rows and collapse to one 401 BEFORE any token is minted — then (3)
// exchanges the code and verifies the ID token (sig/iss/aud/exp via go-oidc)
// and asserts the nonce matches, and (4) applies the account-resolution
// decision table. The exchange sits between the claim and the decision by
// design: a DB transaction is never held open across the network round-trip.
func (s *Service) CallbackOIDC(ctx context.Context, orgSlug, provider, code, state, ip, userAgent string) (OIDCResult, error) {
	if code == "" || state == "" {
		return OIDCResult{}, apperr.Unauthorized("invalid oidc callback")
	}
	pr, err := s.resolveEnabledProvider(ctx, orgSlug, provider)
	if err != nil {
		return OIDCResult{}, err
	}

	// (2) Claim the single-use flow row.
	var verifier, nonce string
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		e := tx.QueryRow(ctx, `
			UPDATE oidc_flow SET used_at = now()
			WHERE state_hash = $1 AND provider_id = $2 AND used_at IS NULL
			  AND created_at > now() - interval '10 minutes'
			RETURNING pkce_verifier, nonce`,
			auth.TokenHash(state), pr.id).Scan(&verifier, &nonce)
		if errors.Is(e, pgx.ErrNoRows) {
			return apperr.Unauthorized("invalid or expired oidc state")
		}
		if e != nil {
			return apperr.Internal("claim oidc flow", e)
		}
		return nil
	})
	if err != nil {
		return OIDCResult{}, err
	}

	// (3) Exchange + verify, all through the egress-guarded client.
	provider2, cfg, cctx, err := s.discover(ctx, pr, s.oidcRedirectURI(orgSlug, provider))
	if err != nil {
		return OIDCResult{}, apperr.Internal("oidc discovery", err)
	}
	tok, err := cfg.Exchange(cctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return OIDCResult{}, apperr.Unauthorized("oidc code exchange failed")
	}
	rawID, ok := tok.Extra("id_token").(string)
	if !ok || rawID == "" {
		return OIDCResult{}, apperr.Unauthorized("no id_token in token response")
	}
	idTok, err := provider2.VerifierContext(cctx, &oidc.Config{ClientID: pr.clientID}).Verify(cctx, rawID)
	if err != nil {
		return OIDCResult{}, apperr.Unauthorized("id_token verification failed")
	}
	// go-oidc does not check the nonce (its docs are explicit); the flow row's
	// nonce is the replay/mix-up defense and MUST match the token's claim.
	if idTok.Nonce != nonce {
		return OIDCResult{}, apperr.Unauthorized("oidc nonce mismatch")
	}
	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
	}
	if err := idTok.Claims(&claims); err != nil {
		return OIDCResult{}, apperr.Unauthorized("malformed id_token claims")
	}

	// (4) Decision table, in one tx: kind=1 live humans only, deactivated
	// always excluded, NO JIT.
	out := OIDCResult{OrgID: pr.orgID}
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		userID, err := s.resolveOIDCAccount(ctx, tx, pr, idTok.Subject, claims.Email, claims.EmailVerified)
		if err != nil {
			return err
		}
		token, err := auth.CreateSession(ctx, tx, userID, ip, userAgent)
		if err != nil {
			return apperr.Internal("create session", err)
		}
		out.UserID, out.Token = userID, token
		return nil
	})
	if err != nil {
		return OIDCResult{}, err
	}
	return out, nil
}

// resolveOIDCAccount is the decision table (kind=1 live humans only):
//
//  1. An existing external_identity(provider, subject) → that user. DONE.
//  2. Else a VERIFIED email matching exactly one live human in the org → LINK a
//     new external_identity and use them. The user_account_email_key unique
//     index makes "exactly one" structural. An UNVERIFIED email must NEVER
//     link (the account-takeover shape: an IdP handing out someone else's
//     unverified mailbox must not grant their account) — this is the
//     load-bearing guard.
//  3. Else refuse (Forbidden) — the invite is the authorization; no JIT.
//
// Both link resolves pin the USER to the provider's org: providers are
// org-scoped, so a link row whose user_id crossed orgs can only exist through
// a bug elsewhere — and even then it must never resolve, or one org's IdP
// would mint sessions for another org's users (the cross-org red/green).
func (s *Service) resolveOIDCAccount(ctx context.Context, tx pgx.Tx, pr providerRow, subject, email string, emailVerified bool) (int64, error) {
	var userID int64
	err := tx.QueryRow(ctx, `
		SELECT u.id FROM external_identity ei
		JOIN user_account u ON u.id = ei.user_id
		WHERE ei.provider_id = $1 AND ei.subject = $2 AND u.org_id = $3
		  AND u.deactivated_at IS NULL AND u.kind = 1`,
		pr.id, subject, pr.orgID).Scan(&userID)
	if err == nil {
		return userID, nil // rule 1
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, apperr.Internal("oidc identity lookup", err)
	}

	// rule 2 — the verified-email link. Dropping the emailVerified conjunct
	// here is exactly the account-takeover shape the test's red/green pins.
	email = strings.TrimSpace(email)
	if !emailVerified || email == "" {
		return 0, apperr.Forbidden(noAccountMsg)
	}
	var matchID int64
	err = tx.QueryRow(ctx, `
		SELECT id FROM user_account
		WHERE org_id = $1 AND lower(email) = lower($2)
		  AND deactivated_at IS NULL AND kind = 1`,
		pr.orgID, email).Scan(&matchID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, apperr.Forbidden(noAccountMsg)
	}
	if err != nil {
		return 0, apperr.Internal("oidc email match", err)
	}
	// Link. ON CONFLICT keeps a concurrent first login of the SAME identity
	// from erroring; the re-select below then resolves the winning row.
	if _, err := tx.Exec(ctx, `
		INSERT INTO external_identity (org_id, user_id, provider_id, subject, email_at_link)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (provider_id, subject) DO NOTHING`,
		pr.orgID, matchID, pr.id, subject, strings.ToLower(email)); err != nil {
		return 0, apperr.Internal("oidc link identity", err)
	}
	// The org pin makes a conflicting CROSS-ORG row unresolvable by design:
	// Internal (never a session) is the only safe answer to that corruption.
	if err := tx.QueryRow(ctx, `
		SELECT u.id FROM external_identity ei
		JOIN user_account u ON u.id = ei.user_id
		WHERE ei.provider_id = $1 AND ei.subject = $2 AND u.org_id = $3
		  AND u.deactivated_at IS NULL AND u.kind = 1`,
		pr.id, subject, pr.orgID).Scan(&userID); err != nil {
		return 0, apperr.Internal("oidc link resolve", err)
	}
	// Audit the link — an external identity gaining access to an account is a
	// security-relevant event. The subject/secret never appear in the payload.
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: pr.orgID, ActorKind: enum.ActorHuman, ActorID: &userID,
		EntityType: enum.EntityUser, EntityID: userID, Verb: "identity.linked",
		Payload: eventlog.MustPayload(map[string]any{
			"provider_id": pr.id, "user_id": userID}),
	}); err != nil {
		return 0, apperr.Internal("append event", err)
	}
	return userID, nil
}

// randToken returns 32 bytes of entropy as hex — the state and nonce minting
// shared with the invite/session token shape.
func randToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
