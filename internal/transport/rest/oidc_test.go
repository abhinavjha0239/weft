package rest

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// fakeIdP is a fully offline OpenID Connect provider: an RS256-signing token
// endpoint, a JWKS exposing the public key, and a discovery document — enough
// for go-oidc to discover, exchange, and verify. The claims the next token
// carries (sub/email/email_verified/nonce) are set per scenario; everything is
// served over an httptest listener reached through the loopback-allowed egress
// client (the fixtures discipline — no real IdP, no network).
type fakeIdP struct {
	key      *rsa.PrivateKey
	clientID string

	mu            sync.Mutex
	issuer        string // the httptest URL, set once the server is up
	sub           string
	email         string
	emailVerified bool
	nonce         string
	tokenHits     int
	sawVerifier   bool // proves the token exchange carried the PKCE verifier
}

func (f *fakeIdP) setIssuer(v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.issuer = v
}

func (f *fakeIdP) setClaims(sub, email string, verified bool, nonce string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sub, f.email, f.emailVerified, f.nonce = sub, email, verified, nonce
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (f *fakeIdP) newServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		iss := f.issuer
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"issuer":                                iss,
			"authorization_endpoint":                iss + "/authorize",
			"token_endpoint":                        iss + "/token",
			"jwks_uri":                              iss + "/jwks",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"scopes_supported":                      []string{"openid", "email"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		pub := &f.key.PublicKey
		writeJSON(w, http.StatusOK, map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "test",
			"n": b64url(pub.N.Bytes()),
			"e": b64url(big.NewInt(int64(pub.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.tokenHits++
		_ = r.ParseForm()
		if r.FormValue("code_verifier") != "" {
			f.sawVerifier = true
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": "fake-access-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
			"id_token":     f.signLocked(),
		})
	})
	return httptest.NewServer(mux)
}

// signLocked mints an RS256 ID token from the current claims; the caller holds
// f.mu. The JWT is hand-built from stdlib crypto so go-jose stays a pure
// transitive dependency of go-oidc (never a direct import of this codebase).
func (f *fakeIdP) signLocked() string {
	now := time.Now()
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": "test"}
	claims := map[string]any{
		"iss":            f.issuer,
		"sub":            f.sub,
		"aud":            f.clientID,
		"exp":            now.Add(time.Hour).Unix(),
		"iat":            now.Unix(),
		"nonce":          f.nonce,
		"email":          f.email,
		"email_verified": f.emailVerified,
	}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	signingInput := b64url(hb) + "." + b64url(cb)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, sum[:])
	if err != nil {
		panic(err)
	}
	return signingInput + "." + b64url(sig)
}

// TestOIDCLogin drives the P-30 flow end to end against a fake IdP: provider
// enable-via-discovery-probe, the decision table (link-by-verified-email,
// ride-the-link, no-link-for-unverified, no-JIT, deactivated-exclusion,
// OIDC-only accounts), state replay, nonce mismatch, disabled providers,
// password coexistence, cross-org isolation (admin reach and the org-pinned
// link resolves), and provider CRUD (write-only secret, 422, non-admin).
func TestOIDCLogin(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	idp := &fakeIdP{key: key, clientID: "test-client-id"}
	idpSrv := idp.newServer()
	defer idpSrv.Close()
	idp.setIssuer(idpSrv.URL)

	permsSvc := perms.New(pool)
	identitySvc := identity.New(pool, permsSvc)
	hub := gateway.NewHub(pool, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity: identitySvc, Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()
	// The egress client allows loopback so discovery/JWKS/token reach the
	// httptest IdP; baseURL is the REST server's own origin so the redirect_uri
	// is byte-identical at start and callback. SetOIDC is a composition setter —
	// safe to call now that ts.URL is known and before any request arrives.
	identitySvc.SetOIDC(egress.New(egress.Options{
		UserAgent: "oidc-test/1.0", AllowLoopbackForTests: true,
	}), ts.URL)

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	if code := postRetry(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "acme", "email": "owner@acme.test",
		"password": "password123", "full_name": "Owner",
	}, &boot); code != http.StatusCreated {
		t.Fatalf("bootstrap = %d", code)
	}

	// Seed the humans the IdP will assert. alice has a password (coexistence);
	// carol has NO credential (OIDC-only); dave is deactivated (excluded).
	aliceID := seedUser(t, ctx, pool, boot.OrgID, "alice@acme.test", "Alice", "alicepassword123", false)
	carolID := seedUser(t, ctx, pool, boot.OrgID, "carol@acme.test", "Carol", "", false)
	seedUser(t, ctx, pool, boot.OrgID, "dave@acme.test", "Dave", "davepassword123", true)

	// Seed the flow provider directly, DISABLED: its http + high-port issuer
	// (the httptest URL) cannot pass the CRUD https/shape checks, so the login
	// flow's provider is inserted here; CRUD validation is exercised separately.
	var googleID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO auth_provider (org_id, name, issuer, client_id, client_secret, enabled)
		VALUES ($1, 'google', $2, 'test-client-id', 'test-secret', false) RETURNING id`,
		boot.OrgID, idpSrv.URL).Scan(&googleID); err != nil {
		t.Fatalf("seed provider: %v", err)
	}

	// Enable it via the admin PATCH — this runs the REAL discovery probe
	// against the fake IdP (the enable-success path) and flips enabled=true.
	var enabled identity.AuthProvider
	if code := patchJSONInto(t, fmt.Sprintf("%s/api/v1/admin/auth-providers/%d", ts.URL, googleID),
		boot.Token, map[string]any{"enabled": true}, &enabled); code != http.StatusOK {
		t.Fatalf("enable provider = %d", code)
	}
	if !enabled.Enabled {
		t.Fatal("provider not enabled after discovery probe")
	}

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	// start hits /start, asserts the authorize redirect shape, and returns the
	// raw state + nonce the IdP would echo.
	start := func(providerName string) (state, nonce string) {
		t.Helper()
		resp := getRetry(t, noRedirect, ts.URL+"/api/v1/auth/oidc/acme/"+providerName+"/start")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("start %q = %d, want 302", providerName, resp.StatusCode)
		}
		u, err := url.Parse(resp.Header.Get("Location"))
		if err != nil {
			t.Fatalf("bad Location: %v", err)
		}
		q := u.Query()
		if q.Get("client_id") != "test-client-id" || q.Get("response_type") != "code" ||
			q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" ||
			!strings.Contains(q.Get("scope"), "openid") {
			t.Fatalf("authorize URL missing OIDC/PKCE params: %s", u.RawQuery)
		}
		want := ts.URL + "/api/v1/auth/oidc/acme/" + providerName + "/callback"
		if q.Get("redirect_uri") != want {
			t.Fatalf("redirect_uri = %q, want %q", q.Get("redirect_uri"), want)
		}
		return q.Get("state"), q.Get("nonce")
	}
	callback := func(providerName, state string) (int, identity.OIDCResult) {
		t.Helper()
		resp := getRetry(t, http.DefaultClient,
			ts.URL+"/api/v1/auth/oidc/acme/"+providerName+"/callback?code=fakecode&state="+url.QueryEscape(state))
		defer resp.Body.Close()
		var out identity.OIDCResult
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	// login runs a fresh start→callback with the given asserted claims.
	login := func(providerName, sub, email string, verified, wrongNonce bool) (int, identity.OIDCResult) {
		t.Helper()
		state, nonce := start(providerName)
		if wrongNonce {
			nonce = "tampered-" + nonce
		}
		idp.setClaims(sub, email, verified, nonce)
		return callback(providerName, state)
	}

	// 1. Happy first login: link by verified email, session works via /me.
	aliceState, aliceNonce := start("google")
	idp.setClaims("alice-sub", "alice@acme.test", true, aliceNonce)
	code, res := callback("google", aliceState)
	if code != http.StatusOK || res.UserID != aliceID || res.OrgID != boot.OrgID || res.Token == "" {
		t.Fatalf("happy login: code=%d res=%+v (want 200, user=%d)", code, res, aliceID)
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "alice-sub"); n != 1 {
		t.Fatalf("link rows for alice-sub = %d, want 1", n)
	}
	var me identity.MyProfile
	if c := getJSON(t, ts.URL+"/api/v1/me", res.Token, &me); c != http.StatusOK || me.UserID != aliceID {
		t.Fatalf("/me via oidc session = %d user=%d, want 200 user=%d", c, me.UserID, aliceID)
	}
	if !idp.sawVerifier {
		t.Fatal("token exchange did not carry the PKCE code_verifier")
	}

	// 2. Second login rides external_identity: same sub, DIFFERENT + unverified
	//    email must still resolve via rule 1 and never add a second link.
	code, res = login("google", "alice-sub", "someoneelse@acme.test", false, false)
	if code != http.StatusOK || res.UserID != aliceID {
		t.Fatalf("second login: code=%d user=%d, want 200 user=%d", code, res.UserID, aliceID)
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "alice-sub"); n != 1 {
		t.Fatalf("re-link created a duplicate: rows=%d, want 1", n)
	}

	// 3. RED/GREEN PIN 1 — the account-takeover shape. A NEW subject presents
	//    alice's email UNVERIFIED: must 403 and must NOT link. Dropping the
	//    email_verified conjunct in resolveOIDCAccount flips this row: the
	//    takeover links attacker-sub to alice and this assert goes red.
	code, _ = login("google", "attacker-sub", "alice@acme.test", false, false)
	if code != http.StatusForbidden {
		t.Fatalf("unverified-email login = %d, want 403", code)
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "attacker-sub"); n != 0 {
		t.Fatalf("ACCOUNT TAKEOVER: unverified email linked attacker-sub (%d rows), want 0", n)
	}

	// 4. Unknown email → 403, no JIT provisioning (no user, no link).
	code, _ = login("google", "ghost-sub", "ghost@acme.test", true, false)
	if code != http.StatusForbidden {
		t.Fatalf("unknown-email login = %d, want 403", code)
	}
	if n := userCount(t, ctx, pool, boot.OrgID, "ghost@acme.test"); n != 0 {
		t.Fatalf("no-JIT violated: %d accounts for ghost@acme.test, want 0", n)
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "ghost-sub"); n != 0 {
		t.Fatalf("unknown email linked, want 0 rows")
	}

	// 5. Deactivated user with a verified matching email → 403, no link.
	code, _ = login("google", "dave-sub", "dave@acme.test", true, false)
	if code != http.StatusForbidden {
		t.Fatalf("deactivated-user login = %d, want 403", code)
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "dave-sub"); n != 0 {
		t.Fatalf("deactivated user linked, want 0 rows")
	}

	// 6. OIDC-only account: carol links + gets a session, but cannot password
	//    login (no user_credential row).
	code, cres := login("google", "carol-sub", "carol@acme.test", true, false)
	if code != http.StatusOK || cres.UserID != carolID {
		t.Fatalf("carol oidc login: code=%d user=%d, want 200 user=%d", code, cres.UserID, carolID)
	}
	var cme identity.MyProfile
	if c := getJSON(t, ts.URL+"/api/v1/me", cres.Token, &cme); c != http.StatusOK || cme.UserID != carolID {
		t.Fatalf("carol /me = %d user=%d", c, cme.UserID)
	}
	if lc := postRetry(t, ts.URL+"/api/v1/auth/login", "", map[string]any{
		"org_slug": "acme", "email": "carol@acme.test", "password": "anypassword12",
	}, nil); lc != http.StatusUnauthorized {
		t.Fatalf("carol password login = %d, want 401 (OIDC-only, no credential)", lc)
	}

	// 7. Password coexistence: alice (now linked) still logs in with a password.
	if lc := postRetry(t, ts.URL+"/api/v1/auth/login", "", map[string]any{
		"org_slug": "acme", "email": "alice@acme.test", "password": "alicepassword123",
	}, nil); lc != http.StatusOK {
		t.Fatalf("linked user password login = %d, want 200", lc)
	}

	// 8. RED/GREEN PIN 2 — state replay. Re-submitting alice's already-used
	//    state must 401 and must NOT mint a second session. Dropping the
	//    `used_at IS NULL` claim clause lets the replay proceed to a second
	//    session and this assert goes red.
	// Present alice's original claims + nonce so that, IF the single-use guard
	// were dropped, the replay would sail through verify/nonce to a second
	// session — isolating the used_at clause as the sole thing that 401s it.
	idp.setClaims("alice-sub", "alice@acme.test", true, aliceNonce)
	before := sessionCount(t, ctx, pool, aliceID)
	code, _ = callback("google", aliceState)
	if code != http.StatusUnauthorized {
		t.Fatalf("state replay = %d, want 401", code)
	}
	if after := sessionCount(t, ctx, pool, aliceID); after != before {
		t.Fatalf("REPLAY MINTED A SESSION: alice sessions %d → %d", before, after)
	}

	// 9. Nonce mismatch → 401 (the token's nonce ≠ the flow's nonce).
	code, _ = login("google", "alice-sub", "alice@acme.test", true, true)
	if code != http.StatusUnauthorized {
		t.Fatalf("nonce-mismatch login = %d, want 401", code)
	}

	// 10. Disabled provider → one oracle-free 404 (seed a disabled sibling),
	//     and an unknown provider name is indistinguishable.
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_provider (org_id, name, issuer, client_id, client_secret, enabled)
		VALUES ($1, 'off', $2, 'test-client-id', 'test-secret', false)`,
		boot.OrgID, idpSrv.URL); err != nil {
		t.Fatalf("seed disabled provider: %v", err)
	}
	for _, name := range []string{"off", "nope"} {
		resp := getRetry(t, noRedirect, ts.URL+"/api/v1/auth/oidc/acme/"+name+"/start")
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("start %q = %d, want 404", name, resp.StatusCode)
		}
	}

	// 11. Cross-org admin reach: mallory owns org "rival", so she HAS
	//     manage_org — in HER org. Acme's provider is a 404 to her PATCH and
	//     DELETE (the org-pinned load), and stays enabled.
	var rival struct {
		OrgID  int64  `json:"org_id"`
		UserID int64  `json:"user_id"`
		Token  string `json:"token"`
	}
	if code := postRetry(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "rival", "email": "mallory@rival.test",
		"password": "password123", "full_name": "Mallory",
	}, &rival); code != http.StatusCreated {
		t.Fatalf("rival bootstrap = %d", code)
	}
	adminURL := fmt.Sprintf("%s/api/v1/admin/auth-providers/%d", ts.URL, googleID)
	if c := patchJSONInto(t, adminURL, rival.Token, map[string]any{"enabled": false}, nil); c != http.StatusNotFound {
		t.Fatalf("cross-org provider patch = %d, want 404", c)
	}
	if c := deleteReq(t, adminURL, rival.Token); c != http.StatusNotFound {
		t.Fatalf("cross-org provider delete = %d, want 404", c)
	}
	var stillEnabled bool
	if err := pool.QueryRow(ctx,
		`SELECT enabled FROM auth_provider WHERE id = $1`, googleID).Scan(&stillEnabled); err != nil || !stillEnabled {
		t.Fatalf("cross-org admin touched the provider: enabled=%v err=%v", stillEnabled, err)
	}

	// 12. RED/GREEN PIN 3 — the cross-org resolve. A link row that (through
	//     some future bug) points at ANOTHER org's user must never resolve:
	//     acme's IdP asserting that subject gets rule 3's 403, not a session
	//     as the rival-org user. Dropping the `u.org_id` pin from the rule-1
	//     resolve in resolveOIDCAccount mints exactly that session and flips
	//     this red.
	if _, err := pool.Exec(ctx, `
		INSERT INTO external_identity (org_id, user_id, provider_id, subject, email_at_link)
		VALUES ($1, $2, $3, 'crossorg-sub', 'mallory@rival.test')`,
		boot.OrgID, rival.UserID, googleID); err != nil {
		t.Fatalf("seed cross-org link: %v", err)
	}
	before = sessionCount(t, ctx, pool, rival.UserID)
	code, _ = login("google", "crossorg-sub", "mallory@rival.test", true, false)
	if code != http.StatusForbidden {
		t.Fatalf("cross-org link login = %d, want 403", code)
	}
	if after := sessionCount(t, ctx, pool, rival.UserID); after != before {
		t.Fatalf("CROSS-ORG SESSION: rival user sessions %d → %d via acme's IdP", before, after)
	}

	// 13. RED/GREEN PIN 4 — the link re-select. Same corrupt row, but the IdP
	//     now asserts an email that DOES match an acme human, so rule 2 tries
	//     to link — and the INSERT's ON CONFLICT collides with the corrupt
	//     row. The org-pinned re-select cannot resolve it: corruption is
	//     loudly unresolvable (500), never a session for EITHER user, and the
	//     tx rollback keeps erin unlinked. Dropping the pin from the re-select
	//     resolves mallory instead and flips this red.
	erinID := seedUser(t, ctx, pool, boot.OrgID, "erin@acme.test", "Erin", "", false)
	rivalBefore := sessionCount(t, ctx, pool, rival.UserID)
	erinBefore := sessionCount(t, ctx, pool, erinID)
	code, _ = login("google", "crossorg-sub", "erin@acme.test", true, false)
	if code != http.StatusInternalServerError {
		t.Fatalf("conflicted cross-org link login = %d, want 500", code)
	}
	if sessionCount(t, ctx, pool, rival.UserID) != rivalBefore ||
		sessionCount(t, ctx, pool, erinID) != erinBefore {
		t.Fatal("conflicted cross-org link minted a session")
	}
	if n := extIdentityCount(t, ctx, pool, googleID, "crossorg-sub"); n != 1 {
		t.Fatalf("conflicted link rows = %d, want 1 (rollback keeps erin unlinked)", n)
	}

	oidcProviderCRUD(t, ctx, pool, ts, boot.OrgID, boot.ChannelID, boot.Token)
}

// oidcProviderCRUD exercises the manage_org admin surface: write-only secret,
// the https/shape validation, the enable discovery-probe 422, and non-admin
// rejection. These ride withAuth (per-user apiLimit), not the pre-auth bucket.
func oidcProviderCRUD(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ts *httptest.Server, orgID, channelID int64, ownerTok string) {
	t.Helper()
	base := ts.URL + "/api/v1/admin/auth-providers"

	// http issuer → 400; userinfo issuer → 400.
	if c := postJSONStatus(t, base, ownerTok, map[string]any{
		"name": "insecure", "issuer": "http://idp.example.com",
		"client_id": "x", "client_secret": "y",
	}); c != http.StatusBadRequest {
		t.Fatalf("http issuer = %d, want 400", c)
	}
	if c := postJSONStatus(t, base, ownerTok, map[string]any{
		"name": "userinfo", "issuer": "https://u:p@idp.example.com",
		"client_id": "x", "client_secret": "y",
	}); c != http.StatusBadRequest {
		t.Fatalf("userinfo issuer = %d, want 400", c)
	}

	// Valid create: https + standard port. The secret must never be echoed.
	var created map[string]any
	if c := postJSONStatus2(t, base, ownerTok, map[string]any{
		"name": "probe", "issuer": "https://127.0.0.1/",
		"client_id": "client-x", "client_secret": "s3cr3t-value",
	}, &created); c != http.StatusCreated {
		t.Fatalf("create provider = %d, want 201", c)
	}
	if _, leaked := created["client_secret"]; leaked {
		t.Fatalf("client_secret echoed in create response: %v", created)
	}
	if created["has_secret"] != true || created["enabled"] != false {
		t.Fatalf("created provider view = %+v, want has_secret=true enabled=false", created)
	}
	probeID := int64(created["id"].(float64))

	// List never leaks the secret either; has_secret is true.
	var list []map[string]any
	if c := getJSON(t, base, ownerTok, &list); c != http.StatusOK {
		t.Fatalf("list providers = %d", c)
	}
	for _, p := range list {
		if _, leaked := p["client_secret"]; leaked {
			t.Fatalf("client_secret echoed in list: %v", p)
		}
	}

	// Enabling probes discovery; https://127.0.0.1/ refuses the connection
	// (offline), so the probe fails → 422, never stranding logins on a typo.
	if c := patchJSONInto(t, fmt.Sprintf("%s/%d", base, probeID), ownerTok,
		map[string]any{"enabled": true}, nil); c != http.StatusUnprocessableEntity {
		t.Fatalf("enable with dead issuer = %d, want 422", c)
	}

	// Non-admin (a plain member) cannot touch the CRUD surface.
	bobTok := addChannelMember(t, ctx, pool, orgID, channelID, "bob@acme.test", "Bob", "bob-oidc-token")
	if c := postJSONStatus(t, base, bobTok, map[string]any{
		"name": "sneaky", "issuer": "https://idp.example.com",
		"client_id": "x", "client_secret": "y",
	}); c != http.StatusForbidden {
		t.Fatalf("non-admin create = %d, want 403", c)
	}
}

// --- helpers ---------------------------------------------------------------

// seedUser inserts a live kind=1 human, optionally with a password credential
// and optionally deactivated.
func seedUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, email, name, password string, deactivated bool) int64 {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role, deactivated_at)
		VALUES ($1, 1, $2, $3, 40, CASE WHEN $4 THEN now() END) RETURNING id`,
		orgID, email, name, deactivated).Scan(&uid); err != nil {
		t.Fatalf("seed user %s: %v", email, err)
	}
	if password != "" {
		hash, err := auth.HashPassword(password)
		if err != nil {
			t.Fatalf("hash password: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`,
			uid, hash); err != nil {
			t.Fatalf("seed credential: %v", err)
		}
	}
	return uid
}

func extIdentityCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, providerID int64, subject string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM external_identity WHERE provider_id = $1 AND subject = $2`,
		providerID, subject).Scan(&n); err != nil {
		t.Fatalf("count external_identity: %v", err)
	}
	return n
}

func userCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, email string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM user_account WHERE org_id = $1 AND lower(email) = lower($2)`,
		orgID, email).Scan(&n); err != nil {
		t.Fatalf("count user_account: %v", err)
	}
	return n
}

func sessionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM auth_session
		WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()`,
		userID).Scan(&n); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	return n
}

// getRetry issues a GET, waiting out the pre-auth per-IP 429 (the token bucket
// refills at 0.5/s); a many-scenario flow test exceeds the burst honestly.
func getRetry(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	for i := 0; i < 120; i++ {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return resp
		}
		resp.Body.Close()
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("GET %s: still rate-limited after retries", url)
	return nil
}

// postRetry POSTs JSON with the same pre-auth 429 patience as getRetry.
func postRetry(t *testing.T, url, token string, body, out any) int {
	t.Helper()
	for i := 0; i < 120; i++ {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", url, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", url, err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if out != nil {
			_ = json.NewDecoder(resp.Body).Decode(out)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	t.Fatalf("POST %s: still rate-limited after retries", url)
	return 0
}

// patchJSON PATCHes JSON (the authed admin surface — no pre-auth limiter), with
// optional response decode.
func patchJSONInto(t *testing.T, url, token string, body, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("PATCH", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}
