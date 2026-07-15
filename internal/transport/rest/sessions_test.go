package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestSessions: P-29 session management. A user lists their own live sessions
// (ip/ua metadata, newest first, `current` flagged), revokes one (the victim
// token 401s immediately), revokes all others, and logs out by revoking the
// current one. Revocation is strictly self-scoped and oracle-free: a foreign,
// absent, already-revoked, or expired session id is the same 404, and the
// foreign owner's token stays valid.
func TestSessions(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID  int64  `json:"org_id"`
		UserID int64  `json:"user_id"`
		Token  string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "ses", "email": "alice@ses.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// bob: credentialed but with NO session yet, so his first two sessions are
	// exactly the two logins below (the spec's "list=2" case).
	var bobID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, 'bob@ses.test', 'Bob Ray', 40) RETURNING id`, boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob: %v", err)
	}
	hash, err := auth.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`, bobID, hash); err != nil {
		t.Fatalf("credential: %v", err)
	}

	login := func(ua string) string {
		t.Helper()
		var out struct {
			Token string `json:"token"`
		}
		if code := postJSONUA(t, ts.URL+"/api/v1/auth/login", ua, map[string]any{
			"org_slug": "ses", "email": "bob@ses.test", "password": "password123",
		}, &out); code != http.StatusOK || out.Token == "" {
			t.Fatalf("login (%s) = %d", ua, code)
		}
		return out.Token
	}
	listSessions := func(token string) []identity.Session {
		t.Helper()
		var out struct {
			Sessions []identity.Session `json:"sessions"`
		}
		if code := getJSON(t, ts.URL+"/api/v1/me/sessions", token, &out); code != http.StatusOK {
			t.Fatalf("list sessions = %d", code)
		}
		return out.Sessions
	}
	revoke := func(token string, sessionID int64) int {
		t.Helper()
		return deleteReq(t, fmt.Sprintf("%s/api/v1/me/sessions/%d", ts.URL, sessionID), token)
	}
	meStatus := func(token string) int {
		t.Helper()
		return getJSON(t, ts.URL+"/api/v1/me", token, nil)
	}

	// Two logins with distinct UAs → list=2, newest first, correct `current`,
	// ip/ua recorded.
	tok1 := login("device-one/1.0")
	tok2 := login("device-two/2.0")
	got := listSessions(tok2)
	if len(got) != 2 {
		t.Fatalf("sessions = %d, want 2", len(got))
	}
	if !got[0].Current || got[0].UserAgent != "device-two/2.0" || got[0].IP != "127.0.0.1" {
		t.Fatalf("newest session = %+v, want current device-two from 127.0.0.1", got[0])
	}
	if got[1].Current || got[1].UserAgent != "device-one/1.0" {
		t.Fatalf("older session = %+v, want non-current device-one", got[1])
	}
	if got[0].ID <= got[1].ID {
		t.Fatalf("sessions not newest-first: %d <= %d", got[0].ID, got[1].ID)
	}
	session1ID := got[1].ID

	// A pre-metadata row (NULL ip/ua, the pre-slice shape) reads back "".
	var sqlSessID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256('bobsqltok'::bytea), 'hex'), now() + interval '1 day')
		RETURNING id`, bobID).Scan(&sqlSessID); err != nil {
		t.Fatalf("sql session: %v", err)
	}
	got = listSessions(tok2)
	if len(got) != 3 || got[0].ID != sqlSessID || got[0].IP != "" || got[0].UserAgent != "" {
		t.Fatalf("pre-slice row = %+v (len %d), want newest with empty ip/ua", got[0], len(got))
	}

	// An EXPIRED session is absent from the list and 404 on revoke.
	var expiredID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256('bobexpired'::bytea), 'hex'), now() - interval '1 hour')
		RETURNING id`, bobID).Scan(&expiredID); err != nil {
		t.Fatalf("expired session: %v", err)
	}
	if got := listSessions(tok2); len(got) != 3 {
		t.Fatalf("expired session leaked into the list: %d rows", len(got))
	}
	if code := revoke(tok2, expiredID); code != http.StatusNotFound {
		t.Fatalf("revoke expired = %d, want 404", code)
	}

	// Revoke session 1 → 204; its token 401s IMMEDIATELY; the current is fine.
	if code := revoke(tok2, session1ID); code != http.StatusNoContent {
		t.Fatalf("revoke = %d, want 204", code)
	}
	if code := meStatus(tok1); code != http.StatusUnauthorized {
		t.Fatalf("revoked token = %d, want 401", code)
	}
	if code := meStatus(tok2); code != http.StatusOK {
		t.Fatalf("current token after revoking other = %d, want 200", code)
	}

	// Oracle-free: nonexistent and already-revoked ids are the same 404.
	if code := revoke(tok2, 99999999); code != http.StatusNotFound {
		t.Fatalf("nonexistent revoke = %d, want 404", code)
	}
	if code := revoke(tok2, session1ID); code != http.StatusNotFound {
		t.Fatalf("double revoke = %d, want 404", code)
	}

	// FOREIGN session: bob revoking ALICE's session id is the same 404 and
	// alice's token stays valid. THE load-bearing assertion — drop the user_id
	// pin from RevokeSession's UPDATE and this catches the cross-user kill.
	var aliceSessID int64
	if err := pool.QueryRow(ctx, `
		SELECT id FROM auth_session WHERE user_id = $1 ORDER BY id DESC LIMIT 1`,
		boot.UserID).Scan(&aliceSessID); err != nil {
		t.Fatalf("alice session id: %v", err)
	}
	if code := revoke(tok2, aliceSessID); code != http.StatusNotFound {
		t.Fatalf("foreign revoke = %d, want 404", code)
	}
	if code := meStatus(boot.Token); code != http.StatusOK {
		t.Fatalf("alice's token after bob's foreign revoke = %d, want 200 (victim must survive)", code)
	}

	// Revoke-all-others with 3 live (tok2 + the SQL session + a fresh login)
	// → {revoked: 2}; the others 401, the current survives.
	tok3 := login("device-three/3.0")
	var revoked struct {
		Revoked int64 `json:"revoked"`
	}
	req, _ := http.NewRequest("DELETE", ts.URL+"/api/v1/me/sessions", nil)
	req.Header.Set("Authorization", "Bearer "+tok2)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("revoke others: %v", err)
	}
	if err := jsonDecode(resp.Body, &revoked); err != nil {
		resp.Body.Close()
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || revoked.Revoked != 2 {
		t.Fatalf("revoke others = %d {revoked:%d}, want 200 {revoked:2}", resp.StatusCode, revoked.Revoked)
	}
	if code := meStatus(tok3); code != http.StatusUnauthorized {
		t.Fatalf("tok3 after revoke-others = %d, want 401", code)
	}
	if code := meStatus(tok2); code != http.StatusOK {
		t.Fatalf("current after revoke-others = %d, want 200", code)
	}
	if got := listSessions(tok2); len(got) != 1 || !got[0].Current {
		t.Fatalf("post-revoke-others list = %+v, want only the current session", got)
	}

	// Logout: revoking the CURRENT session is allowed; the next request 401s.
	cur := listSessions(tok2)[0].ID
	if code := revoke(tok2, cur); code != http.StatusNoContent {
		t.Fatalf("logout revoke = %d, want 204", code)
	}
	if code := meStatus(tok2); code != http.StatusUnauthorized {
		t.Fatalf("token after logout = %d, want 401", code)
	}
}
