package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

func newIdentityServer(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *httptest.Server {
	t.Helper()
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestProfileEdit: P-29 — PATCH /api/v1/me renames the caller's own account
// under the pinned rules (trimmed, non-empty, ≤100 runes, control-char free)
// and the new name is visible in both Me and the batch profile surface.
func TestProfileEdit(t *testing.T) {
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
	ts := newIdentityServer(t, ctx, pool)

	var boot struct {
		OrgID  int64  `json:"org_id"`
		UserID int64  `json:"user_id"`
		Token  string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "prof", "email": "a@prof.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	patchName := func(name string) int {
		t.Helper()
		return patchJSON(t, ts.URL+"/api/v1/me", boot.Token, map[string]any{"full_name": name})
	}

	// Valid rename, whitespace trimmed, reflected in Me AND Profiles.
	if code := patchName("  Alice Q. Chen  "); code != http.StatusOK {
		t.Fatalf("rename = %d, want 200", code)
	}
	var me identity.MyProfile
	if code := getJSON(t, ts.URL+"/api/v1/me", boot.Token, &me); code != 200 || me.FullName != "Alice Q. Chen" {
		t.Fatalf("me after rename = %d %q, want trimmed 'Alice Q. Chen'", code, me.FullName)
	}
	var users struct {
		Users []identity.Profile `json:"users"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/users?ids=%d", ts.URL, boot.UserID), boot.Token, &users); code != 200 ||
		len(users.Users) != 1 || users.Users[0].FullName != "Alice Q. Chen" {
		t.Fatalf("profile after rename = %d %+v", code, users.Users)
	}

	// The pinned rejection matrix: whitespace-only, >100 runes, control chars.
	if code := patchName("   "); code != http.StatusBadRequest {
		t.Fatalf("whitespace-only name = %d, want 400", code)
	}
	// Rune counting, not bytes: 100 two-byte runes (200 bytes) is exactly the
	// cap → 200; 101 runes → 400.
	if code := patchName(strings.Repeat("é", 100)); code != http.StatusOK {
		t.Fatalf("100-rune name = %d, want 200 (limit is runes, not bytes)", code)
	}
	if code := patchName(strings.Repeat("é", 101)); code != http.StatusBadRequest {
		t.Fatalf("101-rune name = %d, want 400", code)
	}
	if code := patchName("Ann\x07a"); code != http.StatusBadRequest {
		t.Fatalf("control-char name = %d, want 400", code)
	}
	// Interior whitespace is fine — names contain spaces (already proven by
	// the rename above); the failed PATCHes must not have changed the name.
	if code := getJSON(t, ts.URL+"/api/v1/me", boot.Token, &me); code != 200 ||
		me.FullName != strings.Repeat("é", 100) {
		t.Fatalf("name after rejections = %q, want the last accepted value", me.FullName)
	}
}

// TestChangePassword: P-29 — POST /api/v1/me/password verifies the current
// password (wrong → 403, token still valid), enforces the new-password rules
// (min 8, max 72 bytes — the bcrypt bound Bootstrap lacks), and on success
// swaps the credential AND revokes every other live session in the same
// transaction: the old password stops logging in, the other session's token
// 401s, the presenting token survives, the new password logs in.
func TestChangePassword(t *testing.T) {
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
	ts := newIdentityServer(t, ctx, pool)

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "pwd", "email": "a@pwd.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	loginStatus := func(password string) (int, string) {
		t.Helper()
		var out struct {
			Token string `json:"token"`
		}
		req := map[string]any{"org_slug": "pwd", "email": "a@pwd.test", "password": password}
		code := postJSONUA(t, ts.URL+"/api/v1/auth/login", "pwd-test/1.0", req, &out)
		return code, out.Token
	}
	changePW := func(token, current, next string) (int, int64) {
		t.Helper()
		var out struct {
			Revoked int64 `json:"revoked_sessions"`
		}
		code := postJSONStatus2(t, ts.URL+"/api/v1/me/password", token,
			map[string]any{"current_password": current, "new_password": next}, &out)
		return code, out.Revoked
	}
	meStatus := func(token string) int {
		t.Helper()
		return getJSON(t, ts.URL+"/api/v1/me", token, nil)
	}

	// A second live session (token B).
	code, tokB := loginStatus("password123")
	if code != http.StatusOK || tokB == "" {
		t.Fatalf("second login = %d", code)
	}

	// Wrong current → 403; nothing changed: the old password still logs in
	// and both tokens stay valid.
	if code, _ := changePW(boot.Token, "not-the-password", "newpassword1"); code != http.StatusForbidden {
		t.Fatalf("wrong current = %d, want 403", code)
	}
	if code, _ := loginStatus("password123"); code != http.StatusOK {
		t.Fatalf("old password after failed change = %d, want 200", code)
	}

	// New-password rules: Bootstrap's min 8, plus the 72-byte bcrypt bound.
	if code, _ := changePW(boot.Token, "password123", "short"); code != http.StatusBadRequest {
		t.Fatalf("short new password = %d, want 400", code)
	}
	if code, _ := changePW(boot.Token, "password123", strings.Repeat("a", 73)); code != http.StatusBadRequest {
		t.Fatalf("73-byte new password = %d, want 400", code)
	}

	// Correct change via the bootstrap token: the OTHER live sessions (B and
	// the verification login above) die in the same tx.
	code, revoked := changePW(boot.Token, "password123", "newpassword1")
	if code != http.StatusOK || revoked != 2 {
		t.Fatalf("change = %d revoked=%d, want 200 revoked=2", code, revoked)
	}
	if code := meStatus(tokB); code != http.StatusUnauthorized {
		t.Fatalf("other session after change = %d, want 401", code)
	}
	if code := meStatus(boot.Token); code != http.StatusOK {
		t.Fatalf("presenting session after change = %d, want 200 (survives)", code)
	}
	if code, _ := loginStatus("password123"); code != http.StatusUnauthorized {
		t.Fatalf("old password after change = %d, want 401", code)
	}
	if code, tok := loginStatus("newpassword1"); code != http.StatusOK || tok == "" {
		t.Fatalf("new password login = %d, want 200", code)
	}

	// A user with NO credential row (agent-style provisioning) gets the same
	// Forbidden — indistinguishable from a wrong password.
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@pwd.test", "Bob Ray", "bobpwdtok")
	if code, _ := changePW(bobTok, "password123", "newpassword1"); code != http.StatusForbidden {
		t.Fatalf("missing credential = %d, want 403", code)
	}
}

// postJSONStatus2 posts JSON and returns the status without fataling on
// non-2xx (postJSON does), decoding the body into out on success.
func postJSONStatus2(t *testing.T, url, token string, body any, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
	}
	return resp.StatusCode
}
