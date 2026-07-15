package rest

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// resetTokenRE extracts the 32-byte (64 hex char) reset token from the mail
// body; it is the only hex run that long in the message.
var resetTokenRE = regexp.MustCompile(`[0-9a-f]{64}`)

// TestPasswordReset: P-35 emailed single-use reset. The full loop (request →
// capture the mail → confirm → old password dead, new works, every prior
// session 401, second confirm of the same token 401) plus the oracle-free
// surfaces (unknown email/org and validation errors reveal nothing about
// account existence), the per-user throttle, token expiry, deactivated and
// credential-less silence, and the password-change void. Each scenario runs on
// a FRESH server so its pre-auth IP-limiter budget (burst 10) is independent;
// they share the reset DB and use distinct org slugs.
func TestPasswordReset(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)

	newServer := func() (*httptest.Server, *captureSender) {
		t.Helper()
		permsSvc := perms.New(pool)
		idSvc := identity.New(pool, permsSvc)
		capture := &captureSender{}
		idSvc.SetMailer(capture)
		ts := httptest.NewServer(Handler(ctx, Deps{
			Pool: pool, Hub: hub, Log: slog.Default(),
			Identity:  idSvc,
			Messaging: messaging.New(pool, permsSvc),
		}))
		t.Cleanup(ts.Close)
		return ts, capture
	}
	bootstrap := func(tsURL, org, email, pw string) string {
		t.Helper()
		var boot struct {
			Token string `json:"token"`
		}
		postJSON(t, tsURL+"/api/v1/orgs/bootstrap", "", map[string]any{
			"org_slug": org, "email": email, "password": pw, "full_name": "Alice Chen",
		}, &boot)
		return boot.Token
	}
	login := func(tsURL, org, email, pw string) (int, string) {
		t.Helper()
		var out struct {
			Token string `json:"token"`
		}
		code := postJSONUA(t, tsURL+"/api/v1/auth/login", "pr-test/1.0",
			map[string]any{"org_slug": org, "email": email, "password": pw}, &out)
		return code, out.Token
	}
	requestReset := func(tsURL, org, email string) int {
		t.Helper()
		return postJSONStatus(t, tsURL+"/api/v1/password-reset/request", "",
			map[string]any{"org_slug": org, "email": email})
	}
	confirmReset := func(tsURL, token, newPW string) int {
		t.Helper()
		return postJSONStatus(t, tsURL+"/api/v1/password-reset/confirm", "",
			map[string]any{"token": token, "new_password": newPW})
	}
	meStatus := func(tsURL, token string) int {
		t.Helper()
		return getJSON(t, tsURL+"/api/v1/me", token, nil)
	}

	// --- Scenario 1: the full loop, with two live sessions to revoke. ---
	ts1, cap1 := newServer()
	tokBoot := bootstrap(ts1.URL, "prfull", "alice@prfull.test", "password123")
	code2, tok2 := login(ts1.URL, "prfull", "alice@prfull.test", "password123")
	if code2 != http.StatusOK || tok2 == "" {
		t.Fatalf("second login = %d", code2)
	}
	if code := requestReset(ts1.URL, "prfull", "alice@prfull.test"); code != http.StatusOK {
		t.Fatalf("request = %d, want 200", code)
	}
	mails := cap1.take()
	if len(mails) != 1 || mails[0].to != "alice@prfull.test" {
		t.Fatalf("reset mail = %+v, want exactly one to alice@prfull.test", mails)
	}
	if !strings.Contains(mails[0].subject, "Password reset") {
		t.Fatalf("subject = %q, want it to name a password reset", mails[0].subject)
	}
	if mails[0].html != "" {
		t.Fatalf("reset mail should be text-only, got html %q", mails[0].html)
	}
	token := resetTokenRE.FindString(mails[0].text)
	if token == "" {
		t.Fatalf("no reset token in mail body:\n%s", mails[0].text)
	}
	if code := confirmReset(ts1.URL, token, "newpassword1"); code != http.StatusOK {
		t.Fatalf("confirm = %d, want 200", code)
	}
	if code, _ := login(ts1.URL, "prfull", "alice@prfull.test", "password123"); code != http.StatusUnauthorized {
		t.Fatalf("old password login after reset = %d, want 401", code)
	}
	if code, tok3 := login(ts1.URL, "prfull", "alice@prfull.test", "newpassword1"); code != http.StatusOK || tok3 == "" {
		t.Fatalf("new password login = %d, want 200", code)
	}
	if code := meStatus(ts1.URL, tokBoot); code != http.StatusUnauthorized {
		t.Fatalf("bootstrap session after reset = %d, want 401 (all sessions revoked)", code)
	}
	if code := meStatus(ts1.URL, tok2); code != http.StatusUnauthorized {
		t.Fatalf("second session after reset = %d, want 401 (all sessions revoked)", code)
	}
	// Single-use: replaying the same token 401s. THE load-bearing assertion —
	// red/green proven by dropping `used_at IS NULL` from the claim UPDATE.
	if code := confirmReset(ts1.URL, token, "newpassword2"); code != http.StatusUnauthorized {
		t.Fatalf("second confirm of the same token = %d, want 401 (already used)", code)
	}

	// --- Scenario 2: enumeration-safe surfaces (unknown email/org, validation,
	// unknown token) reveal nothing and send nothing. ---
	ts2, cap2 := newServer()
	bootstrap(ts2.URL, "proracle", "alice@proracle.test", "password123")
	if code := requestReset(ts2.URL, "proracle", "nobody@proracle.test"); code != http.StatusOK {
		t.Fatalf("unknown-email request = %d, want 200", code)
	}
	if code := requestReset(ts2.URL, "no-such-org", "alice@proracle.test"); code != http.StatusOK {
		t.Fatalf("unknown-org request = %d, want 200", code)
	}
	if got := cap2.take(); len(got) != 0 {
		t.Fatalf("unknown requests sent %d mails, want 0 (no oracle)", len(got))
	}
	// New-password rules are checked before the token is looked up, so a bogus
	// token still 400s on a bad password (not 401) — order proven.
	if code := confirmReset(ts2.URL, "deadbeef", "short"); code != http.StatusBadRequest {
		t.Fatalf("short new password = %d, want 400", code)
	}
	if code := confirmReset(ts2.URL, "deadbeef", strings.Repeat("a", 73)); code != http.StatusBadRequest {
		t.Fatalf("73-byte new password = %d, want 400", code)
	}
	// A well-formed password with an unknown token → oracle-free 401.
	if code := confirmReset(ts2.URL, "deadbeefdeadbeef", "validpassword1"); code != http.StatusUnauthorized {
		t.Fatalf("unknown token = %d, want 401", code)
	}

	// --- Scenario 3: per-user throttle — the 4th outstanding request is silent. ---
	ts3, cap3 := newServer()
	bootstrap(ts3.URL, "prthrottle", "alice@prthrottle.test", "password123")
	for i := 0; i < 4; i++ {
		if code := requestReset(ts3.URL, "prthrottle", "alice@prthrottle.test"); code != http.StatusOK {
			t.Fatalf("throttle request %d = %d, want 200", i, code)
		}
	}
	if got := cap3.take(); len(got) != 3 {
		t.Fatalf("4 requests sent %d mails, want 3 (4th throttled, still 200)", len(got))
	}

	// --- Scenario 4: an expired token is an oracle-free 401. ---
	ts4, cap4 := newServer()
	bootstrap(ts4.URL, "prexpire", "alice@prexpire.test", "password123")
	if code := requestReset(ts4.URL, "prexpire", "alice@prexpire.test"); code != http.StatusOK {
		t.Fatalf("request = %d", code)
	}
	m4 := cap4.take()
	if len(m4) != 1 {
		t.Fatalf("want 1 mail, got %d", len(m4))
	}
	expiredToken := resetTokenRE.FindString(m4[0].text)
	if _, err := pool.Exec(ctx, `
		UPDATE password_reset SET expires_at = now() - interval '1 hour'
		WHERE token_hash = encode(sha256($1::bytea), 'hex')`, expiredToken); err != nil {
		t.Fatalf("backdate expiry: %v", err)
	}
	if code := confirmReset(ts4.URL, expiredToken, "newpassword1"); code != http.StatusUnauthorized {
		t.Fatalf("expired token confirm = %d, want 401", code)
	}

	// --- Scenario 5: deactivated and credential-less accounts are silent. ---
	ts5, cap5 := newServer()
	bootstrap(ts5.URL, "prsilent", "alice@prsilent.test", "password123")
	var deactID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role, deactivated_at)
		SELECT id, 1, 'bob@prsilent.test', 'Bob Ray', 40, now() FROM org WHERE slug = 'prsilent'
		RETURNING id`).Scan(&deactID); err != nil {
		t.Fatalf("deactivated user: %v", err)
	}
	hash, err := auth.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`, deactID, hash); err != nil {
		t.Fatalf("credential: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		SELECT id, 1, 'carol@prsilent.test', 'Carol Ng', 40 FROM org WHERE slug = 'prsilent'`); err != nil {
		t.Fatalf("credential-less user: %v", err)
	}
	if code := requestReset(ts5.URL, "prsilent", "bob@prsilent.test"); code != http.StatusOK {
		t.Fatalf("deactivated request = %d, want 200", code)
	}
	if code := requestReset(ts5.URL, "prsilent", "carol@prsilent.test"); code != http.StatusOK {
		t.Fatalf("credential-less request = %d, want 200", code)
	}
	if got := cap5.take(); len(got) != 0 {
		t.Fatalf("silent requests sent %d mails, want 0", len(got))
	}

	// --- Scenario 6: a password change (P-29) voids an outstanding reset. ---
	ts6, cap6 := newServer()
	tokBoot6 := bootstrap(ts6.URL, "prchange", "alice@prchange.test", "password123")
	if code := requestReset(ts6.URL, "prchange", "alice@prchange.test"); code != http.StatusOK {
		t.Fatalf("request = %d", code)
	}
	m6 := cap6.take()
	if len(m6) != 1 {
		t.Fatalf("want 1 mail, got %d", len(m6))
	}
	voidedToken := resetTokenRE.FindString(m6[0].text)
	if code := postJSONStatus(t, ts6.URL+"/api/v1/me/password", tokBoot6,
		map[string]any{"current_password": "password123", "new_password": "changed12345"}); code != http.StatusOK {
		t.Fatalf("change password = %d, want 200", code)
	}
	if code := confirmReset(ts6.URL, voidedToken, "newpassword1"); code != http.StatusUnauthorized {
		t.Fatalf("confirm after password change = %d, want 401 (token voided)", code)
	}
}
