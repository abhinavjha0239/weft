package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// authRaw issues an authenticated request and returns the status and the RAW
// body — the P-16 pattern for byte-identical oracle-free comparisons (getJSON
// only surfaces the code). A nil body sends none (GET); token "" omits auth.
func authRaw(t *testing.T, method, url, token string, body any) (int, string) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// addAdmin inserts an org ADMIN — placed in the role:admins group, which holds
// administer_channel org-wide by the bootstrap defaults — who is deliberately
// NOT a member of any channel, with a live session. This is the principal that
// must retain admin surfaces on a private channel it cannot otherwise see:
// P-34 masks the UNPRIVILEGED, never the administrator (the admin verb is
// resolved through the group closure, before any membership gate). The role
// column (20) only keeps IsGuest() false; the grant is the group membership.
func addAdmin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, email, name, token string) string {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 20) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("admin: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT id, $2 FROM user_group WHERE org_id = $1 AND name = 'role:admins'`,
		orgID, uid); err != nil {
		t.Fatalf("grant admin role: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return perms.New(pool).RebuildClosure(ctx, tx, orgID)
	}); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("admin session: %v", err)
	}
	return token
}

// TestPrivateChannelMasking: P-34 — a private channel is INVISIBLE to a
// non-member. Every by-id gate answers a private-non-member exactly as it
// answers an absent or foreign-org id: an oracle-free 404 with a byte-identical
// body, so a stranger can never confirm the channel exists. PUBLIC channels
// stay 403 (directory-knowable); members are unaffected; a non-member admin
// keeps the admin surface; guests ride the same gates.
//
// RED/GREEN (the load-bearing pin): in messaging/threads.go revert the private
// mapping in requireMember AND requireChannelRead — `visibility ==
// visibilityPrivate { return apperr.NotFound(...) }` back to
// `apperr.Forbidden("not a channel member")`. The private-non-member cases
// below then return 403 while the absent/foreign cases stay 404: the three-way
// codes AND bodies diverge, and every probe3 assertion fails — a 403
// distinguishes "denied" from "absent", so the existence oracle reopens.
func TestPrivateChannelMasking(t *testing.T) {
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

	// Org A: alice owns it and creates a PRIVATE #vault (she is its only member)
	// and a PUBLIC #plaza. casey is a plain org member (of #general), a
	// non-member of both. dana is an org admin, member of neither.
	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "mask", "email": "alice@mask.test", "password": "password123",
		"full_name": "Alice Owner",
	}, &boot)
	caseyTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"casey@mask.test", "Casey Member", "caseymasktok")
	danaTok := addAdmin(t, ctx, pool, boot.OrgID, "dana@mask.test", "Dana Admin", "danamasktok")
	addGuest(t, ctx, pool, boot.OrgID, boot.ChannelID, "gwen@mask.test", "Gwen Guest", "gwenmasktok")
	const guestTok = "gwenmasktok"

	var vault, plaza struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "visibility": "private"}, &vault)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "plaza", "visibility": "public"}, &plaza)
	// A thread + message in #vault, so mark-read has a real thread to probe and
	// the channel is genuinely populated (the mask must hold regardless).
	var welcome struct {
		ThreadID int64 `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, vault.ChannelID),
		boot.Token, map[string]any{"title": "secret plan", "content": "top secret"}, &welcome)

	// Org B (foreign): a private channel whose id a stranger in org A must not
	// be able to distinguish from any other unseeable id.
	var bootB struct {
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "otherorg", "email": "zoe@other.test", "password": "password123",
		"full_name": "Zoe Other",
	}, &bootB)
	var foreign struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", bootB.Token,
		map[string]any{"name": "foreign-vault", "visibility": "private"}, &foreign)

	const absentID = int64(999999999)

	// The three subjects a non-member must NOT tell apart, per endpoint: an
	// absent id, a foreign-org id, and #vault (private, casey is not in it).
	probe3 := func(name string, call func(channelID int64) (int, string)) {
		t.Helper()
		cAbsent, bAbsent := call(absentID)
		cForeign, bForeign := call(foreign.ChannelID)
		cMasked, bMasked := call(vault.ChannelID)
		if cAbsent != http.StatusNotFound || cForeign != http.StatusNotFound || cMasked != http.StatusNotFound {
			t.Fatalf("%s three-way codes = %d/%d/%d, want 404/404/404 (absent/foreign/masked)",
				name, cAbsent, cForeign, cMasked)
		}
		if bAbsent != bForeign || bForeign != bMasked {
			t.Fatalf("%s three-way bodies differ (oracle):\n absent =%q\n foreign=%q\n masked =%q",
				name, bAbsent, bForeign, bMasked)
		}
		if !strings.Contains(bMasked, "channel not found") || strings.Contains(bMasked, fmt.Sprint(vault.ChannelID)) {
			t.Fatalf("%s masked body = %q, want generic channel-not-found that never echoes the id", name, bMasked)
		}
	}

	probe3("ListThreads", func(cid int64) (int, string) {
		return authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, cid), caseyTok, nil)
	})
	probe3("Join", func(cid int64) (int, string) {
		return authRaw(t, "POST", fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, cid), caseyTok, map[string]any{})
	})
	probe3("Send", func(cid int64) (int, string) {
		return authRaw(t, "POST", fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, cid),
			caseyTok, map[string]any{"content": "knock knock"})
	})
	probe3("ListChannelPins", func(cid int64) (int, string) {
		return authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/pins", ts.URL, cid), caseyTok, nil)
	})

	// mark-read is thread-keyed, so its own absent case is "thread not found";
	// the P-34 requirement is that a non-member's mark-read on a private
	// channel's thread masks to the SAME body the channel gate returns for an
	// absent channel (never a 403 that would confirm the channel). Capture that
	// canonical body from an absent-channel probe and require a byte match.
	_, canonMasked := authRaw(t, "GET",
		fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, absentID), caseyTok, nil)
	if codeMR, bodyMR := authRaw(t, "POST",
		fmt.Sprintf("%s/api/v1/threads/%d/read", ts.URL, welcome.ThreadID),
		caseyTok, map[string]any{"up_to": 0}); codeMR != http.StatusNotFound || bodyMR != canonMasked {
		t.Fatalf("mark-read on a private thread = %d %q, want 404 with the masked body %q (was 403 pre-P-34)",
			codeMR, bodyMR, canonMasked)
	}

	// PUBLIC channels stay 403 for a non-member — existence is org-knowable, so
	// the send-before-join affordance and the read denial are honest, NOT masked.
	for _, tc := range []struct {
		name, method, path string
		body               any
	}{
		{"public ListThreads", "GET", fmt.Sprintf("/api/v1/channels/%d/threads", plaza.ChannelID), nil},
		{"public Send", "POST", fmt.Sprintf("/api/v1/channels/%d/messages", plaza.ChannelID), map[string]any{"content": "hi"}},
		{"public ListChannelPins", "GET", fmt.Sprintf("/api/v1/channels/%d/pins", plaza.ChannelID), nil},
	} {
		if code, _ := authRaw(t, tc.method, ts.URL+tc.path, caseyTok, tc.body); code != http.StatusForbidden {
			t.Fatalf("%s (non-member) = %d, want 403 (public stays knowable)", tc.name, code)
		}
	}

	// A member is unaffected: alice reads and writes #vault normally.
	if code, _ := authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, vault.ChannelID), boot.Token, nil); code != http.StatusOK {
		t.Fatalf("member ListThreads(vault) = %d, want 200", code)
	}
	if code, _ := authRaw(t, "POST", fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, vault.ChannelID), boot.Token, map[string]any{"content": "mine"}); code != http.StatusCreated {
		t.Fatalf("member Send(vault) = %d, want 201", code)
	}

	// An admin who is NOT a member retains the ADMIN surface (administer_channel
	// is resolved before any membership gate): dana can rename/describe #vault.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, vault.ChannelID),
		danaTok, map[string]any{"description": "admin was here"}); code != http.StatusOK {
		t.Fatalf("admin PATCH(vault) = %d, want 200 (masking must not lock admins out)", code)
	}
	var gotDesc string
	if err := pool.QueryRow(ctx, `SELECT description FROM channel WHERE id = $1`, vault.ChannelID).Scan(&gotDesc); err != nil || gotDesc != "admin was here" {
		t.Fatalf("admin edit did not persist: desc=%q err=%v", gotDesc, err)
	}
	// The admin still is NOT a member, so the unprivileged READ gate masks
	// #vault from dana exactly as from casey — administer is not membership.
	if code, _ := authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, vault.ChannelID), danaTok, nil); code != http.StatusNotFound {
		t.Fatalf("non-member admin ListThreads(vault) = %d, want 404 (admin verb is not read access)", code)
	}

	// Guests ride the SAME gates (no guest special-casing): a private channel is
	// masked (404), a public channel stays knowable (403).
	if code, _ := authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, vault.ChannelID), guestTok, nil); code != http.StatusNotFound {
		t.Fatalf("guest ListThreads(vault private) = %d, want 404 (masked)", code)
	}
	if code, _ := authRaw(t, "GET", fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, plaza.ChannelID), guestTok, nil); code != http.StatusForbidden {
		t.Fatalf("guest ListThreads(plaza public) = %d, want 403 (public existence is org-public)", code)
	}
}
