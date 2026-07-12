package rest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// pngBytes returns something http.DetectContentType sniffs as image/png: the
// 8-byte PNG signature plus distinguishing padding (so different seeds hash to
// different content-addressed keys).
func pngBytes(seed string, size int) []byte {
	b := make([]byte, size)
	copy(b, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'})
	copy(b[8:], seed)
	return b
}

func gifBytes(seed string) []byte {
	return append([]byte("GIF89a"), []byte(seed)...)
}

// addGuestMember inserts a role-50 guest who is a member of channelID with a
// session (distinct name from the pins slice's helper to avoid a post-merge
// clash). The avatar guest gate reads channel_member only, so no group grant
// is needed.
func addGuestMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, channelID int64, email, name, token string) int64 {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 50) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("guest: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`, channelID, uid); err != nil {
		t.Fatalf("guest join: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("guest session: %v", err)
	}
	return uid
}

func putAvatar(t *testing.T, base, token string, data []byte) (int, int64) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "avatar.bin")
	_, _ = fw.Write(data)
	mw.Close()
	req, _ := http.NewRequest("PUT", base+"/api/v1/me/avatar", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put avatar: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		AvatarFileID int64 `json:"avatar_file_id"`
	}
	_ = jsonDecode(resp.Body, &out)
	return resp.StatusCode, out.AvatarFileID
}

func getAvatar(t *testing.T, base, token string, userID int64) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/users/%d/avatar", base, userID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get avatar: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

// TestAvatars: P-06 avatars — magic-byte + size gating BEFORE the store, the
// org-public read ACL (guest-gated), the safe inline/nosniff/cache headers,
// avatar_file_id surfaced on profiles, and the clear→GC-eligible lifecycle.
func TestAvatars(t *testing.T) {
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

	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
		DM:        dm.New(pool),
		Files:     files.New(pool, store),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "ava", "email": "a@ava.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@ava.test", "Bob Ray", "bobavatok")
	// charlie is a member with no avatar (the no-avatar 404 case).
	_ = addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@ava.test", "Charlie Kim", "charlieavatok")
	bobID := userIDByEmail(t, ctx, pool, boot.OrgID, "bob@ava.test")
	charlieID := userIDByEmail(t, ctx, pool, boot.OrgID, "charlie@ava.test")

	// Non-image content is rejected by magic bytes, before the store — a
	// renamed text file, an SVG (XSS vector), and an oversize image.
	if code, _ := putAvatar(t, ts.URL, boot.Token, []byte("plain text, not an image at all")); code != http.StatusBadRequest {
		t.Fatalf("text-as-avatar = %d, want 400", code)
	}
	if code, _ := putAvatar(t, ts.URL, boot.Token,
		[]byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`)); code != http.StatusBadRequest {
		t.Fatalf("svg-as-avatar = %d, want 400 (XSS vector rejected)", code)
	}
	if code, _ := putAvatar(t, ts.URL, boot.Token, pngBytes("big", avatarMaxBytes+100)); code != http.StatusBadRequest {
		t.Fatalf("oversize avatar = %d, want 400", code)
	}

	// Happy path: a real PNG round-trips, served inline with the safe headers.
	alicePNG := pngBytes("alice-avatar", 64)
	code, aliceFileID := putAvatar(t, ts.URL, boot.Token, alicePNG)
	if code != http.StatusOK || aliceFileID == 0 {
		t.Fatalf("set avatar = %d, id=%d", code, aliceFileID)
	}
	resp, body := getAvatar(t, ts.URL, boot.Token, boot.UserID)
	if resp.StatusCode != 200 || !bytes.Equal(body, alicePNG) {
		t.Fatalf("own avatar = %d, %d bytes (want %d)", resp.StatusCode, len(body), len(alicePNG))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
		t.Fatalf("avatar content-type = %q, want image/png", ct)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
		resp.Header.Get("Cache-Control") != "private, max-age=3600" ||
		resp.Header.Get("Content-Disposition") != "inline" {
		t.Fatalf("avatar headers wrong: nosniff=%q cache=%q disp=%q",
			resp.Header.Get("X-Content-Type-Options"), resp.Header.Get("Cache-Control"),
			resp.Header.Get("Content-Disposition"))
	}
	// Any org member may fetch any member's avatar (org-public).
	if resp, _ := getAvatar(t, ts.URL, bobTok, boot.UserID); resp.StatusCode != 200 {
		t.Fatalf("member fetch of avatar = %d, want 200", resp.StatusCode)
	}
	// A user with no avatar is a 404 (oracle-free).
	if resp, _ := getAvatar(t, ts.URL, boot.Token, charlieID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no-avatar fetch = %d, want 404", resp.StatusCode)
	}
	// Animated-friendly GIF is allowed too.
	if code, _ := putAvatar(t, ts.URL, bobTok, gifBytes("bob-gif")); code != http.StatusOK {
		t.Fatalf("gif avatar = %d, want 200", code)
	}
	if resp, _ := getAvatar(t, ts.URL, boot.Token, bobID); resp.StatusCode != 200 ||
		resp.Header.Get("Content-Type") != "image/gif" {
		t.Fatalf("bob gif avatar = %d ct=%q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	// Guest visibility: a guest sharing #general sees alice's avatar, but not
	// a user they share no channel with.
	var ch2 struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "elsewhere"}, &ch2)
	eveTok := addChannelMember(t, ctx, pool, boot.OrgID, ch2.ChannelID,
		"eve@ava.test", "Eve Stone", "eveavatok")
	eveID := userIDByEmail(t, ctx, pool, boot.OrgID, "eve@ava.test")
	if code, _ := putAvatar(t, ts.URL, eveTok, pngBytes("eve-avatar", 48)); code != http.StatusOK {
		t.Fatalf("eve avatar = %d", code)
	}
	addGuestMember(t, ctx, pool, boot.OrgID, boot.ChannelID, "gina@ava.test", "Gina Guest", "ginaavatok")
	if resp, _ := getAvatar(t, ts.URL, "ginaavatok", boot.UserID); resp.StatusCode != 200 {
		t.Fatalf("guest fetch of channel-mate avatar = %d, want 200", resp.StatusCode)
	}
	if resp, _ := getAvatar(t, ts.URL, "ginaavatok", eveID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("guest fetch of non-mate avatar = %d, want 404", resp.StatusCode)
	}

	// Profiles surface avatar_file_id so clients know to build the URL.
	var dir struct {
		Users []struct {
			ID           int64  `json:"id"`
			AvatarFileID *int64 `json:"avatar_file_id"`
		} `json:"users"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/users?ids=%d,%d", ts.URL, boot.UserID, charlieID), boot.Token, &dir)
	var aliceProfileAvatar *int64
	var charlieProfileAvatar *int64
	for _, u := range dir.Users {
		if u.ID == boot.UserID {
			aliceProfileAvatar = u.AvatarFileID
		}
		if u.ID == charlieID {
			charlieProfileAvatar = u.AvatarFileID
		}
	}
	if aliceProfileAvatar == nil || *aliceProfileAvatar != aliceFileID {
		t.Fatalf("alice profile avatar_file_id = %v, want %d", aliceProfileAvatar, aliceFileID)
	}
	if charlieProfileAvatar != nil {
		t.Fatalf("charlie has no avatar; profile should omit it, got %v", *charlieProfileAvatar)
	}

	// clear→GC lifecycle. Backdate alice's avatar file past the unclaimed
	// grace; while pointed-to it survives a sweep, but clearing the pointer
	// makes it GC-eligible and the next sweep purges it (row + blob).
	var storageKey string
	if err := pool.QueryRow(ctx,
		`SELECT storage_key FROM file WHERE id = $1`, aliceFileID).Scan(&storageKey); err != nil {
		t.Fatalf("storage key: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, aliceFileID); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	janitor := compliance.NewJanitor(pool, store, slog.Default())
	if _, err := janitor.SweepOnce(ctx, time.Now()); err != nil {
		t.Fatalf("sweep (pinned): %v", err)
	}
	if !fileLive(t, ctx, pool, aliceFileID) {
		t.Fatal("avatar file purged while still pointed to — the FK guard failed")
	}
	if code := deleteReq(t, ts.URL+"/api/v1/me/avatar", boot.Token); code != http.StatusOK {
		t.Fatalf("clear avatar = %d", code)
	}
	rep, err := janitor.SweepOnce(ctx, time.Now())
	if err != nil {
		t.Fatalf("sweep (cleared): %v", err)
	}
	if rep.UnclaimedPurged < 1 || fileLive(t, ctx, pool, aliceFileID) {
		t.Fatalf("cleared avatar not GC'd: purged=%d live=%v", rep.UnclaimedPurged, fileLive(t, ctx, pool, aliceFileID))
	}
	if rc, err := store.Open(ctx, storageKey); err == nil {
		rc.Close()
		t.Fatal("avatar blob survived GC despite no other row sharing its key")
	}
}

func fileLive(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64) bool {
	t.Helper()
	var live bool
	if err := pool.QueryRow(ctx,
		`SELECT deleted_at IS NULL FROM file WHERE id = $1`, id).Scan(&live); err != nil {
		t.Fatalf("file live %d: %v", id, err)
	}
	return live
}
