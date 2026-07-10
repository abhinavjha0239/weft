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

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestBlobGC: the ADR-012 F-1 rule end to end — a file is purged only when
// references hit zero AND the window elapsed AND no legal hold covers it.
// Unclaimed lane (never referenced, grace elapsed), dead-reference lane
// (every referencing message deleted, restore window elapsed), the
// content-dedup twin rule (a purged row's blob survives while a live row
// shares the key), and hold blocking on both lanes with purge resuming
// after release.
func TestBlobGC(t *testing.T) {
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
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	msgSvc.SetFiles(filesSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:   identity.New(pool, permsSvc),
		Messaging:  msgSvc,
		DM:         dm.New(pool),
		Files:      filesSvc,
		Compliance: compliance.New(pool, permsSvc),
	}))
	defer ts.Close()
	janitor := compliance.NewJanitor(pool, store, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "gc", "email": "a@gc.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@gc.test", "Bob Ray", "bobgctok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@gc.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant = %d, want 200", code)
	}

	backdateFile := func(id int64) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, id); err != nil {
			t.Fatalf("backdate file %d: %v", id, err)
		}
	}
	blobKey := func(id int64) string {
		t.Helper()
		var k string
		if err := pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, id).Scan(&k); err != nil {
			t.Fatalf("key of %d: %v", id, err)
		}
		return k
	}
	blobExists := func(key string) bool {
		t.Helper()
		rc, err := store.Open(ctx, key)
		if err != nil {
			return false
		}
		rc.Close()
		return true
	}
	fileLive := func(id int64) bool {
		t.Helper()
		var live bool
		if err := pool.QueryRow(ctx,
			`SELECT deleted_at IS NULL FROM file WHERE id = $1`, id).Scan(&live); err != nil {
			t.Fatalf("liveness of %d: %v", id, err)
		}
		return live
	}
	sendWithFile := func(tok string, f files.File) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			tok, map[string]any{"content": fmt.Sprintf("grab [%s](%s)", f.Name, f.URL)}, &sent)
		if sent.MessageID == 0 {
			t.Fatalf("send with file %d failed", f.ID)
		}
		return sent.MessageID
	}

	// ---- Unclaimed lane -------------------------------------------------
	_, u1 := uploadFile(t, ts.URL, boot.Token, "orphan.txt", "text/plain", "orphan bytes")
	_, u2 := uploadFile(t, ts.URL, boot.Token, "kept.txt", "text/plain", "kept bytes")
	_, u3 := uploadFile(t, ts.URL, boot.Token, "fresh.txt", "text/plain", "fresh bytes")
	_, u4 := uploadFile(t, ts.URL, bobTok, "held.txt", "text/plain", "held bytes")
	sendWithFile(boot.Token, u2) // u2 gains a live reference
	backdateFile(u1.ID)
	backdateFile(u2.ID)
	backdateFile(u4.ID)
	var hold compliance.LegalHold
	postJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Bob custodian", "custodian_user_id": bobID}, &hold)

	u1Key, u4Key := blobKey(u1.ID), blobKey(u4.ID)
	rep, err := janitor.SweepOnce(ctx, time.Now())
	if err != nil {
		t.Fatalf("sweep 1: %v", err)
	}
	if rep.UnclaimedPurged != 1 || rep.BlobsDeleted != 1 {
		t.Fatalf("sweep 1 report = %+v, want 1 unclaimed / 1 blob", rep)
	}
	if fileLive(u1.ID) || blobExists(u1Key) {
		t.Fatal("u1 (orphan, past grace) must be purged with its blob")
	}
	if !fileLive(u2.ID) {
		t.Fatal("u2 (live reference) must survive")
	}
	if !fileLive(u3.ID) {
		t.Fatal("u3 (inside grace) must survive")
	}
	if !fileLive(u4.ID) || !blobExists(u4Key) {
		t.Fatal("u4 (custodian hold) must survive")
	}
	// A purged file 404s like it never existed.
	if resp, _ := download(t, ts.URL, boot.Token, u1.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("purged download = %d, want 404", resp.StatusCode)
	}

	// Release the hold: the next sweep reclaims u4.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/admin/legal-holds/%d/release", ts.URL, hold.ID),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("release = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.UnclaimedPurged != 1 {
		t.Fatalf("sweep 2 = %+v (%v), want u4 purged", rep, err)
	}
	if fileLive(u4.ID) || blobExists(u4Key) {
		t.Fatal("u4 must be purged after hold release")
	}

	// ---- Dedup twin rule ------------------------------------------------
	_, c1 := uploadFile(t, ts.URL, boot.Token, "twin1.txt", "text/plain", "twin bytes")
	_, c2 := uploadFile(t, ts.URL, boot.Token, "twin2.txt", "text/plain", "twin bytes")
	sendWithFile(boot.Token, c1) // c1 stays claimed
	backdateFile(c2.ID)
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil ||
		rep.UnclaimedPurged != 1 || rep.BlobsDeleted != 0 {
		t.Fatalf("twin sweep = %+v (%v), want row purged but blob kept", rep, err)
	}
	if fileLive(c2.ID) {
		t.Fatal("c2 row must be purged")
	}
	if resp, body := download(t, ts.URL, boot.Token, c1.ID); resp.StatusCode != http.StatusOK || body != "twin bytes" {
		t.Fatalf("twin download = %d %q, want live bytes", resp.StatusCode, body)
	}

	// ---- Dead-reference lane --------------------------------------------
	backdateMsg := func(id int64, interval string) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE message SET deleted_at = now() - $2::interval WHERE id = $1`, id, interval); err != nil {
			t.Fatalf("backdate message %d: %v", id, err)
		}
	}
	_, d1 := uploadFile(t, ts.URL, boot.Token, "gone.txt", "text/plain", "gone bytes")
	_, d2 := uploadFile(t, ts.URL, boot.Token, "recent.txt", "text/plain", "recent bytes")
	_, d3 := uploadFile(t, ts.URL, boot.Token, "chanheld.txt", "text/plain", "chanheld bytes")
	m1 := sendWithFile(boot.Token, d1)
	m2 := sendWithFile(boot.Token, d2)
	// d3's message lives in its own channel: a hold there must block d3
	// and ONLY d3 — the general channel stays unheld.
	var legal struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "legal"}, &legal)
	var sent3 struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, legal.ChannelID),
		boot.Token, map[string]any{"content": fmt.Sprintf("grab [%s](%s)", d3.Name, d3.URL)}, &sent3)
	m3 := sent3.MessageID
	var chanHold compliance.LegalHold
	postJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Channel scope", "channel_id": legal.ChannelID}, &chanHold)
	for _, mid := range []int64{m1, m2, m3} {
		if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, mid), boot.Token); code != http.StatusOK {
			t.Fatalf("delete message %d = %d", mid, code)
		}
	}
	backdateMsg(m1, "40 days")
	backdateMsg(m3, "40 days") // held: window elapsed but the channel hold blocks
	d1Key, d3Key := blobKey(d1.ID), blobKey(d3.ID)

	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil ||
		rep.DeadRefPurged != 1 || rep.UnclaimedPurged != 0 {
		t.Fatalf("dead-ref sweep = %+v (%v), want exactly d1", rep, err)
	}
	if fileLive(d1.ID) || blobExists(d1Key) {
		t.Fatal("d1 (dead reference, window elapsed) must be purged")
	}
	if !fileLive(d2.ID) {
		t.Fatal("d2 (inside restore window) must survive")
	}
	if !fileLive(d3.ID) || !blobExists(d3Key) {
		t.Fatal("d3 (channel hold) must survive")
	}
	// Release the channel hold: d3 is reclaimed on the next pass.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/admin/legal-holds/%d/release", ts.URL, chanHold.ID),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("release channel hold = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.DeadRefPurged != 1 {
		t.Fatalf("post-release sweep = %+v (%v), want d3 purged", rep, err)
	}
	if fileLive(d3.ID) || blobExists(d3Key) {
		t.Fatal("d3 must be purged after channel hold release")
	}

	// The purge trail is in the event log: one file.purged row per purge.
	var purgedEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND verb = 'file.purged'`, boot.OrgID).Scan(&purgedEvents); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if purgedEvents != 5 {
		t.Fatalf("file.purged events = %d, want 5 (u1,u4,c2,d1,d3)", purgedEvents)
	}
}
