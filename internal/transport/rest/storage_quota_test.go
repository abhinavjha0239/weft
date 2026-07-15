package rest

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// TestStorageQuota: P-19 per-org storage quota. A cap rejects an upload that
// would push live usage past it (413, inclusive boundary), used_bytes tracks
// row-accounted live size, a GC purge frees room, max_bytes 0 is unlimited,
// only manage_org holders may read or set the cap, and every set is
// event-logged.
func TestStorageQuota(t *testing.T) {
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

	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	filesSvc.SetPerms(permsSvc)
	msgSvc.SetFiles(filesSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		DM:        dm.New(pool),
		Files:     filesSvc,
	}))
	defer ts.Close()
	janitor := compliance.NewJanitor(pool, store, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "quota", "email": "a@quota.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@quota.test", "Bob Ray", "bobquotatok")

	setQuota := func(token string, maxBytes int64) int {
		t.Helper()
		return putJSON(t, ts.URL+"/api/v1/admin/storage-quota", token,
			map[string]any{"max_bytes": maxBytes})
	}
	getQuota := func(token string) (int, files.StorageQuotaInfo) {
		t.Helper()
		var info files.StorageQuotaInfo
		code := getJSON(t, ts.URL+"/api/v1/admin/storage-quota", token, &info)
		return code, info
	}
	upload := func(name string, size int) (int, files.File) {
		t.Helper()
		return uploadFile(t, ts.URL, boot.Token, name, "text/plain", strings.Repeat(name[:1], size))
	}
	assertQuota := func(wantMax, wantUsed int64) {
		t.Helper()
		code, info := getQuota(boot.Token)
		if code != http.StatusOK || info.MaxBytes != wantMax || info.UsedBytes != wantUsed {
			t.Fatalf("quota = %d {max %d used %d}, want {max %d used %d}",
				code, info.MaxBytes, info.UsedBytes, wantMax, wantUsed)
		}
	}

	// Unlimited by default; usage starts at 0 then tracks a live upload.
	assertQuota(0, 0)
	_, f1 := upload("aaa", 100)
	assertQuota(0, 100)

	// Cap 150 (event-logged). Boundary is inclusive: 100+50 == 150 passes.
	if code := setQuota(boot.Token, 150); code != http.StatusOK {
		t.Fatalf("set quota = %d, want 200", code)
	}
	assertQuota(150, 100)
	if code, _ := upload("bbb", 50); code != http.StatusCreated {
		t.Fatalf("boundary upload (used→150==cap) = %d, want 201", code)
	}
	assertQuota(150, 150)

	// One more byte is over cap → 413, and nothing is stored (used unchanged).
	// THE load-bearing assertion — red/green: neuter checkQuota → this 201s.
	if code, _ := upload("ccc", 1); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over-cap upload = %d, want 413", code)
	}
	assertQuota(150, 150)

	// A GC purge frees quota: backdate the unreferenced f1 past the grace
	// window, sweep, and its 100 bytes drop out of the sum.
	if _, err := pool.Exec(ctx,
		`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, f1.ID); err != nil {
		t.Fatalf("backdate f1: %v", err)
	}
	if rep, err := janitor.SweepOnce(ctx, time.Now()); err != nil || rep.UnclaimedPurged != 1 {
		t.Fatalf("sweep = %+v err %v, want UnclaimedPurged 1", rep, err)
	}
	assertQuota(150, 50)
	if code, _ := upload("ddd", 90); code != http.StatusCreated {
		t.Fatalf("upload after purge (50+90<=150) = %d, want 201", code)
	}
	assertQuota(150, 140)

	// Non-admin: a plain member may neither read nor set the quota (manage_org).
	if code, _ := getQuota(bobTok); code != http.StatusForbidden {
		t.Fatalf("non-admin GET quota = %d, want 403", code)
	}
	if code := setQuota(bobTok, 999); code != http.StatusForbidden {
		t.Fatalf("non-admin PUT quota = %d, want 403", code)
	}

	// max_bytes 0 = unlimited: a large upload passes despite prior usage.
	if code := setQuota(boot.Token, 0); code != http.StatusOK {
		t.Fatalf("set quota 0 = %d, want 200", code)
	}
	assertQuota(0, 140)
	if code, _ := upload("eee", 500); code != http.StatusCreated {
		t.Fatalf("upload under unlimited cap = %d, want 201", code)
	}
	assertQuota(0, 640)

	// Every set is event-logged (org.quota_changed on the org entity); the 150
	// set carries its max_bytes as a JSON number in the payload.
	var events int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND verb = 'org.quota_changed' AND entity_id = $1`,
		boot.OrgID).Scan(&events); err != nil {
		t.Fatalf("quota events: %v", err)
	}
	if events != 2 {
		t.Fatalf("org.quota_changed events = %d, want 2 (cap set to 150 then 0)", events)
	}
	var has150 bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM event_log
		  WHERE org_id = $1 AND verb = 'org.quota_changed'
		    AND (payload->>'max_bytes')::bigint = 150)`,
		boot.OrgID).Scan(&has150); err != nil {
		t.Fatalf("quota event payload: %v", err)
	}
	if !has150 {
		t.Fatal("no org.quota_changed event with max_bytes=150 payload")
	}
}
