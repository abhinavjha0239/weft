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

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type statusUser struct {
	ID         int64  `json:"id"`
	Emoji      string `json:"emoji"`
	StatusText string `json:"status_text"`
}

type statusUsersResp struct {
	Users []statusUser `json:"users"`
}

// find returns the entry for id, or a zero-value probe that fails the caller's
// assertions if the id is absent.
func (r statusUsersResp) find(id int64) statusUser {
	for _, u := range r.Users {
		if u.ID == id {
			return u
		}
	}
	return statusUser{}
}

// TestUserStatus: ADR-011 N-3 — a durable manual status set by its owner and
// read by others through the directory and batch-profile surfaces, updated,
// expired (filtered by the read-side expiry clause), cleared, and validated.
func TestUserStatus(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sts", "email": "a@sts.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@sts.test", "Bob Ray", "bobststok")

	dirURL := ts.URL + "/api/v1/users"
	batchURL := fmt.Sprintf("%s/api/v1/users?ids=%d", ts.URL, boot.UserID)

	// Baseline: no status set → both read surfaces omit the fields (bob sees
	// alice, but empty status).
	var dir statusUsersResp
	if code := getJSON(t, dirURL, bobTok, &dir); code != http.StatusOK {
		t.Fatalf("directory = %d", code)
	}
	if a := dir.find(boot.UserID); a.ID == 0 || a.Emoji != "" || a.StatusText != "" {
		t.Fatalf("baseline alice status = %+v, want empty", a)
	}

	// Alice sets a status → bob sees it in BOTH the directory and the batch
	// profile lookup.
	if code := putJSON(t, ts.URL+"/api/v1/status", boot.Token,
		map[string]any{"emoji": "🌴", "status_text": "on vacation"}); code != http.StatusOK {
		t.Fatalf("set status = %d", code)
	}
	dir = statusUsersResp{}
	getJSON(t, dirURL, bobTok, &dir)
	if a := dir.find(boot.UserID); a.Emoji != "🌴" || a.StatusText != "on vacation" {
		t.Fatalf("directory status = %+v, want 🌴/on vacation", a)
	}
	var batch statusUsersResp
	getJSON(t, batchURL, bobTok, &batch)
	if a := batch.find(boot.UserID); a.Emoji != "🌴" || a.StatusText != "on vacation" {
		t.Fatalf("batch status = %+v, want 🌴/on vacation", a)
	}

	// Update: a second set overwrites (text-only, emoji cleared).
	if code := putJSON(t, ts.URL+"/api/v1/status", boot.Token,
		map[string]any{"status_text": "heads down"}); code != http.StatusOK {
		t.Fatalf("update status = %d", code)
	}
	batch = statusUsersResp{}
	getJSON(t, batchURL, bobTok, &batch)
	if a := batch.find(boot.UserID); a.Emoji != "" || a.StatusText != "heads down" {
		t.Fatalf("updated status = %+v, want no emoji / heads down", a)
	}

	// Expiry filter: set a status that expires in the future (visible), then
	// backdate its expiry via SQL — the read-side clause must hide it.
	if code := putJSON(t, ts.URL+"/api/v1/status", boot.Token, map[string]any{
		"emoji": "🍽️", "status_text": "lunch",
		"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339),
	}); code != http.StatusOK {
		t.Fatalf("set expiring status = %d", code)
	}
	batch = statusUsersResp{}
	getJSON(t, batchURL, bobTok, &batch)
	if a := batch.find(boot.UserID); a.StatusText != "lunch" {
		t.Fatalf("pre-expiry status = %+v, want lunch", a)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE user_status SET expires_at = now() - interval '1 minute' WHERE user_id = $1`,
		boot.UserID); err != nil {
		t.Fatalf("backdate expiry: %v", err)
	}
	batch = statusUsersResp{}
	getJSON(t, batchURL, bobTok, &batch)
	if a := batch.find(boot.UserID); a.Emoji != "" || a.StatusText != "" {
		t.Fatalf("expired status = %+v, want filtered out", a)
	}

	// Clear removes the row entirely.
	if code := putJSON(t, ts.URL+"/api/v1/status", boot.Token,
		map[string]any{"emoji": "🎯", "status_text": "focus"}); code != http.StatusOK {
		t.Fatalf("re-set status = %d", code)
	}
	if code := deleteReq(t, ts.URL+"/api/v1/status", boot.Token); code != http.StatusOK {
		t.Fatalf("clear status = %d", code)
	}
	batch = statusUsersResp{}
	getJSON(t, batchURL, bobTok, &batch)
	if a := batch.find(boot.UserID); a.Emoji != "" || a.StatusText != "" {
		t.Fatalf("cleared status = %+v, want gone", a)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM user_status WHERE user_id = $1`, boot.UserID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("user_status rows after clear = %d (%v), want 0", count, err)
	}

	// Validation 400s.
	long := ""
	for i := 0; i < 121; i++ {
		long += "x"
	}
	for name, body := range map[string]map[string]any{
		"empty set":        {},
		"oversize text":    {"status_text": long},
		"control in text":  {"status_text": "line\nbreak"},
		"emoji whitespace": {"emoji": "a b"},
		"emoji too long":   {"emoji": "0123456789012345678901234567890123"},
		"past expiry": {"status_text": "x",
			"expires_at": time.Now().Add(-time.Hour).Format(time.RFC3339)},
	} {
		if code := putJSON(t, ts.URL+"/api/v1/status", boot.Token, body); code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", name, code)
		}
	}
}
