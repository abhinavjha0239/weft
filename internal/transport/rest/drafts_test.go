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

// TestDrafts: private per-user compose state — CRUD scoped strictly to the
// owner, container hints pinned to the org, nothing visible to anyone else.
func TestDrafts(t *testing.T) {
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
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "drf", "email": "a@drf.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@drf.test", "Bob Ray", "bobdrftok")

	// Create: channel-targeted and bare; empty source is rejected.
	var d1, d2 messaging.Draft
	postJSON(t, ts.URL+"/api/v1/drafts", boot.Token,
		map[string]any{"channel_id": boot.ChannelID, "source": "half-written thought"}, &d1)
	postJSON(t, ts.URL+"/api/v1/drafts", boot.Token,
		map[string]any{"source": "note to self"}, &d2)
	if d1.ID == 0 || d2.ID == 0 {
		t.Fatalf("create = %+v / %+v", d1, d2)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/drafts", boot.Token,
		map[string]any{"source": ""}); code != http.StatusBadRequest {
		t.Fatalf("empty draft = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/drafts", boot.Token,
		map[string]any{"channel_id": 999999, "source": "x"}); code != http.StatusNotFound {
		t.Fatalf("foreign channel = %d, want 404", code)
	}

	// Privacy: bob sees none of alice's drafts; alice sees exactly hers.
	var mine struct {
		Drafts []messaging.Draft `json:"drafts"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/drafts", bobTok, &mine); code != 200 || len(mine.Drafts) != 0 {
		t.Fatalf("bob's drafts = %d %+v, want empty", code, mine.Drafts)
	}
	if code := getJSON(t, ts.URL+"/api/v1/drafts", boot.Token, &mine); code != 200 || len(mine.Drafts) != 2 {
		t.Fatalf("alice's drafts = %d %+v, want 2", code, mine.Drafts)
	}

	// Update: own only — bob patching alice's draft 404s (no oracle).
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/drafts/%d", ts.URL, d1.ID),
		bobTok, map[string]any{"source": "hijack"}); code != http.StatusNotFound {
		t.Fatalf("bob update = %d, want 404", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/drafts/%d", ts.URL, d1.ID),
		boot.Token, map[string]any{"channel_id": boot.ChannelID, "source": "finished thought"}); code != http.StatusOK {
		t.Fatalf("update = %d", code)
	}
	getJSON(t, ts.URL+"/api/v1/drafts", boot.Token, &mine)
	if mine.Drafts[0].ID != d1.ID || mine.Drafts[0].Source != "finished thought" {
		t.Fatalf("updated draft ordering/content = %+v", mine.Drafts[0])
	}

	// Delete: own only; second delete 404s.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/drafts/%d", ts.URL, d2.ID), bobTok); code != http.StatusNotFound {
		t.Fatalf("bob delete = %d, want 404", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/drafts/%d", ts.URL, d2.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/drafts/%d", ts.URL, d2.ID), boot.Token); code != http.StatusNotFound {
		t.Fatalf("re-delete = %d, want 404", code)
	}
}
