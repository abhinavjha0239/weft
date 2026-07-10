package rest

import (
	"context"
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

// TestVerbAssignment: the ADR-006 reassignment surface. An admin retargets
// create_channel from members to admins and a member's access flips off;
// pointing it back flips it on (the assignment is an upsert). Members can
// never operate the endpoint themselves, and only registry verbs and real
// groups are accepted.
func TestVerbAssignment(t *testing.T) {
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
		"org_slug": "adm", "email": "a@adm.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@adm.test", "Bob Ray", "bobadmtok")

	// A plain member may not reassign verbs.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", bobTok,
		map[string]any{"verb": "create_channel", "group": "role:admins"}); code != http.StatusForbidden {
		t.Fatalf("member reassign = %d, want 403", code)
	}
	// Bad inputs: unknown verb 400, unknown group 404.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "launch_missiles", "group": "role:admins"}); code != http.StatusBadRequest {
		t.Fatalf("unknown verb = %d, want 400", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "create_channel", "group": "role:wizards"}); code != http.StatusNotFound {
		t.Fatalf("unknown group = %d, want 404", code)
	}

	// Baseline: a member can create channels (seeded default).
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", bobTok,
		map[string]any{"name": "bobs-lab"}); code != http.StatusCreated {
		t.Fatalf("member create baseline = %d, want 201", code)
	}
	// Retarget to admins: the member's access flips off…
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "create_channel", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("reassign = %d, want 200", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", bobTok,
		map[string]any{"name": "bobs-lab-2"}); code != http.StatusForbidden {
		t.Fatalf("member create after retarget = %d, want 403", code)
	}
	// …and back on when pointed at members again (upsert, not insert).
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "create_channel", "group": "role:members"}); code != http.StatusOK {
		t.Fatalf("restore = %d, want 200", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", bobTok,
		map[string]any{"name": "bobs-lab-2"}); code != http.StatusCreated {
		t.Fatalf("member create after restore = %d, want 201", code)
	}
}
