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

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestComplianceAdmin: the ADR-013 opener. compliance_officer is never
// seeded (F-9), so even the org OWNER is refused until the verb is
// explicitly granted through the admin surface; then retention policies
// upsert per scope and legal holds create/list/release with released holds
// kept as the audit record.
func TestComplianceAdmin(t *testing.T) {
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
		Identity:   identity.New(pool, permsSvc),
		Messaging:  messaging.New(pool, permsSvc),
		Compliance: compliance.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "cmp", "email": "a@cmp.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	var bobID int64
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@cmp.test", "Bob Ray", "bobcmptok")
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@cmp.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	// F-9: adminship is NOT compliance standing — the owner is refused.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": -1}); code != http.StatusForbidden {
		t.Fatalf("owner without grant = %d, want 403", code)
	}
	// Explicit grant via the verb-assignment surface, then it works.
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant = %d, want 200", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": -1, "keep_edits": true}); code != http.StatusOK {
		t.Fatalf("org policy = %d, want 200", code)
	}
	// A member still has nothing.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", bobTok,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": -1}); code != http.StatusForbidden {
		t.Fatalf("member = %d, want 403", code)
	}

	// Validation: zero duration, unsupported scope rung, foreign channel.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": 0}); code != http.StatusBadRequest {
		t.Fatalf("zero duration = %d, want 400", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 2, "scope_id": 1, "duration_days": 30}); code != http.StatusBadRequest {
		t.Fatalf("workspace scope = %d, want 400", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 3, "scope_id": 999999, "duration_days": 30}); code != http.StatusNotFound {
		t.Fatalf("unknown channel = %d, want 404", code)
	}

	// Channel override + org upsert; the list shows the effective set.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 3, "scope_id": boot.ChannelID, "duration_days": 90, "keep_edits": false}); code != http.StatusOK {
		t.Fatalf("channel policy = %d, want 200", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": 365, "keep_edits": true}); code != http.StatusOK {
		t.Fatalf("org upsert = %d, want 200", code)
	}
	var plist struct {
		Policies []compliance.RetentionPolicy `json:"policies"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token, &plist); code != http.StatusOK {
		t.Fatalf("list policies = %d, want 200", code)
	}
	if len(plist.Policies) != 2 || plist.Policies[0].DurationDays != 365 ||
		plist.Policies[1].ScopeID != boot.ChannelID || plist.Policies[1].KeepEdits {
		t.Fatalf("policy list wrong: %+v", plist.Policies)
	}

	// Legal holds: scope required; custodian must be a real org member.
	if code := postJSONStatus(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Case 42"}); code != http.StatusBadRequest {
		t.Fatalf("scopeless hold = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Case 42", "custodian_user_id": 999999}); code != http.StatusNotFound {
		t.Fatalf("unknown custodian = %d, want 404", code)
	}
	var hold compliance.LegalHold
	postJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Case 42", "custodian_user_id": bobID}, &hold)
	if hold.ID == 0 || hold.CustodianUserID == nil || *hold.CustodianUserID != bobID {
		t.Fatalf("hold create wrong: %+v", hold)
	}

	// Release once, not twice; unknown id is a 404, not a conflict.
	relURL := ts.URL + "/api/v1/admin/legal-holds/" + itoa(hold.ID) + "/release"
	if code := postJSONStatus(t, relURL, boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("release = %d, want 200", code)
	}
	if code := postJSONStatus(t, relURL, boot.Token, map[string]any{}); code != http.StatusConflict {
		t.Fatalf("double release = %d, want 409", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/admin/legal-holds/999999/release",
		boot.Token, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("release unknown = %d, want 404", code)
	}
	var hlist struct {
		Holds []compliance.LegalHold `json:"holds"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token, &hlist); code != http.StatusOK {
		t.Fatalf("list holds = %d, want 200", code)
	}
	if len(hlist.Holds) != 1 || hlist.Holds[0].ReleasedAt == nil {
		t.Fatalf("released hold must stay listed: %+v", hlist.Holds)
	}
}
