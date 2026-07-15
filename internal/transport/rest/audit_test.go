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
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type auditResp struct {
	Events     []compliance.AuditEvent `json:"events"`
	NextCursor int64                   `json:"next_cursor"`
}

// TestAuditReadAPI: P-31. The compliance_officer read over the raw event log —
// F-9 gated (owners without the grant are refused), newest-first, filterable,
// keyset-paginated, and strictly org-scoped.
func TestAuditReadAPI(t *testing.T) {
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
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:   identity.New(pool, permsSvc),
		Messaging:  messaging.New(pool, permsSvc),
		Compliance: compliance.New(pool, permsSvc),
	}))
	defer ts.Close()

	type boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	var org1, org2 boot
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "aud1", "email": "alice@aud.test", "password": "password123",
		"full_name": "Alice Owner",
	}, &org1)
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "aud2", "email": "dave@aud.test", "password": "password123",
		"full_name": "Dave Owner",
	}, &org2)
	bobTok := addChannelMember(t, ctx, pool, org1.OrgID, org1.ChannelID,
		"bob@aud.test", "Bob Ray", "bobaudtok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@aud.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	getAudit := func(t *testing.T, token, query string) (int, auditResp) {
		t.Helper()
		var out auditResp
		code := getJSON(t, ts.URL+"/api/v1/audit/events"+query, token, &out)
		return code, out
	}
	idsOf := func(p auditResp) []int64 {
		out := []int64{}
		for _, e := range p.Events {
			out = append(out, e.ID)
		}
		return out
	}

	// F-9 load-bearing: the OWNER, without the explicit grant, is refused
	// (adminship is not compliance standing). Drop the Require and this is 200.
	if code, _ := getAudit(t, org1.Token, ""); code != http.StatusForbidden {
		t.Fatalf("owner without grant = %d, want 403", code)
	}
	// A plain member is likewise refused.
	if code, _ := getAudit(t, bobTok, ""); code != http.StatusForbidden {
		t.Fatalf("member without grant = %d, want 403", code)
	}

	// Grant compliance_officer to admins in both orgs (owner ⊂ admins).
	for _, b := range []boot{org1, org2} {
		if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", b.Token,
			map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
			t.Fatalf("grant = %d, want 200", code)
		}
	}

	// A controlled set of org1 events. Baseline (bootstrap channel.created +
	// the grant's org.verb_assigned) uses entity_types 3/8, verbs that are not
	// "audit.*", and actor = alice — so these markers are exclusive.
	base := time.Now().Truncate(time.Second)
	insertEvent := func(actorID int64, entityType int16, verb string, occurredAt time.Time) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO event_log (org_id, actor_kind, actor_id, entity_type, entity_id, verb, payload, occurred_at)
			VALUES ($1, 1, $2, $3, $4, $5, '{}'::jsonb, $6) RETURNING id`,
			org1.OrgID, actorID, entityType, 999, verb, occurredAt).Scan(&id); err != nil {
			t.Fatalf("insert event: %v", err)
		}
		return id
	}
	e1 := insertEvent(bobID, 5, "audit.dm", base.Add(1*time.Minute))
	e2 := insertEvent(bobID, 7, "audit.file", base.Add(2*time.Minute))
	e3 := insertEvent(org1.UserID, 7, "audit.dm", base.Add(3*time.Minute))
	// A distractor that matches none of the filters below (alice, type 5,
	// verb audit.other) — proving each filter excludes non-matching rows.
	insertEvent(org1.UserID, 5, "audit.other", base.Add(4*time.Minute))
	e5 := insertEvent(bobID, 5, "audit.dm", base.Add(5*time.Minute))

	// Newest-first: the whole page is strictly descending by id.
	_, all := getAudit(t, org1.Token, "?limit=200")
	if len(all.Events) < 7 { // 5 inserted + 2 baseline
		t.Fatalf("expected >=7 events, got %d", len(all.Events))
	}
	for i := 1; i < len(all.Events); i++ {
		if all.Events[i-1].ID <= all.Events[i].ID {
			t.Fatalf("events not newest-first at %d: %d <= %d", i, all.Events[i-1].ID, all.Events[i].ID)
		}
	}

	// verb filter: exactly the three audit.dm events, newest first.
	_, dm := getAudit(t, org1.Token, "?verb=audit.dm")
	if got := idsOf(dm); !equalInt64Slice(got, []int64{e5, e3, e1}) {
		t.Fatalf("verb=audit.dm = %v, want [%d %d %d]", got, e5, e3, e1)
	}
	// entity_type filter: exactly the two entity_type-7 events (baseline has none).
	_, et := getAudit(t, org1.Token, "?entity_type=7")
	if got := idsOf(et); !equalInt64Slice(got, []int64{e3, e2}) {
		t.Fatalf("entity_type=7 = %v, want [%d %d]", got, e3, e2)
	}
	// actor_id filter: exactly bob's three events (baseline is alice's).
	_, byBob := getAudit(t, org1.Token, fmt.Sprintf("?actor_id=%d", bobID))
	if got := idsOf(byBob); !equalInt64Slice(got, []int64{e5, e2, e1}) {
		t.Fatalf("actor_id=bob = %v, want [%d %d %d]", got, e5, e2, e1)
	}

	// since/until on occurred_at (isolated to audit.dm). since is inclusive,
	// until exclusive (the search before/after precedent).
	rfc := func(d time.Duration) string { return base.Add(d).UTC().Format(time.RFC3339) }
	_, since := getAudit(t, org1.Token, "?verb=audit.dm&since="+rfc(2*time.Minute))
	if got := idsOf(since); !equalInt64Slice(got, []int64{e5, e3}) {
		t.Fatalf("since +2m = %v, want [%d %d]", got, e5, e3)
	}
	_, until := getAudit(t, org1.Token, "?verb=audit.dm&until="+rfc(4*time.Minute))
	if got := idsOf(until); !equalInt64Slice(got, []int64{e3, e1}) {
		t.Fatalf("until +4m = %v, want [%d %d]", got, e3, e1)
	}
	_, window := getAudit(t, org1.Token, "?verb=audit.dm&since="+rfc(2*time.Minute)+"&until="+rfc(4*time.Minute))
	if got := idsOf(window); !equalInt64Slice(got, []int64{e3}) {
		t.Fatalf("window [+2m,+4m) = %v, want [%d]", got, e3)
	}

	// Cursor walk over ALL org1 events in pages of 3: no overlap, no gap versus
	// the one-shot read.
	var walked []int64
	cursor := int64(0)
	for {
		q := "?limit=3"
		if cursor > 0 {
			q += fmt.Sprintf("&cursor=%d", cursor)
		}
		_, page := getAudit(t, org1.Token, q)
		walked = append(walked, idsOf(page)...)
		if page.NextCursor == 0 {
			break
		}
		cursor = page.NextCursor
	}
	if !equalInt64Slice(walked, idsOf(all)) {
		t.Fatalf("cursor walk %v != one-shot %v", walked, idsOf(all))
	}

	// Two-org isolation: dave (org2 officer) sees only org2's events — never
	// any of org1's audit.* markers.
	_, daveView := getAudit(t, org2.Token, "?limit=200")
	for _, e := range daveView.Events {
		if len(e.Verb) >= 6 && e.Verb[:6] == "audit." {
			t.Fatalf("org2 officer saw an org1 event: %+v", e)
		}
	}

	// Malformed since → 400.
	if code, _ := getAudit(t, org1.Token, "?since=not-a-timestamp"); code != http.StatusBadRequest {
		t.Fatalf("malformed since = %d, want 400", code)
	}

	// Limit clamps: bulk-insert enough events to exceed both bounds.
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_log (org_id, actor_kind, actor_id, entity_type, entity_id, verb, payload, occurred_at)
		SELECT $1, 1, $2, 1, g, 'audit.bulk', '{}'::jsonb, now()
		FROM generate_series(1, 250) g`, org1.OrgID, org1.UserID); err != nil {
		t.Fatalf("bulk insert: %v", err)
	}
	if _, page := getAudit(t, org1.Token, "?limit=0"); len(page.Events) != 50 {
		t.Fatalf("limit=0 returned %d, want 50 (default clamp)", len(page.Events))
	}
	if _, page := getAudit(t, org1.Token, "?limit=500"); len(page.Events) != 200 {
		t.Fatalf("limit=500 returned %d, want 200 (max clamp)", len(page.Events))
	}
}

func equalInt64Slice(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
