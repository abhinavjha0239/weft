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

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// The org-administration permission matrix (P-47). Nineteen non-test
// references to one verb guard EIGHTEEN gates that fan out to these
// TWENTY-FOUR REST endpoints — emoji 2, channel-folders 4, default-channels
// 2, storage-quota 2, link-previews 2, auth-providers 4, admin/verbs 1,
// automations 7 (one `requireScopeAdmin` reached from Create, Update, Delete,
// List, ListRuns, ListDeliveries and RotateWebhookToken).
//
// This table is the BEFORE/AFTER instrument for splitting that verb into its
// ADR-006 siblings. It lands, green, in its own commit against the
// un-split tree, so "the answers are identical before and after" has a
// referent outside the binary that changed: the same table, byte for byte,
// must still be green once each gate checks its own verb.
//
// Two rules keep a cell honest:
//
//   - every payload is VALID. Five of these gates validate input before the
//     permission check (CreateEmoji, CreateFolder, SetStorageQuota,
//     CreateAuthProvider/UpdateAuthProvider, AssignVerb), so a placeholder
//     body answers 400 for holder and non-holder alike and measures nothing.
//     A denied cell asserts 403 EXACTLY — never "some 4xx".
//   - the expected statuses are hardcoded here, not read back from the code
//     under test.
//
// `POST /automations/{id}/consent` and `POST /automations/slash` are
// deliberately absent: neither is gated by this verb.

// permCase is one gated endpoint: the request to issue, the status a holder
// of its gate must get, and the status a non-holder must get.
type permCase struct {
	name        string
	run         func(t *testing.T, f *permFixture, token string) int
	wantAllowed int
	wantDenied  int
}

// permFixture is one org's matrix subject. The objects the update/delete
// cells operate on are minted by the org OWNER immediately after bootstrap —
// BEFORE any assignment is touched — so a run's outcome never depends on the
// gate the run is measuring.
type permFixture struct {
	base        string
	orgID       int64
	channelID   int64
	workspaceID int64
	folderID    int64
	providerID  int64
	autoID      int64
}

// permWriteVerbs is the event-log signature of ONE allowed pass: every gated
// endpoint that writes, exactly once, in table order. A denied pass must mint
// none of them. (`PUT /default-channels` writes but logs nothing, so its
// effect is asserted on the row count instead.)
var permWriteVerbs = []string{
	"emoji.created", "emoji.deleted",
	"folder.created", "folder.updated", "folder.deleted",
	"org.quota_changed",
	"org.link_previews_changed",
	"auth_provider.created", "auth_provider.updated", "auth_provider.deleted",
	"org.verb_assigned",
	"automation.created", "automation.updated",
	"automation.webhook_token_rotated", "automation.deleted",
}

const (
	permEmojiName    = "matrixemoji"
	permProviderName = "matrix-idp"
	permIssuer       = "https://idp.example.test"
)

func permMatrix() []permCase {
	autoURL := func(f *permFixture, suffix string) string {
		return fmt.Sprintf("%s/api/v1/automations/%d%s", f.base, f.autoID, suffix)
	}
	providerURL := func(f *permFixture) string {
		return fmt.Sprintf("%s/api/v1/admin/auth-providers/%d", f.base, f.providerID)
	}
	folderURL := func(f *permFixture) string {
		return fmt.Sprintf("%s/api/v1/channel-folders/%d", f.base, f.folderID)
	}
	return []permCase{
		// --- custom emoji (messaging/emoji.go) ---
		{"POST /emoji", func(t *testing.T, f *permFixture, tok string) int {
			return postEmoji(t, f.base, tok, permEmojiName, pngBytes("perm-matrix", 40))
		}, http.StatusCreated, http.StatusForbidden},
		{"DELETE /emoji/{name}", func(t *testing.T, f *permFixture, tok string) int {
			return deleteReq(t, f.base+"/api/v1/emoji/"+permEmojiName, tok)
		}, http.StatusOK, http.StatusForbidden},

		// --- channel folders + default channels (messaging/folders.go) ---
		{"POST /channel-folders", func(t *testing.T, f *permFixture, tok string) int {
			return postJSONStatus(t, f.base+"/api/v1/channel-folders", tok,
				map[string]any{"name": "Matrix Folder"})
		}, http.StatusCreated, http.StatusForbidden},
		{"GET /channel-folders", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, f.base+"/api/v1/channel-folders", tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"PATCH /channel-folders/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return patchJSON(t, folderURL(f), tok, map[string]any{"name": "Matrix Renamed"})
		}, http.StatusOK, http.StatusForbidden},
		{"DELETE /channel-folders/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return deleteReq(t, folderURL(f), tok)
		}, http.StatusOK, http.StatusForbidden},
		// CLEARING the set is the observable write: bootstrap seeds the
		// general channel as the workspace default, so "set it to the same
		// channel" would leave a row count a refused call also produces.
		{"PUT /default-channels", func(t *testing.T, f *permFixture, tok string) int {
			return putJSON(t, f.base+"/api/v1/default-channels", tok,
				map[string]any{"channel_ids": []int64{}})
		}, http.StatusOK, http.StatusForbidden},
		{"GET /default-channels", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, f.base+"/api/v1/default-channels", tok, nil)
		}, http.StatusOK, http.StatusForbidden},

		// --- storage quota (files/quota.go) ---
		{"GET /admin/storage-quota", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, f.base+"/api/v1/admin/storage-quota", tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"PUT /admin/storage-quota", func(t *testing.T, f *permFixture, tok string) int {
			return putJSON(t, f.base+"/api/v1/admin/storage-quota", tok,
				map[string]any{"max_bytes": 1 << 30})
		}, http.StatusOK, http.StatusForbidden},

		// --- link previews (unfurl/unfurl.go) ---
		{"GET /admin/link-previews", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, f.base+"/api/v1/admin/link-previews", tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"PUT /admin/link-previews", func(t *testing.T, f *permFixture, tok string) int {
			return putJSON(t, f.base+"/api/v1/admin/link-previews", tok,
				map[string]any{"enabled": false})
		}, http.StatusOK, http.StatusForbidden},

		// --- auth providers (identity/oidc_admin.go) ---
		{"POST /admin/auth-providers", func(t *testing.T, f *permFixture, tok string) int {
			return postJSONStatus(t, f.base+"/api/v1/admin/auth-providers", tok,
				map[string]any{"name": permProviderName, "issuer": permIssuer,
					"client_id": "matrix-client", "client_secret": "matrix-secret"})
		}, http.StatusCreated, http.StatusForbidden},
		{"GET /admin/auth-providers", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, f.base+"/api/v1/admin/auth-providers", tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		// No enabled:true — that would fire the discovery probe (422 with no
		// IdP listening) and stop measuring the gate.
		{"PATCH /admin/auth-providers/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return patchJSON(t, providerURL(f), tok, map[string]any{"client_id": "rotated-client"})
		}, http.StatusOK, http.StatusForbidden},
		{"DELETE /admin/auth-providers/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return deleteReq(t, providerURL(f), tok)
		}, http.StatusOK, http.StatusForbidden},

		// --- verb reassignment (identity/admin.go) ---
		// compliance_officer is the inert target: never seeded, checked by
		// nothing this table touches, so the write has an observable effect
		// (a row + an event) without moving any other cell's answer.
		{"PUT /admin/verbs", func(t *testing.T, f *permFixture, tok string) int {
			return putJSON(t, f.base+"/api/v1/admin/verbs", tok,
				map[string]any{"verb": perms.VerbComplianceOfficer, "group": perms.GroupOwners})
		}, http.StatusOK, http.StatusForbidden},

		// --- automations: one gate, seven endpoints (automation.go
		// requireScopeAdmin at org scope) ---
		{"POST /automations", func(t *testing.T, f *permFixture, tok string) int {
			return postJSONStatus(t, f.base+"/api/v1/automations", tok, map[string]any{
				"scope_type": 1, "scope_id": f.orgID, "name": "matrix-created",
				"definition": map[string]any{
					"trigger": map[string]any{"verb": "message.created"},
					"steps": []any{map[string]any{"kind": "post_message",
						"channel_id": f.channelID, "content": "matrix"}},
				}})
		}, http.StatusCreated, http.StatusForbidden},
		{"GET /automations", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, fmt.Sprintf("%s/api/v1/automations?scope_type=1&scope_id=%d",
				f.base, f.orgID), tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"PATCH /automations/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return patchJSON(t, autoURL(f, ""), tok, map[string]any{"name": "matrix-renamed"})
		}, http.StatusOK, http.StatusForbidden},
		{"GET /automations/{id}/runs", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, autoURL(f, "/runs"), tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"GET /automations/{id}/deliveries", func(t *testing.T, f *permFixture, tok string) int {
			return getJSON(t, autoURL(f, "/deliveries"), tok, nil)
		}, http.StatusOK, http.StatusForbidden},
		{"POST /automations/{id}/webhook-token", func(t *testing.T, f *permFixture, tok string) int {
			return postJSONStatus(t, autoURL(f, "/webhook-token"), tok, map[string]any{})
		}, http.StatusOK, http.StatusForbidden},
		{"DELETE /automations/{id}", func(t *testing.T, f *permFixture, tok string) int {
			return deleteReq(t, autoURL(f, ""), tok)
		}, http.StatusOK, http.StatusForbidden},
	}
}

// permMatrixServer wires every service the 24 endpoints reach. The routes
// register unconditionally and the handlers have no nil guards, so a missing
// dependency is a panic, not a skip.
func permMatrixServer(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *httptest.Server {
	t.Helper()
	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	filesSvc := files.New(pool, store)
	filesSvc.SetPerms(permsSvc)
	unfurlSvc := unfurl.New(pool, egress.New(egress.Options{}))
	unfurlSvc.SetPerms(permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:    identity.New(pool, permsSvc),
		Messaging:   messaging.New(pool, permsSvc),
		Files:       filesSvc,
		Unfurl:      unfurlSvc,
		Automations: automation.New(pool, permsSvc),
	}))
	t.Cleanup(ts.Close)
	return ts
}

// newPermFixture bootstraps an org and mints the folder / auth provider /
// webhook rule the update-and-delete cells act on, all as the owner and all
// before any assignment is touched. Returns the fixture and the owner token.
func newPermFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	base, slug, email string) (*permFixture, string) {
	t.Helper()
	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, base+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": slug, "email": email, "password": "password123",
		"full_name": "Alice Owner",
	}, &boot)
	f := &permFixture{base: base, orgID: boot.OrgID, channelID: boot.ChannelID}
	if err := pool.QueryRow(ctx,
		`SELECT id FROM workspace WHERE org_id = $1 ORDER BY id LIMIT 1`,
		f.orgID).Scan(&f.workspaceID); err != nil {
		t.Fatalf("workspace: %v", err)
	}

	var folder messaging.Folder
	postJSON(t, base+"/api/v1/channel-folders", boot.Token,
		map[string]any{"name": "Fixture Folder"}, &folder)
	f.folderID = folder.ID

	var provider identity.AuthProvider
	postJSON(t, base+"/api/v1/admin/auth-providers", boot.Token, map[string]any{
		"name": "fixture-idp", "issuer": permIssuer,
		"client_id": "fixture-client", "client_secret": "fixture-secret",
	}, &provider)
	f.providerID = provider.ID

	// A WEBHOOK trigger: the rotate-token cell 400s on any other kind, which
	// would mask its gate.
	var rule automation.Automation
	postJSON(t, base+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 1, "scope_id": f.orgID, "name": "fixture-rule",
		"definition": map[string]any{
			"trigger": map[string]any{"kind": "webhook"},
			"steps": []any{map[string]any{"kind": "post_message",
				"channel_id": f.channelID, "content": "fixture"}},
		}}, &rule)
	f.autoID = rule.ID

	if f.folderID == 0 || f.providerID == 0 || f.autoID == 0 {
		t.Fatalf("fixture incomplete: %+v", f)
	}
	return f, boot.Token
}

func permMaxEventID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(max(id), 0) FROM event_log WHERE org_id = $1`, orgID).Scan(&id); err != nil {
		t.Fatalf("event high-water: %v", err)
	}
	return id
}

// runPermMatrix issues all 24 requests as token and asserts the hardcoded
// status for each, then asserts the STATE the pass must (or must not) have
// left: one event per writing endpoint and the default-channel row, or none
// of either. Each org supports at most ONE allowed pass — the pass consumes
// its fixtures.
func runPermMatrix(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	f *permFixture, token string, allowed bool) {
	t.Helper()
	since := permMaxEventID(t, ctx, pool, f.orgID)
	for _, c := range permMatrix() {
		want := c.wantDenied
		if allowed {
			want = c.wantAllowed
		}
		if got := c.run(t, f, token); got != want {
			t.Errorf("[allowed=%v] %s = %d, want %d", allowed, c.name, got, want)
		}
	}

	wantEvents := 0
	if allowed {
		wantEvents = 1
	}
	for _, verb := range permWriteVerbs {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM event_log
			WHERE org_id = $1 AND id > $2 AND verb = $3`,
			f.orgID, since, verb).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", verb, err)
		}
		if n != wantEvents {
			t.Errorf("[allowed=%v] %s events = %d, want %d", allowed, verb, n, wantEvents)
		}
	}
	// PUT /default-channels is the one write with no event of its own. It
	// CLEARS the set, so bootstrap's seeded row is gone iff the call landed.
	wantDefaults := 1
	if allowed {
		wantDefaults = 0
	}
	var defaults int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM default_channel WHERE workspace_id = $1 AND bundle IS NULL`,
		f.workspaceID).Scan(&defaults); err != nil {
		t.Fatalf("count default channels: %v", err)
	}
	if defaults != wantDefaults {
		t.Errorf("[allowed=%v] default_channel rows = %d, want %d", allowed, defaults, wantDefaults)
	}
}

// TestManageOrgPermissionMatrix pins the whole org-administration surface at
// once: a plain member is refused all 24 endpoints with a 403 and leaves no
// trace, and an admin passes all 24 and leaves exactly the expected write
// trail. The same table must answer identically once the umbrella verb is
// split into its ADR-006 siblings (P-47) — that is what this table is for.
func TestManageOrgPermissionMatrix(t *testing.T) {
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

	ts := permMatrixServer(t, ctx, pool)
	f, ownerTok := newPermFixture(t, ctx, pool, ts.URL, "mtx", "alice@mtx.test")
	memberTok := addChannelMember(t, ctx, pool, f.orgID, f.channelID,
		"bob@mtx.test", "Bob Member", "bobmtxtok")

	// Denied first: a refused pass changes nothing, so the fixtures survive
	// for the allowed pass that follows.
	runPermMatrix(t, ctx, pool, f, memberTok, false)
	runPermMatrix(t, ctx, pool, f, ownerTok, true)
}
