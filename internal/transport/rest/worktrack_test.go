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
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestWorkItemFusion proves the founding thesis (ADR-001 D2) end-to-end:
// a chat thread becomes a work item (its messages ARE the item's discussion),
// commenting on the item is replying in the same thread, and transitioning to
// Done DERIVES resolution (W-3) — then reopening clears it.
func TestWorkItemFusion(t *testing.T) {
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
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "fuse", "email": "a@f.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// A Space with seeded workflow.
	var space struct {
		ID  int64  `json:"id"`
		Key string `json:"key"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "fuse", "name": "Fusion Build"}, &space)
	if space.Key != "FUSE" {
		t.Fatalf("space key = %q, want normalized FUSE", space.Key)
	}
	var sts struct {
		Statuses []worktrack.Status `json:"statuses"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/statuses", ts.URL, space.ID), boot.Token, &sts)
	if len(sts.Statuses) != 3 {
		t.Fatalf("seeded statuses = %d, want 3", len(sts.Statuses))
	}
	var doneID, todoID int64
	for _, st := range sts.Statuses {
		if st.Category == 3 {
			doneID = st.ID
		}
		if st.Category == 1 {
			todoID = st.ID
		}
	}

	// Direct item creation: description becomes the thread's root message.
	var task struct {
		ID       int64  `json:"id"`
		Key      string `json:"key"`
		ThreadID int64  `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID), boot.Token,
		map[string]any{"title": "Ship the fusion", "description": "the **why** lives here",
			"type": "Bug"}, &task)
	if task.Key != "FUSE-1" {
		t.Fatalf("first item key = %q, want FUSE-1", task.Key)
	}
	var desc struct {
		Messages []struct {
			Rendered string `json:"rendered"`
		} `json:"messages"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages?limit=10", ts.URL, task.ThreadID),
		boot.Token, &desc)
	if len(desc.Messages) != 1 || !contains(desc.Messages[0].Rendered, "<strong>why</strong>") {
		t.Fatalf("description-as-root-message wrong: %+v", desc.Messages)
	}
	// Commenting on the item = posting to its (space-governed) thread.
	var comment struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, task.ThreadID),
		boot.Token, map[string]any{"content": "triage note"}, &comment)
	// Space-thread messages are org-visible: a user with NO channel in common
	// with the message can still fetch it by id (the membership-join Get
	// used to 404 exactly this).
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@f.test", "Bob Ray", "bobfusetok")
	var fetched struct {
		ChannelID int64  `json:"channel_id"`
		ThreadID  int64  `json:"thread_id"`
		Source    string `json:"source"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, comment.MessageID),
		bobTok, &fetched); code != http.StatusOK {
		t.Fatalf("space-thread message fetch by org member: %d, want 200", code)
	}
	if fetched.ChannelID != 0 || fetched.ThreadID != task.ThreadID || fetched.Source != "triage note" {
		t.Fatalf("space-thread message fetch wrong: %+v", fetched)
	}

	// THE FUSION: a chat thread in #general → promote → WEFT-2, same thread.
	var th struct {
		ThreadID int64 `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, boot.ChannelID), boot.Token,
		map[string]any{"title": "prod is on fire", "content": "errors spiking after deploy"}, &th)
	var promoted struct {
		ID       int64  `json:"id"`
		Key      string `json:"key"`
		ThreadID int64  `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, th.ThreadID), boot.Token,
		map[string]any{"space_id": space.ID, "type": "Bug"}, &promoted)
	if promoted.Key != "FUSE-2" || promoted.ThreadID != th.ThreadID {
		t.Fatalf("promotion wrong: %+v (thread must be THE SAME)", promoted)
	}
	// A chat reply in the channel thread is now also the item's discussion.
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID), boot.Token,
		map[string]any{"thread_id": th.ThreadID, "content": "rolled back, monitoring"}, nil)
	var disc struct {
		Messages []struct {
			Source string `json:"source"`
		} `json:"messages"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages?limit=10", ts.URL, promoted.ThreadID),
		boot.Token, &disc)
	if len(disc.Messages) != 2 || disc.Messages[0].Source != "rolled back, monitoring" {
		t.Fatalf("fused discussion = %+v, want chat reply visible via the item's thread", disc.Messages)
	}
	// Double promotion is rejected.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, th.ThreadID),
		boot.Token, map[string]any{"space_id": space.ID}); code != http.StatusConflict {
		t.Fatalf("double promote: %d, want 409", code)
	}

	// W-3: transition to Done derives resolution; back to To Do clears it.
	itemURL := fmt.Sprintf("%s/api/v1/items/%d", ts.URL, promoted.ID)
	if code := patchJSON(t, itemURL, boot.Token, map[string]any{"status_id": doneID}); code != 200 {
		t.Fatalf("transition: %d", code)
	}
	items := listItems(t, ts.URL, boot.Token, space.ID)
	if it := items[promoted.Key]; !it.Resolved || it.StatusCategory != 3 {
		t.Fatalf("done item not derived-resolved: %+v", it)
	}
	if code := patchJSON(t, itemURL, boot.Token, map[string]any{"status_id": todoID}); code != 200 {
		t.Fatalf("reopen: %d", code)
	}
	items = listItems(t, ts.URL, boot.Token, space.ID)
	if it := items[promoted.Key]; it.Resolved {
		t.Fatalf("reopened item still resolved: %+v", it)
	}
	// Cross-space status ids are rejected (workflow integrity).
	var other struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "OPS", "name": "Ops"}, &other)
	var otherSts struct {
		Statuses []worktrack.Status `json:"statuses"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/statuses", ts.URL, other.ID), boot.Token, &otherSts)
	if code := patchJSON(t, itemURL, boot.Token,
		map[string]any{"status_id": otherSts.Statuses[0].ID}); code != http.StatusBadRequest {
		t.Fatalf("cross-space status: %d, want 400", code)
	}

	// Duplicate space key → 409 (keys are never reused).
	if code := postJSONStatus(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "FUSE", "name": "Again"}); code != http.StatusConflict {
		t.Fatalf("duplicate key: %d, want 409", code)
	}
}

func listItems(t *testing.T, base, token string, spaceID int64) map[string]worktrack.Item {
	t.Helper()
	var resp struct {
		Items []worktrack.Item `json:"items"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", base, spaceID), token, &resp); code != 200 {
		t.Fatalf("list items: %d", code)
	}
	out := map[string]worktrack.Item{}
	for _, it := range resp.Items {
		out[it.Key] = it
	}
	return out
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestPromoteThreadMask: the recorded P-34 residual, closed — promotion must
// never confirm a hidden thread exists. Every answer about a thread in a
// private channel the prober cannot see (the membership 403, the root-thread
// 400, the already-promoted 409) collapses to ONE 404, byte-identical to a
// nonexistent id; a public channel's non-member keeps the honest 403; a
// member's promote is untouched. RED/GREEN: flip the mask's private branch
// back to Forbidden (red at the hidden-discussion probe) or move the shape
// 400s back above the access check (red at the hidden-root probe).
func TestPromoteThreadMask(t *testing.T) {
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
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "pmk", "email": "a@pmk.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@pmk.test", "Bob Ray", "bobpmktok")
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "pmk", "name": "Mask"}, &space)

	// Alice's private channel holds three probe shapes a non-member must not
	// distinguish: a plain discussion (pre-mask: 403), the channel ROOT
	// (pre-mask: the kind-2 400 fired before any access check), and a thread
	// alice ALREADY promoted (pre-mask: 409 for a member — reachable order
	// leaks aside, any non-404 confirms existence).
	var vault struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "visibility": "private"}, &vault)
	if vault.ChannelID == 0 {
		t.Fatal("private channel create failed")
	}
	mkThread := func(channelID int64, title string) int64 {
		t.Helper()
		var th struct {
			ThreadID int64 `json:"thread_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, channelID),
			boot.Token, map[string]any{"title": title, "content": "body"}, &th)
		if th.ThreadID == 0 {
			t.Fatalf("thread create in channel %d failed", channelID)
		}
		return th.ThreadID
	}
	hidden := mkThread(vault.ChannelID, "secret plan")
	already := mkThread(vault.ChannelID, "already tracked")
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, already),
		boot.Token, map[string]any{"space_id": space.ID}); code != http.StatusCreated {
		t.Fatalf("alice promote = %d, want 201", code)
	}
	var vaultRoot int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM thread WHERE channel_id = $1 AND kind = 2`,
		vault.ChannelID).Scan(&vaultRoot); err != nil {
		t.Fatalf("vault root thread: %v", err)
	}

	// Absent-id baseline; every hidden shape must match it byte-for-byte.
	body := map[string]any{"space_id": space.ID}
	promoteURL := func(id int64) string {
		return fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, id)
	}
	absentCode, absentBody := authRaw(t, "POST", promoteURL(999999), bobTok, body)
	if absentCode != http.StatusNotFound {
		t.Fatalf("absent promote = %d, want 404", absentCode)
	}
	probes := []struct {
		name string
		id   int64
	}{
		{"hidden discussion", hidden},
		{"hidden root", vaultRoot},
		{"hidden promoted", already},
	}
	for _, p := range probes {
		code, respBody := authRaw(t, "POST", promoteURL(p.id), bobTok, body)
		if code != absentCode || respBody != absentBody {
			t.Fatalf("%s probe = %d %q, want byte-identical to absent (%d %q) — existence oracle",
				p.name, code, respBody, absentCode, absentBody)
		}
	}

	// A PUBLIC channel's thread keeps the honest 403 for a non-member —
	// existence is org-knowable, the P-34 decision table's other branch.
	var town struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "town"}, &town)
	pub := mkThread(town.ChannelID, "open plan")
	if code := postJSONStatus(t, promoteURL(pub), bobTok, body); code != http.StatusForbidden {
		t.Fatalf("public non-member promote = %d, want 403", code)
	}

	// The mask never blocks a member: bob promotes in his own channel.
	mine := mkThread(boot.ChannelID, "bob can see this")
	if code := postJSONStatus(t, promoteURL(mine), bobTok, body); code != http.StatusCreated {
		t.Fatalf("member promote = %d, want 201", code)
	}
}
