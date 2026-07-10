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
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, task.ThreadID),
		boot.Token, map[string]any{"content": "triage note"}); code != http.StatusCreated {
		t.Fatalf("item comment: %d, want 201", code)
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
