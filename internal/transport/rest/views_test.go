package rest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
)

// TestSavedViews proves personal saved-view CRUD: create with a folded
// org-local space_id and a validated filter query, list/get, patch with
// re-validation, the write-time 400s (bad field/op/layout/name, non-org space
// 404), and owner isolation (another member gets an oracle-free 404 and never
// sees the view in their list), then delete.
func TestSavedViews(t *testing.T) {
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
	ts, boot := boardSetup(t, ctx, pool, "views")

	space := createSpace(t, ts.URL, boot.Token, "VIEW", "Views")

	validQuery := map[string]any{"filters": []map[string]any{
		{"field": "status_id", "op": "eq", "value": 5},
		{"field": "label", "op": "in", "value": []string{"urgent", "p0"}},
	}}
	var view worktrack.ViewSummary
	if code := postJSONStatus2(t, ts.URL+"/api/v1/views", boot.Token, map[string]any{
		"name": "My Kanban", "layout": 2, "space_id": space, "query": validQuery,
	}, &view); code != http.StatusCreated {
		t.Fatalf("create view: %d, want 201", code)
	}
	if view.ID == 0 || view.SpaceID != space || view.Layout != 2 || view.OwnerID == 0 {
		t.Fatalf("create view result wrong: %+v", view)
	}

	// List and get return the caller's own view with the folded space_id.
	var list struct {
		Views []worktrack.ViewSummary `json:"views"`
	}
	getJSON(t, ts.URL+"/api/v1/views", boot.Token, &list)
	if len(list.Views) != 1 || list.Views[0].ID != view.ID {
		t.Fatalf("list views = %+v, want the one view", list.Views)
	}
	var got worktrack.ViewSummary
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token, &got); code != 200 {
		t.Fatalf("get view: %d", code)
	}
	if got.SpaceID != space || len(got.Query) == 0 {
		t.Fatalf("get view space/query wrong: %+v", got)
	}

	// Patch name + layout, then a valid query; a re-query reflects them and
	// keeps the folded space_id (space is not changeable via PATCH).
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token,
		map[string]any{"name": "Renamed", "layout": 1}); code != 200 {
		t.Fatalf("patch view: %d", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token,
		map[string]any{"query": map[string]any{"filters": []map[string]any{
			{"field": "assignee_id", "op": "eq", "value": 1}}}}); code != 200 {
		t.Fatalf("patch query: %d", code)
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token, &got)
	if got.Name != "Renamed" || got.Layout != 1 || got.SpaceID != space {
		t.Fatalf("patched view wrong: %+v", got)
	}

	// Write-time validation 400s (POST): bad field, bad op, bad layout, empty name.
	badPosts := []map[string]any{
		{"name": "x", "layout": 2, "space_id": space, "query": map[string]any{"filters": []map[string]any{{"field": "bogus", "op": "eq", "value": 1}}}},
		{"name": "x", "layout": 2, "space_id": space, "query": map[string]any{"filters": []map[string]any{{"field": "status_id", "op": "bogus", "value": 1}}}},
		{"name": "x", "layout": 9, "space_id": space, "query": validQuery},
		{"name": "", "layout": 2, "space_id": space, "query": validQuery},
	}
	for i, body := range badPosts {
		if code := postJSONStatus(t, ts.URL+"/api/v1/views", boot.Token, body); code != http.StatusBadRequest {
			t.Fatalf("bad POST %d: %d, want 400", i, code)
		}
	}
	// A non-org-local space is a 404.
	if code := postJSONStatus(t, ts.URL+"/api/v1/views", boot.Token, map[string]any{
		"name": "x", "layout": 2, "space_id": int64(999999), "query": validQuery,
	}); code != http.StatusNotFound {
		t.Fatalf("non-org space: %d, want 404", code)
	}
	// Validation also applies to PATCH.
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token,
		map[string]any{"query": map[string]any{"filters": []map[string]any{{"field": "nope", "op": "eq", "value": 1}}}}); code != http.StatusBadRequest {
		t.Fatalf("bad PATCH query: %d, want 400", code)
	}

	// Owner isolation: a second member sees an oracle-free 404 and an empty list.
	bob := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID, "bob@views.test", "Bob Ray", "bobviewtok")
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), bob, nil); code != http.StatusNotFound {
		t.Fatalf("bob GET alice view: %d, want 404", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), bob,
		map[string]any{"name": "hijack"}); code != http.StatusNotFound {
		t.Fatalf("bob PATCH alice view: %d, want 404", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), bob); code != http.StatusNotFound {
		t.Fatalf("bob DELETE alice view: %d, want 404", code)
	}
	var bobList struct {
		Views []worktrack.ViewSummary `json:"views"`
	}
	getJSON(t, ts.URL+"/api/v1/views", bob, &bobList)
	if len(bobList.Views) != 0 {
		t.Fatalf("bob sees %d views, want 0", len(bobList.Views))
	}

	// Owner deletes; it is then gone (404 on re-get and re-delete).
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token); code != 200 {
		t.Fatalf("delete view: %d", code)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token, nil); code != http.StatusNotFound {
		t.Fatalf("get deleted view: %d, want 404", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/views/%d", ts.URL, view.ID), boot.Token); code != http.StatusNotFound {
		t.Fatalf("re-delete view: %d, want 404", code)
	}
}
