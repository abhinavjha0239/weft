package rest

import (
	"context"
	"encoding/json"
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

// worktrackServer spins up the minimal API surface (identity + messaging +
// worktrack) that the custom-field and link tests exercise, plus a live pool.
func worktrackServer(t *testing.T) (*httptest.Server, *pgxpool.Pool, context.Context) {
	t.Helper()
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
	t.Cleanup(func() { cancel(); pool.Close() })
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
	t.Cleanup(ts.Close)
	return ts, pool, ctx
}

// TestCustomFields proves the ADR-009 W-2 field system end-to-end: typed def
// CRUD (dup-key 409, options + applies_to validation, immutable key/type),
// per-item value validation woven into create/update, the merge/clear
// semantics, the GIN JSONB round-trip, changed-keys-only events, and that
// deleting a def leaves stored values as inert orphans.
func TestCustomFields(t *testing.T) {
	ts, pool, ctx := worktrackServer(t)

	var boot struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "cf", "email": "a@cf.test", "password": "password123",
		"full_name": "Ada",
	}, &boot)

	var space struct {
		ID  int64  `json:"id"`
		Key string `json:"key"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "cf", "name": "Custom Fields"}, &space)
	fdURL := fmt.Sprintf("%s/api/v1/spaces/%d/field-defs", ts.URL, space.ID)

	// A number field and a select field land.
	var pointsDef struct {
		ID       int64 `json:"id"`
		Position int   `json:"position"`
	}
	if code := postJSONStatus2(t, fdURL, boot.Token, map[string]any{
		"key": "points", "name": "Story Points", "field_type": "number",
	}, &pointsDef); code != http.StatusCreated {
		t.Fatalf("create number field: %d", code)
	}
	if code := postJSONStatus2(t, fdURL, boot.Token, map[string]any{
		"key": "priority", "name": "Priority", "field_type": "select",
		"options": map[string]any{"choices": []string{"low", "high"}},
	}, nil); code != http.StatusCreated {
		t.Fatalf("create select field: %d", code)
	}

	// Def-creation negatives, none of which persist.
	for _, c := range []struct {
		name string
		body map[string]any
		want int
	}{
		{"dup key", map[string]any{"key": "points", "name": "Dup", "field_type": "text_short"}, http.StatusConflict},
		{"select without choices", map[string]any{"key": "nochoice", "name": "Bad", "field_type": "select"}, http.StatusBadRequest},
		{"select empty choices", map[string]any{"key": "nochoice", "name": "Bad", "field_type": "multi_select", "options": map[string]any{"choices": []string{}}}, http.StatusBadRequest},
		{"unsupported taxonomy type", map[string]any{"key": "link", "name": "URL", "field_type": "url"}, http.StatusBadRequest},
		{"unknown type name", map[string]any{"key": "weird", "name": "W", "field_type": "banana"}, http.StatusBadRequest},
		{"bad key", map[string]any{"key": "BadKey", "name": "N", "field_type": "text_short"}, http.StatusBadRequest},
	} {
		if code := postJSONStatus(t, fdURL, boot.Token, c.body); code != c.want {
			t.Fatalf("%s: %d, want %d", c.name, code, c.want)
		}
	}

	// applies_to must name the space's OWN item types.
	var ownType, foreignType int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM item_type WHERE space_id = $1 ORDER BY id LIMIT 1`, space.ID).Scan(&ownType); err != nil {
		t.Fatalf("own item type: %v", err)
	}
	var space2 struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "cf2", "name": "Other"}, &space2)
	if err := pool.QueryRow(ctx,
		`SELECT id FROM item_type WHERE space_id = $1 ORDER BY id LIMIT 1`, space2.ID).Scan(&foreignType); err != nil {
		t.Fatalf("foreign item type: %v", err)
	}
	if code := postJSONStatus(t, fdURL, boot.Token, map[string]any{
		"key": "scoped", "name": "Scoped", "field_type": "text_short",
		"applies_to": []int64{ownType},
	}); code != http.StatusCreated {
		t.Fatalf("applies_to own type: %d, want 201", code)
	}
	if code := postJSONStatus(t, fdURL, boot.Token, map[string]any{
		"key": "badscope", "name": "Bad", "field_type": "text_short",
		"applies_to": []int64{foreignType},
	}); code != http.StatusBadRequest {
		t.Fatalf("applies_to foreign type: %d, want 400", code)
	}

	// GET orders by position, id: points, priority, scoped.
	var defs struct {
		FieldDefs []worktrack.FieldDef `json:"field_defs"`
	}
	getJSON(t, fdURL, boot.Token, &defs)
	if len(defs.FieldDefs) != 3 {
		t.Fatalf("field defs = %d, want 3", len(defs.FieldDefs))
	}
	if defs.FieldDefs[0].Key != "points" || defs.FieldDefs[0].FieldType != "number" {
		t.Fatalf("first def wrong: %+v", defs.FieldDefs[0])
	}

	// Create an item carrying custom values.
	itemsURL := fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID)
	var item struct {
		ID     int64          `json:"id"`
		Key    string         `json:"key"`
		Fields map[string]any `json:"fields"`
	}
	postJSON(t, itemsURL, boot.Token, map[string]any{
		"title":  "Do the thing",
		"fields": map[string]any{"points": 5, "priority": "high"},
	}, &item)
	if item.Fields["priority"] != "high" || item.Fields["points"].(float64) != 5 {
		t.Fatalf("create item fields wrong: %+v", item.Fields)
	}

	// Value-validation negatives at create (none persist).
	for _, c := range []struct {
		name   string
		fields map[string]any
	}{
		{"unknown key", map[string]any{"nope": 1}},
		{"string into number", map[string]any{"points": "five"}},
		{"non-choice into select", map[string]any{"priority": "urgent"}},
	} {
		if code := postJSONStatus(t, itemsURL, boot.Token, map[string]any{
			"title": "bad", "fields": c.fields,
		}); code != http.StatusBadRequest {
			t.Fatalf("create %s: %d, want 400", c.name, code)
		}
	}

	// Round-trip through the GIN-indexed JSONB column, both via the API and a
	// literal containment query against the index.
	items := listItems(t, ts.URL, boot.Token, space.ID)
	if it := items[item.Key]; it.Fields["priority"] != "high" || it.Fields["points"].(float64) != 5 {
		t.Fatalf("field round-trip via list wrong: %+v", it.Fields)
	}
	var hit int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM work_item WHERE space_id = $1 AND fields @> '{"priority":"high"}'`,
		space.ID).Scan(&hit); err != nil || hit != item.ID {
		t.Fatalf("GIN containment: id=%d err=%v, want %d", hit, err, item.ID)
	}

	// Update: merge sets points, explicit null clears priority.
	itemURL := fmt.Sprintf("%s/api/v1/items/%d", ts.URL, item.ID)
	if code := patchJSON(t, itemURL, boot.Token, map[string]any{
		"fields": map[string]any{"points": 8, "priority": nil},
	}); code != http.StatusOK {
		t.Fatalf("merge/clear update: %d", code)
	}
	items = listItems(t, ts.URL, boot.Token, space.ID)
	if it := items[item.Key]; it.Fields["points"].(float64) != 8 {
		t.Fatalf("merge set wrong: %+v", it.Fields)
	}
	if _, present := items[item.Key].Fields["priority"]; present {
		t.Fatalf("priority should be cleared: %+v", items[item.Key].Fields)
	}
	// Value-validation negatives at update.
	if code := patchJSON(t, itemURL, boot.Token, map[string]any{"fields": map[string]any{"nope": 1}}); code != http.StatusBadRequest {
		t.Fatalf("unknown key on update: %d, want 400", code)
	}
	if code := patchJSON(t, itemURL, boot.Token, map[string]any{"fields": map[string]any{"points": "eight"}}); code != http.StatusBadRequest {
		t.Fatalf("string into number on update: %d, want 400", code)
	}

	// The workitem.updated payload lists changed KEYS only, never values.
	var raw string
	if err := pool.QueryRow(ctx, `
		SELECT payload::text FROM event_log
		WHERE org_id = $1 AND verb = 'workitem.updated' AND entity_id = $2
		ORDER BY id DESC LIMIT 1`, boot.OrgID, item.ID).Scan(&raw); err != nil {
		t.Fatalf("workitem.updated event: %v", err)
	}
	var pl struct {
		Fields []string `json:"fields"`
	}
	if err := json.Unmarshal([]byte(raw), &pl); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(pl.Fields) != 2 || pl.Fields[0] != "points" || pl.Fields[1] != "priority" {
		t.Fatalf("changed keys = %v, want [points priority]", pl.Fields)
	}
	if contains(raw, "eight") || contains(raw, "high") {
		t.Fatalf("payload leaked a value: %s", raw)
	}

	// PATCH mutates name/required; key + field_type are immutable (ignored).
	priDefID := defs.FieldDefs[1].ID
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/field-defs/%d", ts.URL, priDefID), boot.Token,
		map[string]any{"name": "Prio", "required": true, "key": "hacked", "field_type": "number"}); code != http.StatusOK {
		t.Fatalf("patch field def: %d", code)
	}
	getJSON(t, fdURL, boot.Token, &defs)
	var pri worktrack.FieldDef
	for _, d := range defs.FieldDefs {
		if d.ID == priDefID {
			pri = d
		}
	}
	if pri.Name != "Prio" || !pri.Required || pri.Key != "priority" || pri.FieldType != "select" {
		t.Fatalf("immutable key/type or mutable name/required wrong: %+v", pri)
	}

	// DELETE removes the def but leaves the stored value as an inert orphan.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/field-defs/%d", ts.URL, pointsDef.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("delete field def: %d", code)
	}
	getJSON(t, fdURL, boot.Token, &defs)
	if len(defs.FieldDefs) != 2 {
		t.Fatalf("after delete field defs = %d, want 2", len(defs.FieldDefs))
	}
	items = listItems(t, ts.URL, boot.Token, space.ID)
	if it := items[item.Key]; it.Fields["points"].(float64) != 8 {
		t.Fatalf("deleted def should leave orphan value: %+v", it.Fields)
	}
}

// TestItemLinks proves ADR-008 C-6 item links end-to-end: one row rendered
// both ways (outward/inward), self-link 400, duplicate 409, links surviving a
// status/rank change on either item (keyed by internal id), unlink from either
// endpoint, cross-org 404s, and the linked/unlinked events.
func TestItemLinks(t *testing.T) {
	ts, pool, ctx := worktrackServer(t)

	var boot struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "lk", "email": "a@lk.test", "password": "password123",
		"full_name": "Lin",
	}, &boot)
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "lk", "name": "Links"}, &space)
	itemsURL := fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID)

	mkItem := func(title string) (int64, string) {
		var it struct {
			ID  int64  `json:"id"`
			Key string `json:"key"`
		}
		postJSON(t, itemsURL, boot.Token, map[string]any{"title": title}, &it)
		return it.ID, it.Key
	}
	itemA, _ := mkItem("A")
	itemB, keyB := mkItem("B")
	itemC, _ := mkItem("C")

	linkTypeID := func(orgID int64, outward string) int64 {
		var id int64
		if err := pool.QueryRow(ctx,
			`SELECT id FROM link_type WHERE org_id = $1 AND outward = $2`, orgID, outward).Scan(&id); err != nil {
			t.Fatalf("link type %q: %v", outward, err)
		}
		return id
	}
	blocks := linkTypeID(boot.OrgID, "blocks")
	relates := linkTypeID(boot.OrgID, "relates to")

	linksURL := func(itemID int64) string {
		return fmt.Sprintf("%s/api/v1/items/%d/links", ts.URL, itemID)
	}

	// Self-link 400.
	if code := postJSONStatus(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": itemA, "link_type_id": blocks}); code != http.StatusBadRequest {
		t.Fatalf("self-link: %d, want 400", code)
	}
	// A blocks B.
	var link struct {
		ID int64 `json:"id"`
	}
	if code := postJSONStatus2(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": itemB, "link_type_id": blocks}, &link); code != http.StatusCreated {
		t.Fatalf("create link: %d, want 201", code)
	}
	// Duplicate 409.
	if code := postJSONStatus(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": itemB, "link_type_id": blocks}); code != http.StatusConflict {
		t.Fatalf("dup link: %d, want 409", code)
	}

	type linkResp struct {
		Links []worktrack.LinkView `json:"links"`
	}
	// A's side renders the outward phrase toward B.
	var la linkResp
	getJSON(t, linksURL(itemA), boot.Token, &la)
	if len(la.Links) != 1 || la.Links[0].Phrase != "blocks" || la.Links[0].Item.ID != itemB ||
		la.Links[0].Item.Key != keyB || la.Links[0].Item.Status != "To Do" {
		t.Fatalf("A links wrong: %+v", la.Links)
	}
	// B's side renders the inward phrase toward A — the SAME single row.
	var lb linkResp
	getJSON(t, linksURL(itemB), boot.Token, &lb)
	if len(lb.Links) != 1 || lb.Links[0].Phrase != "is blocked by" || lb.Links[0].Item.ID != itemA {
		t.Fatalf("B links wrong: %+v", lb.Links)
	}

	// The link survives a status change AND a rank change on B (keyed by id).
	var sts struct {
		Statuses []worktrack.Status `json:"statuses"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/statuses", ts.URL, space.ID), boot.Token, &sts)
	var inProgress int64
	for _, s := range sts.Statuses {
		if s.Category == 2 {
			inProgress = s.ID
		}
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/items/%d", ts.URL, itemB), boot.Token,
		map[string]any{"status_id": inProgress}); code != http.StatusOK {
		t.Fatalf("transition B: %d", code)
	}
	if _, err := pool.Exec(ctx, `UPDATE work_item SET rank = 'zzzzzzzz' WHERE id = $1`, itemB); err != nil {
		t.Fatalf("rank change: %v", err)
	}
	getJSON(t, linksURL(itemA), boot.Token, &la)
	if len(la.Links) != 1 || la.Links[0].Item.ID != itemB || la.Links[0].Item.Status != "In Progress" {
		t.Fatalf("link did not survive item change (live status): %+v", la.Links)
	}

	// Symmetric type renders "relates to" from both sides.
	if code := postJSONStatus(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": itemC, "link_type_id": relates}); code != http.StatusCreated {
		t.Fatalf("relates link: %d", code)
	}
	var lc linkResp
	getJSON(t, linksURL(itemC), boot.Token, &lc)
	if len(lc.Links) != 1 || lc.Links[0].Phrase != "relates to" || lc.Links[0].Item.ID != itemA {
		t.Fatalf("C relates link wrong: %+v", lc.Links)
	}

	// Cross-org isolation: a second org's items and link types are invisible.
	var boot2 struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "lk2", "email": "a@lk2.test", "password": "password123",
		"full_name": "Otto",
	}, &boot2)
	var space2 struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot2.Token,
		map[string]any{"key": "lk2", "name": "Other Links"}, &space2)
	var foreignItem struct {
		ID int64 `json:"id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space2.ID), boot2.Token,
		map[string]any{"title": "X"}, &foreignItem)
	foreignBlocks := linkTypeID(boot2.OrgID, "blocks")

	if code := postJSONStatus(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": foreignItem.ID, "link_type_id": blocks}); code != http.StatusNotFound {
		t.Fatalf("link to foreign item: %d, want 404", code)
	}
	if code := postJSONStatus(t, linksURL(itemA), boot.Token,
		map[string]any{"to_item_id": itemB, "link_type_id": foreignBlocks}); code != http.StatusNotFound {
		t.Fatalf("link with foreign type: %d, want 404", code)
	}
	if code := getJSON(t, linksURL(foreignItem.ID), boot.Token, &la); code != http.StatusNotFound {
		t.Fatalf("list foreign item links: %d, want 404", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/items/%d/links/999999", ts.URL, itemA), boot.Token); code != http.StatusNotFound {
		t.Fatalf("delete nonexistent link: %d, want 404", code)
	}

	// Unlink from the OTHER endpoint (B's), though the row is A→B.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/items/%d/links/%d", ts.URL, itemB, link.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("unlink from target endpoint: %d", code)
	}
	getJSON(t, linksURL(itemA), boot.Token, &la)
	if len(la.Links) != 1 || la.Links[0].Phrase != "relates to" {
		t.Fatalf("after unlink A should keep only the relates link: %+v", la.Links)
	}

	// The linked + unlinked events landed.
	var linked, unlinked int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'workitem.linked'`,
		boot.OrgID).Scan(&linked); err != nil {
		t.Fatalf("linked count: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'workitem.unlinked'`,
		boot.OrgID).Scan(&unlinked); err != nil {
		t.Fatalf("unlinked count: %v", err)
	}
	if linked != 2 || unlinked != 1 {
		t.Fatalf("events: linked=%d unlinked=%d, want 2 and 1", linked, unlinked)
	}
}
