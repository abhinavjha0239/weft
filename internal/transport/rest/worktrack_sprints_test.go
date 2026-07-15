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

// TestSprints proves the P-11 sprint lifecycle end-to-end over REST against a
// real Postgres: create (future) → start (active, one-per-space) → close
// (carry-over of unfinished items), plus item↔sprint assignment and the
// oracle-free foreign-org boundary. The `sprint` table and work_item.sprint_id
// have existed since migration 0005 with no writers; this is those writers.
func TestSprints(t *testing.T) {
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
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "spr", "email": "a@spr.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// A second org: its spaces and sprints must be oracle-free 404s from org 1.
	var boot2 struct {
		OrgID int64  `json:"org_id"`
		Token string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "spr2", "email": "b@spr2.test", "password": "password123",
		"full_name": "Bob Ray",
	}, &boot2)
	var space2 struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot2.Token,
		map[string]any{"key": "OTHER", "name": "Other Org Space"}, &space2)
	var sprintForeign struct {
		ID int64 `json:"id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, space2.ID), boot2.Token,
		map[string]any{"name": "Foreign Sprint"}, &sprintForeign)

	// Org 1's planning space.
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "SPR", "name": "Sprint Planning"}, &space)
	sprintsURL := fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, space.ID)

	// --- Create (future) + date validity ---
	createStart := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	createEnd := createStart.Add(14 * 24 * time.Hour)
	var sprint1 worktrack.SprintSummary
	postJSON(t, sprintsURL, boot.Token, map[string]any{
		"name": "Sprint 1", "goal": "ship the fusion",
		"starts_at": createStart, "ends_at": createEnd,
	}, &sprint1)
	if sprint1.State != 1 || sprint1.ID == 0 {
		t.Fatalf("created sprint = %+v, want state 1 with id", sprint1)
	}
	if sprint1.StartsAt == nil || !sprint1.StartsAt.Equal(createStart) {
		t.Fatalf("created starts_at = %v, want %v", sprint1.StartsAt, createStart)
	}
	if countEvents(t, ctx, pool, boot.OrgID, "sprint.created") != 1 {
		t.Fatalf("want one sprint.created event")
	}

	var sprint2 worktrack.SprintSummary
	postJSON(t, sprintsURL, boot.Token, map[string]any{"name": "Sprint 2"}, &sprint2)

	// Order is state ASC, id DESC. Both future here, so newest id first.
	list := listSprints(t, ts.URL, boot.Token, space.ID)
	assertSprintOrder(t, list)
	if list[0].ID != sprint2.ID {
		t.Fatalf("future sprints not id-DESC: got %d first, want %d", list[0].ID, sprint2.ID)
	}

	// Create validation 400s.
	longName := ""
	for range 101 {
		longName += "x"
	}
	longGoal := ""
	for range 2001 {
		longGoal += "y"
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"empty name", map[string]any{"name": "   "}},
		{"name too long", map[string]any{"name": longName}},
		{"goal too long", map[string]any{"name": "ok", "goal": longGoal}},
		{"ends not after starts", map[string]any{
			"name": "ok", "starts_at": createStart, "ends_at": createStart}},
	} {
		if code := postJSONStatus(t, sprintsURL, boot.Token, tc.body); code != http.StatusBadRequest {
			t.Fatalf("%s: create status %d, want 400", tc.name, code)
		}
	}
	// Foreign-org space: creating a sprint there is an oracle-free 404.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, space2.ID),
		boot.Token, map[string]any{"name": "x"}); code != http.StatusNotFound {
		t.Fatalf("create sprint in foreign space: %d, want 404", code)
	}

	// --- Start (1→2), stamping body dates; one active sprint per space ---
	startBody := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	endBody := startBody.Add(10 * 24 * time.Hour)
	sprint1Start := fmt.Sprintf("%s/api/v1/sprints/%d/start", ts.URL, sprint1.ID)
	if code := postJSONStatus(t, sprint1Start, boot.Token,
		map[string]any{"starts_at": startBody, "ends_at": endBody}); code != http.StatusOK {
		t.Fatalf("start sprint 1: %d, want 200", code)
	}
	s1 := findSprint(t, listSprints(t, ts.URL, boot.Token, space.ID), sprint1.ID)
	if s1.State != 2 {
		t.Fatalf("started sprint state = %d, want 2", s1.State)
	}
	if s1.StartsAt == nil || !s1.StartsAt.Equal(startBody) || s1.EndsAt == nil || !s1.EndsAt.Equal(endBody) {
		t.Fatalf("start did not stamp body dates: starts=%v ends=%v", s1.StartsAt, s1.EndsAt)
	}
	assertSprintOrder(t, listSprints(t, ts.URL, boot.Token, space.ID))
	if countEvents(t, ctx, pool, boot.OrgID, "sprint.started") != 1 {
		t.Fatalf("want one sprint.started event")
	}
	// Re-starting a non-future sprint is a 400.
	if code := postJSONStatus(t, sprint1Start, boot.Token, map[string]any{}); code != http.StatusBadRequest {
		t.Fatalf("restart active sprint: %d, want 400", code)
	}
	// A second active sprint in the SAME space is a 409.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/start", ts.URL, sprint2.ID),
		boot.Token, map[string]any{}); code != http.StatusConflict {
		t.Fatalf("second active sprint: %d, want 409", code)
	}
	// Foreign-org sprint start is a 404.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/start", ts.URL, sprintForeign.ID),
		boot.Token, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("start foreign sprint: %d, want 404", code)
	}

	// --- Carry-over on close (a dedicated space keeps item bookkeeping clean) ---
	var carry struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "CARRY", "name": "Carry Over"}, &carry)
	doneID := doneStatusID(t, ts.URL, boot.Token, carry.ID)

	// Sprint A (active) holds the work; C stays future and must be untouched.
	sprintA := createSprint(t, ts.URL, boot.Token, carry.ID, "A")
	sprintC := createSprint(t, ts.URL, boot.Token, carry.ID, "C")
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/start", ts.URL, sprintA),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("start sprint A: %d", code)
	}
	// starts_at defaulted to now() (no body value given).
	if a := findSprint(t, listSprints(t, ts.URL, boot.Token, carry.ID), sprintA); a.StartsAt == nil ||
		time.Since(*a.StartsAt) > 2*time.Minute {
		t.Fatalf("start without body did not stamp starts_at≈now: %v", a.StartsAt)
	}

	i1 := createItem(t, ts.URL, boot.Token, carry.ID, "unfinished 1")
	i2 := createItem(t, ts.URL, boot.Token, carry.ID, "finished")
	i3 := createItem(t, ts.URL, boot.Token, carry.ID, "unfinished 2")
	i4 := createItem(t, ts.URL, boot.Token, carry.ID, "trashed")
	i5 := createItem(t, ts.URL, boot.Token, carry.ID, "in another sprint")
	assignSprint(t, ts.URL, boot.Token, i5, sprintC)
	for _, it := range []int64{i1, i2, i3, i4} {
		assignSprint(t, ts.URL, boot.Token, it, sprintA)
	}
	// i2 becomes finished (resolved via a done-category transition).
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/items/%d", ts.URL, i2), boot.Token,
		map[string]any{"status_id": doneID}); code != http.StatusOK {
		t.Fatalf("resolve i2: %d", code)
	}
	// i4 is trashed — it must never carry over, nor be counted.
	if _, err := pool.Exec(ctx, `UPDATE work_item SET trashed_at = now() WHERE id = $1`, i4); err != nil {
		t.Fatalf("trash i4: %v", err)
	}
	// item_count is live-only: A has i1,i2,i3 (i4 trashed excluded); C has i5.
	if a := findSprint(t, listSprints(t, ts.URL, boot.Token, carry.ID), sprintA); a.ItemCount != 3 {
		t.Fatalf("sprint A item_count = %d, want 3 (trashed excluded)", a.ItemCount)
	}
	if c := findSprint(t, listSprints(t, ts.URL, boot.Token, carry.ID), sprintC); c.ItemCount != 1 {
		t.Fatalf("sprint C item_count = %d, want 1", c.ItemCount)
	}

	// Close A, carrying unfinished items over to B (a future target).
	sprintB := createSprint(t, ts.URL, boot.Token, carry.ID, "B")
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/close", ts.URL, sprintA),
		boot.Token, map[string]any{"move_to_sprint_id": sprintB}); code != http.StatusOK {
		t.Fatalf("close sprint A: %d, want 200", code)
	}
	// Unfinished move to B; finished (i2) STAYS on A — the RED/GREEN pin; trashed
	// (i4) STAYS on A; the item in C is untouched.
	wantSprint(t, itemSprintID(t, ctx, pool, i1), sprintB, "i1 (unfinished)")
	wantSprint(t, itemSprintID(t, ctx, pool, i3), sprintB, "i3 (unfinished)")
	wantSprint(t, itemSprintID(t, ctx, pool, i2), sprintA, "i2 (finished stays)")
	wantSprint(t, itemSprintID(t, ctx, pool, i4), sprintA, "i4 (trashed stays)")
	wantSprint(t, itemSprintID(t, ctx, pool, i5), sprintC, "i5 (other sprint untouched)")
	// completed_at stamped, state closed.
	if a := findSprint(t, listSprints(t, ts.URL, boot.Token, carry.ID), sprintA); a.State != 3 || a.CompletedAt == nil {
		t.Fatalf("closed sprint A = %+v, want state 3 with completed_at", a)
	}
	assertClosedEvent(t, ctx, pool, boot.OrgID, sprintA, 2, &sprintB)

	// Close validation before the second (backlog) close.
	// Closing a future sprint is a 400.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/close", ts.URL, sprintB),
		boot.Token, map[string]any{}); code != http.StatusBadRequest {
		t.Fatalf("close future sprint: %d, want 400", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/start", ts.URL, sprintB),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("start sprint B: %d", code)
	}
	closeB := fmt.Sprintf("%s/api/v1/sprints/%d/close", ts.URL, sprintB)
	// Move target = the sprint being closed → 400.
	if code := postJSONStatus(t, closeB, boot.Token,
		map[string]any{"move_to_sprint_id": sprintB}); code != http.StatusBadRequest {
		t.Fatalf("self move target: %d, want 400", code)
	}
	// Move target already closed (A) → 400.
	if code := postJSONStatus(t, closeB, boot.Token,
		map[string]any{"move_to_sprint_id": sprintA}); code != http.StatusBadRequest {
		t.Fatalf("closed move target: %d, want 400", code)
	}
	// Move target in a different space (sprint 2 lives in SPR) → 400.
	if code := postJSONStatus(t, closeB, boot.Token,
		map[string]any{"move_to_sprint_id": sprint2.ID}); code != http.StatusBadRequest {
		t.Fatalf("cross-space move target: %d, want 400", code)
	}
	// Close B with no target → unfinished items fall to the backlog (NULL).
	if code := postJSONStatus(t, closeB, boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("close sprint B to backlog: %d, want 200", code)
	}
	wantBacklog(t, itemSprintID(t, ctx, pool, i1), "i1 after backlog close")
	wantBacklog(t, itemSprintID(t, ctx, pool, i3), "i3 after backlog close")
	assertClosedEvent(t, ctx, pool, boot.OrgID, sprintB, 2, nil)

	// --- Item↔sprint assignment: assign, clear, and the 400 boundaries ---
	i6 := createItem(t, ts.URL, boot.Token, carry.ID, "assign me")
	item6 := fmt.Sprintf("%s/api/v1/items/%d", ts.URL, i6)
	// Assign to a live same-space sprint (C).
	if code := patchJSON(t, item6, boot.Token, map[string]any{"sprint_id": sprintC}); code != http.StatusOK {
		t.Fatalf("assign i6 to C: %d", code)
	}
	wantSprint(t, itemSprintID(t, ctx, pool, i6), sprintC, "i6 assigned to C")
	// Clear (0) is always allowed → backlog.
	if code := patchJSON(t, item6, boot.Token, map[string]any{"sprint_id": 0}); code != http.StatusOK {
		t.Fatalf("clear i6 sprint: %d", code)
	}
	wantBacklog(t, itemSprintID(t, ctx, pool, i6), "i6 cleared")
	// Assigning into a closed sprint (A) → 400.
	if code := patchJSON(t, item6, boot.Token, map[string]any{"sprint_id": sprintA}); code != http.StatusBadRequest {
		t.Fatalf("assign into closed sprint: %d, want 400", code)
	}
	// A sprint from another space (sprint 2 in SPR) → 400.
	if code := patchJSON(t, item6, boot.Token, map[string]any{"sprint_id": sprint2.ID}); code != http.StatusBadRequest {
		t.Fatalf("assign cross-space sprint: %d, want 400", code)
	}

	// --- Foreign-org 404 everywhere for sprint-targeted verbs ---
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/sprints/%d/close", ts.URL, sprintForeign.ID),
		boot.Token, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("close foreign sprint: %d, want 404", code)
	}
}

// --- helpers ---

func createSprint(t *testing.T, base, token string, spaceID int64, name string) int64 {
	t.Helper()
	var s struct {
		ID int64 `json:"id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", base, spaceID), token,
		map[string]any{"name": name}, &s)
	return s.ID
}

func createItem(t *testing.T, base, token string, spaceID int64, title string) int64 {
	t.Helper()
	var it struct {
		ID int64 `json:"id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", base, spaceID), token,
		map[string]any{"title": title}, &it)
	return it.ID
}

func assignSprint(t *testing.T, base, token string, itemID, sprintID int64) {
	t.Helper()
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/items/%d", base, itemID), token,
		map[string]any{"sprint_id": sprintID}); code != http.StatusOK {
		t.Fatalf("assign item %d to sprint %d: %d", itemID, sprintID, code)
	}
}

func listSprints(t *testing.T, base, token string, spaceID int64) []worktrack.SprintSummary {
	t.Helper()
	var resp struct {
		Sprints []worktrack.SprintSummary `json:"sprints"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", base, spaceID), token, &resp); code != http.StatusOK {
		t.Fatalf("list sprints: %d", code)
	}
	return resp.Sprints
}

func findSprint(t *testing.T, sprints []worktrack.SprintSummary, id int64) worktrack.SprintSummary {
	t.Helper()
	for _, s := range sprints {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("sprint %d not in list %+v", id, sprints)
	return worktrack.SprintSummary{}
}

// assertSprintOrder verifies the list is sorted state ASC, then id DESC.
func assertSprintOrder(t *testing.T, sprints []worktrack.SprintSummary) {
	t.Helper()
	for i := 1; i < len(sprints); i++ {
		a, b := sprints[i-1], sprints[i]
		if a.State > b.State || (a.State == b.State && a.ID < b.ID) {
			t.Fatalf("sprint order violated at %d: %+v then %+v", i, a, b)
		}
	}
}

func doneStatusID(t *testing.T, base, token string, spaceID int64) int64 {
	t.Helper()
	var sts struct {
		Statuses []worktrack.Status `json:"statuses"`
	}
	getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/statuses", base, spaceID), token, &sts)
	for _, st := range sts.Statuses {
		if st.Category == 3 {
			return st.ID
		}
	}
	t.Fatalf("no done-category status in space %d", spaceID)
	return 0
}

func itemSprintID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, itemID int64) *int64 {
	t.Helper()
	var sid *int64
	if err := pool.QueryRow(ctx, `SELECT sprint_id FROM work_item WHERE id = $1`, itemID).Scan(&sid); err != nil {
		t.Fatalf("read sprint_id of item %d: %v", itemID, err)
	}
	return sid
}

func wantSprint(t *testing.T, got *int64, want int64, label string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: sprint_id = NULL, want %d", label, want)
	}
	if *got != want {
		t.Fatalf("%s: sprint_id = %d, want %d", label, *got, want)
	}
}

func wantBacklog(t *testing.T, got *int64, label string) {
	t.Helper()
	if got != nil {
		t.Fatalf("%s: sprint_id = %d, want NULL (backlog)", label, *got)
	}
}

func countEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, verb string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = $2`, orgID, verb).Scan(&n); err != nil {
		t.Fatalf("count %s events: %v", verb, err)
	}
	return n
}

// assertClosedEvent pins the sprint.closed payload {sprint_id, moved, moved_to}.
func assertClosedEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, sprintID int64, wantMoved int, wantMovedTo *int64) {
	t.Helper()
	var moved int
	var movedTo *int64
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'moved')::int, (payload->>'moved_to')::bigint
		FROM event_log
		WHERE org_id = $1 AND verb = 'sprint.closed' AND (payload->>'sprint_id')::bigint = $2`,
		orgID, sprintID).Scan(&moved, &movedTo); err != nil {
		t.Fatalf("read sprint.closed for %d: %v", sprintID, err)
	}
	if moved != wantMoved {
		t.Fatalf("sprint %d closed moved = %d, want %d", sprintID, moved, wantMoved)
	}
	switch {
	case wantMovedTo == nil && movedTo != nil:
		t.Fatalf("sprint %d moved_to = %d, want null (backlog)", sprintID, *movedTo)
	case wantMovedTo != nil && (movedTo == nil || *movedTo != *wantMovedTo):
		t.Fatalf("sprint %d moved_to = %v, want %d", sprintID, movedTo, *wantMovedTo)
	}
}
