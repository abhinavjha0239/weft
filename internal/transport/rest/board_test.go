package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type boardBoot struct {
	OrgID     int64  `json:"org_id"`
	ChannelID int64  `json:"channel_id"`
	Token     string `json:"token"`
}

func boardSetup(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slug string) (*httptest.Server, boardBoot) {
	t.Helper()
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
	var boot boardBoot
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": slug, "email": "a@" + slug + ".test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	return ts, boot
}

func createSpace(t *testing.T, base, token, key, name string) int64 {
	t.Helper()
	var sp struct {
		ID int64 `json:"id"`
	}
	postJSON(t, base+"/api/v1/spaces", token, map[string]any{"key": key, "name": name}, &sp)
	return sp.ID
}

func createBoardItem(t *testing.T, base, token string, spaceID int64, title string) int64 {
	t.Helper()
	var it struct {
		ID int64 `json:"id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", base, spaceID), token,
		map[string]any{"title": title}, &it)
	return it.ID
}

func boardOrder(t *testing.T, base, token string, spaceID int64) []int64 {
	t.Helper()
	var resp struct {
		Items []worktrack.Item `json:"items"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", base, spaceID), token, &resp); code != 200 {
		t.Fatalf("list items: %d", code)
	}
	ids := make([]int64, len(resp.Items))
	for i, it := range resp.Items {
		ids[i] = it.ID
	}
	return ids
}

func moveStatus(t *testing.T, base, token string, itemID int64, body map[string]any) int {
	t.Helper()
	return postJSONStatus(t, fmt.Sprintf("%s/api/v1/items/%d/move", base, itemID), token, body)
}

func assertOrder(t *testing.T, label string, got, want []int64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: order = %v, want %v", label, got, want)
	}
}

// TestBoardOrder proves LexoRank board ordering end-to-end: after/before
// reorders surfaced by a live re-query, the pre-P-12 NULL-rank backfill, a
// forced rebalance-and-retry when a gap is exhausted, and the cross-context /
// missing-neighbour rejections — plus the workitem.reordered event.
//
// RED/GREEN pin: make rankBetween return its lo bound (see rank.go) and the
// moved item collides with `after` on rank; ORDER BY rank, id then tie-breaks
// by id, so the first reorder assertion below fails ([2 1 3 4] not [2 3 1 4]).
func TestBoardOrder(t *testing.T) {
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
	ts, boot := boardSetup(t, ctx, pool, "board")

	space := createSpace(t, ts.URL, boot.Token, "BOARD", "Board")
	i1 := createBoardItem(t, ts.URL, boot.Token, space, "one")
	i2 := createBoardItem(t, ts.URL, boot.Token, space, "two")
	i3 := createBoardItem(t, ts.URL, boot.Token, space, "three")
	i4 := createBoardItem(t, ts.URL, boot.Token, space, "four")
	assertOrder(t, "initial", boardOrder(t, ts.URL, boot.Token, space), []int64{i1, i2, i3, i4})

	// Move i1 between i3 and i4. This is the RED/GREEN assertion.
	if code := moveStatus(t, ts.URL, boot.Token, i1,
		map[string]any{"after_item_id": i3, "before_item_id": i4}); code != 200 {
		t.Fatalf("move i1: %d", code)
	}
	assertOrder(t, "after i1 between i3,i4", boardOrder(t, ts.URL, boot.Token, space), []int64{i2, i3, i1, i4})

	// The reorder recorded exactly one workitem.reordered with its neighbours.
	var reordered int
	var afterPayload, beforePayload *int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), max((payload->>'after')::bigint), max((payload->>'before')::bigint)
		FROM event_log WHERE org_id = $1 AND verb = 'workitem.reordered'
		  AND (payload->>'item_id')::bigint = $2`, boot.OrgID, i1).
		Scan(&reordered, &afterPayload, &beforePayload); err != nil {
		t.Fatalf("event query: %v", err)
	}
	if reordered != 1 || afterPayload == nil || *afterPayload != i3 || beforePayload == nil || *beforePayload != i4 {
		t.Fatalf("reordered event = %d after=%v before=%v, want 1 after=%d before=%d",
			reordered, afterPayload, beforePayload, i3, i4)
	}

	// Move i4 to the top (before only, no after -> start sentinel).
	if code := moveStatus(t, ts.URL, boot.Token, i4,
		map[string]any{"before_item_id": i2}); code != 200 {
		t.Fatalf("move i4 to top: %d", code)
	}
	assertOrder(t, "i4 to top", boardOrder(t, ts.URL, boot.Token, space), []int64{i4, i2, i3, i1})

	// NULL-rank backfill: null every rank (pre-P-12 rows), then a move must
	// backfill in id order first and still land i1 between i2 and i3.
	if _, err := pool.Exec(ctx, `UPDATE work_item SET rank = NULL WHERE space_id = $1`, space); err != nil {
		t.Fatalf("null ranks: %v", err)
	}
	if code := moveStatus(t, ts.URL, boot.Token, i1,
		map[string]any{"after_item_id": i2, "before_item_id": i3}); code != 200 {
		t.Fatalf("move after backfill: %d", code)
	}
	assertOrder(t, "backfilled then i1 between i2,i3",
		boardOrder(t, ts.URL, boot.Token, space), []int64{i2, i1, i3, i4})

	// Forced rebalance: seed two adjacent ranks with no midpoint ("ab"/"aba"),
	// then dropping a third item between them exhausts the gap, forcing a
	// whole-context respace and a single retry.
	rebal := createSpace(t, ts.URL, boot.Token, "REBAL", "Rebalance")
	a := createBoardItem(t, ts.URL, boot.Token, rebal, "a")
	b := createBoardItem(t, ts.URL, boot.Token, rebal, "b")
	c := createBoardItem(t, ts.URL, boot.Token, rebal, "c")
	if _, err := pool.Exec(ctx, `UPDATE work_item SET rank = 'ab' WHERE id = $1`, a); err != nil {
		t.Fatalf("seed rank a: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE work_item SET rank = 'aba' WHERE id = $1`, b); err != nil {
		t.Fatalf("seed rank b: %v", err)
	}
	assertOrder(t, "seeded rebal", boardOrder(t, ts.URL, boot.Token, rebal), []int64{a, b, c})
	if code := moveStatus(t, ts.URL, boot.Token, c,
		map[string]any{"after_item_id": a, "before_item_id": b}); code != 200 {
		t.Fatalf("move c into exhausted gap: %d", code)
	}
	assertOrder(t, "rebalanced c between a,b", boardOrder(t, ts.URL, boot.Token, rebal), []int64{a, c, b})
	var aRank string
	if err := pool.QueryRow(ctx, `SELECT rank FROM work_item WHERE id = $1`, a).Scan(&aRank); err != nil {
		t.Fatalf("read a rank: %v", err)
	}
	if aRank == "ab" {
		t.Fatalf("rebalance did not respace item a (rank still %q)", aRank)
	}

	// Cross-context neighbour is a 400 (cross-space moves are not this slice).
	if code := moveStatus(t, ts.URL, boot.Token, i1,
		map[string]any{"after_item_id": a}); code != http.StatusBadRequest {
		t.Fatalf("cross-context neighbour: %d, want 400", code)
	}
	// Neither neighbour is a 400.
	if code := moveStatus(t, ts.URL, boot.Token, i1, map[string]any{}); code != http.StatusBadRequest {
		t.Fatalf("no neighbour: %d, want 400", code)
	}
	// Unknown moved item is a 404; unknown neighbour is a 404.
	if code := moveStatus(t, ts.URL, boot.Token, 999999,
		map[string]any{"after_item_id": i2}); code != http.StatusNotFound {
		t.Fatalf("unknown item: %d, want 404", code)
	}
	if code := moveStatus(t, ts.URL, boot.Token, i1,
		map[string]any{"after_item_id": int64(999999)}); code != http.StatusNotFound {
		t.Fatalf("unknown neighbour: %d, want 404", code)
	}
}
