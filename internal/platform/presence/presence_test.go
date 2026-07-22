package presence

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/migrations"
)

// TestEncodeDecode pins the pg_notify payload codec: a Delta round-trips, and
// malformed payloads are rejected (never decoded into a bogus zero Delta that
// would fan presence for user 0).
func TestEncodeDecode(t *testing.T) {
	d := Delta{OrgID: 7, UserID: 42, State: stateActive}
	got, ok := decode(encode(d))
	if !ok || got != d {
		t.Fatalf("round-trip: decode(%q) = %+v, %v; want %+v", encode(d), got, ok, d)
	}
	for _, bad := range []string{"", "7", "7:42", "x:42:active", "7:y:idle", "7:42:"} {
		if got, ok := decode(bad); ok {
			t.Fatalf("decode(%q) = %+v, true; want rejected", bad, got)
		}
	}
}

// TestLocalPlane pins the in-process driver: publish updates the org-scoped
// view, offline removes the user, a foreign org never sees the user, and a
// subscriber receives the published delta.
func TestLocalPlane(t *testing.T) {
	p := Local()
	ctx := context.Background()

	if err := p.Publish(ctx, Delta{OrgID: 1, UserID: 100, State: stateActive}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := p.Publish(ctx, Delta{OrgID: 1, UserID: 101, State: stateIdle}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	snap, _ := p.Snapshot(ctx, 1)
	if snap[100] != stateActive || snap[101] != stateIdle {
		t.Fatalf("snapshot org 1 = %v, want 100 active + 101 idle", snap)
	}
	// Cell/org isolation: org 2 never sees org 1's users.
	if other, _ := p.Snapshot(ctx, 2); len(other) != 0 {
		t.Fatalf("snapshot org 2 = %v, want empty (org-scoped)", other)
	}
	// Offline drops the user from the view.
	if err := p.Publish(ctx, Delta{OrgID: 1, UserID: 100, State: stateOffline}); err != nil {
		t.Fatalf("publish offline: %v", err)
	}
	if snap, _ := p.Snapshot(ctx, 1); snap[100] != "" {
		t.Fatalf("snapshot after offline = %v, want 100 absent", snap)
	}

	// Subscribe delivers published deltas. The bus is buffered, so it replays
	// the earlier publishes above too — drain until the one we just published
	// arrives (or time out), which proves delivery without depending on order.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	got := make(chan Delta, 16)
	go p.Subscribe(subCtx, func(_ context.Context, d Delta) { got <- d })
	want := Delta{OrgID: 1, UserID: 102, State: stateActive}
	if err := p.Publish(ctx, want); err != nil {
		t.Fatalf("publish for subscribe: %v", err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case d := <-got:
			if d == want {
				return
			}
		case <-deadline:
			t.Fatal("subscribe never delivered the published delta")
		}
	}
}

// TestPGPlane pins the shipped driver against real Postgres: the UNLOGGED
// presence table is the shared store (org-scoped via the user_account join),
// offline drops from the snapshot, and a subscriber receives the LISTEN/NOTIFY
// delta.
func TestPGPlane(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Two orgs, one user each — the org-scoping negative uses a REAL foreign org.
	orgA := insertOrg(t, ctx, pool, "pg-plane-a")
	orgB := insertOrg(t, ctx, pool, "pg-plane-b")
	userA := insertUser(t, ctx, pool, orgA, "Ann")
	userB := insertUser(t, ctx, pool, orgB, "Bea")

	p, err := Open("pg", pool, slog.Default())
	if err != nil {
		t.Fatalf("open pg plane: %v", err)
	}

	if err := p.Publish(ctx, Delta{OrgID: orgA, UserID: userA, State: stateActive}); err != nil {
		t.Fatalf("publish A: %v", err)
	}
	if err := p.Publish(ctx, Delta{OrgID: orgB, UserID: userB, State: stateIdle}); err != nil {
		t.Fatalf("publish B: %v", err)
	}
	snapA, err := p.Snapshot(ctx, orgA)
	if err != nil {
		t.Fatalf("snapshot A: %v", err)
	}
	if snapA[userA] != stateActive {
		t.Fatalf("snapshot org A = %v, want user %d active", snapA, userA)
	}
	// Org-scoping: org A's snapshot must NOT carry org B's user, even though
	// both share the one cell-wide presence table.
	if _, leaked := snapA[userB]; leaked {
		t.Fatalf("snapshot org A leaked org B's user %d: %v", userB, snapA)
	}

	// Subscribe receives the LISTEN/NOTIFY delta (retry publish until the LISTEN
	// is established — the notify only reaches an active listener).
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	got := make(chan Delta, 8)
	go p.Subscribe(subCtx, func(_ context.Context, d Delta) { got <- d })
	want := Delta{OrgID: orgA, UserID: userA, State: stateActive}
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	if err := p.Publish(ctx, want); err != nil {
		t.Fatalf("publish for subscribe: %v", err)
	}
	for {
		select {
		case d := <-got:
			if d == want {
				goto delivered
			}
		case <-ticker.C:
			if err := p.Publish(ctx, want); err != nil {
				t.Fatalf("republish for subscribe: %v", err)
			}
		case <-deadline:
			t.Fatal("subscribe never delivered the presence delta over LISTEN/NOTIFY")
		}
	}
delivered:

	// Offline drops the user from the org-wide snapshot.
	if err := p.Publish(ctx, Delta{OrgID: orgA, UserID: userA, State: stateOffline}); err != nil {
		t.Fatalf("publish offline: %v", err)
	}
	snapA, err = p.Snapshot(ctx, orgA)
	if err != nil {
		t.Fatalf("snapshot A after offline: %v", err)
	}
	if _, present := snapA[userA]; present {
		t.Fatalf("snapshot after offline = %v, want user %d absent", snapA, userA)
	}
}

func insertOrg(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slug string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO org (name, slug) VALUES ($1, $2) RETURNING id`, slug, slug).Scan(&id); err != nil {
		t.Fatalf("insert org: %v", err)
	}
	return id
}

func insertUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO user_account (org_id, full_name) VALUES ($1, $2) RETURNING id`,
		orgID, name).Scan(&id); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	return id
}
