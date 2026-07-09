package eventlog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
)

// Integration tests: need a real Postgres. Set TEST_DATABASE_URL, e.g.
//
//	docker run -d --rm -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17-alpine
//	TEST_DATABASE_URL=postgres://postgres:test@localhost:55432/postgres go test ./...
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	applyMigrations(t, pool)
	return pool
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	// Fresh schema per test run.
	if _, err := pool.Exec(ctx,
		`DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	files, err := filepath.Glob("../../migrations/0*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found: %v", err)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

func seedOrg(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var orgID int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO org (name, slug) VALUES ('Test', 'test') RETURNING id`).Scan(&orgID)
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	return orgID
}

func appendInTx(t *testing.T, pool *pgxpool.Pool, orgID int64, verb string) int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	id, err := Append(ctx, tx, Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: verb,
		Payload: json.RawMessage(`{"n":1}`),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return id
}

// TestCommitOrderGate is the F-1 proof: a committed row must stay invisible
// while an OLDER (lower-id, still-uncommitted) transaction is in flight, so a
// consumer can never advance its cursor past an id that hasn't landed yet.
func TestCommitOrderGate(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	consumer := NewConsumer(pool, "test", 100)

	// Tx A: takes the lower id, stays open.
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	idA, err := Append(ctx, txA, Event{
		OrgID: orgID, ActorKind: enum.ActorSystem,
		EntityType: enum.EntityMessage, EntityID: 1, Verb: "a.created",
	})
	if err != nil {
		t.Fatalf("append A: %v", err)
	}

	// Tx B: higher id, commits FIRST — the race.
	idB := appendInTx(t, pool, orgID, "b.created")
	if idB <= idA {
		t.Fatalf("expected idB > idA, got A=%d B=%d", idA, idB)
	}

	// The gate: B is committed but must NOT be visible (A still in flight).
	rows, err := consumer.Poll(ctx, orgID)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("gate failed: consumer saw %d rows while older tx in flight (would skip id %d)", len(rows), idA)
	}

	// A commits; now both appear, in id order, nothing skipped.
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	rows, err = consumer.Poll(ctx, orgID)
	if err != nil {
		t.Fatalf("poll 2: %v", err)
	}
	if len(rows) != 2 || rows[0].ID != idA || rows[1].ID != idB {
		t.Fatalf("expected [%d %d], got %+v", idA, idB, rows)
	}
}

// TestCursorAdvance: Ack is durable, monotone, and at-least-once shaped.
func TestCursorAdvance(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgID := seedOrg(t, pool)
	consumer := NewConsumer(pool, "test", 100)

	id1 := appendInTx(t, pool, orgID, "one")
	id2 := appendInTx(t, pool, orgID, "two")

	rows, err := consumer.Poll(ctx, orgID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("poll: %v rows=%d", err, len(rows))
	}
	if err := consumer.Ack(ctx, orgID, id2); err != nil {
		t.Fatalf("ack: %v", err)
	}
	rows, err = consumer.Poll(ctx, orgID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("expected empty after ack, got %d rows (err %v)", len(rows), err)
	}
	// Monotone: a stale ack never rewinds the cursor.
	if err := consumer.Ack(ctx, orgID, id1); err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	rows, _ = consumer.Poll(ctx, orgID)
	if len(rows) != 0 {
		t.Fatalf("stale ack rewound cursor: %d rows", len(rows))
	}

	// A second consumer name starts from zero (E2: replay = new cursor).
	fresh := NewConsumer(pool, "fresh", 100)
	rows, err = fresh.Poll(ctx, orgID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("fresh consumer: err=%v rows=%d", err, len(rows))
	}
	_ = id1

	// Backdated import events keep their occurred_at (ADR-003 E3).
	past := time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC)
	tx, _ := pool.Begin(ctx)
	if _, err := Append(ctx, tx, Event{
		OrgID: orgID, ActorKind: enum.ActorImporter,
		EntityType: enum.EntityMessage, EntityID: 2, Verb: "imported",
		OccurredAt: past,
	}); err != nil {
		t.Fatalf("append backdated: %v", err)
	}
	_ = tx.Commit(ctx)
	rows, _ = fresh.Poll(ctx, orgID)
	last := rows[len(rows)-1]
	if !last.OccurredAt.Equal(past) {
		t.Fatalf("occurred_at not preserved: %v", last.OccurredAt)
	}
	if !last.RecordedAt.After(past) {
		t.Fatalf("recorded_at should be ingest time")
	}
}
