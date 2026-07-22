package rest

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/presence"
)

// TestMultiNodePresence pins the S5 shared presence plane: two in-process
// gateway nodes on ONE cell (one Postgres), each its OWN Hub with its OWN pg
// presence plane over the shared pool — the plane (the UNLOGGED presence table
// + LISTEN/NOTIFY) is what they share. A user connected on node 1 is visible on
// node 2 — both as a LIVE presence.changed delta AND in node 2's org-wide
// PresenceSnapshot — and goes offline on both once its last connection drops.
// This is the fragmentation the slice fixes: before it, node 2 (a different
// process) never learned about a user connected only to node 1.
func TestMultiNodePresence(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	// Two in-process "nodes" (hubs) share this pool, and EACH pins two
	// connections for its LISTEN loops (event-log + the presence plane), plus
	// per-shard multicast readers. The pgxpool default max (max(4, NumCPU))
	// starves on a low-core CI runner — the hubs' listeners exhaust it and the
	// next query blocks to the context deadline. Real gateway nodes are
	// separate processes with their own pools; size this shared test pool for
	// both simulated nodes plus query/WS headroom (mirrors unread_counters_test).
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		cancel()
		t.Fatalf("parse config: %v", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// Two nodes on one cell. RED PIN: swap presence.Open("pg", ...) below to
	// presence.Open("local", ...) — the per-process status quo — and the
	// load-bearing assertion further down (observer.waitForPresence "active")
	// goes red: node 2 never sees the user connected only to node 1.
	newNode := func() (*gateway.Hub, *httptest.Server) {
		hub := gateway.NewHub(pool, slog.Default())
		plane, err := presence.Open("pg", pool, slog.Default())
		if err != nil {
			t.Fatalf("presence plane: %v", err)
		}
		hub.SetPresencePlane(plane)
		go hub.Run(ctx)
		permsSvc := perms.New(pool)
		ts := httptest.NewServer(Handler(ctx, Deps{
			Pool: pool, Hub: hub, Log: slog.Default(),
			Identity:  identity.New(pool, permsSvc),
			Messaging: messaging.New(pool, permsSvc),
		}))
		return hub, ts
	}
	hub1, node1 := newNode()
	defer node1.Close()
	hub2, node2 := newNode()
	defer node2.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, node1.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "mnp", "email": "a@mnp.test", "password": "password123",
		"full_name": "Alice",
	}, &boot)
	bobTok := bareOrgMember(t, ctx, pool, boot.OrgID, "bob@mnp.test", "Bob", "mnp-bob")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE email='bob@mnp.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	// The OBSERVER (Alice) connects to node 2; the SUBJECT (Bob) connects to
	// node 1. They share no process — only the cell.
	observer := dialClient(t, ctx, node2.URL, boot.Token)
	defer observer.conn.CloseNow()
	observer.waitFor(t, "ready")

	subject := dialClient(t, ctx, node1.URL, bobTok)
	subject.waitFor(t, "ready")

	// LOAD-BEARING (S5 red/green): Bob, connected only on node 1, is announced
	// as a live presence.changed delta to Alice on node 2 — the cross-node fan
	// only the shared plane makes possible. RED: build both nodes with
	// presence.Open("local", ...) (per-process, status quo) and this times out.
	observer.waitForPresence(t, bobID, "active")

	// node 2's org-wide snapshot, read from the shared store, also shows Bob
	// active even though he holds no connection on node 2.
	if snap := hub2.PresenceSnapshot(boot.OrgID); snap[bobID] != "active" {
		t.Fatalf("node 2 snapshot = %v, want bob(%d) active from the shared plane", snap, bobID)
	}
	// Symmetry: Alice, connected only on node 2, is active in node 1's snapshot.
	if snap := hub1.PresenceSnapshot(boot.OrgID); snap[boot.UserID] != "active" {
		t.Fatalf("node 1 snapshot = %v, want alice(%d) active from the shared plane", snap, boot.UserID)
	}

	// Bob's last (only) connection drops → node 2 sees OFFLINE, on both the live
	// delta and the shared store.
	_ = subject.conn.CloseNow()
	observer.waitForPresence(t, bobID, "offline")
	deadline := time.Now().Add(8 * time.Second)
	for {
		if _, present := hub2.PresenceSnapshot(boot.OrgID)[bobID]; !present {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node 2 snapshot still shows bob after his disconnect")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
