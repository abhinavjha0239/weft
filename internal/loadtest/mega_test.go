package loadtest

import (
	"context"
	"expvar"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
	"github.com/abhinavjha0239/weft/internal/transport/rest"
	"github.com/abhinavjha0239/weft/migrations"
)

// TestMegaOrgHarnessSmoke is the S0 proof that the mega-org harness measures
// the gateway fan-out blowup — the whole point of building the proving ground
// BEFORE S3 fixes it. A single send to a channel with `conns` live connections
// must cost ~`conns` catch-up queries (one per connection per wake), because
// today every connection runs its OWN pump query. Bounded for CI (2k members,
// 200 connections); the full 100k run is the operator procedure beside PERF.md.
func TestMegaOrgHarnessSmoke(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	// Cancel BEFORE Close so the hub's LISTEN connection releases promptly.
	defer func() { cancel(); pool.Close() }()
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// The hub records into an expvar registry; the server also serves it at
	// /debug/vars so the harness reads the pump-query counter over HTTP exactly
	// as the real cross-process loadgen does.
	hub := gateway.NewHub(pool, slog.Default())
	hub.SetMetrics(metrics.NewExpvar())
	go hub.Run(ctx)

	mux := http.NewServeMux()
	mux.Handle("/api/", rest.Handler(ctx, rest.Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	mux.Handle("/debug/vars", expvar.Handler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const members = 2000
	const conns = 200
	res, err := RunMega(ctx, pool, MegaConfig{
		BaseURL:       ts.URL,
		Members:       members,
		Connections:   conns,
		Sends:         1,
		SendRate:      40,
		ProvisionConc: 16,
	})
	if err != nil {
		t.Fatalf("RunMega: %v", err)
	}

	// Every connection is a channel member, so the one send reaches all of them.
	if res.Delivered != conns {
		t.Fatalf("delivered = %d, want %d (each connection sees the one send)", res.Delivered, conns)
	}
	// THE pin: one send fans out to a per-connection catch-up query, so the
	// pump-query counter rises by ~connection-count. This is the S3 blowup made
	// MEASURABLE before S3 (per-org multicast) drives it to O(1).
	if res.PumpQueriesDelta < int64(conns) {
		t.Fatalf("pump queries rose by %d for one send to %d connections; want >= %d (the O(N) fan-out)",
			res.PumpQueriesDelta, conns, conns)
	}
	// Sanity ceiling: it must scale ~linearly with connections, not explode —
	// a runaway loop or repeated re-pump would blow past this.
	if res.PumpQueriesDelta > int64(conns)*4 {
		t.Fatalf("pump queries rose by %d, far above connection count %d — unexpected re-pump loop?",
			res.PumpQueriesDelta, conns)
	}
	// The harness also recorded a real closure-rebuild wall-time for the
	// mega-org's injected group edit (the perms scale-tier target).
	if res.ClosureRebuildSec <= 0 {
		t.Fatalf("closure rebuild time = %v, want > 0", res.ClosureRebuildSec)
	}
	t.Logf("mega smoke: %d conns, 1 send → pump-queries +%d (%.1f/msg), delivered %d, p99 %s, closure rebuild %.3fs",
		conns, res.PumpQueriesDelta, res.PerMsgPumpQueries, res.Delivered, res.DelP99, res.ClosureRebuildSec)
}
