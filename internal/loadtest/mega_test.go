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

// TestMegaOrgHarnessSmoke pins the S3 per-org multicast fix on the mega-org
// harness S0 built. A single send to a channel with `conns` live connections
// now costs O(1) catch-up queries — ONE per-org multicast read fans the shared
// rows to every connection in memory — not ~`conns` (the pre-S3 blowup: one
// pump query per connection per wake, which THIS assertion measured red before
// the fix). Delivery is unchanged: every connection still sees the send.
// Bounded for CI (2k members, 200 connections); the full 100k run is the
// operator procedure beside PERF.md.
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
	// THE S3 pin (this assertion was `>= conns` before the fix): one send now
	// fans out via a SINGLE per-org multicast read, so the pump-query counter
	// rises by O(1) — a small constant independent of the connection count —
	// not by ~connections (the blowup this harness was built to catch, now
	// proven fixed by the same smoke that measured it). A stray sweep tick may
	// add a query or two, so the ceiling is loose but stays FAR below the
	// connection count; reverting to per-connection pump blows past it (red).
	if res.PumpQueriesDelta > int64(conns)/4 {
		t.Fatalf("pump queries rose by %d for one send to %d connections; want O(1) per-org multicast, not O(N) (ceiling %d)",
			res.PumpQueriesDelta, conns, int64(conns)/4)
	}
	// But the reader DID run its one hoisted read — a zero rise would mean the
	// send never reached the multicast lane at all.
	if res.PumpQueriesDelta < 1 {
		t.Fatalf("pump queries did not rise (%d); the multicast reader must run its one hoisted read per event-batch",
			res.PumpQueriesDelta)
	}
	// The harness also recorded a real closure-rebuild wall-time for the
	// mega-org's injected group edit (the perms scale-tier target).
	if res.ClosureRebuildSec <= 0 {
		t.Fatalf("closure rebuild time = %v, want > 0", res.ClosureRebuildSec)
	}
	t.Logf("mega smoke: %d conns, 1 send → pump-queries +%d (%.1f/msg), delivered %d, p99 %s, closure rebuild %.3fs",
		conns, res.PumpQueriesDelta, res.PerMsgPumpQueries, res.Delivered, res.DelP99, res.ClosureRebuildSec)
}
