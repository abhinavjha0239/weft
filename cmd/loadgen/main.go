// Command loadgen drives extreme load against a running server and reports
// throughput + correlated latency. Provisioning connects to the same database
// the server uses; load goes over the server's HTTP/WebSocket surface.
//
//	loadgen -db "$WEFT_DATABASE_URL" -url http://localhost:8080 \
//	        -orgs 200 -subs 3 -rate 40 -duration 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/loadtest"
)

func main() {
	var (
		dbURL    = flag.String("db", os.Getenv("WEFT_DATABASE_URL"), "database URL (same as the server)")
		baseURL  = flag.String("url", "http://localhost:8080", "server base URL")
		orgs     = flag.Int("orgs", 100, "tenant count (each an isolated user/channel)")
		subs     = flag.Int("subs", 3, "gateway subscribers per org")
		ratePer  = flag.Float64("rate", 40, "sends/sec per org (keep < per-user limit 50)")
		duration = flag.Duration("duration", 30*time.Second, "measured send window")
		provConc = flag.Int("provision-concurrency", 32, "parallel provisioning")
		// Mega-org mode (S0): ONE org, one channel of -members, -conns gateway
		// connections, -sends measured messages. Proves the gateway fan-out
		// blowup against a single huge org (see docs/PERF-megaorg.md). Reads the
		// server's pump-query counter from /debug/vars, so run the server with
		// WEFT_METRICS_DRIVER=expvar.
		mega    = flag.Bool("mega", false, "single mega-org fan-out mode")
		members = flag.Int("members", 100_000, "mega: channel members to provision")
		conns   = flag.Int("conns", 100_000, "mega: gateway connections to open")
		sends   = flag.Int("sends", 1, "mega: messages sent in the measured window")
	)
	flag.Parse()

	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "error: -db (or WEFT_DATABASE_URL) required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, *dbURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "db connect:", err)
		os.Exit(1)
	}
	defer pool.Close()

	if *mega {
		runMega(ctx, pool, loadtest.MegaConfig{
			BaseURL:       *baseURL,
			Members:       *members,
			Connections:   *conns,
			Sends:         *sends,
			SendRate:      *ratePer,
			ProvisionConc: *provConc,
		})
		return
	}

	cfg := loadtest.Config{
		BaseURL:           *baseURL,
		Orgs:              *orgs,
		SubscribersPerOrg: *subs,
		SendRatePerOrg:    *ratePer,
		Duration:          *duration,
		ProvisionConc:     *provConc,
	}
	fmt.Printf("provisioning %d orgs, connecting %d subscribers, sending %.0f/s/org for %s...\n",
		cfg.Orgs, cfg.Orgs*cfg.SubscribersPerOrg, cfg.SendRatePerOrg, cfg.Duration)

	res, err := loadtest.Run(ctx, pool, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		os.Exit(1)
	}

	fmt.Printf(`
================ load test result ================
tenants (orgs)        %d
gateway connections   %d
window                %s

sends
  total               %d
  throughput          %.0f msg/s
  errors / 429s       %d / %d
  latency p50/p99/max %s / %s / %s

fan-out delivery
  delivered           %d
  throughput          %.0f deliveries/s
  latency p50/p99/max %s / %s / %s
==================================================
`,
		res.Orgs, res.Subscribers, res.Elapsed.Round(time.Millisecond),
		res.Sent, res.SendThroughput(), res.SendErr, res.Send429,
		res.SendP50, res.SendP99, res.SendMax,
		res.Delivered, res.DeliveryThroughput(),
		res.DelP50, res.DelP99, res.DelMax,
	)
}

func runMega(ctx context.Context, pool *pgxpool.Pool, cfg loadtest.MegaConfig) {
	fmt.Printf("provisioning ONE mega-org: %d members, opening %d connections, sending %d msg(s)...\n",
		cfg.Members, cfg.Connections, cfg.Sends)
	res, err := loadtest.RunMega(ctx, pool, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mega run:", err)
		os.Exit(1)
	}
	fmt.Printf(`
================ mega-org result ================
channel members      %d
gateway connections   %d
provision time        %s (setup, not measured)

per-message fan-out
  messages sent       %d
  delivered           %d
  pump queries        +%d  (%.1f per message)
  delivery p50/p99    %s / %s

closure rebuild       %.3f s  (one injected group edit)
==================================================
`,
		res.Members, res.Connections, res.ProvisionElapsed.Round(time.Millisecond),
		res.Sent, res.Delivered, res.PumpQueriesDelta, res.PerMsgPumpQueries,
		res.DelP50, res.DelP99, res.ClosureRebuildSec,
	)
}
