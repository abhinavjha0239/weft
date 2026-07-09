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
