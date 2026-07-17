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

	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// autoURL is the automations resource path for id — shared by the trigger-kind
// tests in this package.
func autoURL(ts *httptest.Server, id int64) string {
	return fmt.Sprintf("%s/api/v1/automations/%d", ts.URL, id)
}

// TestAutomationSchedules: the scheduler lane end to end. Enabling a schedule
// rule computes its next fire; the CAS-shaped claim fires a due rule EXACTLY
// once, appending automation.schedule_due which the consumer turns into one
// run that posts; the fire time advances so a second claim does nothing; and
// disabling NULLs the fire time (the index predicate then excludes it).
func TestAutomationSchedules(t *testing.T) {
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
		Identity:    identity.New(pool, permsSvc),
		Messaging:   msgSvc,
		Automations: automation.New(pool, permsSvc),
	}))
	defer ts.Close()
	runner := automation.NewRunner(pool, msgSvc, permsSvc, notification.New(pool), slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sched", "email": "a@sched.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// Drain bootstrap history before any rule exists.
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)

	// A channel-scope schedule rule posting into its own channel every 5 min.
	var rule automation.Automation
	postJSON(t, ts.URL+"/api/v1/automations", boot.Token, map[string]any{
		"scope_type": 3, "scope_id": boot.ChannelID, "name": "heartbeat",
		"definition": map[string]any{
			"trigger": map[string]any{"kind": "schedule",
				"schedule": map[string]any{"every": "minutes", "n": 5}},
			"steps": []any{map[string]any{"kind": "post_message", "content": "tick"}},
		}}, &rule)
	if rule.ID == 0 {
		t.Fatal("create schedule rule failed")
	}

	nextAt := func() *time.Time {
		t.Helper()
		var at *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT schedule_next_at FROM automation WHERE id = $1`, rule.ID).Scan(&at); err != nil {
			t.Fatalf("read next_at: %v", err)
		}
		return at
	}
	countTicks := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM message
			WHERE channel_id = $1 AND source = 'tick' AND deleted_at IS NULL`,
			boot.ChannelID).Scan(&n); err != nil {
			t.Fatalf("count ticks: %v", err)
		}
		return n
	}

	// Disabled at creation: no fire time.
	if at := nextAt(); at != nil {
		t.Fatalf("created rule has next_at %v, want NULL until enabled", at)
	}

	// Enabling computes the next fire, in the future.
	if code := patchJSON(t, autoURL(ts, rule.ID), boot.Token,
		map[string]any{"enabled": true}); code != http.StatusOK {
		t.Fatalf("enable = %d", code)
	}
	at := nextAt()
	if at == nil || !at.After(time.Now()) {
		t.Fatalf("after enable next_at = %v, want a future instant", at)
	}

	// Force it due, then run the claim: exactly one schedule fires.
	if _, err := pool.Exec(ctx,
		`UPDATE automation SET schedule_next_at = now() - interval '1 minute' WHERE id = $1`,
		rule.ID); err != nil {
		t.Fatalf("force due: %v", err)
	}
	fired, err := runner.RunDueSchedules(ctx, time.Now())
	if err != nil || fired != 1 {
		t.Fatalf("claim fired = %d (%v), want 1", fired, err)
	}
	// The fire time moved forward past now (no burst catch-up).
	if at := nextAt(); at == nil || !at.After(time.Now()) {
		t.Fatalf("after claim next_at = %v, want advanced into the future", at)
	}
	// The consumer turns the schedule_due event into exactly one run posting.
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	if n := countTicks(); n != 1 {
		t.Fatalf("ticks after one fire = %d, want 1", n)
	}
	var runCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM automation_run WHERE automation_id = $1 AND status = 2`,
		rule.ID).Scan(&runCount); err != nil || runCount != 1 {
		t.Fatalf("success runs = %d (%v), want 1", runCount, err)
	}

	// A second claim now finds nothing due (the fire time is in the future).
	fired, err = runner.RunDueSchedules(ctx, time.Now())
	if err != nil || fired != 0 {
		t.Fatalf("second claim fired = %d (%v), want 0", fired, err)
	}
	drainConsumer(t, ctx, pool, "automations", boot.OrgID, runner.ProcessOrg)
	if n := countTicks(); n != 1 {
		t.Fatalf("ticks after second claim = %d, want still 1", n)
	}

	// Disabling NULLs the fire time, so the claim index excludes it forever.
	if code := patchJSON(t, autoURL(ts, rule.ID), boot.Token,
		map[string]any{"enabled": false}); code != http.StatusOK {
		t.Fatalf("disable = %d", code)
	}
	if at := nextAt(); at != nil {
		t.Fatalf("after disable next_at = %v, want NULL", at)
	}
}
