package rest

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestGatewaySharedSpaceSet pins the space-visibility set as PER-ORG state
// against real Postgres and real WebSockets: its cost, and — because it is now
// state that connections SHARE — the two things sharing could have broken.
//
// The cost claim is written as a CONSTANCY assert, not a smallness one: a
// "queries stayed small" bound passes trivially on a small fixture, so the same
// space.created is measured at two different live-connection counts and the two
// deltas must be EQUAL. A per-connection load makes them 6 and 24.
//
// The two security claims are written the gateway_acl_test.go way — the
// connection that must NOT receive an event is a LIVE fan target whose drain
// terminates on a LATER event it IS entitled to, so a withheld event was
// provably offered to it and dropped by its own filter, never merely late:
//   - a GUEST connected to an org whose shared set is POPULATED still resolves
//     no Space. That is the case the old `AND NOT $2` covered and a shared
//     query cannot: the restriction now lives at the hand-off
//     (gateway.Hub.syncSpaceView), which never gives a guest the view.
//   - the set is PER ORG. Two orgs run on one hub with deliberately DIFFERENT
//     answers (org one has no visibility_scope, org two has one), so either
//     direction of leakage between them fails a positive assert.
func TestGatewaySharedSpaceSet(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	hub.SetMetrics(metrics.NewExpvar())
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
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "shared", "email": "alice@shared.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	// Created by the cost subtest below and reused by the two after it, which
	// need an org whose shared view is already POPULATED. The subtests run in
	// order (none calls t.Parallel).
	var spaceA, spaceB int64

	// THE COST PIN. One space.created in an org with N live connections used to
	// cost 3N pool queries — channels, DMs and spaces, reloaded on every
	// connection's own delivery goroutine. The space half is now per-org, so it
	// costs ONE query however many connections observe it.
	t.Run("space queries are constant in the connection count", func(t *testing.T) {
		const small, large = 6, 24
		tokens := bulkChannelMembers(t, ctx, pool, boot.OrgID, boot.ChannelID, "shs", large)
		subs := make([]*wsClient, 0, large)
		connect := func(from, to int) {
			for _, tok := range tokens[from:to] {
				c := dialClientLast(t, ctx, ts.URL, tok, "-1")
				t.Cleanup(func() { c.conn.CloseNow() })
				c.waitFor(t, "ready")
				subs = append(subs, c)
			}
		}
		// A drain terminator every connection is entitled to: seeing it proves
		// each one was offered — and therefore finished processing — every event
		// logged before it, which is what makes the counter sample meaningful.
		settle := func(label string) {
			ping := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, label)
			for i, s := range subs {
				drainMessagesUntil(t, s, ping)
				_ = i
			}
		}

		coldBefore := readSpaceQueries(t)
		connect(0, small)
		settle("small pool ready")
		coldDelta := readSpaceQueries(t) - coldBefore

		smallBefore := readSpaceQueries(t)
		var created struct {
			ID int64 `json:"id"`
		}
		postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
			map[string]any{"key": "alpha", "name": "Alpha"}, &created)
		spaceA = created.ID
		settle("after the first Space")
		smallDelta := readSpaceQueries(t) - smallBefore

		// Four times as many connections join, so the second space.created below
		// is measured against a much larger live set than the first.
		joinBefore := readSpaceQueries(t)
		connect(small, large)
		settle("large pool ready")
		joinDelta := readSpaceQueries(t) - joinBefore

		largeBefore := readSpaceQueries(t)
		postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
			map[string]any{"key": "beta", "name": "Beta"}, &created)
		spaceB = created.ID
		settle("after the second Space")
		largeDelta := readSpaceQueries(t) - largeBefore

		// The headline: the SAME event at 6 and at 24 live connections. Equal,
		// not merely small.
		if smallDelta != largeDelta {
			t.Fatalf("one space.created cost %g space queries at %d live connections and %g at %d; "+
				"the space set is per ORG, so the count must not scale with connections",
				smallDelta, small, largeDelta, large)
		}
		if largeDelta != 1 {
			t.Fatalf("one space.created cost %g space queries for the whole org, want exactly 1", largeDelta)
		}
		// CONNECT is deliberately NOT free, and the numbers say so out loud. A
		// registering connection requires a view read AFTER it arrived, because
		// itemSecurityActive has no refresh verb and reconnecting is its only
		// recovery (gateway_acl_test.go pins that). Dialled one at a time these
		// coalesce with nothing, so it stays exactly the one query per connect
		// dev already paid — what this slice removes is the query on the FAN
		// path. Asserted as <= so the herd coalescing (concurrent registrations
		// share one in-flight load) can only ever make it better.
		if coldDelta > float64(small) || joinDelta > float64(large-small) {
			t.Fatalf("connects cost more than one space query each: %g for %d and %g for %d",
				coldDelta, small, joinDelta, large-small)
		}
		t.Logf("space queries: %d cold connects -> +%g, one space.created at %d conns -> +%g, "+
			"%d more connects -> +%g, one space.created at %d conns -> +%g",
			small, coldDelta, small, smallDelta, large-small, joinDelta, large, largeDelta)
	})

	// THE GUEST PIN on the new shared path. The old guest restriction was a SQL
	// parameter on a per-connection query; a shared per-org query cannot carry
	// one, so the restriction moved to the hand-off. This is the case that
	// proves it: the org's shared view is POPULATED (a non-guest connection
	// loaded it, and two Spaces exist) before the guest ever connects.
	t.Run("a guest never reads the populated shared set", func(t *testing.T) {
		var vault struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
			map[string]any{"name": "vault", "private": true}, &vault)
		var ginv identity.Invite
		postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
			"role": 50, "channel_ids": []int64{vault.ChannelID}}, &ginv)
		var gina identity.AcceptInviteResult
		postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
			"token": ginv.Token, "email": "gina@shared.test", "password": "password123",
			"full_name": "Gina Guest"}, &gina)
		var ginaRole int16
		if err := pool.QueryRow(ctx, `SELECT role FROM user_account WHERE id = $1`,
			gina.UserID).Scan(&ginaRole); err != nil || ginaRole < 50 {
			t.Fatalf("gina role = %d (%v), want the guest ceiling; this subtest would be vacuous otherwise",
				ginaRole, err)
		}

		// alice populates the org's shared view first, so the guest that
		// follows connects into an org that HAS one to read.
		if spaceA == 0 || spaceB == 0 {
			t.Fatalf("the org holds no Space (%d, %d); the shared set would be empty "+
				"and this subtest vacuous", spaceA, spaceB)
		}
		alice := dialClientLast(t, ctx, ts.URL, boot.Token, "-1")
		defer alice.conn.CloseNow()
		alice.waitFor(t, "ready")

		ginaC := dialClientLast(t, ctx, ts.URL, gina.Token, "-1")
		defer ginaC.conn.CloseNow()
		ginaC.waitFor(t, "ready")

		var item, sprint struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, spaceA),
			boot.Token, map[string]any{"title": "not for the guest"}, &item)
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, spaceA),
			boot.Token, map[string]any{"name": "Sprint G"}, &sprint)

		// Both connections are LIVE fan targets: each drain terminates on a
		// message in a channel that connection IS in, logged AFTER the
		// space-scoped events above.
		ginaPing := sendChannel(t, ts.URL, boot.Token, vault.ChannelID, "guest ping")
		alicePing := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "member ping")

		aliceSaw := drainUntil(t, alice, alicePing)
		if !hasItemEvent(aliceSaw, "workitem.created", item.ID) {
			t.Fatalf("a non-guest sharing the org's view did not receive workitem.created for item %d; "+
				"without this the guest assert below would pass vacuously. saw %v",
				item.ID, envTypes(aliceSaw))
		}
		if !hasEvent(aliceSaw, "sprint.created") {
			t.Fatalf("a non-guest sharing the org's view did not receive sprint.created; saw %v",
				envTypes(aliceSaw))
		}
		ginaSaw := drainUntil(t, ginaC, ginaPing)
		for _, e := range ginaSaw {
			if strings.HasPrefix(e.Type, "workitem.") || strings.HasPrefix(e.Type, "space.") ||
				strings.HasPrefix(e.Type, "sprint.") {
				t.Fatalf("a guest received the space-scoped event %q from an org whose SHARED space view "+
					"is populated; a guest must never be handed that view. saw %v",
					e.Type, envTypes(ginaSaw))
			}
		}
	})

	// THE ISOLATION PIN. Shared state is where cross-org bugs live, so the two
	// orgs are given answers that DISAGREE: org one defines no visibility_scope
	// (work-item events flow) and org two defines one (they are blacked out).
	// Either direction of leakage kills a positive assert — a hub-wide view
	// hands the second org the first org's Space set and its sprint.created
	// vanishes; the reverse buries org one's work-item events under org two's
	// item security.
	t.Run("the shared set is per org", func(t *testing.T) {
		var two struct {
			OrgID     int64  `json:"org_id"`
			ChannelID int64  `json:"channel_id"`
			Token     string `json:"token"`
		}
		postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
			"org_slug": "shared2", "email": "bob@shared2.test", "password": "password123",
			"full_name": "Bob Ray",
		}, &two)
		var spaceTwo struct {
			ID int64 `json:"id"`
		}
		postJSON(t, ts.URL+"/api/v1/spaces", two.Token,
			map[string]any{"key": "gamma", "name": "Gamma"}, &spaceTwo)
		// Only the SECOND org defines a scope. Asserted as one row so a schema
		// change cannot quietly make the two orgs agree again.
		if ct, err := pool.Exec(ctx, `
			INSERT INTO visibility_scope (space_id, name, rule)
			VALUES ($1, 'restricted', '{"roles":["reporter"]}'::jsonb)`,
			spaceTwo.ID); err != nil || ct.RowsAffected() != 1 {
			t.Fatalf("define visibility scope in org two: %d rows (%v)", ct.RowsAffected(), err)
		}
		var scopes int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM visibility_scope vs
			JOIN space s ON s.id = vs.space_id WHERE s.org_id = $1`,
			boot.OrgID).Scan(&scopes); err != nil || scopes != 0 {
			t.Fatalf("org one has %d visibility scopes (%v), want 0; the two orgs must disagree",
				scopes, err)
		}

		if spaceA == 0 {
			t.Fatal("org one has no Space; this subtest would be vacuous")
		}
		one := dialClientLast(t, ctx, ts.URL, boot.Token, "-1")
		defer one.conn.CloseNow()
		one.waitFor(t, "ready")
		twoC := dialClientLast(t, ctx, ts.URL, two.Token, "-1")
		defer twoC.conn.CloseNow()
		twoC.waitFor(t, "ready")

		var itemOne, itemTwo, sprintTwo struct {
			ID int64 `json:"id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, spaceA),
			boot.Token, map[string]any{"title": "org one item"}, &itemOne)
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, spaceTwo.ID),
			two.Token, map[string]any{"title": "org two item"}, &itemTwo)
		postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/sprints", ts.URL, spaceTwo.ID),
			two.Token, map[string]any{"name": "Sprint Two"}, &sprintTwo)
		pingOne := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "org one ping")
		pingTwo := sendChannel(t, ts.URL, two.Token, two.ChannelID, "org two ping")

		sawOne := drainUntil(t, one, pingOne)
		sawTwo := drainUntil(t, twoC, pingTwo)

		if !hasItemEvent(sawOne, "workitem.created", itemOne.ID) {
			t.Fatalf("org one stopped delivering its own workitem.created for item %d; "+
				"org two's item security must not reach it. saw %v", itemOne.ID, envTypes(sawOne))
		}
		if !hasEvent(sawTwo, "sprint.created") {
			t.Fatalf("org two did not receive sprint.created for its own Space; "+
				"it must not be reading org one's space set. saw %v", envTypes(sawTwo))
		}
		if hasItemEvent(sawTwo, "workitem.created", itemTwo.ID) {
			t.Fatalf("org two delivered a work-item event while it defines a visibility_scope; "+
				"org one's (absent) item security must not reach it. saw %v", envTypes(sawTwo))
		}
		// Nothing from the other org reaches either connection at all.
		for _, e := range sawOne {
			if e.OrgID != boot.OrgID {
				t.Fatalf("org one's connection received an envelope stamped org %d", e.OrgID)
			}
		}
		for _, e := range sawTwo {
			if e.OrgID != two.OrgID {
				t.Fatalf("org two's connection received an envelope stamped org %d", e.OrgID)
			}
		}
		if hasItemEvent(sawOne, "workitem.created", itemTwo.ID) ||
			hasItemEvent(sawTwo, "workitem.created", itemOne.ID) {
			t.Fatal("a work-item event crossed orgs")
		}
	})
}

// readSpaceQueries reads the process-global gateway_space_queries_total counter
// — how many times the space-visibility set was read from the database — for a
// before/after delta, the same way readPumpQueries reads the S3 series.
func readSpaceQueries(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("gateway_space_queries_total")
	if v == nil {
		return 0
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatalf("gateway_space_queries_total is %T, want *expvar.Float", v)
	}
	return f.Value()
}
