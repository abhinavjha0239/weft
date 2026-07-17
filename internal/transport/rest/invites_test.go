package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestInvites: the onboarding lane. Capability tokens shown once and
// stored hashed; role ceiling structural (member/guest only); guest
// invites must enumerate channels (P-5); acceptance provisions the
// account, credentials, group membership, pre-joined channels (private
// included — the invite IS the authorization), and a session; uses,
// expiry, email pins, and revocation all enforce.
func TestInvites(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "inv", "email": "a@inv.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "wolfpack", "private": true}, &priv)

	// Validation: bad role, guest without channels, foreign channel.
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites", boot.Token,
		map[string]any{"role": 20}); code != http.StatusBadRequest {
		t.Fatalf("admin invite = %d, want 400 (role ceiling)", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites", boot.Token,
		map[string]any{"role": 50}); code != http.StatusBadRequest {
		t.Fatalf("channel-less guest invite = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites", boot.Token,
		map[string]any{"channel_ids": []int64{999999}}); code != http.StatusNotFound {
		t.Fatalf("foreign channel = %d, want 404", code)
	}

	// Member invite: 2 uses, pre-joins the PRIVATE channel.
	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"max_uses": 2, "channel_ids": []int64{priv.ChannelID}}, &inv)
	if inv.Token == "" || inv.Role != 40 {
		t.Fatalf("invite = %+v, want member token", inv)
	}
	// Listing never re-shows the token.
	var list struct {
		Invites []identity.Invite `json:"invites"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/invites", boot.Token, &list); code != 200 ||
		len(list.Invites) != 1 || list.Invites[0].Token != "" {
		t.Fatalf("list = %d %+v, want one tokenless row", code, list.Invites)
	}

	// Accept: carol lands with a session, member group, private channel.
	var carol identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv.Token, "email": "carol@inv.test",
		"password": "password123", "full_name": "Carol Wu"}, &carol)
	if carol.Token == "" || carol.Role != 40 || len(carol.ChannelIDs) != 1 {
		t.Fatalf("accept = %+v", carol)
	}
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, priv.ChannelID),
		carol.Token, map[string]any{"content": "hello from carol"}, &sent)
	if sent.MessageID == 0 {
		t.Fatal("carol must speak in her pre-joined private channel")
	}
	var inMembers bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM user_group_member gm
		 JOIN user_group g ON g.id = gm.group_id
		 WHERE gm.user_id = $1 AND g.name = 'role:members')`,
		carol.UserID).Scan(&inMembers); err != nil || !inMembers {
		t.Fatalf("carol in members = %v (%v)", inMembers, err)
	}

	// Duplicate email, then use #2 and exhaustion. (Pre-auth calls stay
	// under the IP limiter's burst; the refill sleep below buys headroom.)
	time.Sleep(2 * time.Second)
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": inv.Token, "email": "carol@inv.test", "password": "password123"}); code != http.StatusConflict {
		t.Fatalf("duplicate email = %d, want 409", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": inv.Token, "email": "dave@inv.test", "password": "password123"}); code != http.StatusCreated {
		t.Fatalf("second use = %d, want 201", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": inv.Token, "email": "erin@inv.test", "password": "password123"}); code != http.StatusConflict {
		t.Fatalf("exhausted = %d, want 409", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": "deadbeef", "email": "x@inv.test", "password": "password123"}); code != http.StatusNotFound {
		t.Fatalf("unknown token = %d, want 404", code)
	}

	// Email pin: only the pinned address may accept.
	var pinned identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"email": "Frank@inv.test"}, &pinned)
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": pinned.Token, "email": "mallory@inv.test", "password": "password123"}); code != http.StatusForbidden {
		t.Fatalf("pin mismatch = %d, want 403", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": pinned.Token, "email": "frank@inv.test", "password": "password123"}); code != http.StatusCreated {
		t.Fatalf("pin match = %d, want 201 (case-insensitive)", code)
	}

	// Revocation kills a live token.
	var doomed identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{}, &doomed)
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/invites/%d", ts.URL, doomed.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites/accept", "",
		map[string]any{"token": doomed.Token, "email": "late@inv.test", "password": "password123"}); code != http.StatusConflict {
		t.Fatalf("revoked accept = %d, want 409", code)
	}

	// A guest (everyone-group only, not members) cannot mint invites.
	var ginv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 50, "channel_ids": []int64{boot.ChannelID}}, &ginv)
	var gina identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": ginv.Token, "email": "gina@inv.test", "password": "password123",
		"full_name": "Gina Guest"}, &gina)
	if gina.Role != 50 {
		t.Fatalf("guest accept = %+v", gina)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/invites", gina.Token,
		map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("guest minting invites = %d, want 403", code)
	}
}

// gaugeSetCounter is a metrics.Registry that counts Gauge.Set invocations per
// metric name — the S0 seam reused as a test probe: RebuildClosure sets
// closure_rebuild_seconds exactly once per full-org rebuild, so the Set count
// IS the rebuild count.
type gaugeSetCounter struct {
	mu   sync.Mutex
	sets map[string]int
}

func (c *gaugeSetCounter) Counter(name string, labelNames ...string) metrics.Counter {
	return metrics.Nop().Counter(name, labelNames...)
}

func (c *gaugeSetCounter) Gauge(name string, _ ...string) metrics.Gauge {
	return &countedGauge{c: c, name: name}
}

func (c *gaugeSetCounter) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sets[name]
}

type countedGauge struct {
	c    *gaugeSetCounter
	name string
}

func (g *countedGauge) Set(float64, ...string) {
	g.c.mu.Lock()
	defer g.c.mu.Unlock()
	g.c.sets[g.name]++
}

// TestInviteAcceptSingleClosureRebuild: accepting an invite maintains the
// group closure exactly ONCE — inside AddUserToGroup. The accept flow
// historically rebuilt a second time right after (the S2 prep fix), doubling
// the O(org) closure cost of every onboarding.
func TestInviteAcceptSingleClosureRebuild(t *testing.T) {
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
	rebuilds := &gaugeSetCounter{sets: map[string]int{}}
	permsSvc := perms.New(pool)
	permsSvc.SetMetrics(rebuilds)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "one", "email": "a@one.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	base := rebuilds.count("closure_rebuild_seconds") // SeedOrg's own rebuild

	var inv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{}, &inv)
	var mia identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": inv.Token, "email": "mia@one.test", "password": "password123",
		"full_name": "Mia New"}, &mia)
	if mia.Token == "" {
		t.Fatal("accept did not provision a session")
	}

	if got := rebuilds.count("closure_rebuild_seconds") - base; got != 1 {
		t.Fatalf("invite accept ran %d closure rebuilds, want exactly 1 (AddUserToGroup already maintains the closure)", got)
	}

	// The single maintenance pass left the closure CORRECT. End-to-end: mia
	// resolves send_message through role:everyone — members ⊂ everyone, so
	// this is transitive nesting, not just her direct role:members row.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		mia.Token, map[string]any{"content": "hello from mia"}, &sent)
	if sent.MessageID == 0 {
		t.Fatal("new member must send via closure transitivity")
	}
	// And structurally: both the direct row and the transitive row exist.
	var direct, transitive bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM user_group_closure gc
		  JOIN user_group g ON g.id = gc.group_id
		  WHERE g.org_id = $1 AND g.name = 'role:members' AND gc.user_id = $2)`,
		boot.OrgID, mia.UserID).Scan(&direct)
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM user_group_closure gc
		  JOIN user_group g ON g.id = gc.group_id
		  WHERE g.org_id = $1 AND g.name = 'role:everyone' AND gc.user_id = $2)`,
		boot.OrgID, mia.UserID).Scan(&transitive)
	if !direct || !transitive {
		t.Fatalf("closure after accept: members=%v everyone=%v, want both", direct, transitive)
	}
}
