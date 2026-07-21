package rest

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
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
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestGatewayMulticast pins the S3 per-org multicast fix and its correctness
// carry-over. With many live connections on one org, a single message costs
// ONE hoisted event-log read (O(1)) — the reader fans the shared rows to every
// connection in memory — not one catch-up query per connection (the pre-S3
// O(N) blowup). The shared batch is still ACL-filtered per connection (a
// non-member never sees a channel it isn't in), and a connection resuming
// behind the org head still replays its gap through the per-connection pump
// and rejoins the live lane. gateway_pump_queries_total is read from the SAME
// expvar series the S0 mega harness scrapes over /debug/vars.
func TestGatewayMulticast(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
		"org_slug": "mcast", "email": "a@mc.test", "password": "password123",
		"full_name": "Alice",
	}, &boot)

	// A pool of channel members, each on its own LIVE (tail-mode) connection,
	// plus one org member who is NOT in the channel — the ACL negative.
	const conns = 40
	memberTokens := bulkChannelMembers(t, ctx, pool, boot.OrgID, boot.ChannelID, "mcm", conns)
	subs := make([]*wsClient, conns)
	for i, tok := range memberTokens {
		subs[i] = dialClientLast(t, ctx, ts.URL, tok, "-1")
		defer subs[i].conn.CloseNow()
	}
	outsiderTok := bareOrgMember(t, ctx, pool, boot.OrgID, "outsider@mc.test", "Outsider", "mc-outsider")
	outsider := dialClientLast(t, ctx, ts.URL, outsiderTok, "-1")
	defer outsider.conn.CloseNow()
	for _, s := range subs {
		s.waitFor(t, "ready")
	}
	outsider.waitFor(t, "ready")

	// Warmup send: draining it to every member is the synchronization point
	// that leaves the per-org reader idle at the head before we sample the
	// counter — no sleep, and any coalesced registration wakes are absorbed.
	sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "warmup")
	for _, s := range subs {
		s.waitFor(t, "message.created")
	}

	before := readPumpQueries(t)
	encBefore := readEncoded(t)
	msgID := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "one to many")
	// Every LIVE member sees the one send, with the right id.
	for i, s := range subs {
		ev := s.waitFor(t, "message.created")
		var p struct {
			MessageID int64 `json:"message_id"`
		}
		_ = json.Unmarshal(ev.Payload, &p)
		if p.MessageID != msgID {
			t.Fatalf("connection %d got message id %d, want %d", i, p.MessageID, msgID)
		}
	}
	after := readPumpQueries(t)
	encAfter := readEncoded(t)

	// ACL carry-over: the outsider is a live fan target too (it received the
	// shared batch), but its own filter drops the channel event — so it stays
	// silent while every member got it.
	outsider.expectSilence(t, "message.created", 500*time.Millisecond)

	// THE marshal-once pin: the per-org reader encodes the event ONCE for the
	// whole org, so gateway_envelopes_encoded_total rises O(events) (~1 here),
	// NOT O(connections). Reverting deliverShared to marshal per connection
	// makes this delta ~conns — the red state this assertion guards (the CPU
	// twin of the pump-query O(1) pin above).
	encDelta := encAfter - encBefore
	if encDelta > float64(conns)/4 {
		t.Fatalf("envelope encodes rose by %g for one send to %d connections; want marshal-once O(events), not O(connections)",
			encDelta, conns)
	}
	if encDelta < 1 {
		t.Fatalf("envelope encodes did not rise (%g); the event must be marshaled once for the fan", encDelta)
	}
	t.Logf("marshal-once: 1 send → %d deliveries, envelope-encodes +%g (O(events))", conns, encDelta)

	// THE S3 pin: one send to `conns` live connections cost O(1) reads. The
	// per-org multicast reader runs ~once; per-connection pump would have run
	// ~conns times (reverting the reader to per-connection pump makes this
	// delta ~conns — the red state this assertion guards).
	delta := after - before
	if delta > float64(conns)/4 {
		t.Fatalf("pump queries rose by %g for one send to %d live connections; want O(1) per-org multicast, not O(N)",
			delta, conns)
	}
	if delta < 1 {
		t.Fatalf("pump queries did not rise (%g); the multicast reader must run its hoisted read per event-batch", delta)
	}
	t.Logf("multicast: 1 send → %d live deliveries, pump-queries +%g (O(1))", conns, delta)

	// Resume-lane carry-over: a connection that falls behind the org head
	// replays its gap through the per-connection pump, then rejoins the live
	// lane. Use a fresh member so the live pool above is undisturbed.
	resumeTok := bulkChannelMembers(t, ctx, pool, boot.OrgID, boot.ChannelID, "mcr", 1)[0]
	m := dialClientLast(t, ctx, ts.URL, resumeTok, "-1")
	m.waitFor(t, "ready")
	sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "before disconnect")
	seen := m.waitFor(t, "message.created")
	if seen.Seq == 0 {
		t.Fatalf("resume subject saw seq=0 for a live message")
	}
	_ = m.conn.CloseNow() // go away

	// Sent while away — the live members get it now; the disconnected one must
	// replay it on reconnect.
	missedID := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "missed while away")
	for _, s := range subs {
		s.waitFor(t, "message.created")
	}

	m2 := dialClientLast(t, ctx, ts.URL, resumeTok, fmt.Sprintf("%d", seen.Seq))
	defer m2.conn.CloseNow()
	m2.waitFor(t, "ready")
	replayed := m2.waitFor(t, "message.created")
	var rp struct {
		MessageID int64 `json:"message_id"`
	}
	_ = json.Unmarshal(replayed.Payload, &rp)
	if rp.MessageID != missedID {
		t.Fatalf("resume replay got message id %d, want %d (the gap message)", rp.MessageID, missedID)
	}
	if replayed.Seq <= seen.Seq {
		t.Fatalf("resume seq must advance past the resume point: %d <= %d", replayed.Seq, seen.Seq)
	}
}

// readPumpQueries reads the process-global gateway_pump_queries_total counter
// (published by the expvar metrics driver) — the same series the mega harness
// scrapes over /debug/vars, read here in-process for a before/after delta.
func readPumpQueries(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("gateway_pump_queries_total")
	if v == nil {
		return 0
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatalf("gateway_pump_queries_total is %T, want *expvar.Float", v)
	}
	return f.Value()
}

// readEncoded reads the process-global gateway_envelopes_encoded_total counter
// — the marshal-once signal, read in-process for a before/after delta.
func readEncoded(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("gateway_envelopes_encoded_total")
	if v == nil {
		return 0
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatalf("gateway_envelopes_encoded_total is %T, want *expvar.Float", v)
	}
	return f.Value()
}

// sendChannel posts one message to a channel and returns its id.
func sendChannel(t *testing.T, base, token string, channelID int64, content string) int64 {
	t.Helper()
	var out struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", base, channelID),
		token, map[string]any{"content": content}, &out)
	return out.MessageID
}

// bulkChannelMembers inserts n org members joined to the channel and mints a
// session for each — set-based, no role group or closure rebuild, because a
// receiver needs only channel membership (the fan-out ACL) plus a bearer
// token. Returns the tokens (prefix-<i>).
func bulkChannelMembers(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, channelID int64, prefix string, n int) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		SELECT $1, 1, $2 || g || '@mc.test', 'MC ' || g, 40
		FROM generate_series(1, $3) g
		RETURNING id`, orgID, prefix, n)
	if err != nil {
		t.Fatalf("bulk members: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatalf("scan member: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("member rows: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) SELECT $1, unnest($2::bigint[])`,
		channelID, ids); err != nil {
		t.Fatalf("join channel: %v", err)
	}
	tokens := make([]string, n)
	for i, id := range ids {
		tokens[i] = fmt.Sprintf("%s-tok-%d", prefix, i)
		if _, err := pool.Exec(ctx, `
			INSERT INTO auth_session (user_id, token_hash, expires_at)
			VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
			id, tokens[i]); err != nil {
			t.Fatalf("session: %v", err)
		}
	}
	return tokens
}

// bareOrgMember inserts an org member who joins NO channel (the ACL negative),
// with a session, and returns the token.
func bareOrgMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, email, name, token string) string {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 40) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("bare member: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("session: %v", err)
	}
	return token
}
