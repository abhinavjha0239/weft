package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestUnreadCounters is the S6 (F-17 twin) proof: the O(1) counter equals the
// live aggregate across sends / mark-reads / deletes on both container kinds,
// the mention badge finally lights, the /unreads read scans ZERO message rows
// (no re-aggregation over messages), and the reconciliation sweep repairs a
// corrupted counter and Warn-logs it. Every expected value is derived from the
// test's own actions and independently cross-checked against a live aggregate
// computed HERE — never read back from the counter code under test.
func TestUnreadCounters(t *testing.T) {
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
	go hub.Run(ctx)
	msgSvc := messaging.New(pool, perms.New(pool))
	runner := notification.NewRunner(pool, hub, slog.Default())
	runner.SetUnread(msgSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: msgSvc,
		DM:        dm.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "s6", "email": "alice@s6.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@s6.test", "Bob Ray", "bobs6tok")
	var bobID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id=$1 AND email='bob@s6.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	c1 := boot.ChannelID
	c1Root := channelRootThread(t, ctx, pool, c1)
	drain := func() { drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg) }
	const bulkN = 60
	var c2 int64 // the high-volume channel, shared by the O(1) and reconcile pins

	// The subtests are INDEPENDENT (t.Run isolates a Fatalf), so one run under
	// the "point Unreads back at the live aggregate" revert shows BOTH the
	// mention-badge and the scan-cost pins going red while the correctness
	// subtests (results equal) stay green — the proof the pins distinguish the
	// implementations rather than just re-reading the code under test.

	t.Run("channel_correctness", func(t *testing.T) {
		m1 := sendChannel(t, ts.URL, boot.Token, c1, "one")
		_ = sendChannel(t, ts.URL, boot.Token, c1, "two")
		m3 := sendChannel(t, ts.URL, boot.Token, c1, "three")
		drain()
		// bob (recipient) sees 3; alice (author) sees 0 — own never count.
		assertChannelUnread(t, ts.URL, bobTok, c1, 3, false)
		assertLiveEquals(t, ctx, pool, bobID, c1, 3)
		assertChannelUnread(t, ts.URL, boot.Token, c1, 0, false)
		assertLiveEquals(t, ctx, pool, boot.UserID, c1, 0)

		// bob reads up to m2 → 1 unread (m3); counter matches the aggregate.
		markRead(t, ts.URL, bobTok, c1Root, m1+1)
		assertChannelUnread(t, ts.URL, bobTok, c1, 1, false)
		assertLiveEquals(t, ctx, pool, bobID, c1, 1)

		// alice deletes m3 (the one unread) → 0; delete decrements in-tx.
		if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, m3), boot.Token); code != 200 {
			t.Fatalf("delete m3 = %d, want 200", code)
		}
		assertChannelUnread(t, ts.URL, bobTok, c1, 0, false)
		assertLiveEquals(t, ctx, pool, bobID, c1, 0)
	})

	t.Run("mention_badge", func(t *testing.T) {
		sendChannel(t, ts.URL, boot.Token, c1, "@**Bob Ray** ping") // m4: mentions bob
		drain()
		assertChannelUnread(t, ts.URL, bobTok, c1, 1, true) // Mentioned now TRUE
		if u, m := counterRow(t, ctx, pool, bobID, c1); u != 1 || m != 1 {
			t.Fatalf("after mention, counter (unread,mention) = (%d,%d), want (1,1)", u, m)
		}
		sendChannel(t, ts.URL, boot.Token, c1, "hello again") // m5: no mention
		drain()
		// Badge stays lit while the mention is unread; unread grows to 2.
		assertChannelUnread(t, ts.URL, bobTok, c1, 2, true)
		assertLiveEquals(t, ctx, pool, bobID, c1, 2)
	})

	t.Run("dm_correctness", func(t *testing.T) {
		var opened dm.Summary
		postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
			map[string]any{"user_ids": []int64{bobID}}, &opened)
		sendThread(t, ts.URL, boot.Token, opened.RootThreadID, "dm one")
		sendThread(t, ts.URL, boot.Token, opened.RootThreadID, "dm two")
		drain()
		if got := dmUnread(t, ts.URL, bobTok, opened.ID); got != 2 {
			t.Fatalf("bob DM unread = %d, want 2", got)
		}
		assertLiveEqualsDM(t, ctx, pool, bobID, opened.ID, 2)
		markRead(t, ts.URL, bobTok, opened.RootThreadID, 0) // read to head → 0
		if got := dmUnread(t, ts.URL, bobTok, opened.ID); got != 0 {
			t.Fatalf("after DM mark-read, bob DM unread = %d, want 0", got)
		}
		assertLiveEqualsDM(t, ctx, pool, bobID, opened.ID, 0)
	})

	t.Run("o1_read_no_message_scan", func(t *testing.T) {
		// A high-volume channel (bounded for CI) the live aggregate would
		// re-scan message-by-message.
		var ch struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
			map[string]any{"name": "bulk", "visibility": "public"}, &ch)
		c2 = ch.ChannelID
		if c2 == 0 {
			t.Fatal("bulk channel not created")
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`, c2, bobID); err != nil {
			t.Fatalf("bob join bulk: %v", err)
		}
		for i := 0; i < bulkN; i++ {
			sendChannel(t, ts.URL, boot.Token, c2, fmt.Sprintf("bulk %d", i))
		}
		drain()

		// Trace the ACTUAL Unreads query and EXPLAIN ANALYZE it: the counter
		// read touches container_unread_counter only (0 message rows).
		// Pointing Unreads back at the live aggregate scans ~bulkN message rows
		// here — the RED — while the RESULT (bulk count) stays equal.
		cap := &sqlCapture{}
		tcfg, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			t.Fatalf("trace cfg: %v", err)
		}
		tcfg.ConnConfig.Tracer = cap
		tracedPool, err := pgxpool.NewWithConfig(ctx, tcfg)
		if err != nil {
			t.Fatalf("trace pool: %v", err)
		}
		defer tracedPool.Close()
		if _, err := tracedPool.Exec(ctx, `SELECT 1`); err != nil { // warm the conn
			t.Fatalf("warm: %v", err)
		}
		tracedMsg := messaging.New(tracedPool, perms.New(tracedPool))
		bobActor := auth.Identity{UserID: bobID, OrgID: boot.OrgID}

		cap.reset()
		unreads, err := tracedMsg.Unreads(ctx, bobActor)
		if err != nil {
			t.Fatalf("traced unreads: %v", err)
		}
		// Result correctness (stays equal under the revert): bulk shows bulkN.
		got := 0
		for _, u := range unreads {
			if u.ChannelID == c2 {
				got = u.UnreadCount
			}
		}
		if got != bulkN {
			t.Fatalf("bulk unread via counter = %d, want %d", got, bulkN)
		}
		// Cost: the read scanned zero message rows.
		scanned := 0
		for _, q := range cap.queries {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(q.sql)), "SELECT") {
				scanned += messageRowsScanned(t, ctx, pool, q.sql, q.args)
			}
		}
		if scanned != 0 {
			t.Fatalf("O(1) pin: /unreads scanned %d message rows, want 0 "+
				"(a per-message re-aggregation crept back in)", scanned)
		}
	})

	t.Run("markread_o_delta", func(t *testing.T) {
		if c2 == 0 {
			t.Skip("bulk channel not built (o1 subtest did not run)")
		}
		// A small thread inside the bulk channel: marking IT read must cost
		// O(its slice), even though the channel's root thread holds bulkN
		// unread messages — an O(container-history) recompute inside the
		// MarkRead tx is the RED this pin guards (mark-read is the
		// highest-volume user action).
		var th struct {
			ThreadID int64 `json:"thread_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, c2),
			boot.Token, map[string]any{"title": "delta", "content": "d1"}, &th)
		d2 := sendThread(t, ts.URL, boot.Token, th.ThreadID, "d2")
		_ = sendThread(t, ts.URL, boot.Token, th.ThreadID, "d3")
		drain()
		if u, _ := counterRow(t, ctx, pool, bobID, c2); u != bulkN+3 {
			t.Fatalf("bulk counter before mark = %d, want %d", u, bulkN+3)
		}

		// Trace the ACTUAL MarkRead the service runs (partial mark, up to d2:
		// a 2-message delta) and EXPLAIN its reads: message-relation rows
		// touched must be O(the marked slice) — the thread-head lookup plus
		// the delta count — never the container's history.
		cap := &sqlCapture{}
		tcfg, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			t.Fatalf("trace cfg: %v", err)
		}
		tcfg.ConnConfig.Tracer = cap
		tracedPool, err := pgxpool.NewWithConfig(ctx, tcfg)
		if err != nil {
			t.Fatalf("trace pool: %v", err)
		}
		defer tracedPool.Close()
		tracedMsg := messaging.New(tracedPool, perms.New(tracedPool))
		bobActor := auth.Identity{UserID: bobID, OrgID: boot.OrgID}
		cap.reset()
		if _, err := tracedMsg.MarkRead(ctx, bobActor, th.ThreadID, d2); err != nil {
			t.Fatalf("traced mark-read: %v", err)
		}
		scanned := 0
		for _, q := range cap.queries {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(q.sql)), "SELECT") {
				scanned += messageRowsScanned(t, ctx, pool, q.sql, q.args)
			}
		}
		if scanned > 10 {
			t.Fatalf("MarkRead scanned %d message rows for a 2-message delta in a "+
				"%d-message channel, want O(delta) <= 10 (an O(container-history) "+
				"recompute crept back into the mark-read tx)", scanned, bulkN+3)
		}
		// Correctness of the delta: 2 read → bulkN+1 remain, equal to the
		// independently-computed live aggregate.
		if u, _ := counterRow(t, ctx, pool, bobID, c2); u != bulkN+1 {
			t.Fatalf("bulk counter after partial mark = %d, want %d", u, bulkN+1)
		}
		assertLiveEquals(t, ctx, pool, bobID, c2, bulkN+1)
		// Full mark of the thread (monotone, to head) → back to bulkN.
		markRead(t, ts.URL, bobTok, th.ThreadID, 0)
		if u, _ := counterRow(t, ctx, pool, bobID, c2); u != bulkN {
			t.Fatalf("bulk counter after full mark = %d, want %d", u, bulkN)
		}
		assertLiveEquals(t, ctx, pool, bobID, c2, bulkN)
	})

	t.Run("reconcile_repairs_and_warns", func(t *testing.T) {
		if c2 == 0 {
			t.Skip("bulk channel not built (o1 subtest did not run)")
		}
		if _, err := pool.Exec(ctx,
			`UPDATE container_unread_counter SET unread_count = 4242
			 WHERE user_id = $1 AND channel_id = $2`, bobID, c2); err != nil {
			t.Fatalf("corrupt: %v", err)
		}
		var logBuf bytes.Buffer
		reconMsg := messaging.New(pool, perms.New(pool))
		reconMsg.SetLogger(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		if err := reconMsg.ReconcileUnreadOnce(ctx); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		// Repaired back to the live truth (bulkN, none read).
		if u, _ := counterRow(t, ctx, pool, bobID, c2); u != bulkN {
			t.Fatalf("after reconcile, bulk counter = %d, want %d (repair failed)", u, bulkN)
		}
		logs := logBuf.String()
		if !strings.Contains(logs, "unread counter diverged (repaired)") ||
			!strings.Contains(logs, "stored=4242") {
			t.Fatalf("reconcile did not Warn the divergence; log =\n%s", logs)
		}
	})
}

// sqlCapture is a pgx QueryTracer that records every query's SQL + args, so
// the O(1) pin measures the ACTUAL read the method issues (not a hand-copied
// string the revert wouldn't change).
type sqlCapture struct {
	mu      sync.Mutex
	queries []capturedQuery
}

type capturedQuery struct {
	sql  string
	args []any
}

func (c *sqlCapture) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	c.mu.Lock()
	c.queries = append(c.queries, capturedQuery{sql: data.SQL, args: data.Args})
	c.mu.Unlock()
	return ctx
}

func (c *sqlCapture) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (c *sqlCapture) reset() {
	c.mu.Lock()
	c.queries = nil
	c.mu.Unlock()
}

// messageRowsScanned EXPLAIN (ANALYZE) runs sql and sums the actual rows read
// from the `message` relation across the whole plan — the direct measure of
// per-message re-aggregation. Zero means the query never touched a message row.
func messageRowsScanned(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args []any) int {
	t.Helper()
	var js []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+sql, args...).Scan(&js); err != nil {
		t.Fatalf("explain: %v (sql=%q)", err, sql)
	}
	var top []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal(js, &top); err != nil {
		t.Fatalf("explain json: %v", err)
	}
	sum := 0
	var walk func(n planNode)
	walk = func(n planNode) {
		if n.RelationName == "message" {
			sum += int(n.ActualRows)
		}
		for _, c := range n.Plans {
			walk(c)
		}
	}
	for _, p := range top {
		walk(p.Plan)
	}
	return sum
}

type planNode struct {
	RelationName string     `json:"Relation Name"`
	ActualRows   float64    `json:"Actual Rows"`
	Plans        []planNode `json:"Plans"`
}

// --- helpers deriving expectations independently of the counter code ---

func sendThread(t *testing.T, base, token string, threadID int64, content string) int64 {
	t.Helper()
	var out struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", base, threadID),
		token, map[string]any{"content": content}, &out)
	return out.MessageID
}

func assertChannelUnread(t *testing.T, base, token string, channelID int64, wantUnread int, wantMention bool) {
	t.Helper()
	var resp struct {
		Channels []messaging.ChannelUnread `json:"channels"`
	}
	if code := getJSON(t, base+"/api/v1/unreads", token, &resp); code != 200 {
		t.Fatalf("unreads: %d", code)
	}
	for _, c := range resp.Channels {
		if c.ChannelID == channelID {
			if c.UnreadCount != wantUnread || c.Mentioned != wantMention {
				t.Fatalf("channel %d unread=(%d,mention=%v), want (%d,%v)",
					channelID, c.UnreadCount, c.Mentioned, wantUnread, wantMention)
			}
			return
		}
	}
	if wantUnread != 0 || wantMention {
		t.Fatalf("channel %d absent from unreads, want unread=%d mention=%v",
			channelID, wantUnread, wantMention)
	}
}

func dmUnread(t *testing.T, base, token string, dmSpaceID int64) int {
	t.Helper()
	var resp struct {
		DMs []messaging.DMUnread `json:"dms"`
	}
	if code := getJSON(t, base+"/api/v1/unreads", token, &resp); code != 200 {
		t.Fatalf("unreads: %d", code)
	}
	for _, d := range resp.DMs {
		if d.DMSpaceID == dmSpaceID {
			return d.UnreadCount
		}
	}
	return 0
}

func counterRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, channelID int64) (unread, mention int) {
	t.Helper()
	err := pool.QueryRow(ctx, `
		SELECT unread_count, mention_count FROM container_unread_counter
		WHERE user_id = $1 AND channel_id = $2`, userID, channelID).Scan(&unread, &mention)
	if err == pgx.ErrNoRows {
		return 0, 0
	}
	if err != nil {
		t.Fatalf("counter row: %v", err)
	}
	return unread, mention
}

// liveUnreadChannel is the independent oracle: unread = messages in the channel
// after the user's per-thread watermarks, authored by someone else, still
// visible. Written here (not the counter code) so counter == aggregate is a
// real cross-check of the incremental maintenance.
func liveUnreadChannel(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, channelID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_member cm
		JOIN message m ON m.channel_id = cm.channel_id
		     AND m.author_id <> cm.user_id AND m.deleted_at IS NULL
		LEFT JOIN thread_read_watermark w
		     ON w.user_id = cm.user_id AND w.thread_id = m.thread_id
		WHERE cm.user_id = $1 AND cm.channel_id = $2 AND cm.unsubscribed_at IS NULL
		  AND m.id > COALESCE(w.last_read_message_id, 0)`,
		userID, channelID).Scan(&n); err != nil {
		t.Fatalf("live aggregate: %v", err)
	}
	return n
}

func assertLiveEquals(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, channelID int64, want int) {
	t.Helper()
	if got := liveUnreadChannel(t, ctx, pool, userID, channelID); got != want {
		t.Fatalf("live aggregate channel %d = %d, want %d (counter/aggregate disagree)", channelID, got, want)
	}
}

func liveUnreadDM(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, dmSpaceID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM dm_participant dp
		JOIN message m ON m.dm_space_id = dp.dm_space_id
		     AND m.author_id <> dp.user_id AND m.deleted_at IS NULL
		LEFT JOIN thread_read_watermark w
		     ON w.user_id = dp.user_id AND w.thread_id = m.thread_id
		WHERE dp.user_id = $1 AND dp.dm_space_id = $2
		  AND m.id > COALESCE(w.last_read_message_id, 0)`,
		userID, dmSpaceID).Scan(&n); err != nil {
		t.Fatalf("live aggregate dm: %v", err)
	}
	return n
}

func assertLiveEqualsDM(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, dmSpaceID int64, want int) {
	t.Helper()
	if got := liveUnreadDM(t, ctx, pool, userID, dmSpaceID); got != want {
		t.Fatalf("live aggregate dm %d = %d, want %d", dmSpaceID, got, want)
	}
}
