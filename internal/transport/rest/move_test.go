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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

func moveMsg(t *testing.T, base, token string, msgID, threadID int64) int {
	t.Helper()
	return postJSONStatus(t, fmt.Sprintf("%s/api/v1/messages/%d/move", base, msgID),
		token, map[string]any{"thread_id": threadID})
}

// TestMoveMessage: P-04 — Zulip-style intra-channel move as a kind-3 revision,
// gated like delete (author or moderate_messages), with both threads'
// denormalized counters fixed and a message.moved event carrying the channel
// (gateway routing) and both thread ids.
func TestMoveMessage(t *testing.T) {
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
		"org_slug": "mov", "email": "a@mov.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@mov.test", "Bob Ray", "bobmovtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@mov.test", "Charlie Kim", "charliemovtok")

	createThread := func(title, content string) (int64, int64) {
		var r struct {
			ThreadID      int64 `json:"thread_id"`
			RootMessageID int64 `json:"root_message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"title": title, "content": content}, &r)
		return r.ThreadID, r.RootMessageID
	}
	sendTo := func(token string, threadID int64, content string) int64 {
		var m struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, threadID),
			token, map[string]any{"content": content}, &m)
		return m.MessageID
	}
	threadCount := func(id int64) int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT message_count FROM thread WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("thread count %d: %v", id, err)
		}
		return n
	}
	msgThread := func(id int64) int64 {
		var th int64
		if err := pool.QueryRow(ctx, `SELECT thread_id FROM message WHERE id = $1`, id).Scan(&th); err != nil {
			t.Fatalf("msg thread %d: %v", id, err)
		}
		return th
	}
	lastActivity := func(id int64) time.Time {
		var ts time.Time
		if err := pool.QueryRow(ctx, `SELECT last_activity_at FROM thread WHERE id = $1`, id).Scan(&ts); err != nil {
			t.Fatalf("last activity %d: %v", id, err)
		}
		return ts
	}

	threadA, rootA := createThread("Topic A", "root of A")
	threadB, _ := createThread("Topic B", "root of B")
	msgX := sendTo(boot.Token, threadA, "movable message")
	if threadCount(threadA) != 2 || threadCount(threadB) != 1 {
		t.Fatalf("pre-move counts A=%d B=%d, want 2 and 1", threadCount(threadA), threadCount(threadB))
	}
	bBefore := lastActivity(threadB)

	// Author move: alice relocates her own message A→B.
	if code := moveMsg(t, ts.URL, boot.Token, msgX, threadB); code != http.StatusOK {
		t.Fatalf("author move = %d, want 200", code)
	}
	if got := msgThread(msgX); got != threadB {
		t.Fatalf("msgX thread = %d, want %d", got, threadB)
	}
	// Revision kind 3 records where it came from.
	var revs int
	var prevThread int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(max(prev_thread_id), 0) FROM message_revision
		WHERE message_id = $1 AND kind = 3`, msgX).Scan(&revs, &prevThread); err != nil {
		t.Fatalf("revision query: %v", err)
	}
	if revs != 1 || prevThread != threadA {
		t.Fatalf("move revision = %d rows, prev_thread=%d, want 1 and %d", revs, prevThread, threadA)
	}
	// Both counters fixed; the target's activity bumped forward.
	if threadCount(threadA) != 1 || threadCount(threadB) != 2 {
		t.Fatalf("post-move counts A=%d B=%d, want 1 and 2", threadCount(threadA), threadCount(threadB))
	}
	if !lastActivity(threadB).After(bBefore) {
		t.Fatalf("target last_activity not bumped: %v !> %v", lastActivity(threadB), bBefore)
	}
	// Event payload routes by channel and names both threads.
	var evChan, evFrom, evTo int64
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'channel_id')::bigint, (payload->>'from_thread_id')::bigint,
		       (payload->>'to_thread_id')::bigint
		FROM event_log
		WHERE verb = 'message.moved' AND (payload->>'message_id')::bigint = $1`,
		msgX).Scan(&evChan, &evFrom, &evTo); err != nil {
		t.Fatalf("moved event: %v", err)
	}
	if evChan != boot.ChannelID || evFrom != threadA || evTo != threadB {
		t.Fatalf("moved payload chan=%d from=%d to=%d, want %d/%d/%d",
			evChan, evFrom, evTo, boot.ChannelID, threadA, threadB)
	}

	// Same target thread → 400.
	if code := moveMsg(t, ts.URL, boot.Token, msgX, threadB); code != http.StatusBadRequest {
		t.Fatalf("same-thread move = %d, want 400", code)
	}
	// A thread's root message anchors it → 400.
	if code := moveMsg(t, ts.URL, boot.Token, rootA, threadB); code != http.StatusBadRequest {
		t.Fatalf("root-message move = %d, want 400", code)
	}

	// A plain member cannot move another's message (no moderate_messages) → 403.
	msgY := sendTo(boot.Token, threadA, "another message")
	if code := moveMsg(t, ts.URL, bobTok, msgY, threadB); code != http.StatusForbidden {
		t.Fatalf("member move = %d, want 403", code)
	}
	if got := msgThread(msgY); got != threadA {
		t.Fatalf("403 move must not relocate: msgY thread = %d", got)
	}
	// Promote charlie to moderator; the same move now succeeds.
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT g.id, u.id FROM user_group g, user_account u
		WHERE g.org_id = $1 AND g.name = 'role:moderators' AND u.email = 'charlie@mov.test'`,
		boot.OrgID); err != nil {
		t.Fatalf("promote charlie: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return permsSvc.RebuildClosure(ctx, tx, boot.OrgID)
	}); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if code := moveMsg(t, ts.URL, charlieTok, msgY, threadB); code != http.StatusOK {
		t.Fatalf("moderator move = %d, want 200", code)
	}
	if got := msgThread(msgY); got != threadB {
		t.Fatalf("moderator move failed to relocate: msgY thread = %d", got)
	}

	// Cross-channel target → 400 (v1 is intra-channel only).
	var ch2 struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "second"}, &ch2)
	var threadC struct {
		ThreadID int64 `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, ch2.ChannelID),
		boot.Token, map[string]any{"title": "C", "content": "root C"}, &threadC)
	msgZ := sendTo(boot.Token, threadA, "stays home")
	if code := moveMsg(t, ts.URL, boot.Token, msgZ, threadC.ThreadID); code != http.StatusBadRequest {
		t.Fatalf("cross-channel move = %d, want 400", code)
	}

	// DM message → 400 (channel messages only), even for its author.
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@mov.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	dmMsg := sendTo(boot.Token, conv.RootThreadID, "dm hi")
	if code := moveMsg(t, ts.URL, boot.Token, dmMsg, threadB); code != http.StatusBadRequest {
		t.Fatalf("DM move = %d, want 400", code)
	}

	// Only the two successful moves emitted events; every rejection was silent.
	var moved int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'message.moved'`,
		boot.OrgID).Scan(&moved); err != nil || moved != 2 {
		t.Fatalf("message.moved events = %d (%v), want 2", moved, err)
	}
}
