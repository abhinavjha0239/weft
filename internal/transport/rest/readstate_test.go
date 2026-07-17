package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestReadWatermarks: unread counts reflect the watermark, marking read clears
// them, the mark is monotone and clamped, and a user's own messages never
// count as unread.
func TestReadWatermarks(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	// S6: unread now rides the counter maintained on the notification consumer
	// pass, so this test drains that consumer before reading unreads.
	msgSvc := messaging.New(pool, perms.New(pool))
	runner := notification.NewRunner(pool, hub, slog.Default())
	runner.SetUnread(msgSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: msgSvc,
	}))
	defer ts.Close()

	// Owner bootstraps; a second member joins the channel.
	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "reads", "email": "o@r.test", "password": "password123",
		"full_name": "Owner",
	}, &boot)

	readerTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"reader@r.test", "Reader", "readertoken")

	chURL := fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, boot.ChannelID)

	// Owner posts 3 messages to the channel root.
	var last int64
	for i := 0; i < 3; i++ {
		var out struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, chURL+"/messages", boot.Token,
			map[string]any{"content": fmt.Sprintf("msg %d", i)}, &out)
		last = out.MessageID
	}
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)

	rootThreadID := channelRootThread(t, ctx, pool, boot.ChannelID)

	// Reader sees 3 unread; owner sees 0 (own messages don't count).
	if got := unreadForChannel(t, ts.URL, readerTok, boot.ChannelID); got != 3 {
		t.Fatalf("reader unread = %d, want 3", got)
	}
	if got := unreadForChannel(t, ts.URL, boot.Token, boot.ChannelID); got != 0 {
		t.Fatalf("owner unread = %d, want 0 (own messages)", got)
	}

	// Reader marks read up to the 2nd message → 1 unread remains.
	markRead(t, ts.URL, readerTok, rootThreadID, last-1)
	if got := unreadForChannel(t, ts.URL, readerTok, boot.ChannelID); got != 1 {
		t.Fatalf("after partial read, unread = %d, want 1", got)
	}

	// Mark all read (up_to<=0 → latest) → 0 unread.
	markRead(t, ts.URL, readerTok, rootThreadID, 0)
	if got := unreadForChannel(t, ts.URL, readerTok, boot.ChannelID); got != 0 {
		t.Fatalf("after full read, unread = %d, want 0", got)
	}

	// Monotone: a stale mark (older id) must not resurrect unreads.
	markRead(t, ts.URL, readerTok, rootThreadID, last-2)
	if got := unreadForChannel(t, ts.URL, readerTok, boot.ChannelID); got != 0 {
		t.Fatalf("stale mark rewound watermark: unread = %d, want 0", got)
	}

	// Clamp: marking far past the head is capped at the real head (no error,
	// no over-advance). A new message then shows exactly 1 unread.
	markRead(t, ts.URL, readerTok, rootThreadID, 1<<40)
	var out struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, chURL+"/messages", boot.Token, map[string]any{"content": "new one"}, &out)
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	if got := unreadForChannel(t, ts.URL, readerTok, boot.ChannelID); got != 1 {
		t.Fatalf("after clamp + 1 new message, unread = %d, want 1", got)
	}
}

func unreadForChannel(t *testing.T, base, token string, channelID int64) int {
	t.Helper()
	var resp struct {
		Channels []struct {
			ChannelID   int64 `json:"channel_id"`
			UnreadCount int   `json:"unread_count"`
		} `json:"channels"`
	}
	if code := getJSON(t, base+"/api/v1/unreads", token, &resp); code != 200 {
		t.Fatalf("unreads: %d", code)
	}
	for _, c := range resp.Channels {
		if c.ChannelID == channelID {
			return c.UnreadCount
		}
	}
	return 0
}

func markRead(t *testing.T, base, token string, threadID, upTo int64) {
	t.Helper()
	code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/read", base, threadID),
		token, map[string]any{"up_to": upTo})
	if code != 200 {
		t.Fatalf("mark read: %d", code)
	}
}

func addChannelMember(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, channelID int64, email, name, token string) string {
	t.Helper()
	var uid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, 1, $2, $3, 40) RETURNING id`, orgID, email, name).Scan(&uid); err != nil {
		t.Fatalf("member: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`,
		channelID, uid); err != nil {
		t.Fatalf("join: %v", err)
	}
	// Grant the default member role so the user has send_message etc., then
	// rebuild the org's group closure (perms resolve through it).
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT id, $2 FROM user_group WHERE org_id = $1 AND name = 'role:members'`,
		orgID, uid); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return perms.New(pool).RebuildClosure(ctx, tx, orgID)
	}); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO auth_session (user_id, token_hash, expires_at)
		VALUES ($1, encode(sha256($2::bytea), 'hex'), now() + interval '1 day')`,
		uid, token); err != nil {
		t.Fatalf("session: %v", err)
	}
	return token
}

func channelRootThread(t *testing.T, ctx context.Context, pool *pgxpool.Pool, channelID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT root_thread_id FROM channel WHERE id = $1`, channelID).Scan(&id); err != nil {
		t.Fatalf("root thread: %v", err)
	}
	return id
}
