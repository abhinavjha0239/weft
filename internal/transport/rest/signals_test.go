package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// wsClient drains its connection in a background goroutine into an events
// channel. Assertions time out on the channel, never on conn.Read — which
// matters because coder/websocket closes the connection when a Read context
// expires, so "peek-with-timeout then reuse" is impossible on the raw conn.
type wsClient struct {
	conn   *websocket.Conn
	events chan gateway.Envelope
}

func dialClient(t *testing.T, ctx context.Context, base, token string) *wsClient {
	t.Helper()
	u := strings.Replace(base, "http://", "ws://", 1) +
		"/api/v1/gateway?last_id=0&token=" + token
	conn, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	c := &wsClient{conn: conn, events: make(chan gateway.Envelope, 64)}
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				close(c.events)
				return
			}
			var e gateway.Envelope
			if json.Unmarshal(data, &e) == nil {
				c.events <- e
			}
		}
	}()
	return c
}

func (c *wsClient) send(t *testing.T, ctx context.Context, frame map[string]any) {
	t.Helper()
	b, _ := json.Marshal(frame)
	if err := c.conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func (c *wsClient) waitFor(t *testing.T, wantType string) gateway.Envelope {
	t.Helper()
	deadline := time.After(8 * time.Second)
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatalf("connection closed while waiting for %q", wantType)
			}
			if e.Type == wantType {
				return e
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q", wantType)
		}
	}
}

func (c *wsClient) expectSilence(t *testing.T, badType string, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				return
			}
			if e.Type == badType {
				t.Fatalf("received %q on a connection that must not get it", badType)
			}
		case <-deadline:
			return
		}
	}
}

func TestEphemeralSignals(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sig", "email": "a@s.test", "password": "password123",
		"full_name": "Alice",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"b@s.test", "Bob", "bobtoken")

	// Alice on two devices, Bob on one; wait for each `ready`.
	alice1 := dialClient(t, ctx, ts.URL, boot.Token)
	defer alice1.conn.CloseNow()
	alice2 := dialClient(t, ctx, ts.URL, boot.Token)
	defer alice2.conn.CloseNow()
	bob := dialClient(t, ctx, ts.URL, bobTok)
	defer bob.conn.CloseNow()
	alice1.waitFor(t, "ready")
	alice2.waitFor(t, "ready")
	bob.waitFor(t, "ready")

	// Typing: Alice → Bob sees it (ephemeral, seq=0); Alice's own device does not.
	alice1.send(t, ctx, map[string]any{"type": "typing", "channel_id": boot.ChannelID, "state": "start"})
	ev := bob.waitFor(t, "typing.started")
	if ev.Seq != 0 {
		t.Fatalf("ephemeral envelope must carry seq=0, got %d", ev.Seq)
	}
	var tp struct {
		ChannelID int64 `json:"channel_id"`
		UserID    int64 `json:"user_id"`
	}
	_ = json.Unmarshal(ev.Payload, &tp)
	if tp.ChannelID != boot.ChannelID || tp.UserID == 0 {
		t.Fatalf("typing payload = %+v", tp)
	}
	alice2.expectSilence(t, "typing.started", 500*time.Millisecond)

	// Typing into a channel the sender is not a member of is dropped.
	bob.send(t, ctx, map[string]any{"type": "typing", "channel_id": boot.ChannelID + 999, "state": "start"})
	alice1.expectSilence(t, "typing.started", 500*time.Millisecond)

	// read_marker: message exists; Alice marks read on device 1 → device 2
	// gets readstate.synced; Bob must not.
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		bobTok, map[string]any{"content": "unread me"}, nil)
	rootID := channelRootThread(t, ctx, pool, boot.ChannelID)

	alice1.send(t, ctx, map[string]any{"type": "read_marker", "thread_id": rootID, "up_to": 0})
	sync := alice2.waitFor(t, "readstate.synced")
	var rs struct {
		ThreadID int64 `json:"thread_id"`
		LastRead int64 `json:"last_read_message_id"`
	}
	_ = json.Unmarshal(sync.Payload, &rs)
	if rs.ThreadID != rootID || rs.LastRead == 0 {
		t.Fatalf("readstate payload = %+v", rs)
	}
	bob.expectSilence(t, "readstate.synced", 500*time.Millisecond)

	// Durable side effect via the same service: Alice's unreads are now 0.
	if got := unreadForChannel(t, ts.URL, boot.Token, boot.ChannelID); got != 0 {
		t.Fatalf("after ws read_marker, unread = %d, want 0", got)
	}

	// Malformed frame → error envelope, connection survives.
	if err := alice1.conn.Write(ctx, websocket.MessageText, []byte("{not json")); err != nil {
		t.Fatalf("write malformed: %v", err)
	}
	alice1.waitFor(t, "error")
	alice1.send(t, ctx, map[string]any{"type": "typing", "channel_id": boot.ChannelID, "state": "stop"})
	bob.waitFor(t, "typing.stopped")
}
