package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestGatewayReadACL pins the realtime read ACL against real Postgres and real
// WebSockets (P-46). The load-bearing shape is the NEGATIVE, written the
// P-33/P-34 way: every connection that must NOT receive an event is a LIVE fan
// target that demonstrably receives a LATER event on the same stream. Because
// the stream is ordered by event id, seeing the later event proves the earlier
// one was already offered to that connection and dropped by its own filter —
// so no assertion here can pass vacuously by "nothing was ever sent".
//
// Subtests:
//   - "protected history floor": ADR-008 C-2 / F-16b. A member of a PROTECTED
//     channel who resumes from before their join must not replay pre-join
//     events, while the SAME user's shared-history channel replays fully, and
//     the boundary survives leave→rejoin (the preserved channel_member row).
//   - "membership refresh mid-connection": a live connection that joins a
//     channel starts receiving it without reconnecting, and received nothing
//     from it before.
func TestGatewayReadACL(t *testing.T) {
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
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "acl", "email": "alice@acl.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	t.Run("protected history floor", func(t *testing.T) {
		// #ledger is private+protected (history_mode 2): each invited member's
		// view starts at their join stamp. #general (bootstrap) is shared.
		var ledger struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{
			"name": "ledger", "visibility": "private", "protected": true}, &ledger)

		// Two messages BEFORE mia exists: one in the protected channel (which
		// her floor must hide) and one in the shared channel (which it must
		// NOT — the contrast that proves the floor is per-channel, not "mia
		// sees no history at all").
		genPre := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "shared pre-join")
		protPre := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "protected pre-join")

		var inv identity.Invite
		postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
			"role": 40, "channel_ids": []int64{ledger.ChannelID}}, &inv)
		var mia identity.AcceptInviteResult
		postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
			"token": inv.Token, "email": "mia@acl.test", "password": "password123",
			"full_name": "Mia New"}, &mia)
		// The stamp is the boundary under test; assert it exists so a schema
		// change that stops stamping cannot silently make this test vacuous.
		var stamp *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT history_from FROM channel_member
			WHERE channel_id = $1 AND user_id = $2`,
			ledger.ChannelID, mia.UserID).Scan(&stamp); err != nil || stamp == nil {
			t.Fatalf("mia history_from = %v (%v), want stamped", stamp, err)
		}

		protPost := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "protected post-join")

		// Full replay from the very beginning of the org's log.
		mc := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer mc.conn.CloseNow()
		seen := drainMessagesUntil(t, mc, protPost)
		if slices.Contains(seen, protPre) {
			t.Fatalf("protected pre-join message %d replayed to a member who joined after it; got %v",
				protPre, seen)
		}
		if !slices.Contains(seen, genPre) {
			t.Fatalf("shared-history pre-join message %d was withheld; the floor must be per-channel. got %v",
				genPre, seen)
		}

		// The boundary INSTANT itself. history_from is a timestamp and the
		// stream is ordered by id, so the only way to construct an event at
		// exactly the stamp is to backdate its domain time — which is also the
		// only way to pin inclusive-vs-exclusive, the off-by-one that is a
		// silent leak one way and a silent gap the other. REST's rule is
		// `created_at >= history_from` (messaging.ListMessages), so an event AT
		// the stamp is VISIBLE; the gateway must answer the same.
		atBoundary := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "exactly at the stamp")
		if ct, err := pool.Exec(ctx, `
			UPDATE event_log SET occurred_at = $2
			WHERE verb = 'message.created' AND (payload->>'message_id')::bigint = $1`,
			atBoundary, *stamp); err != nil || ct.RowsAffected() != 1 {
			t.Fatalf("backdate to the boundary: %v rows (%v)", ct.RowsAffected(), err)
		}
		after := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "strictly after")
		bc := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer bc.conn.CloseNow()
		seenB := drainMessagesUntil(t, bc, after)
		if !slices.Contains(seenB, atBoundary) {
			t.Fatalf("message %d at exactly history_from was withheld; the floor must be INCLUSIVE (REST shows it). got %v",
				atBoundary, seenB)
		}

		// Leave and rejoin the SAME preserved membership row (#123 shape): the
		// original stamp survives, so the pre-join message stays hidden even
		// though the rejoin is brand new.
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, ledger.ChannelID),
			mia.Token, map[string]any{}); code != http.StatusOK {
			t.Fatalf("mia leave = %d, want 200", code)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE channel_member SET unsubscribed_at = NULL
			WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NOT NULL`,
			ledger.ChannelID, mia.UserID); err != nil {
			t.Fatalf("reactivate: %v", err)
		}
		var afterRejoin *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT history_from FROM channel_member
			WHERE channel_id = $1 AND user_id = $2`,
			ledger.ChannelID, mia.UserID).Scan(&afterRejoin); err != nil ||
			afterRejoin == nil || !afterRejoin.Equal(*stamp) {
			t.Fatalf("history_from after rejoin = %v (%v), want the preserved %v",
				afterRejoin, err, stamp)
		}
		rejoinPost := sendChannel(t, ts.URL, boot.Token, ledger.ChannelID, "after rejoin")

		mc2 := dialClientLast(t, ctx, ts.URL, mia.Token, "0")
		defer mc2.conn.CloseNow()
		seen2 := drainMessagesUntil(t, mc2, rejoinPost)
		if slices.Contains(seen2, protPre) {
			t.Fatalf("rejoin lost the preserved floor: pre-join message %d replayed; got %v",
				protPre, seen2)
		}
		if !slices.Contains(seen2, protPost) {
			t.Fatalf("post-join message %d missing after rejoin; got %v", protPost, seen2)
		}
	})

	t.Run("membership refresh mid-connection", func(t *testing.T) {
		bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
			"bob@acl.test", "Bob Ray", "acl-bob-tok")
		var ops struct {
			ChannelID int64 `json:"channel_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
			map[string]any{"name": "ops"}, &ops)

		bob := dialClientLast(t, ctx, ts.URL, bobTok, "-1")
		defer bob.conn.CloseNow()
		bob.waitFor(t, "ready")

		// Before joining: the #ops message must not reach bob. He is a LIVE fan
		// target — the #general message that follows it in the log DOES reach
		// him, so the shared batch demonstrably passed through his filter.
		outside := sendChannel(t, ts.URL, boot.Token, ops.ChannelID, "not for bob")
		inside := sendChannel(t, ts.URL, boot.Token, boot.ChannelID, "for bob")
		seen := drainMessagesUntil(t, bob, inside)
		if slices.Contains(seen, outside) {
			t.Fatalf("non-member received message %d from a channel he is not in; got %v",
				outside, seen)
		}

		// Joining refreshes the view on the LIVE connection (no reconnect).
		if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, ops.ChannelID),
			bobTok, map[string]any{}); code != http.StatusOK {
			t.Fatalf("bob join #ops = %d, want 200", code)
		}
		afterJoin := sendChannel(t, ts.URL, boot.Token, ops.ChannelID, "now for bob")
		got := drainMessagesUntil(t, bob, afterJoin)
		if slices.Contains(got, outside) {
			t.Fatalf("the refresh replayed pre-join traffic %d; got %v", outside, got)
		}
	})
}

// drainMessagesUntil reads envelopes from c until the message.created carrying
// wantID arrives, returning every message.created id seen on the way (in
// stream order, wantID last). This is what makes the negatives non-vacuous:
// the gateway delivers in event-id order, so an event logged BEFORE wantID has
// already been offered to this connection by the time wantID lands — if it is
// absent from the result it was dropped by that connection's ACL filter, not
// merely late.
func drainMessagesUntil(t *testing.T, c *wsClient, wantID int64) []int64 {
	t.Helper()
	deadline := time.After(8 * time.Second)
	var seen []int64
	for {
		select {
		case e, ok := <-c.events:
			if !ok {
				t.Fatalf("connection closed while waiting for message %d (saw %v)", wantID, seen)
			}
			if e.Type != "message.created" {
				continue
			}
			var p struct {
				MessageID int64 `json:"message_id"`
			}
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decode message.created payload: %v", err)
			}
			seen = append(seen, p.MessageID)
			if p.MessageID == wantID {
				return seen
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %d (saw %v)", wantID, seen)
		}
	}
}
