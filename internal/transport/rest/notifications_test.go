package rest

import (
	"context"
	"encoding/json"
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
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type inboxResp struct {
	Notifications []notification.Notification `json:"notifications"`
	Unseen        int                         `json:"unseen"`
}

// pollInbox retries until the async materializer catches up (or fails the
// test) — the runner is NOTIFY-driven, so this is normally one round.
func pollInbox(t *testing.T, base, token string, wantUnseen int) inboxResp {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		var in inboxResp
		getJSON(t, base+"/api/v1/notifications", token, &in)
		// The count and the page ship from one snapshot, so they agree.
		if in.Unseen == wantUnseen && len(in.Notifications) >= wantUnseen {
			return in
		}
		if time.Now().After(deadline) {
			t.Fatalf("inbox never reached unseen=%d: %+v", wantUnseen, in)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// TestNotificationPipeline: the event-log materializer end-to-end — mention
// and DM reasons, the private-channel ACL gate, importer-backfill skip,
// replay idempotency, live gateway ping, and mark-seen.
func TestNotificationPipeline(t *testing.T) {
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
	runner := notification.NewRunner(pool, hub, slog.Default())
	go runner.Run(ctx)
	permsSvc := perms.New(pool)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "ntf", "email": "a@n.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@n.test", "Bob Ray", "bobntftok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@n.test", "Charlie Kim", "charlientftok")
	var bobID, charlieID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='bob@n.test'`,
		boot.OrgID).Scan(&bobID)
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='charlie@n.test'`,
		boot.OrgID).Scan(&charlieID)

	// Bob is connected: the mention must arrive as a live ping too.
	bobWS := dialClient(t, ctx, ts.URL, bobTok)
	bobWS.waitFor(t, "ready")

	// A mention in #general → one mention notification for bob, none for
	// the author, none for the unmentioned member.
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "review please @**Bob Ray**"}, nil)
	in := pollInbox(t, ts.URL, bobTok, 1)
	if len(in.Notifications) != 1 || in.Notifications[0].Kind != notification.KindMention ||
		in.Notifications[0].ActorID == nil || *in.Notifications[0].ActorID != boot.UserID {
		t.Fatalf("bob inbox wrong: %+v", in.Notifications)
	}
	ev := bobWS.waitFor(t, "notification.created")
	var ping struct {
		Kind     int16 `json:"kind"`
		EntityID int64 `json:"entity_id"`
	}
	_ = json.Unmarshal(ev.Payload, &ping)
	if ping.Kind != notification.KindMention || ping.EntityID != in.Notifications[0].EntityID {
		t.Fatalf("live ping wrong: %+v", ping)
	}
	pollInbox(t, ts.URL, charlieTok, 0)
	pollInbox(t, ts.URL, boot.Token, 0)

	// DM message → dm-kind notification for the other participant only.
	var opened dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &opened)
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, opened.RootThreadID),
		boot.Token, map[string]any{"content": "psst"}, nil)
	in = pollInbox(t, ts.URL, bobTok, 2)
	if in.Notifications[0].Kind != notification.KindDM {
		t.Fatalf("newest bob notification = %+v, want dm kind", in.Notifications[0])
	}
	pollInbox(t, ts.URL, boot.Token, 0)

	// Private-channel ACL gate: mentioning a NON-member must not notify —
	// the notification would leak the channel's existence.
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "warroom", "private": true}, &priv)
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, priv.ChannelID),
		boot.Token, map[string]any{"content": "secret ping @**Charlie Kim**"}, nil)
	time.Sleep(600 * time.Millisecond) // let the materializer see it
	pollInbox(t, ts.URL, charlieTok, 0)

	// Importer-backfilled events never notify (E4 backfill semantics).
	err = db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: boot.OrgID, ActorKind: enum.ActorImporter,
			EntityType: enum.EntityMessage, EntityID: 999999, Verb: "message.created",
			Payload: eventlog.MustPayload(map[string]any{
				"message_id": 999999, "thread_id": 1, "channel_id": boot.ChannelID,
				"mentions": []int64{charlieID}}),
		})
		return err
	})
	if err != nil {
		t.Fatalf("append importer event: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	pollInbox(t, ts.URL, charlieTok, 0)

	// Replay idempotency: reset the cursor and reprocess everything — the
	// dedupe key must keep every count unchanged.
	if _, err := pool.Exec(ctx, `
		UPDATE event_consumer_cursor SET last_id = 0
		WHERE consumer = 'notifications' AND org_id = $1`, boot.OrgID); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	in = pollInbox(t, ts.URL, bobTok, 2)
	if len(in.Notifications) != 2 {
		t.Fatalf("replay duplicated notifications: %d rows", len(in.Notifications))
	}

	// Mark-seen clears the badge; rows remain listed.
	if code := postJSONStatus(t, ts.URL+"/api/v1/notifications/seen", bobTok,
		map[string]any{"up_to": 0}); code != http.StatusOK {
		t.Fatalf("mark seen = %d", code)
	}
	in = pollInbox(t, ts.URL, bobTok, 0)
	if len(in.Notifications) != 2 || !in.Notifications[0].Seen {
		t.Fatalf("after mark-seen: %+v", in.Notifications)
	}
}
