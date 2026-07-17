package rest

import (
	"bytes"
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
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
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

// TestNotificationDepth: N-1 steps 2–3 — followed threads boost, level=all
// yields activity pings, the SEPARATE mute flag suppresses both while
// mentions break through, an unmuted thread revives activity inside a muted
// channel, and more specific reasons win (one row per user+message).
func TestNotificationDepth(t *testing.T) {
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
		"org_slug": "dep", "email": "a@dp.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@dp.test", "Bob Ray", "bobdeptok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@dp.test", "Charlie Kim", "charliedeptok")

	chURL := fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, boot.ChannelID)

	// Resolution reads CURRENT settings, so a settings change racing an
	// unprocessed older event would mint extra (legitimate) rows on slow
	// runners. The cursor is ordered: drain the materializer before every
	// settings mutation to keep expectations exact. ProcessOrg alongside the
	// background runner is safe — the consumer is cursor-idempotent.
	waitForConsumer := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}

	var th struct {
		ThreadID int64 `json:"thread_id"`
	}
	postJSON(t, chURL+"/threads", boot.Token,
		map[string]any{"title": "spec", "content": "spec discussion"}, &th)
	thURL := fmt.Sprintf("%s/api/v1/threads/%d", ts.URL, th.ThreadID)

	// Bob follows the thread → an unmentioned reply pings him (kind 3).
	waitForConsumer()
	if code := putJSON(t, thURL+"/subscription", bobTok, map[string]any{"state": 1}); code != http.StatusOK {
		t.Fatalf("follow = %d", code)
	}
	postJSON(t, thURL+"/messages", boot.Token, map[string]any{"content": "first update"}, nil)
	in := pollInbox(t, ts.URL, bobTok, 1)
	if in.Notifications[0].Kind != notification.KindFollowedThread {
		t.Fatalf("bob kind = %d, want followed", in.Notifications[0].Kind)
	}
	pollInbox(t, ts.URL, charlieTok, 0)

	waitForConsumer()
	// Charlie opts into level=all → a root message pings him (kind 5).
	if code := putJSON(t, chURL+"/notification", charlieTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatalf("level = %d", code)
	}
	postJSON(t, chURL+"/messages", boot.Token, map[string]any{"content": "general chatter"}, nil)
	in = pollInbox(t, ts.URL, charlieTok, 1)
	if in.Notifications[0].Kind != notification.KindChannelActivity {
		t.Fatalf("charlie kind = %d, want activity", in.Notifications[0].Kind)
	}
	pollInbox(t, ts.URL, bobTok, 1) // unchanged

	// Bob mutes the channel (follow kept): the next reply is SUPPRESSED for
	// him, while level=all charlie still gets activity.
	waitForConsumer()
	if code := putJSON(t, chURL+"/notification", bobTok, map[string]any{"muted": true}); code != http.StatusOK {
		t.Fatalf("mute = %d", code)
	}
	postJSON(t, thURL+"/messages", boot.Token, map[string]any{"content": "second update"}, nil)
	pollInbox(t, ts.URL, charlieTok, 2)
	time.Sleep(500 * time.Millisecond)
	pollInbox(t, ts.URL, bobTok, 1) // mute suppressed the follow

	// ...but a direct mention breaks through bob's mute (kind 2), and it is
	// the ONLY row for that message (specificity beats follow).
	postJSON(t, thURL+"/messages", boot.Token,
		map[string]any{"content": "breakthrough @**Bob Ray**"}, nil)
	in = pollInbox(t, ts.URL, bobTok, 2)
	if in.Notifications[0].Kind != notification.KindMention {
		t.Fatalf("breakthrough kind = %d, want mention", in.Notifications[0].Kind)
	}
	if len(in.Notifications) != 2 {
		t.Fatalf("bob rows = %d, want 2 (one per message)", len(in.Notifications))
	}
	pollInbox(t, ts.URL, charlieTok, 3) // activity continues for charlie

	// Charlie mutes too → root activity stops; unmuting THE THREAD revives
	// activity inside the muted channel (state 3).
	waitForConsumer()
	if code := putJSON(t, chURL+"/notification", charlieTok, map[string]any{"muted": true}); code != http.StatusOK {
		t.Fatalf("charlie mute = %d", code)
	}
	postJSON(t, chURL+"/messages", boot.Token, map[string]any{"content": "into the void"}, nil)
	time.Sleep(500 * time.Millisecond)
	pollInbox(t, ts.URL, charlieTok, 3)
	waitForConsumer()
	if code := putJSON(t, thURL+"/subscription", charlieTok, map[string]any{"state": 3}); code != http.StatusOK {
		t.Fatalf("unmute thread = %d", code)
	}
	postJSON(t, thURL+"/messages", boot.Token, map[string]any{"content": "revival"}, nil)
	in = pollInbox(t, ts.URL, charlieTok, 4)
	if in.Notifications[0].Kind != notification.KindChannelActivity {
		t.Fatalf("revival kind = %d, want activity", in.Notifications[0].Kind)
	}

	// The root thread rejects follow state (F-15).
	var rootID int64
	_ = pool.QueryRow(ctx, `SELECT root_thread_id FROM channel WHERE id = $1`,
		boot.ChannelID).Scan(&rootID)
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/subscription", ts.URL, rootID),
		bobTok, map[string]any{"state": 1}); code != http.StatusBadRequest {
		t.Fatalf("follow root = %d, want 400", code)
	}

	// Replay idempotency holds under STABLE settings (the materializer
	// resolves against current preferences — a deliberate cursor reset
	// re-evaluates history under today's settings, so the test first
	// returns charlie to defaults).
	waitForConsumer()
	if code := putJSON(t, chURL+"/notification", charlieTok,
		map[string]any{"level": 0, "muted": false}); code != http.StatusOK {
		t.Fatalf("charlie reset = %d", code)
	}
	if code := putJSON(t, thURL+"/subscription", charlieTok, map[string]any{"state": 0}); code != http.StatusOK {
		t.Fatalf("charlie clear sub = %d", code)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE event_consumer_cursor SET last_id = 0
		WHERE consumer = 'notifications' AND org_id = $1`, boot.OrgID); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	pollInbox(t, ts.URL, bobTok, 2)     // muted follow suppressed, mention deduped
	pollInbox(t, ts.URL, charlieTok, 4) // level back to default: nothing re-qualifies
}

func putJSON(t *testing.T, url, token string, body any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
