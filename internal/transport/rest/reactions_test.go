package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

func react(t *testing.T, base, token string, msgID int64, emoji, method string) (int, messaging.ReactionState) {
	t.Helper()
	u := fmt.Sprintf("%s/api/v1/messages/%d/reactions/%s", base, msgID, url.PathEscape(emoji))
	req, _ := http.NewRequest(method, u, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("react: %v", err)
	}
	defer resp.Body.Close()
	var st messaging.ReactionState
	_ = jsonDecode(resp.Body, &st)
	return resp.StatusCode, st
}

// TestReactions: idempotent toggle semantics with viewer-aware aggregates
// on the message read paths, reaction events carrying the container for
// gateway routing, and the react-needs-read rule — a message you cannot
// see is a message you cannot react to, indistinguishably from missing.
func TestReactions(t *testing.T) {
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
		"org_slug": "rea", "email": "a@rea.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@rea.test", "Bob Ray", "bobreatok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@rea.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	var sent struct {
		MessageID int64 `json:"message_id"`
		ThreadID  int64 `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "ship it?"}, &sent)
	if sent.ThreadID == 0 {
		if err := pool.QueryRow(ctx, `SELECT thread_id FROM message WHERE id = $1`,
			sent.MessageID).Scan(&sent.ThreadID); err != nil {
			t.Fatalf("thread id: %v", err)
		}
	}

	// Add: first add counts, re-add is a silent no-op (idempotent, and no
	// second event); a second user joins the same emoji.
	if code, st := react(t, ts.URL, boot.Token, sent.MessageID, "👍", "PUT"); code != 200 || st.Count != 1 || !st.Me {
		t.Fatalf("first add = %d %+v", code, st)
	}
	if code, st := react(t, ts.URL, boot.Token, sent.MessageID, "👍", "PUT"); code != 200 || st.Count != 1 {
		t.Fatalf("re-add = %d %+v, want unchanged count", code, st)
	}
	if code, st := react(t, ts.URL, bobTok, sent.MessageID, "👍", "PUT"); code != 200 || st.Count != 2 {
		t.Fatalf("bob add = %d %+v", code, st)
	}
	if code, st := react(t, ts.URL, bobTok, sent.MessageID, "🚀", "PUT"); code != 200 || st.Count != 1 {
		t.Fatalf("bob rocket = %d %+v", code, st)
	}
	var events int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log WHERE verb = 'reaction.added' AND org_id = $1`,
		boot.OrgID).Scan(&events); err != nil || events != 3 {
		t.Fatalf("reaction.added events = %d (%v), want 3 (re-add emits nothing)", events, err)
	}
	// The event payload carries the channel — that's what the gateway
	// filter routes by.
	var chanInPayload int64
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'channel_id')::bigint FROM event_log
		WHERE verb = 'reaction.added' ORDER BY id LIMIT 1`).Scan(&chanInPayload); err != nil || chanInPayload != boot.ChannelID {
		t.Fatalf("payload channel = %d (%v), want %d", chanInPayload, err, boot.ChannelID)
	}

	// Aggregates on the paged read: chips ordered by first reaction, with
	// per-viewer me flags.
	var page struct {
		Messages []messaging.Message `json:"messages"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, sent.ThreadID),
		bobTok, &page); code != 200 {
		t.Fatalf("list = %d", code)
	}
	var msg *messaging.Message
	for i := range page.Messages {
		if page.Messages[i].ID == sent.MessageID {
			msg = &page.Messages[i]
		}
	}
	if msg == nil || len(msg.Reactions) != 2 {
		t.Fatalf("reactions on page = %+v, want 2 aggregates", msg)
	}
	if msg.Reactions[0].Emoji != "👍" || msg.Reactions[0].Count != 2 || !msg.Reactions[0].Me ||
		len(msg.Reactions[0].UserIDs) != 2 || msg.Reactions[0].UserIDs[0] != boot.UserID {
		t.Fatalf("thumbs agg = %+v, want count 2, me, alice first", msg.Reactions[0])
	}
	if msg.Reactions[1].Emoji != "🚀" || msg.Reactions[1].Me != true {
		t.Fatalf("rocket agg = %+v", msg.Reactions[1])
	}
	// Alice's view: rocket is not hers.
	var aliceGet messaging.Message
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent.MessageID),
		boot.Token, &aliceGet); code != 200 || len(aliceGet.Reactions) != 2 || aliceGet.Reactions[1].Me {
		t.Fatalf("alice get = %d %+v, want rocket me=false", code, aliceGet.Reactions)
	}

	// Remove: own row only; removing twice stays quiet; bob's rocket gone
	// deletes the whole chip.
	if code, st := react(t, ts.URL, bobTok, sent.MessageID, "🚀", "DELETE"); code != 200 || st.Count != 0 || st.Me {
		t.Fatalf("remove = %d %+v", code, st)
	}
	if code, st := react(t, ts.URL, bobTok, sent.MessageID, "🚀", "DELETE"); code != 200 || st.Count != 0 {
		t.Fatalf("re-remove = %d %+v", code, st)
	}
	var removedEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log WHERE verb = 'reaction.removed' AND org_id = $1`,
		boot.OrgID).Scan(&removedEvents); err != nil || removedEvents != 1 {
		t.Fatalf("reaction.removed events = %d (%v), want 1", removedEvents, err)
	}

	// Validation: junk emoji is a 400.
	if code, _ := react(t, ts.URL, boot.Token, sent.MessageID, "a b", "PUT"); code != http.StatusBadRequest {
		t.Fatalf("spaced emoji = %d, want 400", code)
	}

	// React-needs-read: a DM message is untouchable for a non-participant,
	// indistinguishable from nonexistent; a deleted message likewise.
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@rea.test", "Charlie Kim", "charliereatok")
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "just us"}, &dmSent)
	if code, _ := react(t, ts.URL, charlieTok, dmSent.MessageID, "👀", "PUT"); code != http.StatusNotFound {
		t.Fatalf("outsider react = %d, want 404", code)
	}
	if code, _ := react(t, ts.URL, bobTok, dmSent.MessageID, "👀", "PUT"); code != 200 {
		t.Fatal("participant must react fine")
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent.MessageID), boot.Token); code != 200 {
		t.Fatal("delete message")
	}
	if code, _ := react(t, ts.URL, boot.Token, sent.MessageID, "👍", "PUT"); code != http.StatusNotFound {
		t.Fatalf("react to deleted = %d, want 404", code)
	}
}
