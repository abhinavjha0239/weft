package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestDirectMessages proves the DM container end-to-end, with the emphasis
// on NON-participants: a third org member must not receive the fan-out, must
// not fetch or list the messages, and must not find them in search — DMs are
// the one container where "org member" grants nothing.
func TestDirectMessages(t *testing.T) {
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
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
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
		"org_slug": "dms", "email": "alice@d.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@d.test", "Bob Ray", "bobdmtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@d.test", "Charlie Kim", "charliedmtok")
	var bobID, charlieID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='bob@d.test'`,
		boot.OrgID).Scan(&bobID)
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='charlie@d.test'`,
		boot.OrgID).Scan(&charlieID)

	// Both connect before anything happens; bob must see the DM live,
	// charlie must see none of it.
	bobWS := dialClient(t, ctx, ts.URL, bobTok)
	charlieWS := dialClient(t, ctx, ts.URL, charlieTok)
	bobWS.waitFor(t, "ready")
	charlieWS.waitFor(t, "ready")

	// Open the 1:1 — create, then get the SAME conversation on repeat.
	var opened dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &opened)
	if opened.Kind != 1 || opened.RootThreadID == 0 || len(opened.ParticipantIDs) != 2 {
		t.Fatalf("open 1:1 wrong: %+v", opened)
	}
	var again dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &again)
	if again.ID != opened.ID || again.RootThreadID != opened.RootThreadID {
		t.Fatalf("create-or-get returned a different conversation: %+v vs %+v", again, opened)
	}
	bobWS.waitFor(t, "dm.opened")

	// Alice writes; bob receives the event with the dm scope; charlie hears
	// nothing.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, opened.RootThreadID),
		boot.Token, map[string]any{"content": "psst bob — the launch is friday"}, &sent)
	ev := bobWS.waitFor(t, "message.created")
	var pl struct {
		MessageID int64 `json:"message_id"`
		DMSpaceID int64 `json:"dm_space_id"`
	}
	_ = json.Unmarshal(ev.Payload, &pl)
	if pl.MessageID != sent.MessageID || pl.DMSpaceID != opened.ID {
		t.Fatalf("bob's event wrong: %+v", pl)
	}
	charlieWS.expectSilence(t, "message.created", 900*time.Millisecond)

	// Charlie is an org member and can see NOTHING of it.
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, sent.MessageID), nil)
	req.Header.Set("Authorization", "Bearer "+charlieTok)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("charlie message fetch = %d, want 404", resp.StatusCode)
	}
	// Non-participant on the DM *thread*: read, send, and mark-read are all
	// oracle-free 404s (requireParticipant → NotFound, matching the
	// single-message Get above), and the body never leaks the dm_space_id.
	for _, tc := range []struct{ name, method, url, body string }{
		{"thread read", "GET", fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, opened.RootThreadID), ""},
		{"send", "POST", fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, opened.RootThreadID), `{"content":"let me in"}`},
		{"mark-read", "POST", fmt.Sprintf("%s/api/v1/threads/%d/read", ts.URL, opened.RootThreadID), `{"up_to":1}`},
	} {
		code, body := dmReq(t, tc.method, tc.url, charlieTok, tc.body)
		if code != http.StatusNotFound {
			t.Fatalf("charlie %s = %d, want 404", tc.name, code)
		}
		if !strings.Contains(body, "conversation not found") || strings.Contains(body, fmt.Sprint(opened.ID)) {
			t.Fatalf("charlie %s body = %q, want oracle-free 404 (no dm id)", tc.name, body)
		}
	}
	if got := searchIDs(t, ts.URL, charlieTok, "psst"); len(got) != 0 {
		t.Fatalf("charlie found DM content in search: %d hits", len(got))
	}
	if got := searchIDs(t, ts.URL, bobTok, "psst"); len(got) != 1 {
		t.Fatalf("bob's DM search = %d hits, want 1", len(got))
	}

	// Unread badge for bob → cleared by mark-read on the DM thread.
	var un struct {
		DMs []struct {
			DMSpaceID   int64 `json:"dm_space_id"`
			UnreadCount int   `json:"unread_count"`
		} `json:"dms"`
	}
	getJSON(t, ts.URL+"/api/v1/unreads", bobTok, &un)
	if len(un.DMs) != 1 || un.DMs[0].DMSpaceID != opened.ID || un.DMs[0].UnreadCount != 1 {
		t.Fatalf("bob dm unreads = %+v, want 1 in the conversation", un.DMs)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/read", ts.URL, opened.RootThreadID),
		bobTok, map[string]any{"up_to": 0}); code != http.StatusOK {
		t.Fatalf("bob mark read = %d, want 200", code)
	}
	getJSON(t, ts.URL+"/api/v1/unreads", bobTok, &un)
	if len(un.DMs) != 0 {
		t.Fatalf("after mark-read, bob dm unreads = %+v, want none", un.DMs)
	}

	// Bob's DM list shows the conversation with its last message.
	var list struct {
		DMs []dm.Summary `json:"dms"`
	}
	getJSON(t, ts.URL+"/api/v1/dms", bobTok, &list)
	if len(list.DMs) != 1 || list.DMs[0].ID != opened.ID ||
		list.DMs[0].LastMessageID != sent.MessageID || len(list.DMs[0].ParticipantIDs) != 2 {
		t.Fatalf("bob dm list wrong: %+v", list.DMs)
	}

	// Group and self conversations, and the deactivated guard.
	var group dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID, charlieID}}, &group)
	if group.Kind != 2 || len(group.ParticipantIDs) != 3 {
		t.Fatalf("group dm wrong: %+v", group)
	}
	charlieWS.waitFor(t, "dm.opened") // the group DOES reach charlie
	var self dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{}}, &self)
	if self.Kind != 3 || len(self.ParticipantIDs) != 1 {
		t.Fatalf("self dm wrong: %+v", self)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE user_account SET deactivated_at = now() WHERE id = $1`, charlieID); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{charlieID}}); code != http.StatusBadRequest {
		t.Fatalf("dm to deactivated user = %d, want 400", code)
	}
}
