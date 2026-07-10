package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

func deleteReq(t *testing.T, url, token string) int {
	t.Helper()
	req, _ := http.NewRequest("DELETE", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestMessageEditDelete: the M-3/F-8 arc — author-only edits with revision
// capture and search re-index, edit-diffed mention notifications, and
// delete as revision-append + live-row scrub with moderation rules.
func TestMessageEditDelete(t *testing.T) {
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
		"org_slug": "edt", "email": "a@e2.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@e2.test", "Bob Ray", "bobedttok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@e2.test", "Charlie Kim", "charlieedttok")

	// Bob (a plain member) authors; grant charlie moderator standing.
	var msg struct {
		MessageID int64 `json:"message_id"`
	}
	chMsgURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID)
	postJSON(t, chMsgURL, bobTok, map[string]any{"content": "the plan mentions @**Alice Chen** and zebras"}, &msg)
	msgURL := fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msg.MessageID)

	// Alice was mentioned at creation.
	pollInbox(t, ts.URL, boot.Token, 1)

	// Only the author edits — even a moderator/admin cannot rewrite.
	if code := patchJSON(t, msgURL, boot.Token, map[string]any{"content": "rewritten"}); code != http.StatusForbidden {
		t.Fatalf("owner edit of other's message = %d, want 403", code)
	}

	// Author edit: revision captured, render updated, search re-indexed,
	// and only the NEWLY-mentioned user is notified.
	if code := patchJSON(t, msgURL, bobTok,
		map[string]any{"content": "the plan mentions @**Alice Chen** and @**Charlie Kim** — giraffes now"}); code != http.StatusOK {
		t.Fatalf("author edit = %d, want 200", code)
	}
	var got struct {
		Rendered string `json:"rendered"`
	}
	getJSON(t, msgURL, bobTok, &got)
	if !strings.Contains(got.Rendered, "giraffes") {
		t.Fatalf("edit not applied: %s", got.Rendered)
	}
	var revs int
	var prev string
	_ = pool.QueryRow(ctx, `
		SELECT count(*), max(prev_source) FROM message_revision
		WHERE message_id = $1 AND kind = 1`, msg.MessageID).Scan(&revs, &prev)
	if revs != 1 || !strings.Contains(prev, "zebras") {
		t.Fatalf("revision capture wrong: n=%d prev=%q", revs, prev)
	}
	if got := searchIDs(t, ts.URL, bobTok, "giraffes"); len(got) != 1 {
		t.Fatalf("edited content not searchable: %d hits", len(got))
	}
	if got := searchIDs(t, ts.URL, bobTok, "zebras"); len(got) != 0 {
		t.Fatalf("stale content still indexed: %d hits", len(got))
	}
	pollInbox(t, ts.URL, charlieTok, 1) // newly mentioned by the edit
	pollInbox(t, ts.URL, boot.Token, 1) // NOT re-pinged

	// Delete: a plain member cannot scrub others; a moderator can; the row
	// scrubs (search empty, fetch 404, list shrinks) and the capture lives
	// in a kind=4 revision.
	if code := deleteReq(t, msgURL, charlieTok); code != http.StatusForbidden {
		t.Fatalf("member delete of other's message = %d, want 403", code)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO user_group_member (group_id, user_id)
		SELECT g.id, u.id FROM user_group g, user_account u
		WHERE g.org_id = $1 AND g.name = 'role:moderators' AND u.email = 'charlie@e2.test'`,
		boot.OrgID); err != nil {
		t.Fatalf("promote charlie: %v", err)
	}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return permsSvc.RebuildClosure(ctx, tx, boot.OrgID)
	}); err != nil {
		t.Fatalf("closure: %v", err)
	}
	if code := deleteReq(t, msgURL, charlieTok); code != http.StatusOK {
		t.Fatalf("moderator delete = %d, want 200", code)
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", msgURL, nil)
	req.Header.Set("Authorization", "Bearer "+bobTok)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted message fetch = %d, want 404", resp.StatusCode)
	}
	if got := searchIDs(t, ts.URL, bobTok, "giraffes"); len(got) != 0 {
		t.Fatalf("deleted content still searchable: %d hits", len(got))
	}
	var src string
	var delRevs int
	_ = pool.QueryRow(ctx,
		`SELECT source FROM message WHERE id = $1`, msg.MessageID).Scan(&src)
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM message_revision
		WHERE message_id = $1 AND kind = 4 AND prev_source LIKE '%giraffes%'`,
		msg.MessageID).Scan(&delRevs)
	if src != "" || delRevs != 1 {
		t.Fatalf("scrub wrong: live source=%q, delete revisions=%d", src, delRevs)
	}

	// Author self-delete works without any verb; editing a deleted message
	// is gone-forever.
	var mine struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, chMsgURL, bobTok, map[string]any{"content": "oops"}, &mine)
	mineURL := fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, mine.MessageID)
	if code := deleteReq(t, mineURL, bobTok); code != http.StatusOK {
		t.Fatalf("author self-delete = %d, want 200", code)
	}
	if code := patchJSON(t, mineURL, bobTok, map[string]any{"content": "revive"}); code != http.StatusNotFound {
		t.Fatalf("edit of deleted = %d, want 404", code)
	}

	// The lifecycle wrote its events.
	for _, verb := range []string{"message.edited", "message.deleted"} {
		var n int
		_ = pool.QueryRow(ctx,
			`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = $2`,
			boot.OrgID, verb).Scan(&n)
		if n == 0 {
			t.Fatalf("no %s event recorded", verb)
		}
	}
}
