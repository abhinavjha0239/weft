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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestAlertWords: N-1 kind 4 closes the last reserved reason. Words match
// case-insensitively on WORD boundaries (substrings don't fire), the
// specificity ladder holds (mention and followed beat keyword; keyword
// upgrades plain activity), mute suppresses keywords (they don't break
// through like mentions), edits that newly introduce a word ping without
// double-notifying, and DMs never keyword-ping (the DM row already does).
func TestAlertWords(t *testing.T) {
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
	// F-17: level changes must patch the deliverability set the
	// materializer resolves from (the production main.go wiring).
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, slog.Default()))
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer ts.Close()
	runner := notification.NewRunner(pool, hub, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "alw", "email": "a@alw.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@alw.test", "Bob Ray", "bobalwtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@alw.test", "Charlie Kim", "charliealwtok")
	var bobID, charlieID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@alw.test'`).Scan(&bobID)
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'charlie@alw.test'`).Scan(&charlieID)

	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}
	send := func(content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": content}, &sent)
		return sent.MessageID
	}
	kindsFor := func(uid, msgID int64) []int16 {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT kind FROM notification
			WHERE user_id = $1 AND entity_type = 1 AND entity_id = $2 ORDER BY kind`, uid, msgID)
		if err != nil {
			t.Fatalf("kinds: %v", err)
		}
		defer rows.Close()
		var out []int16
		for rows.Next() {
			var k int16
			_ = rows.Scan(&k)
			out = append(out, k)
		}
		return out
	}

	// CRUD: mixed case stores lower, duplicates collapse, junk rejected.
	var words struct {
		Words []string `json:"words"`
	}
	postStatus := putJSON(t, ts.URL+"/api/v1/alert-words", bobTok,
		map[string]any{"words": []string{"Deploy", "postgres", "DEPLOY"}})
	if postStatus != http.StatusOK {
		t.Fatalf("set words = %d", postStatus)
	}
	if code := getJSON(t, ts.URL+"/api/v1/alert-words", bobTok, &words); code != 200 ||
		len(words.Words) != 2 || words.Words[0] != "deploy" || words.Words[1] != "postgres" {
		t.Fatalf("words = %d %+v", code, words.Words)
	}
	if code := putJSON(t, ts.URL+"/api/v1/alert-words", bobTok,
		map[string]any{"words": []string{"x"}}); code != http.StatusBadRequest {
		t.Fatalf("short word = %d, want 400", code)
	}

	// A word fires on a boundary match — and ONLY on a boundary match.
	hit := send("we deploy tomorrow")
	miss := send("the redeployment saga continues")
	process()
	if got := kindsFor(bobID, hit); len(got) != 1 || got[0] != notification.KindKeyword {
		t.Fatalf("keyword hit = %v, want [4]", got)
	}
	if got := kindsFor(bobID, miss); len(got) != 0 {
		t.Fatalf("substring = %v, want no ping (word boundary)", got)
	}
	// Case-insensitive on the message side too.
	upper := send("POSTGRES is down")
	process()
	if got := kindsFor(bobID, upper); len(got) != 1 || got[0] != notification.KindKeyword {
		t.Fatalf("case hit = %v, want [4]", got)
	}

	// Specificity: a mention in the same message wins — exactly one row.
	both := send("@**Bob Ray** please deploy")
	process()
	if got := kindsFor(bobID, both); len(got) != 1 || got[0] != notification.KindMention {
		t.Fatalf("mention+keyword = %v, want [2] only", got)
	}

	// Keyword upgrades plain channel activity: charlie sets level=all AND a
	// word; one message, one row, kind 4 (not 5).
	if code := putJSON(t, ts.URL+"/api/v1/alert-words", charlieTok,
		map[string]any{"words": []string{"incident"}}); code != http.StatusOK {
		t.Fatal("charlie words")
	}
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		charlieTok, map[string]any{"level": 1}); code != http.StatusOK {
		t.Fatal("charlie level")
	}
	inc := send("incident declared")
	process()
	if got := kindsFor(charlieID, inc); len(got) != 1 || got[0] != notification.KindKeyword {
		t.Fatalf("upgrade = %v, want [4] (keyword beats activity)", got)
	}

	// Mute suppresses keywords (no mention-style breakthrough).
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		bobTok, map[string]any{"muted": true}); code != http.StatusOK {
		t.Fatal("bob mute")
	}
	muted := send("deploy anyway")
	process()
	if got := kindsFor(bobID, muted); len(got) != 0 {
		t.Fatalf("muted keyword = %v, want silence", got)
	}
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/notification", ts.URL, boot.ChannelID),
		bobTok, map[string]any{"muted": false}); code != http.StatusOK {
		t.Fatal("bob unmute")
	}

	// An edit that newly introduces a word pings — once; re-processing
	// the same history never doubles it.
	plain := send("nothing to see")
	process()
	if got := kindsFor(bobID, plain); len(got) != 0 {
		t.Fatalf("pre-edit = %v", got)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, plain),
		boot.Token, map[string]any{"content": "actually, deploy it"}); code != http.StatusOK {
		t.Fatal("edit")
	}
	process()
	process()
	if got := kindsFor(bobID, plain); len(got) != 1 || got[0] != notification.KindKeyword {
		t.Fatalf("edit keyword = %v, want [4] once", got)
	}

	// DMs never keyword-ping: the DM row is the notification.
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "we deploy at dawn"}, &dmSent)
	process()
	if got := kindsFor(bobID, dmSent.MessageID); len(got) != 1 || got[0] != notification.KindDM {
		t.Fatalf("dm keyword = %v, want [1] only", got)
	}
}
