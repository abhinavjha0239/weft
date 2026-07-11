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
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type savedItemT struct {
	MessageID  int64  `json:"message_id"`
	ThreadID   int64  `json:"thread_id"`
	ChannelID  int64  `json:"channel_id"`
	DMSpaceID  int64  `json:"dm_space_id"`
	AuthorID   int64  `json:"author_id"`
	Excerpt    string `json:"excerpt"`
	Deleted    bool   `json:"deleted"`
	Accessible bool   `json:"accessible"`
}

// TestSavedItems: ADR-007 M-6 personal "save for later" (saved_item kind 1).
// Saving needs read visibility (oracle-free 404); the list is private, newest
// SAVE first, carries container ids + an excerpt, and keeps a deleted message
// as a tombstone rather than dropping it. Removal is ungated and idempotent.
func TestSavedItems(t *testing.T) {
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
		"org_slug": "sav", "email": "alice@sav.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	aliceID := boot.UserID
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@sav.test", "Bob Ray", "bobsavtok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@sav.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	send := func(tok string, channelID int64, content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			tok, map[string]any{"content": content}, &sent)
		return sent.MessageID
	}
	save := func(tok string, msgID int64) int {
		t.Helper()
		return putJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/save", ts.URL, msgID), tok, nil)
	}
	unsave := func(tok string, msgID int64) int {
		t.Helper()
		return deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d/save", ts.URL, msgID), tok)
	}
	list := func(tok string) []savedItemT {
		t.Helper()
		var out struct {
			Saved []savedItemT `json:"saved"`
		}
		if code := getJSON(t, ts.URL+"/api/v1/saved", tok, &out); code != http.StatusOK {
			t.Fatalf("list saved = %d", code)
		}
		return out.Saved
	}
	find := func(items []savedItemT, msgID int64) (savedItemT, bool) {
		for _, it := range items {
			if it.MessageID == msgID {
				return it, true
			}
		}
		return savedItemT{}, false
	}

	m1 := send(boot.Token, boot.ChannelID, "message one")
	m2 := send(boot.Token, boot.ChannelID, "message two")
	m3 := send(boot.Token, boot.ChannelID, "message three")

	// Save two of three, newest save first. m3 is backdated to be the OLDER
	// save so the expected order [m1, m3] is the OPPOSITE of message-id order
	// (m3.id > m1.id) — proving the list sorts by save time, not id.
	if code := save(boot.Token, m1); code != http.StatusOK {
		t.Fatalf("save m1 = %d", code)
	}
	if code := save(boot.Token, m3); code != http.StatusOK {
		t.Fatalf("save m3 = %d", code)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE saved_item SET created_at = now() - interval '1 hour' WHERE user_id = $1 AND message_id = $2`,
		aliceID, m3); err != nil {
		t.Fatalf("backdate save: %v", err)
	}
	items := list(boot.Token)
	if len(items) != 2 || items[0].MessageID != m1 || items[1].MessageID != m3 {
		t.Fatalf("saved order = %+v, want [m1(%d), m3(%d)] newest-save-first", items, m1, m3)
	}
	if it := items[0]; it.ChannelID != boot.ChannelID || it.DMSpaceID != 0 ||
		it.AuthorID != aliceID || it.Excerpt != "message one" || it.Deleted {
		t.Fatalf("m1 entry = %+v, want channel/author/excerpt set, not deleted", it)
	}

	// Idempotent re-save: no duplicate row.
	if code := save(boot.Token, m1); code != http.StatusOK {
		t.Fatalf("re-save m1 = %d", code)
	}
	if items := list(boot.Token); len(items) != 2 {
		t.Fatalf("after re-save len = %d, want 2 (idempotent)", len(items))
	}

	// Privacy: bob's saves are his own; neither user sees the other's list.
	if code := save(bobTok, m2); code != http.StatusOK {
		t.Fatalf("bob save m2 = %d", code)
	}
	if bobItems := list(bobTok); len(bobItems) != 1 || bobItems[0].MessageID != m2 {
		t.Fatalf("bob saved = %+v, want just [m2]", bobItems)
	}
	if aliceItems := list(boot.Token); len(aliceItems) != 2 {
		t.Fatalf("alice saved after bob saved = %d items, want 2 (isolation)", len(aliceItems))
	}

	// DM container ids ride the row too (channel_id 0, dm_space_id set).
	var conv struct {
		ID           int64 `json:"id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm to save"}, &dmSent)
	if code := save(boot.Token, dmSent.MessageID); code != http.StatusOK {
		t.Fatalf("save dm = %d", code)
	}
	if it, ok := find(list(boot.Token), dmSent.MessageID); !ok ||
		it.DMSpaceID != conv.ID || it.ChannelID != 0 {
		t.Fatalf("dm saved entry = %+v (ok=%v), want dm_space_id=%d channel_id=0", it, ok, conv.ID)
	}

	// Tombstone: a saved message that is later deleted stays as a tombstone
	// (empty excerpt, deleted:true) — never silently dropped.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, m3), boot.Token); code != http.StatusOK {
		t.Fatalf("delete m3 = %d", code)
	}
	if it, ok := find(list(boot.Token), m3); !ok || !it.Deleted || it.Excerpt != "" {
		t.Fatalf("tombstone m3 = %+v (ok=%v), want deleted:true excerpt:''", it, ok)
	}

	// Removal: ungated and idempotent, and it works on a tombstone.
	if code := unsave(boot.Token, m1); code != http.StatusOK {
		t.Fatalf("unsave m1 = %d", code)
	}
	if _, ok := find(list(boot.Token), m1); ok {
		t.Fatalf("m1 still saved after unsave")
	}
	if code := unsave(boot.Token, m1); code != http.StatusOK {
		t.Fatalf("re-unsave m1 = %d, want idempotent 200", code)
	}
	if code := unsave(boot.Token, m3); code != http.StatusOK {
		t.Fatalf("unsave tombstone m3 = %d", code)
	}
	if _, ok := find(list(boot.Token), m3); ok {
		t.Fatalf("tombstone m3 still saved after unsave")
	}

	// ACL: a message the caller cannot read — private-channel content or a
	// nonexistent id — is an oracle-free 404 on save.
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "secret", "private": true}, &priv)
	mPriv := send(boot.Token, priv.ChannelID, "eyes only")
	if code := save(bobTok, mPriv); code != http.StatusNotFound {
		t.Fatalf("bob save private-channel message = %d, want 404", code)
	}
	if code := save(bobTok, 999999); code != http.StatusNotFound {
		t.Fatalf("save nonexistent message = %d, want 404", code)
	}

	// Revoked access masks, never drops: alice saves in the private channel,
	// then LEAVES it — the bookmark stays but the excerpt is withheld, so
	// content she can no longer read never leaks through the saved list.
	if code := save(boot.Token, mPriv); code != http.StatusOK {
		t.Fatalf("alice save private = %d", code)
	}
	if it, ok := find(list(boot.Token), mPriv); !ok || !it.Accessible || it.Excerpt == "" {
		t.Fatalf("pre-leave entry = %+v (ok=%v), want accessible with excerpt", it, ok)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, priv.ChannelID),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("leave private = %d", code)
	}
	if it, ok := find(list(boot.Token), mPriv); !ok || it.Accessible || it.Excerpt != "" {
		t.Fatalf("post-leave entry = %+v (ok=%v), want masked (accessible:false, empty excerpt)", it, ok)
	}
}
