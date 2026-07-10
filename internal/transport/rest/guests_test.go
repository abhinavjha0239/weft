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

// TestGuestVisibility: P-5 enforced — a guest lives inside their
// enumerated channels and the org beyond them is invisible: channel
// listing shrinks to memberships, the directory and profile resolution
// show only channel-mates, DMs only reach channel-mates, and the members
// verbs (create_channel, invite) stay closed while speaking in their own
// channels works.
func TestGuestVisibility(t *testing.T) {
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
		"org_slug": "gst", "email": "a@gst.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@gst.test", "Bob Ray", "bobgsttok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@gst.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	// #support is PRIVATE; alice (creator) is its only member besides gina.
	var support struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "support", "private": true}, &support)

	var ginv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 50, "channel_ids": []int64{support.ChannelID}}, &ginv)
	var gina identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": ginv.Token, "email": "gina@gst.test", "password": "password123",
		"full_name": "Gina Guest"}, &gina)

	// Channel listing: gina sees ONLY #support — the public #general is
	// invisible; bob (member) still sees both.
	var chans struct {
		Channels []struct {
			ID int64 `json:"id"`
		} `json:"channels"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/channels", gina.Token, &chans); code != 200 ||
		len(chans.Channels) != 1 || chans.Channels[0].ID != support.ChannelID {
		t.Fatalf("guest channels = %d %+v, want only #support", code, chans.Channels)
	}
	// bob (member, not in the private #support) sees the public #general;
	// alice (member of both) sees both — the guest restriction is the only
	// thing hiding PUBLIC channels.
	if code := getJSON(t, ts.URL+"/api/v1/channels", bobTok, &chans); code != 200 || len(chans.Channels) != 1 {
		t.Fatalf("bob channels = %d %+v, want #general", code, chans.Channels)
	}
	if code := getJSON(t, ts.URL+"/api/v1/channels", boot.Token, &chans); code != 200 || len(chans.Channels) != 2 {
		t.Fatalf("alice channels = %d %+v, want both", code, chans.Channels)
	}

	// Speaking: allowed in her channel (everyone-verb + membership), not
	// elsewhere; the members verbs stay shut.
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, support.ChannelID),
		gina.Token, map[string]any{"content": "guest hello"}, &sent)
	if sent.MessageID == 0 {
		t.Fatal("guest must speak in her channel")
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		gina.Token, map[string]any{"content": "sneak"}); code != http.StatusForbidden {
		t.Fatalf("guest in #general = %d, want 403", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", gina.Token,
		map[string]any{"name": "guestchan"}); code != http.StatusForbidden {
		t.Fatalf("guest create channel = %d, want 403", code)
	}

	// Directory: gina resolves herself + alice (channel-mate), never bob;
	// bob still sees the whole org.
	var dir struct {
		Users []struct {
			ID int64 `json:"id"`
		} `json:"users"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/users", gina.Token, &dir); code != 200 || len(dir.Users) != 2 {
		t.Fatalf("guest directory = %d %+v, want self+alice", code, dir.Users)
	}
	for _, u := range dir.Users {
		if u.ID == bobID {
			t.Fatal("guest directory must not show bob")
		}
	}
	if code := getJSON(t, ts.URL+"/api/v1/users", bobTok, &dir); code != 200 || len(dir.Users) != 3 {
		t.Fatalf("member directory = %d %+v, want all three", code, dir.Users)
	}
	// Batch resolution: bob's id is silently absent for gina.
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/users?ids=%d,%d", ts.URL, bobID, boot.UserID),
		gina.Token, &dir); code != 200 || len(dir.Users) != 1 || dir.Users[0].ID != boot.UserID {
		t.Fatalf("guest batch = %d %+v, want alice only", code, dir.Users)
	}

	// DMs: a channel-mate works, a stranger is refused; a MEMBER may still
	// open toward the guest (the restriction is on guest initiation).
	if code := postJSONStatus(t, ts.URL+"/api/v1/dms", gina.Token,
		map[string]any{"user_ids": []int64{boot.UserID}}); code != http.StatusOK {
		t.Fatalf("guest→alice DM = %d, want 200", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/dms", gina.Token,
		map[string]any{"user_ids": []int64{bobID}}); code != http.StatusForbidden {
		t.Fatalf("guest→bob DM = %d, want 403", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/dms", bobTok,
		map[string]any{"user_ids": []int64{gina.UserID}}); code != http.StatusOK {
		t.Fatalf("bob→guest DM = %d, want 200", code)
	}
}
