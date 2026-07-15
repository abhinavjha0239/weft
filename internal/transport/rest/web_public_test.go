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

	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestWebPublicChannels: P-16 (authed surface). Creating a web_public channel
// via the visibility param, the legacy Private-bool fallback, org-wide
// discovery, the diverged read gate (non-members and guests read, only
// members write), non-member self-join, and the channel.created payload.
func TestWebPublicChannels(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "webpub", "email": "o@wp.test", "password": "password123",
		"full_name": "Owner",
	}, &boot)
	caseyTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"casey@wp.test", "Casey Lee", "caseywptok")

	// Create via the visibility param; the column and history_mode land right.
	var town, vault struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "town-square", "visibility": "web_public"}, &town)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "visibility": "private"}, &vault)
	if town.ChannelID == 0 || vault.ChannelID == 0 {
		t.Fatalf("create incomplete: %+v %+v", town, vault)
	}
	var vis, histMode int16
	if err := pool.QueryRow(ctx,
		`SELECT visibility, history_mode FROM channel WHERE id = $1`,
		town.ChannelID).Scan(&vis, &histMode); err != nil || vis != 3 || histMode != 1 {
		t.Fatalf("town-square visibility=%d history_mode=%d (%v), want 3/1", vis, histMode, err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT visibility FROM channel WHERE id = $1`, vault.ChannelID).Scan(&vis); err != nil || vis != 2 {
		t.Fatalf("vault visibility = %d (%v), want 2", vis, err)
	}

	// An unknown visibility is a 400; when both fields are sent, the string
	// wins over the legacy bool.
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "bogus", "visibility": "wat"}); code != http.StatusBadRequest {
		t.Fatalf("bad visibility = %d, want 400", code)
	}
	var mixed struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "mixed", "visibility": "public", "private": true}, &mixed)
	if err := pool.QueryRow(ctx,
		`SELECT visibility FROM channel WHERE id = $1`, mixed.ChannelID).Scan(&vis); err != nil || vis != 1 {
		t.Fatalf("mixed visibility = %d (%v), want 1 (string wins)", vis, err)
	}

	// Discovery: a non-member sees the web_public channel org-wide, tagged.
	list := listChannels(t, ts.URL, caseyTok)
	tc, ok := list["town-square"]
	if !ok || tc.Member || tc.Private || tc.Visibility != "web_public" {
		t.Fatalf("town-square listing = %+v, want visible non-member web_public", tc)
	}
	if _, ok := list["vault"]; ok {
		t.Fatal("private channel leaked to non-member listing")
	}
	if g := list["general"]; g.Visibility != "public" {
		t.Fatalf("general visibility = %q, want public", g.Visibility)
	}

	// The diverged read gate: owner seeds a thread in town-square and a
	// message in vault, then a NON-MEMBER reads the web-public channel.
	var welcome struct {
		ThreadID      int64 `json:"thread_id"`
		RootMessageID int64 `json:"root_message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, town.ChannelID),
		boot.Token, map[string]any{"title": "welcome", "content": "first post"}, &welcome)
	var secret struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, vault.ChannelID),
		boot.Token, map[string]any{"content": "vault secret"}, &secret)
	var tpage struct {
		Threads []struct {
			ID int64 `json:"id"`
		} `json:"threads"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, town.ChannelID),
		caseyTok, &tpage); code != http.StatusOK || len(tpage.Threads) != 1 {
		t.Fatalf("non-member web_public threads = %d (%d threads), want 200 with 1", code, len(tpage.Threads))
	}
	var mpage struct {
		Messages []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, welcome.ThreadID),
		caseyTok, &mpage); code != http.StatusOK || len(mpage.Messages) != 1 {
		t.Fatalf("non-member web_public messages = %d (%d msgs), want 200 with 1", code, len(mpage.Messages))
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, welcome.RootMessageID),
		caseyTok, nil); code != http.StatusOK {
		t.Fatalf("non-member web_public Get = %d, want 200", code)
	}

	// World-readable, member-writable: the non-member's POST still 403s (the
	// audit pin — the send path keeps requireMember).
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, welcome.ThreadID),
		caseyTok, map[string]any{"content": "drive-by"}); code != http.StatusForbidden {
		t.Fatalf("non-member web_public POST = %d, want 403", code)
	}

	// Private stays closed to non-members: 403 on the list, oracle-free 404
	// on Get.
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, vault.ChannelID),
		caseyTok, nil); code != http.StatusForbidden {
		t.Fatalf("non-member private threads = %d, want 403", code)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, secret.MessageID),
		caseyTok, nil); code != http.StatusNotFound {
		t.Fatalf("non-member private Get = %d, want 404", code)
	}

	// A guest reads web-public — world-readable trumps the guest boundary —
	// while their channel list stays memberships-only.
	var ginv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 50, "channel_ids": []int64{boot.ChannelID}}, &ginv)
	var gina identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": ginv.Token, "email": "gina@wp.test", "password": "password123",
		"full_name": "Gina Guest"}, &gina)
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, welcome.ThreadID),
		gina.Token, nil); code != http.StatusOK {
		t.Fatalf("guest web_public read = %d, want 200", code)
	}
	if _, ok := listChannels(t, ts.URL, gina.Token)["town-square"]; ok {
		t.Fatal("web_public listed to a guest non-member (guest list is memberships-only)")
	}

	// Archiving closes the web-public branch for non-members.
	var expo struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "expo", "visibility": "web_public"}, &expo)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, expo.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive expo = %d, want 200", code)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, expo.ChannelID),
		caseyTok, nil); code != http.StatusForbidden {
		t.Fatalf("archived web_public threads = %d, want 403", code)
	}

	// Self-join works for web_public like public; posting follows membership.
	msgURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, town.ChannelID)
	if code := postJSONStatus(t, msgURL, caseyTok, map[string]any{"content": "hi"}); code != http.StatusForbidden {
		t.Fatalf("send before join = %d, want 403", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, town.ChannelID),
		caseyTok, map[string]any{}); code != http.StatusOK {
		t.Fatal("join web_public: want 200")
	}
	if code := postJSONStatus(t, msgURL, caseyTok, map[string]any{"content": "hi"}); code != http.StatusCreated {
		t.Fatalf("send after join: want 201")
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, vault.ChannelID),
		caseyTok, map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("join private = %d, want 403", code)
	}

	// The channel.created payload carries both spellings.
	var pVis string
	var pPriv bool
	if err := pool.QueryRow(ctx, `
		SELECT payload->>'visibility', (payload->>'private')::bool FROM event_log
		WHERE org_id = $1 AND verb = 'channel.created' AND entity_id = $2`,
		boot.OrgID, town.ChannelID).Scan(&pVis, &pPriv); err != nil || pVis != "web_public" || pPriv {
		t.Fatalf("town-square payload visibility=%q private=%v (%v)", pVis, pPriv, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT payload->>'visibility', (payload->>'private')::bool FROM event_log
		WHERE org_id = $1 AND verb = 'channel.created' AND entity_id = $2`,
		boot.OrgID, vault.ChannelID).Scan(&pVis, &pPriv); err != nil || pVis != "private" || !pPriv {
		t.Fatalf("vault payload visibility=%q private=%v (%v)", pVis, pPriv, err)
	}
}
