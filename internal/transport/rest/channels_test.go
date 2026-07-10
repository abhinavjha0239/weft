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

// TestChannelCRUD: create public+private, discoverability rules, self-join
// (public only), duplicate-name 409, leave revokes access, re-join works.
func TestChannelCRUD(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, perms.New(pool)),
		Messaging: messaging.New(pool, perms.New(pool)),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "crud", "email": "o@c.test", "password": "password123",
		"full_name": "Owner",
	}, &boot)
	memberTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"m@c.test", "Member", "membertok")

	// Owner creates a public and a private channel.
	var pub, priv struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "launch", "description": "public plans"}, &pub)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "secrets", "private": true}, &priv)
	if pub.ChannelID == 0 || pub.RootThreadID == 0 || priv.ChannelID == 0 {
		t.Fatalf("create incomplete: %+v %+v", pub, priv)
	}

	// Duplicate name (case-insensitive) → 409.
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "Launch"}); code != http.StatusConflict {
		t.Fatalf("duplicate name: %d, want 409", code)
	}

	// Discoverability: member sees general + launch (public), NOT secrets.
	list := listChannels(t, ts.URL, memberTok)
	if _, ok := list["secrets"]; ok {
		t.Fatal("private channel leaked to non-member listing")
	}
	lc, ok := list["launch"]
	if !ok || lc.Member || lc.Private {
		t.Fatalf("launch listing wrong: %+v (want visible, non-member, public)", lc)
	}
	// Owner sees secrets as member.
	if oc := listChannels(t, ts.URL, boot.Token)["secrets"]; !oc.Member || !oc.Private {
		t.Fatalf("owner's secrets listing wrong: %+v", oc)
	}

	// Member cannot send before joining (visibility gate), can after join.
	msgURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, pub.ChannelID)
	if code := postJSONStatus(t, msgURL, memberTok, map[string]any{"content": "hi"}); code != http.StatusForbidden {
		t.Fatalf("send before join: %d, want 403", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, pub.ChannelID),
		memberTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("join public: want 200")
	}
	if code := postJSONStatus(t, msgURL, memberTok, map[string]any{"content": "hi"}); code != http.StatusCreated {
		t.Fatalf("send after join: %d, want 201", code)
	}

	// Private channels reject self-join.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, priv.ChannelID),
		memberTok, map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("join private: %d, want 403", code)
	}

	// Leave → membership revoked (send 403 again); re-join restores.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, pub.ChannelID),
		memberTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("leave: want 200")
	}
	if code := postJSONStatus(t, msgURL, memberTok, map[string]any{"content": "post-leave"}); code != http.StatusForbidden {
		t.Fatalf("send after leave: %d, want 403", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, pub.ChannelID),
		memberTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("re-join: want 200")
	}
	if code := postJSONStatus(t, msgURL, memberTok, map[string]any{"content": "back"}); code != http.StatusCreated {
		t.Fatalf("send after re-join: %d, want 201", code)
	}

	// member.joined / member.left events exist with channel payloads.
	var joins, leaves int
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE verb = 'member.joined'),
		       count(*) FILTER (WHERE verb = 'member.left')
		FROM event_log WHERE org_id = $1`, boot.OrgID).Scan(&joins, &leaves)
	if joins < 2 || leaves < 1 {
		t.Fatalf("membership events: joins=%d leaves=%d", joins, leaves)
	}
}

func listChannels(t *testing.T, base, token string) map[string]messaging.ChannelSummary {
	t.Helper()
	var resp struct {
		Channels []messaging.ChannelSummary `json:"channels"`
	}
	if code := getJSON(t, base+"/api/v1/channels", token, &resp); code != 200 {
		t.Fatalf("list channels: %d", code)
	}
	out := map[string]messaging.ChannelSummary{}
	for _, c := range resp.Channels {
		out[c.Name] = c
	}
	return out
}
