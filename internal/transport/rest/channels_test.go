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

	// P-34: a private channel a non-member cannot see masks its existence — a
	// self-join is the oracle-free 404, not a 403 that would confirm #secrets.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, priv.ChannelID),
		memberTok, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("join private: %d, want 404", code)
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

// TestChannelLifecycle: rename with F-22 alias reservation, archive
// freezing writes while history stays readable, unarchive with the
// name-collision guard, and the admin gate.
func TestChannelLifecycle(t *testing.T) {
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
		"org_slug": "lfc", "email": "a@l.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@l.test", "Bob Ray", "boblfctok")

	var ops struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "ops"}, &ops)
	chURL := fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, ops.ChannelID)

	// A plain member cannot administer.
	if code := patchJSON(t, chURL, bobTok, map[string]any{"name": "bobs-club"}); code != http.StatusForbidden {
		t.Fatalf("member rename = %d, want 403", code)
	}

	// Rename reserves the old name for THIS channel.
	if code := patchJSON(t, chURL, boot.Token, map[string]any{"name": "operations"}); code != http.StatusOK {
		t.Fatalf("rename = %d, want 200", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "ops"}); code != http.StatusConflict {
		t.Fatalf("create on reserved name = %d, want 409", code)
	}
	var second struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "intruder"}, &second)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, second.ChannelID),
		boot.Token, map[string]any{"name": "ops"}); code != http.StatusConflict {
		t.Fatalf("rename onto reserved name = %d, want 409", code)
	}
	// The owning channel can take its old name back; the reservation flips.
	if code := patchJSON(t, chURL, boot.Token, map[string]any{"name": "ops"}); code != http.StatusOK {
		t.Fatalf("self re-take = %d, want 200", code)
	}
	var aliasOwner int64
	if err := pool.QueryRow(ctx, `
		SELECT channel_id FROM channel_name_alias WHERE org_id = $1 AND name = 'operations'`,
		boot.OrgID).Scan(&aliasOwner); err != nil || aliasOwner != ops.ChannelID {
		t.Fatalf("operations alias owner = %d (%v), want %d", aliasOwner, err, ops.ChannelID)
	}

	// Archive: writes freeze, join blocks, history stays readable, the
	// channel leaves the list.
	postJSON(t, chURL+"/messages", boot.Token, map[string]any{"content": "pre-archive"}, nil)
	if code := patchJSON(t, chURL, boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive = %d, want 200", code)
	}
	if code := postJSONStatus(t, chURL+"/messages", boot.Token,
		map[string]any{"content": "frozen?"}); code != http.StatusBadRequest {
		t.Fatalf("send into archived = %d, want 400", code)
	}
	if code := postJSONStatus(t, chURL+"/threads", boot.Token,
		map[string]any{"content": "frozen thread?"}); code != http.StatusBadRequest {
		t.Fatalf("thread in archived = %d, want 400", code)
	}
	// Join treats archived as gone (the lookup predicate excludes them).
	if code := postJSONStatus(t, chURL+"/join", bobTok, nil); code != http.StatusNotFound {
		t.Fatalf("join archived = %d, want 404", code)
	}
	var hist struct {
		Messages []struct {
			Source string `json:"source"`
		} `json:"messages"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, ops.RootThreadID),
		boot.Token, &hist); code != http.StatusOK || len(hist.Messages) != 1 {
		t.Fatalf("archived history read = %d (%d msgs), want 200 with history", code, len(hist.Messages))
	}
	var list struct {
		Channels []struct {
			ID int64 `json:"id"`
		} `json:"channels"`
	}
	getJSON(t, ts.URL+"/api/v1/channels", boot.Token, &list)
	for _, c := range list.Channels {
		if c.ID == ops.ChannelID {
			t.Fatal("archived channel still listed")
		}
	}

	// While archived the name is free; a squatter blocks unarchive.
	var squat struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "OPS"}, &squat)
	if code := patchJSON(t, chURL, boot.Token, map[string]any{"archived": false}); code != http.StatusConflict {
		t.Fatalf("unarchive with squatted name = %d, want 409", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/channels/%d", ts.URL, squat.ChannelID),
		boot.Token, map[string]any{"archived": true}); code != http.StatusOK {
		t.Fatalf("archive squatter = %d", code)
	}
	if code := patchJSON(t, chURL, boot.Token, map[string]any{"archived": false}); code != http.StatusOK {
		t.Fatalf("unarchive = %d, want 200", code)
	}
	if code := postJSONStatus(t, chURL+"/messages", boot.Token,
		map[string]any{"content": "thawed"}); code != http.StatusCreated {
		t.Fatalf("send after unarchive = %d, want 201", code)
	}

	// The lifecycle wrote its history.
	for _, verb := range []string{"channel.renamed", "channel.archived", "channel.unarchived"} {
		var n int
		_ = pool.QueryRow(ctx,
			`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = $2`,
			boot.OrgID, verb).Scan(&n)
		if n == 0 {
			t.Fatalf("no %s event recorded", verb)
		}
	}
}
