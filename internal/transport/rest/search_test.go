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
	"github.com/abhinavjha0239/weft/internal/domain/search"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestSearch: FTS + operators (from:, in:, has:link) + ACL scoping + the
// empty-query guard, end-to-end over HTTP.
func TestSearch(t *testing.T) {
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
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "srch", "email": "alice@s.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	genURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID)

	// A second member + author.
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@s.test", "Bob Ray", "bobsearchtok")
	var bobID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='bob@s.test'`,
		boot.OrgID).Scan(&bobID)

	// Owner creates a second channel the search should be able to scope to.
	var other struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "ops"}, &other)
	opsURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, other.ChannelID)

	// Seed messages.
	postJSON(t, genURL, boot.Token, map[string]any{"content": "the deployment pipeline is green"}, nil)
	postJSON(t, genURL, bobTok, map[string]any{"content": "deployment rollback plan drafted"}, nil)
	postJSON(t, genURL, boot.Token, map[string]any{"content": "unrelated lunch chatter"}, nil)
	postJSON(t, genURL, boot.Token, map[string]any{"content": "runbook at https://wiki.example/deploy"}, nil)
	postJSON(t, opsURL, boot.Token, map[string]any{"content": "deployment window is 2am"}, nil)

	// Free-text FTS: "deployment" matches 2 in #general + 1 in #ops = 3.
	// (The runbook message has "deploy" inside a URL, which Postgres tokenizes
	// as a URL, not the word "deployment" — correct FTS behavior.)
	if got := searchIDs(t, ts.URL, boot.Token, "deployment"); len(got) != 3 {
		t.Fatalf(`search "deployment" = %d results, want 3`, len(got))
	}
	// from: filter → only Bob's deployment message.
	res := searchResults(t, ts.URL, boot.Token, `deployment from:"Bob Ray"`)
	if len(res) != 1 || res[0].AuthorID != bobID {
		t.Fatalf("from: filter wrong: %+v", res)
	}
	// in: filter → only the #ops deployment message.
	res = searchResults(t, ts.URL, boot.Token, "deployment in:ops")
	if len(res) != 1 || res[0].ChannelID != other.ChannelID {
		t.Fatalf("in: filter wrong: %+v", res)
	}
	// has:link → the runbook message only.
	res = searchResults(t, ts.URL, boot.Token, "has:link")
	if len(res) != 1 {
		t.Fatalf("has:link = %d, want 1", len(res))
	}
	// Snippet highlights the match (guillemet markers, plain text).
	res = searchResults(t, ts.URL, boot.Token, "pipeline")
	if len(res) != 1 || !containsRune(res[0].Snippet, '»') {
		t.Fatalf("snippet not highlighted: %+v", res)
	}

	// ACL: Bob is NOT in #ops → cannot find the #ops deployment message.
	// Bob sees only the #general deployment messages (2: his + Alice's green one).
	if got := searchIDs(t, ts.URL, bobTok, "deployment"); len(got) != 2 {
		t.Fatalf("bob's ACL-scoped search = %d, want 2 (not the #ops message)", len(got))
	}

	// Empty query → 400.
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/search?q=", nil)
	req.Header.Set("Authorization", "Bearer "+boot.Token)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty query = %d, want 400", resp.StatusCode)
	}
}

func searchResults(t *testing.T, base, token, q string) []search.Result {
	t.Helper()
	var resp struct {
		Results []search.Result `json:"results"`
	}
	url := base + "/api/v1/search?q=" + urlQuery(q)
	if code := getJSON(t, url, token, &resp); code != 200 {
		t.Fatalf("search %q: %d", q, code)
	}
	return resp.Results
}

func searchIDs(t *testing.T, base, token, q string) []int64 {
	t.Helper()
	var ids []int64
	for _, r := range searchResults(t, base, token, q) {
		ids = append(ids, r.MessageID)
	}
	return ids
}

func urlQuery(s string) string {
	// minimal query escaping for the test (space, quotes).
	repl := map[rune]string{' ': "%20", '"': "%22", ':': "%3A", '/': "%2F"}
	out := ""
	for _, r := range s {
		if e, ok := repl[r]; ok {
			out += e
		} else {
			out += string(r)
		}
	}
	return out
}

func containsRune(s string, target rune) bool {
	for _, r := range s {
		if r == target {
			return true
		}
	}
	return false
}
