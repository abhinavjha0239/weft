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
	"github.com/abhinavjha0239/weft/internal/domain/search"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
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
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
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

	// Item discussions join the search scope (the org-visible space rule):
	// description and comments become findable, tagged with the item key.
	var space struct {
		ID int64 `json:"id"`
	}
	postJSON(t, ts.URL+"/api/v1/spaces", boot.Token,
		map[string]any{"key": "OPS", "name": "Ops Work"}, &space)
	var item struct {
		ID       int64  `json:"id"`
		Key      string `json:"key"`
		ThreadID int64  `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/spaces/%d/items", ts.URL, space.ID), boot.Token,
		map[string]any{"title": "Deployment tracker",
			"description": "deployment checklist for the beta"}, &item)
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, item.ThreadID),
		boot.Token, map[string]any{"content": "deployment dry-run passed"}, nil)

	// Alice: 3 channel hits + description + item comment = 5.
	res = searchResults(t, ts.URL, boot.Token, "deployment")
	if len(res) != 5 {
		t.Fatalf(`post-item search "deployment" = %d results, want 5`, len(res))
	}
	itemHits := 0
	for _, r := range res {
		if r.ThreadID == item.ThreadID {
			itemHits++
			if r.ChannelID != 0 || r.ItemKey != item.Key || r.SpaceID != space.ID {
				t.Fatalf("item hit not enriched: %+v", r)
			}
		}
	}
	if itemHits != 2 {
		t.Fatalf("item-thread hits = %d, want 2 (description + comment)", itemHits)
	}

	// Bob shares no channel with the space, but space threads are
	// org-visible: his 2 channel hits grow by the same 2 item hits, while
	// the #ops channel message stays hidden from him.
	if got := searchIDs(t, ts.URL, bobTok, "deployment"); len(got) != 4 {
		t.Fatalf("bob's post-item search = %d, want 4 (still not the #ops message)", len(got))
	}
	// in: remains channel-scoped — item messages have no channel.
	res = searchResults(t, ts.URL, boot.Token, "deployment in:ops")
	if len(res) != 1 {
		t.Fatalf("in:ops after items = %d, want 1", len(res))
	}

	// A promoted channel thread keeps channel scoping AND gains its key.
	var th struct {
		ThreadID int64 `json:"thread_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"title": "postmortem",
			"content": "deployment postmortem draft"}, &th)
	var promoted struct {
		Key string `json:"key"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/promote", ts.URL, th.ThreadID),
		boot.Token, map[string]any{"space_id": space.ID}, &promoted)
	res = searchResults(t, ts.URL, boot.Token, `"deployment postmortem"`)
	if len(res) != 1 || res[0].ChannelID != boot.ChannelID || res[0].ItemKey != promoted.Key {
		t.Fatalf("promoted-thread hit wrong: %+v", res)
	}
}

// TestSearchMoreOperators: the P-10 operators added on top of the v1 subset —
// has:attachment, has:image, is:dm, and from:<id> — each alone and combined,
// with the per-viewer read ACL preserved (is:dm returns ONLY the caller's own
// DM messages, never channel messages, and a guest stays inside her own
// visibility). Unknown has:/is: values must be searched as literal text, not
// error.
func TestSearchMoreOperators(t *testing.T) {
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
		Worktrack: worktrack.New(pool, permsSvc, msgSvc),
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
		"org_slug": "srch2", "email": "alice@s2.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	genURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID)

	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@s2.test", "Bob Ray", "bob2searchtok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE org_id=$1 AND email='bob@s2.test'`,
		boot.OrgID).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}

	sendMsg := func(url, token, content string) int64 {
		t.Helper()
		var out struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, url, token, map[string]any{"content": content}, &out)
		if out.MessageID == 0 {
			t.Fatalf("send %q returned no message id", content)
		}
		return out.MessageID
	}
	setFlag := func(id int64, col string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE message SET "+col+" = true WHERE id = $1", id); err != nil {
			t.Fatalf("set %s: %v", col, err)
		}
	}

	// Channel messages in #general.
	sendMsg(genURL, boot.Token, "channel widget alpha")
	sendMsg(genURL, bobTok, "bob channel note")
	sendMsg(genURL, boot.Token, "alice runbook https://wiki.example/alice")
	bobLinkID := sendMsg(genURL, bobTok, "bob runbook https://wiki.example/bob")
	imageID := sendMsg(genURL, boot.Token, "sunset picture here")
	setFlag(imageID, "has_image")
	attachID := sendMsg(genURL, boot.Token, "quarterly report file")
	setFlag(attachID, "has_attachment")
	frobID := sendMsg(genURL, boot.Token, "quux frobnicate widget")

	// A private #vault the guest will live in (alice is its only other member).
	var vault struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "private": true}, &vault)
	vaultURL := fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, vault.ChannelID)
	sendMsg(vaultURL, boot.Token, "vault confidential note")

	// Guest gina: member of #vault only.
	var ginv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 50, "channel_ids": []int64{vault.ChannelID}}, &ginv)
	var gina identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": ginv.Token, "email": "gina@s2.test", "password": "password123",
		"full_name": "Gina Guest"}, &gina)

	// Two DM conversations: alice⇄bob (alice writes) and gina⇄alice (gina
	// writes). Alice participates in both; bob and gina in one each.
	var abDM dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &abDM)
	abMsgID := sendMsg(fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, abDM.RootThreadID),
		boot.Token, "alicebob dm secret")
	var gaDM dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", gina.Token,
		map[string]any{"user_ids": []int64{boot.UserID}}, &gaDM)
	gaMsgID := sendMsg(fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, gaDM.RootThreadID),
		gina.Token, "ginaalice dm hello")

	ids := func(res []search.Result) map[int64]bool {
		m := make(map[int64]bool, len(res))
		for _, r := range res {
			m[r.MessageID] = true
		}
		return m
	}

	// has:image alone → only the flagged image message (a dropped filter would
	// return every visible message, so len != 1 catches it).
	res := searchResults(t, ts.URL, boot.Token, "has:image")
	if len(res) != 1 || res[0].MessageID != imageID {
		t.Fatalf("has:image = %+v, want only msg %d", res, imageID)
	}
	// has:attachment alone → only the flagged attachment message.
	res = searchResults(t, ts.URL, boot.Token, "has:attachment")
	if len(res) != 1 || res[0].MessageID != attachID {
		t.Fatalf("has:attachment = %+v, want only msg %d", res, attachID)
	}

	// from:<id> alone → bob's two channel messages (both author bob).
	res = searchResults(t, ts.URL, boot.Token, fmt.Sprintf("from:%d", bobID))
	if len(res) != 2 {
		t.Fatalf("from:%d = %d results, want 2", bobID, len(res))
	}
	for _, r := range res {
		if r.AuthorID != bobID {
			t.Fatalf("from:%d returned msg by author %d", bobID, r.AuthorID)
		}
	}
	// Combined from:<id> has:link → only bob's link message: dropping either
	// filter would return 2 (both link messages, or both bob messages).
	res = searchResults(t, ts.URL, boot.Token, fmt.Sprintf("from:%d has:link", bobID))
	if len(res) != 1 || res[0].MessageID != bobLinkID || res[0].AuthorID != bobID {
		t.Fatalf("from:%d has:link = %+v, want only bob's link msg %d", bobID, res, bobLinkID)
	}

	// is:dm for alice → both DM messages, and NO channel message (every hit
	// has channel_id 0). A dropped is:dm filter would surface channel hits.
	res = searchResults(t, ts.URL, boot.Token, "is:dm")
	got := ids(res)
	if len(res) != 2 || !got[abMsgID] || !got[gaMsgID] {
		t.Fatalf("alice is:dm = %+v, want exactly {%d,%d}", res, abMsgID, gaMsgID)
	}
	for _, r := range res {
		if r.ChannelID != 0 {
			t.Fatalf("is:dm returned a channel message: %+v", r)
		}
	}
	// is:dm for bob → ONLY the alice⇄bob DM (never gina⇄alice, which he can't
	// see): proves is:dm rides the per-viewer participation ACL, not bypasses it.
	res = searchResults(t, ts.URL, bobTok, "is:dm")
	if len(res) != 1 || res[0].MessageID != abMsgID {
		t.Fatalf("bob is:dm = %+v, want only alice⇄bob msg %d", res, abMsgID)
	}

	// Unknown has:frobnicate is literal text (never a 400): searchResults
	// fatals on a non-200, and the token searches as the word "frobnicate".
	res = searchResults(t, ts.URL, boot.Token, "has:frobnicate")
	if len(res) != 1 || res[0].MessageID != frobID {
		t.Fatalf("has:frobnicate (as text) = %+v, want only msg %d", res, frobID)
	}

	// Guest confinement: gina reads only #vault + her own DM.
	// in:general → empty (not an error): she is not a #general member.
	if res = searchResults(t, ts.URL, gina.Token, "in:general"); len(res) != 0 {
		t.Fatalf("guest in:general = %d results, want 0", len(res))
	}
	// A #general-only word is invisible to her; her #vault word is not.
	if res = searchResults(t, ts.URL, gina.Token, "widget"); len(res) != 0 {
		t.Fatalf("guest sees #general word = %d results, want 0", len(res))
	}
	if res = searchResults(t, ts.URL, gina.Token, "confidential"); len(res) != 1 {
		t.Fatalf("guest #vault search = %d results, want 1", len(res))
	}
	// is:dm for gina → ONLY her gina⇄alice DM, never the alice⇄bob DM.
	res = searchResults(t, ts.URL, gina.Token, "is:dm")
	if len(res) != 1 || res[0].MessageID != gaMsgID {
		t.Fatalf("guest is:dm = %+v, want only gina⇄alice msg %d", res, gaMsgID)
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
