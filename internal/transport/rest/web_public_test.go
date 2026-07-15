package rest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
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

// getRawNoAuth GETs with no Authorization header at all — the anonymous
// caller's exact view — returning status and raw body (for the oracle-free
// body comparisons).
func getRawNoAuth(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestPublicChannelAnon: P-16 (anonymous surface). The /api/v1/public
// allowlist serves a web-public channel's metadata, threads, messages,
// bounded author names, and link previews to callers with no token; private
// and plain-public channels are oracle-free 404s (byte-identical bodies);
// reactions never appear in the projection; deleted messages never appear;
// and everything outside the allowlist stays closed without a token.
func TestPublicChannelAnon(t *testing.T) {
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
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "anonwp", "email": "own@anon.test", "password": "password123",
		"full_name": "Pat Owner",
	}, &boot)

	var town, vault struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "town-square", "description": "open to the world",
			"visibility": "web_public"}, &town)
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "visibility": "private"}, &vault)
	var welcome struct {
		ThreadID      int64 `json:"thread_id"`
		RootMessageID int64 `json:"root_message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, town.ChannelID),
		boot.Token, map[string]any{"title": "welcome", "content": "hello world"}, &welcome)

	// A reaction exists on the message; the anonymous projection must not
	// carry it (ReactionAgg names org members). The link preview is seeded
	// directly — the unfurl pipeline itself is P-15-tested.
	if code := putJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/reactions/%s",
		ts.URL, welcome.RootMessageID, url.PathEscape("👍")), boot.Token, nil); code != http.StatusOK {
		t.Fatalf("seed reaction = %d", code)
	}
	var previewID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO link_preview (url_hash, url, title, status, expires_at)
		VALUES ('anonhash', 'https://example.com/x', 'Example', 1, now() + interval '1 day')
		RETURNING id`).Scan(&previewID); err != nil {
		t.Fatalf("seed preview: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO message_link_preview (message_id, preview_id, position)
		VALUES ($1, $2, 0)`, welcome.RootMessageID, previewID); err != nil {
		t.Fatalf("seed association: %v", err)
	}

	// Metadata, threads, messages — all with NO token.
	pubBase := ts.URL + "/api/v1/public"
	var meta messaging.PublicChannel
	if code := getJSON(t, fmt.Sprintf("%s/channels/%d", pubBase, town.ChannelID), "", &meta); code != http.StatusOK ||
		meta.Name != "town-square" || meta.Description != "open to the world" || meta.RootThreadID == 0 {
		t.Fatalf("anon metadata = %d %+v", code, meta)
	}
	var tpage struct {
		Threads []struct {
			ID    int64  `json:"id"`
			Title string `json:"title"`
		} `json:"threads"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/channels/%d/threads", pubBase, town.ChannelID), "", &tpage); code != http.StatusOK ||
		len(tpage.Threads) != 1 || tpage.Threads[0].Title != "welcome" {
		t.Fatalf("anon threads = %d %+v", code, tpage)
	}
	msgURL := fmt.Sprintf("%s/threads/%d/messages", pubBase, welcome.ThreadID)
	code, rawBody := getRawNoAuth(t, msgURL)
	if code != http.StatusOK {
		t.Fatalf("anon messages = %d", code)
	}
	if strings.Contains(rawBody, "reactions") {
		t.Fatalf("anonymous projection leaked reactions: %s", rawBody)
	}
	var mpage messaging.PublicMessagePage
	if code := getJSON(t, msgURL, "", &mpage); code != http.StatusOK || len(mpage.Messages) != 1 {
		t.Fatalf("anon messages decode = %d (%d msgs)", code, len(mpage.Messages))
	}
	m := mpage.Messages[0]
	if m.ID != welcome.RootMessageID || m.Rendered == "" || m.CreatedAt.IsZero() {
		t.Fatalf("anon message projection = %+v", m)
	}
	if len(m.LinkPreviews) != 1 || m.LinkPreviews[0].Title != "Example" {
		t.Fatalf("anon link previews = %+v, want the seeded one", m.LinkPreviews)
	}
	if a, ok := mpage.Authors[m.AuthorID]; !ok || a.FullName != "Pat Owner" {
		t.Fatalf("anon authors = %+v, want bounded author names", mpage.Authors)
	}

	// Oracle-free 404s: private, plain public (NOT web-public), and absent
	// channels answer byte-identically; same for their threads.
	c1, b1 := getRawNoAuth(t, fmt.Sprintf("%s/channels/%d", pubBase, vault.ChannelID))
	c2, b2 := getRawNoAuth(t, fmt.Sprintf("%s/channels/%d", pubBase, boot.ChannelID))
	c3, b3 := getRawNoAuth(t, fmt.Sprintf("%s/channels/%d", pubBase, int64(99999999)))
	if c1 != http.StatusNotFound || c2 != http.StatusNotFound || c3 != http.StatusNotFound {
		t.Fatalf("anon probe codes = %d/%d/%d, want 404s", c1, c2, c3)
	}
	if b1 != b2 || b2 != b3 {
		t.Fatalf("404 oracle: bodies differ:\n%s\n%s\n%s", b1, b2, b3)
	}
	c4, b4 := getRawNoAuth(t, fmt.Sprintf("%s/threads/%d/messages", pubBase, vault.RootThreadID))
	c5, b5 := getRawNoAuth(t, fmt.Sprintf("%s/threads/%d/messages", pubBase, int64(99999999)))
	if c4 != http.StatusNotFound || c5 != http.StatusNotFound || b4 != b5 {
		t.Fatalf("thread 404 oracle = %d/%d bodies equal=%v", c4, c5, b4 == b5)
	}

	// Deleted messages never appear on the anonymous path.
	var extra struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, welcome.ThreadID),
		boot.Token, map[string]any{"content": "soon gone"}, &extra)
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, extra.MessageID), nil)
	req.Header.Set("Authorization", "Bearer "+boot.Token)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("delete extra: %v", err)
	} else {
		resp.Body.Close()
	}
	if code := getJSON(t, msgURL, "", &mpage); code != http.StatusOK || len(mpage.Messages) != 1 {
		t.Fatalf("anon after delete = %d (%d msgs), want the survivor only", code, len(mpage.Messages))
	}

	// Everything outside the allowlist stays closed to anonymous callers:
	// no POST verbs exist under /public, and the authed API 401s.
	if code := postJSONStatus(t, msgURL, "", map[string]any{"content": "anon spam"}); code != http.StatusMethodNotAllowed {
		t.Fatalf("anon POST on public namespace = %d, want 405", code)
	}
	if code := getJSON(t, ts.URL+"/api/v1/channels", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("anon channel list = %d, want 401", code)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, town.ChannelID),
		"", map[string]any{}); code != http.StatusUnauthorized {
		t.Fatalf("anon join = %d, want 401", code)
	}
	if code := getJSON(t, ts.URL+"/api/v1/search?q=hello", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("anon search = %d, want 401", code)
	}
	if code, _ := getRawNoAuth(t, ts.URL+"/api/v1/gateway"); code == http.StatusOK {
		t.Fatal("anon gateway must not be 200")
	}
}

// TestProtectedHistory: P-16 (protected channels). A private channel created
// with protected=true stamps history_from on invite-accept joins; the
// newcomer's reads start at their join while the creator keeps full history;
// the boundary survives leave and a rejoin reactivation; shared channels
// stay boundary-free; protected without private is a 400.
func TestProtectedHistory(t *testing.T) {
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
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "prot", "email": "ora@prot.test", "password": "password123",
		"full_name": "Ora Owner",
	}, &boot)

	// Protected is private-only: default-public and web_public both 400.
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "x1", "protected": true}); code != http.StatusBadRequest {
		t.Fatalf("protected public = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "x2", "visibility": "web_public", "protected": true}); code != http.StatusBadRequest {
		t.Fatalf("protected web_public = %d, want 400", code)
	}

	var ledger struct {
		ChannelID    int64 `json:"channel_id"`
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "ledger", "visibility": "private", "protected": true}, &ledger)
	var vis, hm int16
	if err := pool.QueryRow(ctx,
		`SELECT visibility, history_mode FROM channel WHERE id = $1`,
		ledger.ChannelID).Scan(&vis, &hm); err != nil || vis != 2 || hm != 2 {
		t.Fatalf("ledger visibility=%d history_mode=%d (%v), want 2/2", vis, hm, err)
	}
	// The creator's own membership carries no boundary (NULL = all history).
	var ownerHF *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT history_from FROM channel_member
		WHERE channel_id = $1 AND user_id = $2`,
		ledger.ChannelID, boot.UserID).Scan(&ownerHF); err != nil || ownerHF != nil {
		t.Fatalf("creator history_from = %v (%v), want NULL", ownerHF, err)
	}

	// m1 exists BEFORE mia joins; #general gets a pre-join message too (the
	// shared-channel contrast).
	var ideas struct {
		ThreadID      int64 `json:"thread_id"`
		RootMessageID int64 `json:"root_message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, ledger.ChannelID),
		boot.Token, map[string]any{"title": "ideas", "content": "before times"}, &ideas)
	var genMsg struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
		boot.Token, map[string]any{"content": "old news"}, &genMsg)

	// Invite mia into the protected channel (plus the default #general).
	var minv identity.Invite
	postJSON(t, ts.URL+"/api/v1/invites", boot.Token, map[string]any{
		"role": 40, "channel_ids": []int64{ledger.ChannelID}}, &minv)
	var mia identity.AcceptInviteResult
	postJSON(t, ts.URL+"/api/v1/invites/accept", "", map[string]any{
		"token": minv.Token, "email": "mia@prot.test", "password": "password123",
		"full_name": "Mia New"}, &mia)
	var miaHF, miaGenHF *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT history_from FROM channel_member
		WHERE channel_id = $1 AND user_id = $2`,
		ledger.ChannelID, mia.UserID).Scan(&miaHF); err != nil || miaHF == nil {
		t.Fatalf("mia ledger history_from = %v (%v), want stamped", miaHF, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT history_from FROM channel_member
		WHERE channel_id = $1 AND user_id = $2`,
		boot.ChannelID, mia.UserID).Scan(&miaGenHF); err != nil || miaGenHF != nil {
		t.Fatalf("mia general history_from = %v (%v), want NULL (shared)", miaGenHF, err)
	}

	// m2 lands after the join.
	var m2 struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, ideas.ThreadID),
		boot.Token, map[string]any{"content": "after mia"}, &m2)

	// mia sees only m2; the creator sees both; the thread summary itself is
	// visible to mia (the boundary hides content, not the conversation).
	var mpage struct {
		Messages []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	msgsURL := fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, ideas.ThreadID)
	if code := getJSON(t, msgsURL, mia.Token, &mpage); code != http.StatusOK ||
		len(mpage.Messages) != 1 || mpage.Messages[0].ID != m2.MessageID {
		t.Fatalf("mia protected view = %d %+v, want only m2", code, mpage.Messages)
	}
	if code := getJSON(t, msgsURL, boot.Token, &mpage); code != http.StatusOK || len(mpage.Messages) != 2 {
		t.Fatalf("creator protected view = %d (%d msgs), want 2", code, len(mpage.Messages))
	}
	var tpage struct {
		Threads []struct {
			ID int64 `json:"id"`
		} `json:"threads"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/threads", ts.URL, ledger.ChannelID),
		mia.Token, &tpage); code != http.StatusOK || len(tpage.Threads) != 1 {
		t.Fatalf("mia thread list = %d (%d), want the summary visible", code, len(tpage.Threads))
	}
	// Get honors the boundary — pre-join is an oracle-free 404, post-join a
	// 200 — and the shared channel has no boundary at all.
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, ideas.RootMessageID),
		mia.Token, nil); code != http.StatusNotFound {
		t.Fatalf("mia Get pre-join = %d, want 404", code)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, m2.MessageID),
		mia.Token, nil); code != http.StatusOK {
		t.Fatalf("mia Get post-join = %d, want 200", code)
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, genMsg.MessageID),
		mia.Token, nil); code != http.StatusOK {
		t.Fatalf("mia Get shared pre-join = %d, want 200", code)
	}

	// Leave: access closes, the stamp survives on the preserved row.
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, ledger.ChannelID),
		mia.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("mia leave = %d", code)
	}
	if code := getJSON(t, msgsURL, mia.Token, nil); code != http.StatusForbidden {
		t.Fatalf("mia after leave = %d, want 403", code)
	}
	if err := pool.QueryRow(ctx, `
		SELECT history_from FROM channel_member
		WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NOT NULL`,
		ledger.ChannelID, mia.UserID).Scan(&miaHF); err != nil || miaHF == nil {
		t.Fatalf("history_from after leave = %v (%v), want preserved", miaHF, err)
	}
	// Reactivate with the exact JoinChannel rejoin shape (private channels
	// gain a member-add endpoint in a later slice — recorded gap): the
	// preserved row keeps its ORIGINAL boundary, so the view is unchanged.
	if _, err := pool.Exec(ctx, `
		UPDATE channel_member SET unsubscribed_at = NULL
		WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NOT NULL`,
		ledger.ChannelID, mia.UserID); err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if code := getJSON(t, msgsURL, mia.Token, &mpage); code != http.StatusOK ||
		len(mpage.Messages) != 1 || mpage.Messages[0].ID != m2.MessageID {
		t.Fatalf("mia after rejoin = %d %+v, want only m2 still", code, mpage.Messages)
	}
}
