package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// TestLinkPreviews: P-15 end to end. A message's external link is fetched
// through the SSRF-guarded egress client by the unfurl consumer, cached
// globally, associated, event-logged, and served on the message read
// surface; internal destinations are refused by the guard and cached as
// disallowed; the org toggle stops fetching entirely.
func TestLinkPreviews(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// The page the messages will link to, instrumented per path.
	var ogHits, og2Hits atomic.Int64
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/og":
			ogHits.Add(1)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, `<html><head>
				<meta property="og:title" content="The Plan">
				<meta property="og:description" content="A page about plans.">
				<meta property="og:site_name" content="Plans Inc">
				<meta property="og:image" content="https://img.example/plan.png">
				<title>fallback</title></head><body>hi</body></html>`)
		case "/og2":
			og2Hits.Add(1)
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<html><head><title>og2</title></head></html>`)
		case "/messy":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprintf(w, `<html><head><title>  A%svery%smessy `+strings.Repeat("long", 60)+`title</title></head></html>`, "\n\n", "\x07\t")
		case "/plain":
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, "just text")
		case "/err":
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	defer page.Close()

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	// AllowLoopbackForTests reaches the httptest listener; the guard still
	// rejects link-local/private classes under it (asserted below).
	unfurlSvc := unfurl.New(pool, egress.New(egress.Options{
		UserAgent:             "weftbot-test",
		AllowLoopbackForTests: true,
	}))
	unfurlSvc.SetPerms(permsSvc)
	runner := unfurl.NewRunner(pool, unfurlSvc, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		DM:        dm.New(pool),
		Unfurl:    unfurlSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "unfurl", "email": "a@unfurl.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@unfurl.test", "Bob Ray", "bobunfurltok")

	send := func(content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
		return sent.MessageID
	}
	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "unfurl", boot.OrgID, runner.ProcessOrg)
	}
	getMsg := func(id int64) messaging.Message {
		t.Helper()
		var m messaging.Message
		if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, id), boot.Token, &m); code != 200 {
			t.Fatalf("get message = %d", code)
		}
		return m
	}

	// Happy path: preview row + association + event + read surface.
	ogURL := page.URL + "/og"
	msg1 := send(fmt.Sprintf("see [the plan](%s)", ogURL))
	process()
	m1 := getMsg(msg1)
	if len(m1.LinkPreviews) != 1 {
		t.Fatalf("previews = %+v, want 1", m1.LinkPreviews)
	}
	p := m1.LinkPreviews[0]
	if p.URL != ogURL || p.Title != "The Plan" || p.Description != "A page about plans." ||
		p.SiteName != "Plans Inc" || p.ImageURL != "https://img.example/plan.png" {
		t.Fatalf("preview = %+v", p)
	}
	var eventCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log
		WHERE org_id = $1 AND verb = 'message.preview_added' AND entity_id = $2`,
		boot.OrgID, msg1).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("preview_added events = %d (%v), want 1", eventCount, err)
	}

	// Cache: a second message with the SAME URL never refetches.
	msg2 := send(fmt.Sprintf("again [plan](%s)", ogURL))
	process()
	if got := getMsg(msg2); len(got.LinkPreviews) != 1 || got.LinkPreviews[0].Title != "The Plan" {
		t.Fatalf("second message previews = %+v", got.LinkPreviews)
	}
	if n := ogHits.Load(); n != 1 {
		t.Fatalf("og fetched %d times, want 1 (cache)", n)
	}

	// The e2e SSRF pin: a link-local (cloud metadata) destination is refused
	// by the guard EVEN under the loopback test option — cached disallowed,
	// never associated, invisible on the message.
	msgBad := send("probe [m](http://169.254.169.254/latest/meta-data/)")
	process()
	if got := getMsg(msgBad); len(got.LinkPreviews) != 0 {
		t.Fatalf("metadata URL must not unfurl, got %+v", got.LinkPreviews)
	}
	var badStatus int16
	if err := pool.QueryRow(ctx, `
		SELECT status FROM link_preview WHERE url = 'http://169.254.169.254/latest/meta-data/'`).
		Scan(&badStatus); err != nil || badStatus != 3 {
		t.Fatalf("metadata URL status = %d (%v), want 3 disallowed", badStatus, err)
	}

	// Failure caching: a 500 page and a non-HTML page land as failed rows
	// with no association.
	msgErr := send(fmt.Sprintf("bad [e](%s/err) and [p](%s/plain)", page.URL, page.URL))
	process()
	if got := getMsg(msgErr); len(got.LinkPreviews) != 0 {
		t.Fatalf("failed fetches must not attach, got %+v", got.LinkPreviews)
	}
	var failedCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM link_preview WHERE status = 2 AND url LIKE $1`,
		page.URL+"%").Scan(&failedCount); err != nil || failedCount != 2 {
		t.Fatalf("failed rows = %d (%v), want 2", failedCount, err)
	}

	// Sanitization: whitespace runs and control chars flatten; the title is
	// capped at 200 runes.
	msgMessy := send(fmt.Sprintf("messy [m](%s/messy)", page.URL))
	process()
	messy := getMsg(msgMessy)
	if len(messy.LinkPreviews) != 1 {
		t.Fatalf("messy previews = %+v", messy.LinkPreviews)
	}
	title := messy.LinkPreviews[0].Title
	if strings.ContainsAny(title, "\n\t\x07") || !strings.HasPrefix(title, "A very messy") {
		t.Fatalf("title not sanitized: %q", title)
	}
	if n := len([]rune(title)); n > 200 {
		t.Fatalf("title length = %d runes, want <= 200", n)
	}

	// Toggle: admin reads the default, a member cannot write it, and with it
	// off the consumer fetches NOTHING (the event is consumed, not deferred).
	var setting unfurl.LinkPreviewSetting
	if code := getJSON(t, ts.URL+"/api/v1/admin/link-previews", boot.Token, &setting); code != 200 || !setting.Enabled {
		t.Fatalf("default toggle = %d %+v, want enabled", code, setting)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/link-previews", bobTok,
		map[string]any{"enabled": false}); code != http.StatusForbidden {
		t.Fatalf("member toggle = %d, want 403", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/link-previews", boot.Token,
		map[string]any{"enabled": false}); code != 200 {
		t.Fatalf("admin toggle off failed")
	}
	send(fmt.Sprintf("quiet [q](%s/og2)", page.URL))
	process()
	if n := og2Hits.Load(); n != 0 {
		t.Fatalf("og2 fetched %d times with previews disabled, want 0", n)
	}
	var toggleEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'org.link_previews_changed'`,
		boot.OrgID).Scan(&toggleEvents); err != nil || toggleEvents != 1 {
		t.Fatalf("toggle events = %d (%v), want 1", toggleEvents, err)
	}
}
