package rest

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
	"github.com/abhinavjha0239/weft/internal/platform/webpush"
)

// capturedPush is one request the fake push service received.
type capturedPush struct {
	path, encoding, auth, ttl string
	body                      []byte
}

// TestPushMedium: P-21 Web Push end to end (real PG + an httptest push service
// + the egress test-loopback option, the P-24 pattern). Covers subscribe →
// mention → lane → an aes128gcm request with a vapid Authorization whose body
// DECRYPTS (test-side RFC 8291) to who/where JSON with NO message content; DND
// snooze skips-then-delivers; a medium-3 pref off silences; 410 deletes the
// row; two subscriptions ring twice; unconfigured 409s and the lane no-ops.
//
// RED/GREEN pins (verified while implementing, see the marked blocks):
//  1. EGRESS BYPASS — replace w.eg.PostRaw in PushWorker.send with a plain
//     http.Client: the strict lane DIALS the loopback /private endpoint, so
//     `privateHits == 0` and the `subscription deleted` assert both go red.
//  2. SEEN DROP — remove `n.seen_at IS NULL` from RunOnce's claim: Frank's
//     already-seen notification is pushed, so the `no push for seen` assert
//     (no /p/frank capture) goes red.
func TestPushMedium(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	defer func() { cancel(); pool.Close() }()
	resetAndMigrate(t, ctx, pool)

	// The fake push service: capture every request, 410 on /gone*, 201 else.
	var (
		mu       sync.Mutex
		captured []capturedPush
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		captured = append(captured, capturedPush{
			path: r.URL.Path, encoding: r.Header.Get("Content-Encoding"),
			auth: r.Header.Get("Authorization"), ttl: r.Header.Get("TTL"), body: b,
		})
		mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/gone") {
			w.WriteHeader(http.StatusGone)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer receiver.Close()
	take := func() []capturedPush {
		mu.Lock()
		defer mu.Unlock()
		out := captured
		captured = nil
		return out
	}
	find := func(caps []capturedPush, path string) *capturedPush {
		for i := range caps {
			if caps[i].path == path {
				return &caps[i]
			}
		}
		return nil
	}

	vapidPub, vapidPriv, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatalf("vapid keys: %v", err)
	}
	sender, err := webpush.NewSender(vapidPub, vapidPriv, "mailto:push@psh.test")
	if err != nil {
		t.Fatalf("sender: %v", err)
	}

	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	notifSvc := notification.New(pool)
	notifSvc.SetPush(sender)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notifSvc,
	}))
	defer ts.Close()
	// A SECOND server whose notification service has NO push sender configured.
	tsUnconfigured := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer tsUnconfigured.Close()

	runner := notification.NewRunner(pool, hub, slog.Default())
	// The loopback lane reaches the httptest receiver; the strict lane is
	// production-shaped (no test allowances) — the receiver's loopback+odd-port
	// address is exactly the destination the guard must refuse.
	loopback := notification.NewPushWorker(pool, sender,
		egress.New(egress.Options{UserAgent: "weftbot-test/1.0", AllowLoopbackForTests: true}), slog.Default())
	strict := notification.NewPushWorker(pool, sender,
		egress.New(egress.Options{UserAgent: "weftbot-test/1.0"}), slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "psh", "email": "a@psh.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	newUser := func(name, email, tok string) (int64, string) {
		t.Helper()
		token := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID, email, name, tok)
		var uid int64
		if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = $1`, email).Scan(&uid); err != nil {
			t.Fatalf("uid %s: %v", email, err)
		}
		return uid, token
	}
	mention := func(name string) {
		t.Helper()
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": "hey @**" + name + "** SECRET-BODY-TEXT"}, nil)
	}
	process := func() {
		t.Helper()
		drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	}
	seedSub := func(uid int64, endpoint string) (*ecdh.PrivateKey, []byte) {
		t.Helper()
		priv, err := ecdh.P256().GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("ua key: %v", err)
		}
		auth := make([]byte, 16)
		if _, err := rand.Read(auth); err != nil {
			t.Fatalf("auth: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO push_subscription (org_id, user_id, endpoint, p256dh, auth)
			VALUES ($1, $2, $3, $4, $5)`,
			boot.OrgID, uid, endpoint, priv.PublicKey().Bytes(), auth); err != nil {
			t.Fatalf("seed sub: %v", err)
		}
		return priv, auth
	}
	subCount := func(uid int64) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_subscription WHERE user_id = $1`, uid).Scan(&n); err != nil {
			t.Fatalf("sub count: %v", err)
		}
		return n
	}
	decryptPayload := func(priv *ecdh.PrivateKey, auth, body []byte) map[string]any {
		t.Helper()
		plain, err := webpush.Decrypt(priv.Bytes(), auth, body)
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if strings.Contains(string(plain), "SECRET-BODY-TEXT") {
			t.Fatalf("payload leaked message content: %s", plain)
		}
		var m map[string]any
		if err := json.Unmarshal(plain, &m); err != nil {
			t.Fatalf("payload json: %v (%s)", err, plain)
		}
		return m
	}

	// ---- Unconfigured: vapid-key 404, subscribe 409, lane no-op. ----
	var vk struct {
		Key string `json:"key"`
	}
	if code := getJSON(t, tsUnconfigured.URL+"/api/v1/push/vapid-key", boot.Token, &vk); code != http.StatusNotFound {
		t.Fatalf("unconfigured vapid-key = %d, want 404", code)
	}
	if code := postJSONStatus(t, tsUnconfigured.URL+"/api/v1/me/push-subscriptions", boot.Token,
		map[string]any{"endpoint": "https://push.example.test/x", "keys": map[string]string{
			"p256dh": vapidPub, "auth": "MDEyMzQ1Njc4OWFiY2RlZg"}}); code != http.StatusConflict {
		t.Fatalf("unconfigured subscribe = %d, want 409", code)
	}
	nilWorker := notification.NewPushWorker(pool, nil, nil, slog.Default())
	if n, err := nilWorker.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("unconfigured lane = %d (%v), want no-op", n, err)
	}

	// ---- Configured: vapid-key discovery returns the server's public key. ----
	if code := getJSON(t, ts.URL+"/api/v1/push/vapid-key", boot.Token, &vk); code != http.StatusOK || vk.Key != vapidPub {
		t.Fatalf("vapid-key = %d %q, want 200 + %q", code, vk.Key, vapidPub)
	}

	// ---- Subscription API: create, list (truncated), validation, delete, cap. ----
	valID, valTok := newUser("Val Idation", "val@psh.test", "valtok")
	goodKeys := map[string]string{"p256dh": vapidPub, "auth": "MDEyMzQ1Njc4OWFiY2RlZg"} // 65 + 16 bytes
	var sub struct {
		ID int64 `json:"id"`
	}
	if code := postJSONStatus2(t, ts.URL+"/api/v1/me/push-subscriptions", valTok,
		map[string]any{"endpoint": "https://push.example.test/val1", "keys": goodKeys}, &sub); code != http.StatusCreated || sub.ID == 0 {
		t.Fatalf("subscribe = %d id %d, want 201", code, sub.ID)
	}
	var listResp struct {
		Subscriptions []notification.PushSubscriptionView `json:"subscriptions"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/me/push-subscriptions", valTok, &listResp); code != http.StatusOK ||
		len(listResp.Subscriptions) != 1 {
		t.Fatalf("list = %d %+v, want 1", code, listResp.Subscriptions)
	}
	if ep := listResp.Subscriptions[0].Endpoint; strings.Contains(ep, "val1") || !strings.HasPrefix(ep, "https://push.example.test") {
		t.Fatalf("endpoint not truncated: %q", ep)
	}
	badCases := []map[string]any{
		{"endpoint": "http://push.example.test/x", "keys": goodKeys},                                                               // not https
		{"endpoint": "https://push.example.test:8443/x", "keys": goodKeys},                                                         // odd port
		{"endpoint": "https://push.example.test/x", "keys": map[string]string{"p256dh": "AAAA", "auth": "MDEyMzQ1Njc4OWFiY2RlZg"}}, // short key
		{"endpoint": "https://push.example.test/x", "keys": map[string]string{"p256dh": vapidPub, "auth": "AAAA"}},                 // short auth
	}
	for i, bc := range badCases {
		if code := postJSONStatus(t, ts.URL+"/api/v1/me/push-subscriptions", valTok, bc); code != http.StatusBadRequest {
			t.Fatalf("bad subscribe %d = %d, want 400", i, code)
		}
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/me/push-subscriptions/%d", ts.URL, sub.ID), valTok); code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/me/push-subscriptions/%d", ts.URL, sub.ID), valTok); code != http.StatusNotFound {
		t.Fatalf("re-delete = %d, want oracle-free 404", code)
	}
	// Cap at 10: the 11th distinct endpoint is refused.
	for i := 0; i < 10; i++ {
		if code := postJSONStatus(t, ts.URL+"/api/v1/me/push-subscriptions", valTok,
			map[string]any{"endpoint": fmt.Sprintf("https://push.example.test/cap%d", i), "keys": goodKeys}); code != http.StatusCreated {
			t.Fatalf("cap subscribe %d = %d, want 201", i, code)
		}
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/me/push-subscriptions", valTok,
		map[string]any{"endpoint": "https://push.example.test/cap11", "keys": goodKeys}); code != http.StatusConflict {
		t.Fatalf("11th subscribe = %d, want 409", code)
	}
	_ = valID

	// ---- Delivery happy path: mention → one push that decrypts to who/where. ----
	bobID, _ := newUser("Bob Ray", "bob@psh.test", "bobtok")
	bobPriv, bobAuth := seedSub(bobID, receiver.URL+"/p/bob")
	mention("Bob Ray")
	process()
	if n, err := loopback.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("bob sweep = %d (%v), want 1", n, err)
	}
	caps := take()
	c := find(caps, "/p/bob")
	if len(caps) != 1 || c == nil {
		t.Fatalf("bob captures = %+v, want one to /p/bob", caps)
	}
	if c.encoding != "aes128gcm" || !strings.HasPrefix(c.auth, "vapid ") || c.ttl != "86400" {
		t.Fatalf("bob request headers: enc=%q auth=%q ttl=%q", c.encoding, c.auth, c.ttl)
	}
	payload := decryptPayload(bobPriv, bobAuth, c.body)
	if payload["kind"].(float64) != 2 || payload["actor_name"] != "Alice Chen" || payload["channel_name"] != "general" {
		t.Fatalf("bob payload = %+v, want kind 2 / Alice Chen / general", payload)
	}

	// ---- Two subscriptions → two deliveries (a phone and a laptop). ----
	graceID, _ := newUser("Grace Poe", "grace@psh.test", "gracetok")
	gaPriv, gaAuth := seedSub(graceID, receiver.URL+"/p/grace-a")
	gbPriv, gbAuth := seedSub(graceID, receiver.URL+"/p/grace-b")
	mention("Grace Poe")
	process()
	if n, err := loopback.RunOnce(ctx); err != nil || n != 2 {
		t.Fatalf("grace sweep = %d (%v), want 2 (both devices)", n, err)
	}
	caps = take()
	ca, cb := find(caps, "/p/grace-a"), find(caps, "/p/grace-b")
	if len(caps) != 2 || ca == nil || cb == nil {
		t.Fatalf("grace captures = %+v, want both devices", caps)
	}
	decryptPayload(gaPriv, gaAuth, ca.body)
	decryptPayload(gbPriv, gbAuth, cb.body)

	// ---- PIN 2: seen before the tick → never pushed. ----
	frankID, frankTok := newUser("Frank Fox", "frank@psh.test", "franktok")
	seedSub(frankID, receiver.URL+"/p/frank")
	mention("Frank Fox")
	process()
	if code := postJSONStatus(t, ts.URL+"/api/v1/notifications/seen", frankTok,
		map[string]any{"up_to": int64(1) << 62}); code != http.StatusOK {
		t.Fatalf("frank mark seen = %d", code)
	}
	if n, err := loopback.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("frank sweep = %d (%v), want 0 (seen)", n, err)
	}
	if caps := take(); find(caps, "/p/frank") != nil {
		t.Fatalf("seen notification was pushed: %+v", caps)
	}
	if subCount(frankID) != 1 {
		t.Fatal("seen must not delete the subscription")
	}

	// ---- Medium-3 pref off → nothing (also exercises the un-reserved PUT). ----
	daveID, daveTok := newUser("Dave Ng", "dave@psh.test", "davetok")
	seedSub(daveID, receiver.URL+"/p/dave")
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", daveTok,
		map[string]any{"kind": 2, "medium": 3, "enabled": false}); code != http.StatusOK {
		t.Fatalf("push pref off = %d, want 200 (medium 3 settable)", code)
	}
	mention("Dave Ng")
	process()
	if n, err := loopback.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("dave sweep = %d (%v), want 0 (push pref off)", n, err)
	}
	if caps := take(); find(caps, "/p/dave") != nil {
		t.Fatalf("push-disabled notification was pushed: %+v", caps)
	}

	// ---- DND snooze skips (unmarked), then delivers after the snooze lapses. ----
	carolID, carolTok := newUser("Carol Kim", "carol@psh.test", "caroltok")
	cPriv, cAuth := seedSub(carolID, receiver.URL+"/p/carol")
	if code := putJSON(t, ts.URL+"/api/v1/dnd", carolTok,
		map[string]any{"snoozed_until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}); code != http.StatusOK {
		t.Fatalf("carol snooze = %d", code)
	}
	mention("Carol Kim")
	process()
	if n, err := loopback.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("carol snoozed sweep = %d (%v), want 0", n, err)
	}
	if caps := take(); find(caps, "/p/carol") != nil {
		t.Fatalf("snoozed notification pushed: %+v", caps)
	}
	if code := putJSON(t, ts.URL+"/api/v1/dnd", carolTok, map[string]any{"snoozed_until": nil}); code != http.StatusOK {
		t.Fatalf("carol clear snooze = %d", code)
	}
	if n, err := loopback.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("carol post-lapse sweep = %d (%v), want 1 (delayed, not dropped)", n, err)
	}
	caps = take()
	if c := find(caps, "/p/carol"); c == nil {
		t.Fatalf("carol not delivered after snooze lapse: %+v", caps)
	} else {
		decryptPayload(cPriv, cAuth, c.body)
	}

	// ---- 410 Gone → the browser revoked it; the row is deleted. ----
	erinID, _ := newUser("Erin Vo", "erin@psh.test", "erintok")
	seedSub(erinID, receiver.URL+"/gone/erin")
	mention("Erin Vo")
	process()
	if n, err := loopback.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("erin sweep = %d (%v), want 0 (410)", n, err)
	}
	if caps := take(); find(caps, "/gone/erin") == nil {
		t.Fatal("erin endpoint should have been dialed (then 410)")
	}
	if subCount(erinID) != 0 {
		t.Fatal("410 must delete the subscription")
	}

	// ---- PIN 1: the SSRF guard refuses a loopback endpoint pre-dial. ----
	// The strict lane's egress has no test allowances, so the receiver's
	// loopback address is exactly the internal destination it rejects. The
	// endpoint is proven NEVER dialed and the row dies via the ErrDisallowed
	// path (a private/odd endpoint that slipped registration).
	heidiID, _ := newUser("Heidi Xu", "heidi@psh.test", "heiditok")
	seedSub(heidiID, receiver.URL+"/private")
	mention("Heidi Xu")
	process()
	sweepN, sweepErr := strict.RunOnce(ctx)
	if sweepErr != nil {
		t.Fatalf("heidi strict sweep: %v", sweepErr)
	}
	// The load-bearing pin: with the guard bypassed this capture appears.
	if caps := take(); find(caps, "/private") != nil {
		t.Fatal("private endpoint dialed — the guard must reject before any dial")
	}
	if subCount(heidiID) != 0 {
		t.Fatal("a disallowed endpoint's subscription must be deleted")
	}
	if sweepN != 0 {
		t.Fatalf("heidi strict sweep delivered %d, want 0", sweepN)
	}

	// Bad prefs still rejected: in-app (medium 1) is never settable.
	if code := putJSON(t, ts.URL+"/api/v1/notification-prefs", daveTok,
		map[string]any{"kind": 2, "medium": 1, "enabled": false}); code != http.StatusBadRequest {
		t.Fatalf("in-app pref = %d, want 400", code)
	}
}
