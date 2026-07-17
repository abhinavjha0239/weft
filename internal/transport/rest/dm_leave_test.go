package rest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

// TestGroupDMLeave proves P-08 HARD-LEAVE: leaving a group DM hard-deletes the
// actor's own dm_participant row, so every read/fan-out ACL site (all of which
// gate on `EXISTS (dm_participant ...)`) excludes the leaver automatically with
// no predicate edits. It also proves rejoin via the create-or-get ensure-actor
// path (dm_key is preserved, so the leaver lands back on the full history).
//
// Note on status codes: reading/sending/marking-read a DM *thread* as a
// non-participant is an oracle-free 404 (messaging.requireParticipant →
// NotFound), matching the single-message Get — participation IS visibility, so
// a leaver cannot tell an absent conversation from a denied one (P-33
// normalized this; it was a 403 through P-08). dm.Leave itself is 404 too.
func TestGroupDMLeave(t *testing.T) {
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
	// The runner is driven synchronously via ProcessOrg (no background
	// goroutine) so fan-out is deterministic; the hub doubles as its Fanout.
	runner := notification.NewRunner(pool, hub, slog.Default())
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:      identity.New(pool, permsSvc),
		Messaging:     messaging.New(pool, permsSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "leave", "email": "alice@l.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@l.test", "Bob Ray", "bobleavetok")
	carolTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"carol@l.test", "Carol Kim", "carolleavetok")
	daveTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"dave@l.test", "Dave Poe", "daveleavetok")
	uid := func(email string) int64 {
		var id int64
		if err := pool.QueryRow(ctx,
			`SELECT id FROM user_account WHERE org_id=$1 AND email=$2`,
			boot.OrgID, email).Scan(&id); err != nil {
			t.Fatalf("uid %s: %v", email, err)
		}
		return id
	}
	aliceID, bobID, carolID := boot.UserID, uid("bob@l.test"), uid("carol@l.test")

	// Bob connects (last_id=0 replays); he must get a live refresh on leave.
	bobWS := dialClient(t, ctx, ts.URL, bobTok)
	bobWS.waitFor(t, "ready")

	// Alice+Bob+Carol group DM.
	var group dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID, carolID}}, &group)
	if group.Kind != 2 || len(group.ParticipantIDs) != 3 {
		t.Fatalf("group open wrong: %+v", group)
	}
	root := group.RootThreadID

	// The canonical dm_key is the sorted 3-id set; capture it to prove leave
	// never rewrites it (that is what makes rejoin resolve THIS conversation).
	wantKey := dmKey(aliceID, bobID, carolID)
	if got := storedKey(t, ctx, pool, group.ID); got != wantKey {
		t.Fatalf("stored dm_key = %q, want %q", got, wantKey)
	}

	// Alice's first message: Bob is still a participant, so he can read it,
	// gets fan-out for it, and it becomes history he keeps after leaving.
	msg1 := sendMsg(t, ts.URL, boot.Token, root, "the launch is friday")
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	if n := notifCount(t, ts.URL, bobTok); n != 1 {
		t.Fatalf("bob notifications after msg1 = %d, want 1", n)
	}
	if n := notifCount(t, ts.URL, carolTok); n != 1 {
		t.Fatalf("carol notifications after msg1 = %d, want 1", n)
	}
	// Sanity: while a participant, Bob can fetch the single message.
	if code := getJSON(t, msgURL(ts.URL, msg1), bobTok, nil); code != http.StatusOK {
		t.Fatalf("bob get msg1 while participant = %d, want 200", code)
	}

	// --- Bob leaves. ---
	if code := deleteReq(t, leaveURL(ts.URL, group.ID), bobTok); code != http.StatusOK {
		t.Fatalf("bob leave = %d, want 200", code)
	}
	// The leaver's DM view refreshes live (gateway folds the new verb into the
	// dm.opened user_ids-scan path).
	bobWS.waitFor(t, "dm.participants_changed")

	// Bob is fully cut off (the load-bearing security assertions):
	// 1. His DM list no longer shows the conversation.
	if listsDM(t, ts.URL, bobTok, group.ID) {
		t.Fatalf("bob's /dms still lists the group after leaving")
	}
	// 2. Reading, sending, AND marking-read the thread are all oracle-free
	//    404s now (requireParticipant → NotFound): the leaver cannot tell the
	//    conversation apart from one that never existed, and the body never
	//    echoes the dm_space_id.
	for _, tc := range []struct{ name, method, url, body string }{
		{"read", "GET", threadURL(ts.URL, root), ""},
		{"send", "POST", threadURL(ts.URL, root), `{"content":"let me back in"}`},
		{"mark-read", "POST", fmt.Sprintf("%s/api/v1/threads/%d/read", ts.URL, root), `{"up_to":1}`},
	} {
		code, body := dmReq(t, tc.method, tc.url, bobTok, tc.body)
		if code != http.StatusNotFound {
			t.Fatalf("bob %s after leaving = %d, want 404", tc.name, code)
		}
		if !strings.Contains(body, "conversation not found") || strings.Contains(body, fmt.Sprint(group.ID)) {
			t.Fatalf("bob %s body = %q, want oracle-free 'conversation not found' (no dm id)", tc.name, body)
		}
	}
	// 3. The single message he could read a moment ago → oracle-free 404.
	if code := getJSON(t, msgURL(ts.URL, msg1), bobTok, nil); code != http.StatusNotFound {
		t.Fatalf("bob get msg1 after leaving = %d, want 404", code)
	}
	// Alice and Carol are unaffected: both read and send fine.
	if code := getJSON(t, threadURL(ts.URL, root), carolTok, nil); code != http.StatusOK {
		t.Fatalf("carol read thread = %d, want 200", code)
	}
	if code := postJSONStatus(t, threadURL(ts.URL, root), carolTok,
		map[string]any{"content": "bye bob"}); code != http.StatusCreated {
		t.Fatalf("carol send = %d, want 201", code)
	}

	// dm_key is unchanged — still the original 3-id key.
	if got := storedKey(t, ctx, pool, group.ID); got != wantKey {
		t.Fatalf("dm_key changed on leave: %q, want %q", got, wantKey)
	}

	// --- Alice posts while Bob is gone: fan-out must exclude the deleted
	// participant. Bob gets NO new notification; Carol does. ---
	msg2 := sendMsg(t, ts.URL, boot.Token, root, "secret while bob is out")
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	if n := notifCount(t, ts.URL, bobTok); n != 1 {
		t.Fatalf("bob notifications after msg2 = %d, want 1 (no fan-out to leaver)", n)
	}
	if n := notifCount(t, ts.URL, carolTok); n != 2 {
		t.Fatalf("carol notifications after msg2 = %d, want 2", n)
	}

	// --- Rejoin via create-or-get: Bob re-opens the same 3-id set. ---
	var rejoined dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", bobTok,
		map[string]any{"user_ids": []int64{aliceID, carolID}}, &rejoined)
	if rejoined.ID != group.ID || rejoined.RootThreadID != root {
		t.Fatalf("rejoin resolved a different conversation: %+v (want id %d)", rejoined, group.ID)
	}
	if !listsDM(t, ts.URL, bobTok, group.ID) {
		t.Fatalf("bob's /dms does not list the group after rejoin")
	}
	// Bob reads the FULL history, including msg2 sent while he was gone.
	code, ids := threadMsgIDs(t, ts.URL, bobTok, root)
	if code != http.StatusOK {
		t.Fatalf("bob read thread after rejoin = %d, want 200", code)
	}
	if !containsID(ids, msg1) || !containsID(ids, msg2) {
		t.Fatalf("bob history after rejoin = %v, want both msg1=%d and msg2=%d", ids, msg1, msg2)
	}

	// Fan-out is restored: a subsequent send notifies Bob again.
	sendMsg(t, ts.URL, boot.Token, root, "welcome back")
	drainConsumer(t, ctx, pool, "notifications", boot.OrgID, runner.ProcessOrg)
	if n := notifCount(t, ts.URL, bobTok); n != 2 {
		t.Fatalf("bob notifications after rejoin+send = %d, want 2 (fan-out restored)", n)
	}
	if n := notifCount(t, ts.URL, carolTok); n != 3 {
		t.Fatalf("carol notifications after msg3 = %d, want 3", n)
	}

	// --- Rejections: 1:1 and self conversations cannot be left. ---
	var oneToOne dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token,
		map[string]any{"user_ids": []int64{bobID}}, &oneToOne)
	if oneToOne.Kind != 1 {
		t.Fatalf("expected 1:1, got kind %d", oneToOne.Kind)
	}
	if code := deleteReq(t, leaveURL(ts.URL, oneToOne.ID), boot.Token); code != http.StatusBadRequest {
		t.Fatalf("leave 1:1 = %d, want 400", code)
	}
	var selfDM dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{}}, &selfDM)
	if selfDM.Kind != 3 {
		t.Fatalf("expected self DM, got kind %d", selfDM.Kind)
	}
	if code := deleteReq(t, leaveURL(ts.URL, selfDM.ID), boot.Token); code != http.StatusBadRequest {
		t.Fatalf("leave self DM = %d, want 400", code)
	}

	// --- Oracle-free 404s: nonexistent, and a group you're not in. Dave is an
	// org member but not a participant, so he must NOT be able to tell the
	// group exists (404, never a 403). ---
	if code := deleteReq(t, leaveURL(ts.URL, 999999), boot.Token); code != http.StatusNotFound {
		t.Fatalf("leave nonexistent = %d, want 404", code)
	}
	if code := deleteReq(t, leaveURL(ts.URL, group.ID), daveTok); code != http.StatusNotFound {
		t.Fatalf("dave (non-participant) leave = %d, want 404", code)
	}

	// A foreign-org dm_space is invisible: the org pin in dm.Leave makes it a
	// 404, never leaking that another org has that conversation id.
	var boot2 struct {
		OrgID  int64  `json:"org_id"`
		Token  string `json:"token"`
		UserID int64  `json:"user_id"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "leave2", "email": "eve@l2.test", "password": "password123",
		"full_name": "Eve Vale",
	}, &boot2)
	var foreign dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot2.Token, map[string]any{"user_ids": []int64{}}, &foreign)
	if code := deleteReq(t, leaveURL(ts.URL, foreign.ID), boot.Token); code != http.StatusNotFound {
		t.Fatalf("alice leave org2's dm = %d, want 404", code)
	}
}

// --- small local helpers (URLs + typed reads) ---

func leaveURL(base string, dmSpaceID int64) string {
	return fmt.Sprintf("%s/api/v1/dms/%d/participants/me", base, dmSpaceID)
}

func threadURL(base string, threadID int64) string {
	return fmt.Sprintf("%s/api/v1/threads/%d/messages", base, threadID)
}

// dmReq issues an authed request WITHOUT decoding and returns the status plus
// the raw body — used to assert an oracle-free 404 whose body must carry the
// generic "conversation not found" and never echo the dm_space_id.
func dmReq(t *testing.T, method, url, token, jsonBody string) (int, string) {
	t.Helper()
	var body io.Reader
	if jsonBody != "" {
		body = strings.NewReader(jsonBody)
	}
	req, _ := http.NewRequest(method, url, body)
	req.Header.Set("Authorization", "Bearer "+token)
	if jsonBody != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func msgURL(base string, msgID int64) string {
	return fmt.Sprintf("%s/api/v1/messages/%d", base, msgID)
}

func sendMsg(t *testing.T, base, token string, threadID int64, content string) int64 {
	t.Helper()
	var out struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, threadURL(base, threadID), token, map[string]any{"content": content}, &out)
	return out.MessageID
}

func notifCount(t *testing.T, base, token string) int {
	t.Helper()
	var in inboxResp
	getJSON(t, base+"/api/v1/notifications", token, &in)
	// unseen and the page ship from one snapshot; no test marks anything seen,
	// so they agree and either is the count.
	if in.Unseen != len(in.Notifications) {
		t.Fatalf("inbox unseen %d disagrees with page length %d", in.Unseen, len(in.Notifications))
	}
	return len(in.Notifications)
}

func listsDM(t *testing.T, base, token string, dmSpaceID int64) bool {
	t.Helper()
	var list struct {
		DMs []dm.Summary `json:"dms"`
	}
	getJSON(t, base+"/api/v1/dms", token, &list)
	for _, d := range list.DMs {
		if d.ID == dmSpaceID {
			return true
		}
	}
	return false
}

func threadMsgIDs(t *testing.T, base, token string, threadID int64) (int, []int64) {
	t.Helper()
	var page struct {
		Messages []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	code := getJSON(t, threadURL(base, threadID), token, &page)
	ids := make([]int64, len(page.Messages))
	for i, m := range page.Messages {
		ids[i] = m.ID
	}
	return code, ids
}

func storedKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dmSpaceID int64) string {
	t.Helper()
	var key string
	if err := pool.QueryRow(ctx, `SELECT dm_key FROM dm_space WHERE id=$1`, dmSpaceID).Scan(&key); err != nil {
		t.Fatalf("read dm_key: %v", err)
	}
	return key
}

func dmKey(ids ...int64) string {
	sorted := append([]int64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	parts := make([]string, len(sorted))
	for i, id := range sorted {
		parts[i] = fmt.Sprint(id)
	}
	return strings.Join(parts, ":")
}

func containsID(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
