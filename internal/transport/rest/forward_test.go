package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestForwarding: P-03 — forwarding copies one message into another thread
// with a quoted block + attribution. Both gates run (read the source, send to
// the target). The critical invariant: mentions in the QUOTE (and the
// attribution) never notify, while mentions in the forwarder's COMMENT do.
// Attachments are not re-referenced, and forwarded_from is surfaced.
func TestForwarding(t *testing.T) {
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

	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	msgSvc.SetFiles(filesSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		DM:        dm.New(pool),
		Files:     filesSvc,
	}))
	defer ts.Close()
	runner := notification.NewRunner(pool, hub, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "fwd", "email": "alice@fwd.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	chanA := boot.ChannelID
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, chanA, "bob@fwd.test", "Bob Ray", "bobfwdtok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, chanA, "charlie@fwd.test", "Charlie Kim", "charliefwdtok")
	var bobID, charlieID int64
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@fwd.test'`).Scan(&bobID)
	_ = pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'charlie@fwd.test'`).Scan(&charlieID)

	send := func(tok string, channelID int64, content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			tok, map[string]any{"content": content}, &sent)
		return sent.MessageID
	}
	createChannel := func(tok, name string, private bool) (int64, int64) {
		t.Helper()
		var out struct {
			ChannelID    int64 `json:"channel_id"`
			RootThreadID int64 `json:"root_thread_id"`
		}
		postJSON(t, ts.URL+"/api/v1/channels", tok,
			map[string]any{"name": name, "private": private}, &out)
		return out.ChannelID, out.RootThreadID
	}
	join := func(tok string, channelID int64) {
		t.Helper()
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/join", ts.URL, channelID), tok, nil, nil)
	}
	forwardOK := func(tok string, srcID, threadID int64, comment string) int64 {
		t.Helper()
		var out struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/messages/%d/forward", ts.URL, srcID), tok,
			map[string]any{"thread_id": threadID, "comment": comment}, &out)
		return out.MessageID
	}
	forwardStatus := func(tok string, srcID, threadID int64, comment string) int {
		t.Helper()
		return postJSONStatus(t, fmt.Sprintf("%s/api/v1/messages/%d/forward", ts.URL, srcID), tok,
			map[string]any{"thread_id": threadID, "comment": comment})
	}
	type msgResp struct {
		Source        string `json:"source"`
		ForwardedFrom *int64 `json:"forwarded_from"`
	}
	getMsg := func(tok string, msgID int64) msgResp {
		t.Helper()
		var m msgResp
		if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msgID), tok, &m); code != http.StatusOK {
			t.Fatalf("get msg %d = %d", msgID, code)
		}
		return m
	}
	process := func() {
		t.Helper()
		if err := runner.ProcessOrg(ctx, boot.OrgID); err != nil {
			t.Fatalf("materialize: %v", err)
		}
	}
	kindsFor := func(uid, msgID int64) []int16 {
		t.Helper()
		rows, err := pool.Query(ctx, `
			SELECT kind FROM notification
			WHERE user_id = $1 AND entity_type = 1 AND entity_id = $2 ORDER BY kind`, uid, msgID)
		if err != nil {
			t.Fatalf("kinds: %v", err)
		}
		defer rows.Close()
		var out []int16
		for rows.Next() {
			var k int16
			_ = rows.Scan(&k)
			out = append(out, k)
		}
		return out
	}

	// Channel B: all three members (cross-channel forward target).
	chanB, rootB := createChannel(boot.Token, "beta", false)
	join(bobTok, chanB)
	join(charlieTok, chanB)

	// Source: bob in A mentions charlie. The SOURCE notifies charlie (sanity
	// that the pipeline works — the forward must then NOT re-notify).
	src := send(bobTok, chanA, "secret @**Charlie Kim** look here")
	process()
	if got := kindsFor(charlieID, src); len(got) != 1 || got[0] != notification.KindMention {
		t.Fatalf("source mention = %v, want [2] (pipeline works)", got)
	}

	// 1. Happy path (no comment): quoted mention does NOT notify, and the
	//    attribution does NOT ping the source author. forwarded_from surfaced.
	fwd1 := forwardOK(boot.Token, src, rootB, "")
	process()
	if got := kindsFor(charlieID, fwd1); len(got) != 0 {
		t.Fatalf("quoted mention notified = %v, want none (neutralized)", got)
	}
	if got := kindsFor(bobID, fwd1); len(got) != 0 {
		t.Fatalf("attribution notified the author = %v, want none (neutralized)", got)
	}
	m1 := getMsg(boot.Token, fwd1)
	if m1.ForwardedFrom == nil || *m1.ForwardedFrom != src {
		t.Fatalf("forwarded_from = %v, want %d", m1.ForwardedFrom, src)
	}
	if !strings.Contains(m1.Source, "> secret") || !strings.Contains(m1.Source, "forwarded from") {
		t.Fatalf("forwarded source missing quote/attribution:\n%s", m1.Source)
	}
	if strings.Contains(m1.Source, "@**") {
		t.Fatalf("un-neutralized mention token survived in the quote/attribution:\n%s", m1.Source)
	}

	// 2. Comment mentions DO notify (same user, same target — only the quote
	//    vs comment placement differs).
	fwd2 := forwardOK(boot.Token, src, rootB, "@**Charlie Kim** please read")
	process()
	if got := kindsFor(charlieID, fwd2); len(got) != 1 || got[0] != notification.KindMention {
		t.Fatalf("comment mention = %v, want [2]", got)
	}
	if got := kindsFor(bobID, fwd2); len(got) != 0 {
		t.Fatalf("fwd2 notified author = %v, want none", got)
	}

	// 3. Quote truncation at 1000 runes with a trailing ellipsis. 'z' is absent
	//    from the attribution line, so its count isolates the quoted original.
	long := send(boot.Token, chanA, strings.Repeat("z", 1500))
	fwdLong := forwardOK(boot.Token, long, rootB, "")
	src3 := getMsg(boot.Token, fwdLong).Source
	if !strings.Contains(src3, "…") {
		t.Fatalf("truncated quote missing ellipsis:\n%.80s", src3)
	}
	if n := strings.Count(src3, "z"); n != 1000 {
		t.Fatalf("quote kept %d 'z's, want exactly 1000 (capped)", n)
	}

	// 4. Attachments do NOT re-reference: the source's file link stays inert
	//    text in the quote, and no new file_reference row is created.
	code, f := uploadFile(t, ts.URL, boot.Token, "plan.txt", "text/plain", "the roadmap")
	if code != http.StatusCreated {
		t.Fatalf("upload = %d", code)
	}
	fileMsg := send(boot.Token, chanA, fmt.Sprintf("plan: [plan](/api/v1/files/%d)", f.ID))
	fileRefs := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM file_reference WHERE file_id = $1`, f.ID).Scan(&n); err != nil {
			t.Fatalf("file refs: %v", err)
		}
		return n
	}
	if fileRefs() != 1 {
		t.Fatalf("source file refs = %d, want 1", fileRefs())
	}
	fwdFile := forwardOK(boot.Token, fileMsg, rootB, "")
	if fileRefs() != 1 {
		t.Fatalf("file refs after forward = %d, want 1 (no re-reference)", fileRefs())
	}
	var fwdHasAttach bool
	if err := pool.QueryRow(ctx,
		`SELECT has_attachment FROM message WHERE id = $1`, fwdFile).Scan(&fwdHasAttach); err != nil {
		t.Fatalf("has_attachment: %v", err)
	}
	if fwdHasAttach {
		t.Fatalf("forwarded message flagged has_attachment, want false")
	}
	if !strings.Contains(getMsg(boot.Token, fwdFile).Source, fmt.Sprintf("/api/v1/files/%d", f.ID)) {
		t.Fatalf("forwarded quote dropped the inert file link")
	}

	// 5. Private-source 404: bob is not in the private channel, so he cannot
	//    forward its content (oracle-free — indistinguishable from nonexistent).
	privChan, privRoot := createChannel(boot.Token, "vault", true)
	secret := send(boot.Token, privChan, "eyes only")
	if code := forwardStatus(bobTok, secret, rootB, ""); code != http.StatusNotFound {
		t.Fatalf("private-source forward = %d, want 404", code)
	}

	// 6. Target send-gate 403: bob may read src (channel A) but cannot post
	//    into the private channel he is not a member of.
	if code := forwardStatus(bobTok, src, privRoot, ""); code != http.StatusForbidden {
		t.Fatalf("target send-gate forward = %d, want 403", code)
	}
}
