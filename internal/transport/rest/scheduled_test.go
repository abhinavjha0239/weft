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

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestScheduledSends: the queue-then-deliver arc with the gate run TWICE —
// at schedule time and again at fire time (revoked access fails the
// delivery visibly, never silently) — plus the ADR-012 not-yet-sent rule:
// a pending send's attachment survives the unclaimed-GC lane via its
// EntityScheduled pin, released once the live message claims its own.
func TestScheduledSends(t *testing.T) {
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
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	msgSvc.SetFiles(filesSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: msgSvc,
		Files:     filesSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sch", "email": "a@sch.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@sch.test", "Bob Ray", "bobschtok")
	var rootThread int64
	if err := pool.QueryRow(ctx, `SELECT root_thread_id FROM channel WHERE id = $1`,
		boot.ChannelID).Scan(&rootThread); err != nil {
		t.Fatalf("root thread: %v", err)
	}

	// Validation: past fire time, empty content, unreadable thread.
	if code := postJSONStatus(t, ts.URL+"/api/v1/scheduled-messages", bobTok,
		map[string]any{"thread_id": rootThread, "content": "x",
			"scheduled_for": time.Now().Add(-time.Minute)}); code != http.StatusBadRequest {
		t.Fatalf("past schedule = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/scheduled-messages", bobTok,
		map[string]any{"thread_id": rootThread, "content": "",
			"scheduled_for": time.Now().Add(time.Hour)}); code != http.StatusBadRequest {
		t.Fatalf("empty schedule = %d, want 400", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/scheduled-messages", bobTok,
		map[string]any{"thread_id": int64(999999), "content": "x",
			"scheduled_for": time.Now().Add(time.Hour)}); code != http.StatusNotFound {
		t.Fatalf("bad thread = %d, want 404", code)
	}

	// Bob schedules a send with an attachment; the pending pin must exist.
	_, att := uploadFile(t, ts.URL, bobTok, "later.txt", "text/plain", "for the future")
	due := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	var sched messaging.ScheduledMessage
	postJSON(t, ts.URL+"/api/v1/scheduled-messages", bobTok, map[string]any{
		"thread_id": rootThread, "scheduled_for": due,
		"content": fmt.Sprintf("as promised: [%s](%s)", att.Name, att.URL)}, &sched)
	if sched.ID == 0 || sched.ChannelID == nil || *sched.ChannelID != boot.ChannelID {
		t.Fatalf("schedule = %+v", sched)
	}
	var pinned bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM file_reference
		 WHERE file_id = $1 AND entity_type = 15 AND entity_id = $2)`,
		att.ID, sched.ID).Scan(&pinned); err != nil || !pinned {
		t.Fatalf("scheduled pin missing (%v)", err)
	}

	// The ADR-012 edge: the upload is past the unclaimed grace, but the
	// pending send's pin keeps it out of the GC's hands.
	if _, err := pool.Exec(ctx,
		`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, att.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	janitor := compliance.NewJanitor(pool, store, slog.Default())
	if rep, err := janitor.SweepOnce(ctx, time.Now()); err != nil || rep.UnclaimedPurged != 0 {
		t.Fatalf("sweep = %+v (%v), want the pinned upload spared", rep, err)
	}

	// Not due yet: the runner leaves it; own listing shows it pending.
	if sent, failed, err := msgSvc.RunDueScheduled(ctx, time.Now()); err != nil || sent+failed != 0 {
		t.Fatalf("early run = %d/%d (%v), want nothing", sent, failed, err)
	}
	var mine struct {
		Scheduled []messaging.ScheduledMessage `json:"scheduled"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/scheduled-messages", bobTok, &mine); code != 200 ||
		len(mine.Scheduled) != 1 || mine.Scheduled[0].FailedReason != nil {
		t.Fatalf("pending list = %d %+v", code, mine.Scheduled)
	}
	if code := getJSON(t, ts.URL+"/api/v1/scheduled-messages", boot.Token, &mine); code != 200 || len(mine.Scheduled) != 0 {
		t.Fatalf("alice sees bob's queue = %+v, want empty", mine.Scheduled)
	}

	// Due: delivered as bob through the full send path — message exists,
	// scheduled pin released, live message reference in its place.
	if sent, failed, err := msgSvc.RunDueScheduled(ctx, due.Add(time.Second)); err != nil || sent != 1 || failed != 0 {
		t.Fatalf("due run = %d/%d (%v), want 1 sent", sent, failed, err)
	}
	var msgID, authorID int64
	if err := pool.QueryRow(ctx, `
		SELECT sent_message_id, author_id FROM scheduled_message WHERE id = $1`,
		sched.ID).Scan(&msgID, &authorID); err != nil || msgID == 0 {
		t.Fatalf("sent pointer missing (%v)", err)
	}
	var delivered struct {
		AuthorID int64  `json:"author_id"`
		Source   string `json:"source"`
	}
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msgID),
		bobTok, &delivered); code != 200 || delivered.AuthorID != authorID {
		t.Fatalf("delivered = %d %+v", code, delivered)
	}
	var schedRefs, msgRefs int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM file_reference WHERE entity_type = 15 AND entity_id = $1`, sched.ID).Scan(&schedRefs)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM file_reference WHERE entity_type = 1 AND entity_id = $1 AND file_id = $2`, msgID, att.ID).Scan(&msgRefs)
	if schedRefs != 0 || msgRefs != 1 {
		t.Fatalf("refs after delivery = sched %d / msg %d, want 0 / 1", schedRefs, msgRefs)
	}
	if code := getJSON(t, ts.URL+"/api/v1/scheduled-messages", bobTok, &mine); code != 200 || len(mine.Scheduled) != 0 {
		t.Fatalf("post-delivery list = %+v, want empty", mine.Scheduled)
	}

	// Fire-time gate: bob schedules, then LEAVES the channel — the delivery
	// must fail with the reason recorded, and no message must exist.
	var sched2 messaging.ScheduledMessage
	postJSON(t, ts.URL+"/api/v1/scheduled-messages", bobTok, map[string]any{
		"thread_id": rootThread, "content": "posthumous post",
		"scheduled_for": time.Now().Add(time.Hour)}, &sched2)
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/channels/%d/leave", ts.URL, boot.ChannelID),
		bobTok, map[string]any{}); code != http.StatusOK {
		t.Fatalf("leave = %d", code)
	}
	if sent, failed, err := msgSvc.RunDueScheduled(ctx, time.Now().Add(2*time.Hour)); err != nil || sent != 0 || failed != 1 {
		t.Fatalf("revoked run = %d/%d (%v), want 1 failed", sent, failed, err)
	}
	getJSON(t, ts.URL+"/api/v1/scheduled-messages", bobTok, &mine)
	if len(mine.Scheduled) != 1 || mine.Scheduled[0].FailedReason == nil {
		t.Fatalf("failed listing = %+v, want visible failure", mine.Scheduled)
	}
	var posthumous int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM message WHERE source = 'posthumous post'`).Scan(&posthumous)
	if posthumous != 0 {
		t.Fatal("revoked delivery must not post")
	}

	// Update and cancel: only the author, only pending rows.
	var sched3 messaging.ScheduledMessage
	postJSON(t, ts.URL+"/api/v1/scheduled-messages", boot.Token, map[string]any{
		"thread_id": rootThread, "content": "v1 text",
		"scheduled_for": time.Now().Add(time.Hour)}, &sched3)
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/scheduled-messages/%d", ts.URL, sched3.ID),
		bobTok, map[string]any{"content": "hijack"}); code != http.StatusNotFound {
		t.Fatalf("foreign update = %d, want 404", code)
	}
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/scheduled-messages/%d", ts.URL, sched3.ID),
		boot.Token, map[string]any{"content": "v2 text"}); code != http.StatusOK {
		t.Fatalf("update = %d", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/scheduled-messages/%d", ts.URL, sched3.ID), boot.Token); code != http.StatusOK {
		t.Fatalf("cancel = %d", code)
	}
	if sent, _, err := msgSvc.RunDueScheduled(ctx, time.Now().Add(2*time.Hour)); err != nil || sent != 0 {
		t.Fatalf("cancelled run = %d (%v), want nothing", sent, err)
	}
}
