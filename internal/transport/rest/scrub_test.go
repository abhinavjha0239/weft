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
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestRevisionScrub: the AD-3 keep_edits=false toggle. The effective policy
// resolves nearest-scope-first (channel override beats the org default; DM
// messages fall to the org default), prior versions are scrubbed while the
// structural record survives, kind-4 deletion capture is never touched, and
// an active custodian hold keeps the originals until release.
func TestRevisionScrub(t *testing.T) {
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
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:   identity.New(pool, permsSvc),
		Messaging:  msgSvc,
		DM:         dm.New(pool),
		Compliance: compliance.New(pool, permsSvc),
	}))
	defer ts.Close()
	janitor := compliance.NewJanitor(pool, store, slog.Default())

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "scr", "email": "a@scr.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@scr.test", "Bob Ray", "bobscrtok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@scr.test'`).Scan(&bobID); err != nil {
		t.Fatalf("bob id: %v", err)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant = %d", code)
	}

	send := func(tok string, channelID int64, content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, channelID),
			tok, map[string]any{"content": content}, &sent)
		if sent.MessageID == 0 {
			t.Fatal("send failed")
		}
		return sent.MessageID
	}
	edit := func(tok string, msgID int64, content string) {
		t.Helper()
		if code := patchJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msgID),
			tok, map[string]any{"content": content}); code != http.StatusOK {
			t.Fatalf("edit %d = %d", msgID, code)
		}
	}
	// prevState reports (rows with prior content, total kind-1/2 rows).
	prevState := func(msgID int64) (withPrev, total int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE prev_source IS NOT NULL OR prev_ast IS NOT NULL),
			       count(*)
			FROM message_revision WHERE message_id = $1 AND kind IN (1, 2)`,
			msgID).Scan(&withPrev, &total); err != nil {
			t.Fatalf("prev state %d: %v", msgID, err)
		}
		return withPrev, total
	}

	// Alice and bob edit in #general; alice also edits in a DM with bob.
	mA := send(boot.Token, boot.ChannelID, "alice v1")
	edit(boot.Token, mA, "alice v2")
	edit(boot.Token, mA, "alice v3") // two kind-1 revisions
	mB := send(bobTok, boot.ChannelID, "bob v1")
	edit(bobTok, mB, "bob v2")
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm v1"}, &dmSent)
	edit(boot.Token, dmSent.MessageID, "dm v2")

	// No policy anywhere → nothing scrubs (keep is the default reading).
	rep, err := janitor.SweepOnce(ctx, time.Now())
	if err != nil || rep.RevisionsScrubbed != 0 {
		t.Fatalf("no-policy sweep = %+v (%v), want zero scrubbed", rep, err)
	}

	// Channel keep_edits=false: #general scrubs, the DM (org default) keeps.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 3, "scope_id": boot.ChannelID, "duration_days": -1, "keep_edits": false}); code != http.StatusOK {
		t.Fatalf("channel policy = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.RevisionsScrubbed != 3 {
		t.Fatalf("channel sweep = %+v (%v), want 3 scrubbed (mA×2 + mB×1)", rep, err)
	}
	if withPrev, total := prevState(mA); withPrev != 0 || total != 2 {
		t.Fatalf("mA = %d/%d, want prior content gone, structural rows kept", withPrev, total)
	}
	if withPrev, _ := prevState(dmSent.MessageID); withPrev != 1 {
		t.Fatal("DM revision must survive: no org policy yet")
	}

	// Org keep_edits=false now reaches the DM; a channel keep_edits=true
	// override outranks it (nearest scope wins).
	var keepCh struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "keeper"}, &keepCh)
	mK := send(boot.Token, keepCh.ChannelID, "keeper v1")
	edit(boot.Token, mK, "keeper v2")
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 3, "scope_id": keepCh.ChannelID, "duration_days": -1, "keep_edits": true}); code != http.StatusOK {
		t.Fatalf("keeper policy = %d", code)
	}
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 1, "scope_id": boot.OrgID, "duration_days": -1, "keep_edits": false}); code != http.StatusOK {
		t.Fatalf("org policy = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.RevisionsScrubbed != 1 {
		t.Fatalf("ladder sweep = %+v (%v), want only the DM revision", rep, err)
	}
	if withPrev, _ := prevState(dmSent.MessageID); withPrev != 0 {
		t.Fatal("DM revision must scrub under the org default")
	}
	if withPrev, _ := prevState(mK); withPrev != 1 {
		t.Fatal("keeper revision must survive: channel override beats org")
	}

	// A custodian hold freezes bob's new prior version until release.
	edit(bobTok, mB, "bob v3")
	var hold compliance.LegalHold
	postJSON(t, ts.URL+"/api/v1/admin/legal-holds", boot.Token,
		map[string]any{"name": "Bob custodian", "custodian_user_id": bobID}, &hold)
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.RevisionsScrubbed != 0 {
		t.Fatalf("held sweep = %+v (%v), want zero", rep, err)
	}
	if withPrev, _ := prevState(mB); withPrev != 1 {
		t.Fatal("held revision must keep its original")
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/admin/legal-holds/%d/release", ts.URL, hold.ID),
		boot.Token, map[string]any{}); code != http.StatusOK {
		t.Fatalf("release = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.RevisionsScrubbed != 1 {
		t.Fatalf("post-release sweep = %+v (%v), want bob's revision", rep, err)
	}

	// Deletion capture (kind 4) is never scrubbed, even in a false scope.
	mDel := send(boot.Token, boot.ChannelID, "delete me")
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, mDel), boot.Token); code != http.StatusOK {
		t.Fatalf("delete = %d", code)
	}
	if rep, err = janitor.SweepOnce(ctx, time.Now()); err != nil || rep.RevisionsScrubbed != 0 {
		t.Fatalf("kind-4 sweep = %+v (%v), want zero", rep, err)
	}
	var delPrev string
	if err := pool.QueryRow(ctx, `
		SELECT prev_source FROM message_revision WHERE message_id = $1 AND kind = 4`,
		mDel).Scan(&delPrev); err != nil {
		t.Fatalf("kind-4 row: %v", err)
	}
	if delPrev != "delete me" {
		t.Fatalf("deletion capture = %q, must keep the final content", delPrev)
	}

	// The audit trail counts every scrub batch per org.
	var scrubEvents int
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum((payload->>'count')::int), 0) FROM event_log
		WHERE org_id = $1 AND verb = 'retention.revisions_scrubbed'`,
		boot.OrgID).Scan(&scrubEvents); err != nil {
		t.Fatalf("scrub events: %v", err)
	}
	if scrubEvents != 5 {
		t.Fatalf("scrubbed-by-event total = %d, want 5", scrubEvents)
	}
}
