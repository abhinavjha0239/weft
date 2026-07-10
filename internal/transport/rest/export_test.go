package rest

import (
	"context"
	"encoding/json"
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
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// exportDoc mirrors the export JSON for assertions.
type exportDoc struct {
	ExportID  int64 `json:"export_id"`
	Truncated bool  `json:"truncated"`
	Messages  []struct {
		MessageID int64      `json:"message_id"`
		ChannelID *int64     `json:"channel_id"`
		DMSpaceID *int64     `json:"dm_space_id"`
		AuthorID  int64      `json:"author_id"`
		Source    string     `json:"source"`
		DeletedAt *time.Time `json:"deleted_at"`
		Revisions []struct {
			Kind       int16   `json:"kind"`
			PrevSource *string `json:"prev_source"`
		} `json:"revisions"`
		Attachments []struct {
			FileID int64  `json:"file_id"`
			SHA256 string `json:"sha256"`
		} `json:"attachments"`
	} `json:"messages"`
	Users []struct {
		ID int64 `json:"id"`
	} `json:"users"`
}

// TestComplianceExport: AD-5 end to end. A scoped export job runs through
// the worker into a downloadable JSON file that INCLUDES private channels,
// DMs, edit history, and deleted-message tombstones; already-purged
// revisions surface as null; attachment evidence is pinned so GC cannot
// reclaim it after the source messages die — the loop PR #42 left open.
func TestComplianceExport(t *testing.T) {
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
	complianceSvc := compliance.New(pool, permsSvc)
	complianceSvc.SetFiles(filesSvc)
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:   identity.New(pool, permsSvc),
		Messaging:  msgSvc,
		DM:         dm.New(pool),
		Files:      filesSvc,
		Compliance: complianceSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "exp", "email": "a@exp.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@exp.test", "Bob Ray", "bobexptok")
	var bobID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM user_account WHERE email = 'bob@exp.test'`).Scan(&bobID); err != nil {
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

	// Evidence corpus: a public message, a PRIVATE-channel message by bob
	// (alice is not a member), a DM, an edited message, a deleted message,
	// and an attachment-bearing message.
	pubMsg := send(boot.Token, boot.ChannelID, "public plans")
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", bobTok,
		map[string]any{"name": "warroom", "private": true}, &priv)
	privMsg := send(bobTok, priv.ChannelID, "the private truth")
	var conv struct {
		RootThreadID int64 `json:"root_thread_id"`
	}
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{"user_ids": []int64{bobID}}, &conv)
	var dmSent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, conv.RootThreadID),
		boot.Token, map[string]any{"content": "dm secret"}, &dmSent)
	edited := send(boot.Token, boot.ChannelID, "draft v1")
	if code := patchJSON(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, edited),
		boot.Token, map[string]any{"content": "draft v2"}); code != http.StatusOK {
		t.Fatal("edit")
	}
	deleted := send(boot.Token, boot.ChannelID, "regrettable take")
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, deleted), boot.Token); code != http.StatusOK {
		t.Fatal("delete")
	}
	_, att := uploadFile(t, ts.URL, boot.Token, "evidence.txt", "text/plain", "the receipts")
	withFile := send(boot.Token, boot.ChannelID,
		fmt.Sprintf("see [%s](%s)", att.Name, att.URL))

	// Gate: a member cannot export; a foreign scope id is a 404.
	if code := postJSONStatus(t, ts.URL+"/api/v1/admin/exports", bobTok, map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("member export = %d, want 403", code)
	}
	if code := postJSONStatus(t, ts.URL+"/api/v1/admin/exports", boot.Token,
		map[string]any{"channel_ids": []int64{999999}}); code != http.StatusNotFound {
		t.Fatalf("foreign scope = %d, want 404", code)
	}

	// Full-org export.
	var job compliance.ExportJob
	postJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token, map[string]any{}, &job)
	if job.ID == 0 || job.Status != 1 {
		t.Fatalf("job = %+v, want pending", job)
	}
	if n, err := complianceSvc.RunPendingExports(ctx); err != nil || n != 1 {
		t.Fatalf("run = %d (%v), want 1", n, err)
	}
	var list struct {
		Exports []compliance.ExportJob `json:"exports"`
	}
	if code := getJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token, &list); code != 200 ||
		len(list.Exports) != 1 || list.Exports[0].Status != 3 || list.Exports[0].ResultFileID == nil {
		t.Fatalf("list = %d %+v, want done with result", code, list.Exports)
	}
	resp, body := download(t, ts.URL, boot.Token, *list.Exports[0].ResultFileID)
	if resp.StatusCode != 200 {
		t.Fatalf("download = %d", resp.StatusCode)
	}
	var doc exportDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("parse export: %v", err)
	}
	byMsg := map[int64]int{}
	for i, m := range doc.Messages {
		byMsg[m.MessageID] = i
	}
	for _, want := range []int64{pubMsg, privMsg, dmSent.MessageID, edited, deleted, withFile} {
		if _, ok := byMsg[want]; !ok {
			t.Fatalf("export missing message %d", want)
		}
	}
	if m := doc.Messages[byMsg[privMsg]]; m.ChannelID == nil || *m.ChannelID != priv.ChannelID {
		t.Fatal("private-channel message must carry its channel")
	}
	if m := doc.Messages[byMsg[dmSent.MessageID]]; m.DMSpaceID == nil {
		t.Fatal("DM message must carry its dm_space")
	}
	if m := doc.Messages[byMsg[edited]]; len(m.Revisions) != 1 ||
		m.Revisions[0].PrevSource == nil || *m.Revisions[0].PrevSource != "draft v1" {
		t.Fatalf("edit history = %+v, want draft v1 revision", m.Revisions)
	}
	del := doc.Messages[byMsg[deleted]]
	if del.DeletedAt == nil || del.Source != "" || len(del.Revisions) != 1 ||
		del.Revisions[0].Kind != 4 || *del.Revisions[0].PrevSource != "regrettable take" {
		t.Fatalf("tombstone = %+v, want scrubbed row + kind-4 capture", del)
	}
	if m := doc.Messages[byMsg[withFile]]; len(m.Attachments) != 1 ||
		m.Attachments[0].FileID != att.ID || m.Attachments[0].SHA256 != att.SHA {
		t.Fatalf("attachments = %+v", m.Attachments)
	}
	if len(doc.Users) < 2 {
		t.Fatalf("users directory = %+v, want alice+bob", doc.Users)
	}

	// The pin: with the attachment's message deleted and the restore window
	// long past, GC must still keep the bytes — the export holds them.
	var pinned bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM file_reference
		 WHERE file_id = $1 AND entity_type = 14 AND entity_id = $2)`,
		att.ID, job.ID).Scan(&pinned); err != nil || !pinned {
		t.Fatalf("export pin missing (%v)", err)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, withFile), boot.Token); code != 200 {
		t.Fatal("delete withFile")
	}
	if _, err := pool.Exec(ctx,
		`UPDATE message SET deleted_at = now() - interval '40 days' WHERE id = $1`, withFile); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	janitor := compliance.NewJanitor(pool, store, slog.Default())
	if rep, err := janitor.SweepOnce(ctx, time.Now()); err != nil || rep.DeadRefPurged != 0 {
		t.Fatalf("sweep = %+v (%v), want the pinned file kept", rep, err)
	}
	if resp, gotBody := download(t, ts.URL, boot.Token, att.ID); resp.StatusCode != 200 || gotBody != "the receipts" {
		t.Fatalf("pinned bytes = %d %q, want intact", resp.StatusCode, gotBody)
	}

	// Scoped export: only the private channel's messages.
	var scoped compliance.ExportJob
	postJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token,
		map[string]any{"channel_ids": []int64{priv.ChannelID}}, &scoped)
	if n, err := complianceSvc.RunPendingExports(ctx); err != nil || n != 1 {
		t.Fatalf("scoped run = %d (%v)", n, err)
	}
	getJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token, &list)
	resp2, body2 := download(t, ts.URL, boot.Token, *list.Exports[0].ResultFileID)
	if resp2.StatusCode != 200 {
		t.Fatalf("scoped download = %d", resp2.StatusCode)
	}
	var scopedDoc exportDoc
	_ = json.Unmarshal([]byte(body2), &scopedDoc)
	if len(scopedDoc.Messages) != 1 || scopedDoc.Messages[0].MessageID != privMsg {
		t.Fatalf("scoped export = %+v, want only the warroom message", scopedDoc.Messages)
	}

	// Retention-purged history is gone from exports by design: scrub the
	// edit revision (keep_edits=false) and re-export — prev_source null.
	if code := putJSON(t, ts.URL+"/api/v1/admin/retention-policies", boot.Token,
		map[string]any{"scope_type": 3, "scope_id": boot.ChannelID, "duration_days": -1, "keep_edits": false}); code != 200 {
		t.Fatal("policy")
	}
	if _, err := janitor.SweepOnce(ctx, time.Now()); err != nil {
		t.Fatalf("scrub sweep: %v", err)
	}
	var rejob compliance.ExportJob
	postJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token,
		map[string]any{"user_ids": []int64{boot.UserID}}, &rejob)
	if n, err := complianceSvc.RunPendingExports(ctx); err != nil || n != 1 {
		t.Fatalf("re-run = %d (%v)", n, err)
	}
	getJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token, &list)
	_, body3 := download(t, ts.URL, boot.Token, *list.Exports[0].ResultFileID)
	var redoc exportDoc
	_ = json.Unmarshal([]byte(body3), &redoc)
	for _, m := range redoc.Messages {
		if m.MessageID == edited {
			if len(m.Revisions) != 1 || m.Revisions[0].PrevSource != nil {
				t.Fatalf("scrubbed revision = %+v, want null prev_source", m.Revisions)
			}
		}
	}

	// The audit trail: request + completion events per job.
	var completed int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'export.completed'`,
		boot.OrgID).Scan(&completed); err != nil || completed != 3 {
		t.Fatalf("export.completed events = %d (%v), want 3", completed, err)
	}
}
