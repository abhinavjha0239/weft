package rest

import (
	"archive/zip"
	"context"
	"encoding/json"
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

	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

type zipExportDoc struct {
	ExportID int64 `json:"export_id"`
	Messages []struct {
		MessageID int64  `json:"message_id"`
		Source    string `json:"source"`
	} `json:"messages"`
}

type zipManifest struct {
	JobID      int64   `json:"job_id"`
	FileCount  int     `json:"file_count"`
	TotalBytes int64   `json:"total_bytes"`
	Missing    []int64 `json:"missing"`
}

// TestExportBundle: P-32 include_files zip bundle. A bundle export produces a
// zip carrying export.json (byte-content matching the plain export of the same
// scope), a manifest, and every pinned attachment's exact bytes; a file whose
// blob has been purged out from under the pins lands in manifest.missing while
// the rest still bundle (the pins keep their bytes even after the source
// messages die); the officer downloads it and a non-officer cannot.
func TestExportBundle(t *testing.T) {
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

	store, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	filesSvc.SetPerms(permsSvc)
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
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "bundle", "email": "a@bundle.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@bundle.test", "Bob Ray", "bobbundletok")
	if code := putJSON(t, ts.URL+"/api/v1/admin/verbs", boot.Token,
		map[string]any{"verb": "compliance_officer", "group": "role:admins"}); code != http.StatusOK {
		t.Fatalf("grant officer = %d", code)
	}

	send := func(content string) int64 {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": content}, &sent)
		return sent.MessageID
	}
	runExport := func(scope map[string]any) int64 {
		t.Helper()
		var job compliance.ExportJob
		postJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token, scope, &job)
		if n, err := complianceSvc.RunPendingExports(ctx); err != nil || n < 1 {
			t.Fatalf("run export = %d (%v), want >=1 done", n, err)
		}
		var rf *int64
		if err := pool.QueryRow(ctx, `SELECT result_file_id FROM export_job WHERE id = $1`, job.ID).Scan(&rf); err != nil {
			t.Fatalf("result file: %v", err)
		}
		if rf == nil {
			t.Fatalf("export job %d finished with no result (failed?)", job.ID)
		}
		return *rf
	}
	readZip := func(body string) map[string][]byte {
		t.Helper()
		zr, err := zip.NewReader(strings.NewReader(body), int64(len(body)))
		if err != nil {
			t.Fatalf("open zip (%d bytes): %v", len(body), err)
		}
		out := map[string][]byte{}
		for _, f := range zr.File {
			rc, err := f.Open()
			if err != nil {
				t.Fatalf("open zip entry %s: %v", f.Name, err)
			}
			b, _ := io.ReadAll(rc)
			rc.Close()
			out[f.Name] = b
		}
		return out
	}
	msgIDs := func(doc zipExportDoc) []int64 {
		ids := make([]int64, 0, len(doc.Messages))
		for _, m := range doc.Messages {
			ids = append(ids, m.MessageID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}

	const f1Content, f2Content = "receipts one alpha", "receipts two beta"
	_, f1 := uploadFile(t, ts.URL, boot.Token, "evidence1.txt", "text/plain", f1Content)
	_, f2 := uploadFile(t, ts.URL, boot.Token, "evidence2.txt", "text/plain", f2Content)
	msg1 := send(fmt.Sprintf("first [%s](%s)", f1.Name, f1.URL))
	msg2 := send(fmt.Sprintf("second [%s](%s)", f2.Name, f2.URL))
	if msg1 == 0 || msg2 == 0 {
		t.Fatal("send failed")
	}

	// Bundle export: a zip with export.json + manifest + both files' bytes.
	zipID := runExport(map[string]any{"include_files": true})
	resp, body := download(t, ts.URL, boot.Token, zipID)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/zip" {
		t.Fatalf("zip download = %d type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	entries := readZip(body)
	var zdoc zipExportDoc
	if err := json.Unmarshal(entries["export.json"], &zdoc); err != nil {
		t.Fatalf("export.json parse: %v", err)
	}
	f1Entry := fmt.Sprintf("files/%d_evidence1.txt", f1.ID)
	f2Entry := fmt.Sprintf("files/%d_evidence2.txt", f2.ID)
	if string(entries[f1Entry]) != f1Content || string(entries[f2Entry]) != f2Content {
		t.Fatalf("bundled file bytes wrong: %q / %q", entries[f1Entry], entries[f2Entry])
	}
	var man zipManifest
	if err := json.Unmarshal(entries["manifest.json"], &man); err != nil {
		t.Fatalf("manifest parse: %v", err)
	}
	if man.FileCount != 2 || len(man.Missing) != 0 || man.TotalBytes != int64(len(f1Content)+len(f2Content)) {
		t.Fatalf("manifest = %+v, want file_count 2, no missing, total %d", man, len(f1Content)+len(f2Content))
	}

	// export.json inside the zip carries the same messages as the plain export
	// of the same scope (per-run fields aside).
	plainID := runExport(map[string]any{"include_files": false})
	presp, pbody := download(t, ts.URL, boot.Token, plainID)
	if presp.StatusCode != http.StatusOK || presp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("plain download = %d type %q, want application/json", presp.StatusCode, presp.Header.Get("Content-Type"))
	}
	var pdoc zipExportDoc
	if err := json.Unmarshal([]byte(pbody), &pdoc); err != nil {
		t.Fatalf("plain export parse: %v", err)
	}
	if fmt.Sprint(msgIDs(zdoc)) != fmt.Sprint(msgIDs(pdoc)) {
		t.Fatalf("zip export.json messages %v != plain export messages %v", msgIDs(zdoc), msgIDs(pdoc))
	}

	// A zero-attachment scope still yields a valid zip (export.json + empty
	// manifest). #general holds the attachment messages, so scope by a fresh
	// channel with a plain message.
	var plain struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token, map[string]any{"name": "nofiles"}, &plain)
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, plain.ChannelID),
		boot.Token, map[string]any{"content": "no attachments here"}, nil)
	emptyID := runExport(map[string]any{"include_files": true, "channel_ids": []int64{plain.ChannelID}})
	_, ebody := download(t, ts.URL, boot.Token, emptyID)
	eentries := readZip(ebody)
	var eman zipManifest
	if err := json.Unmarshal(eentries["manifest.json"], &eman); err != nil {
		t.Fatalf("empty manifest parse: %v", err)
	}
	if eman.FileCount != 0 || len(eman.Missing) != 0 {
		t.Fatalf("zero-attachment manifest = %+v, want file_count 0 no missing", eman)
	}
	if _, ok := eentries["export.json"]; !ok {
		t.Fatal("zero-attachment zip missing export.json")
	}

	// A non-officer (and non-uploader) cannot download the zip result — the
	// existing union-ACL contract (oracle-free 404).
	if r, _ := download(t, ts.URL, bobTok, zipID); r.StatusCode != http.StatusNotFound {
		t.Fatalf("non-officer zip download = %d, want 404", r.StatusCode)
	}

	// Source messages die and one file's blob is purged out from under the pins.
	// Re-export: the purged file is manifest.missing, the other still bundles
	// (the #45 pins kept its bytes). Red/green: make the missing path fail the
	// job → runExport fatals on the absent result.
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msg1), boot.Token); code != http.StatusOK {
		t.Fatalf("delete msg1 = %d", code)
	}
	if code := deleteReq(t, fmt.Sprintf("%s/api/v1/messages/%d", ts.URL, msg2), boot.Token); code != http.StatusOK {
		t.Fatalf("delete msg2 = %d", code)
	}
	var f1Key string
	if err := pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, f1.ID).Scan(&f1Key); err != nil {
		t.Fatalf("f1 key: %v", err)
	}
	if err := store.Delete(ctx, f1Key); err != nil {
		t.Fatalf("purge f1 blob: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE file SET deleted_at = now() WHERE id = $1`, f1.ID); err != nil {
		t.Fatalf("soft-delete f1: %v", err)
	}
	reID := runExport(map[string]any{"include_files": true})
	_, rbody := download(t, ts.URL, boot.Token, reID)
	rentries := readZip(rbody)
	var rman zipManifest
	if err := json.Unmarshal(rentries["manifest.json"], &rman); err != nil {
		t.Fatalf("re-export manifest parse: %v", err)
	}
	if len(rman.Missing) != 1 || rman.Missing[0] != f1.ID {
		t.Fatalf("re-export missing = %v, want [%d] (purged file)", rman.Missing, f1.ID)
	}
	if rman.FileCount != 1 || string(rentries[f2Entry]) != f2Content {
		t.Fatalf("re-export bundled = %d, f2 bytes %q — pins should keep f2 despite dead source",
			rman.FileCount, rentries[f2Entry])
	}
	if _, ok := rentries[f1Entry]; ok {
		t.Fatal("purged file should not appear in the zip")
	}

	// P-19 interaction: an export bundle is the org's own storage usage, so
	// StoreDocumentStream is quota-enforced — under an already-full quota the
	// job FAILS (status 4, no result file) instead of writing past the cap.
	if code := putJSON(t, ts.URL+"/api/v1/admin/storage-quota", boot.Token,
		map[string]any{"max_bytes": 1}); code != 200 {
		t.Fatalf("set quota = %d", code)
	}
	var overJob compliance.ExportJob
	postJSON(t, ts.URL+"/api/v1/admin/exports", boot.Token,
		map[string]any{"include_files": true}, &overJob)
	if _, err := complianceSvc.RunPendingExports(ctx); err != nil {
		t.Fatalf("run over-quota export: %v", err)
	}
	var overStatus int16
	var overResult *int64
	if err := pool.QueryRow(ctx,
		`SELECT status, result_file_id FROM export_job WHERE id = $1`,
		overJob.ID).Scan(&overStatus, &overResult); err != nil {
		t.Fatalf("over-quota job row: %v", err)
	}
	if overStatus != 4 || overResult != nil {
		t.Fatalf("over-quota export = status %d result %v, want failed(4) with no result",
			overStatus, overResult)
	}
}
