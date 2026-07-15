package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
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

// virusScanner quarantines any content containing the marker "VIRUS" (the
// EICAR idea, minus the real string), else passes.
type virusScanner struct{}

func (virusScanner) Scan(_ context.Context, _, _ string, r io.Reader) (files.Verdict, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	if bytes.Contains(b, []byte("VIRUS")) {
		return files.Quarantined, nil
	}
	return files.Clean, nil
}

// downScanner models a scanner outage: every scan errors, which must fail the
// upload CLOSED.
type downScanner struct{}

func (downScanner) Scan(context.Context, string, string, io.Reader) (files.Verdict, error) {
	return 0, errors.New("scanner unavailable")
}

// pngWith returns bytes that http.DetectContentType reads as image/png (the
// 8-byte PNG signature) followed by an arbitrary payload — enough to clear the
// avatar/emoji magic-byte gate so the scan seam, not the image gate, is what
// rejects a marked image.
func pngWith(payload string) []byte {
	return append([]byte("\x89PNG\r\n\x1a\n"), []byte(payload)...)
}

func putMultipartFile(t *testing.T, method, url, token, filename string, content []byte) int {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", filename)
	_, _ = fw.Write(content)
	_ = mw.Close()
	req, _ := http.NewRequest(method, url, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestUploadScan: P-19 upload malware-scan seam. A clean upload records
// scan_status 1 and is downloadable; a quarantined upload is stored as evidence
// (row status 2, bytes present) but rejected 422 and inert — bearer download
// and signed-link mint both 404, a message link forms NO reference, and the
// avatar/emoji set is impossible because the upload path 422s first; GC treats
// the quarantined row like any unclaimed file. A scanner error fails closed
// (500, no row/blob); with no scanner the status stays 0 (pending) and downloads
// work (today's behavior pinned).
func TestUploadScan(t *testing.T) {
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

	newScanServer := func(scanner files.Scanner) (*httptest.Server, blob.Store) {
		t.Helper()
		store, err := blob.Open("fs", t.TempDir())
		if err != nil {
			t.Fatalf("blob: %v", err)
		}
		hub := gateway.NewHub(pool, slog.Default())
		go hub.Run(ctx)
		permsSvc := perms.New(pool)
		msgSvc := messaging.New(pool, permsSvc)
		filesSvc := files.New(pool, store)
		if scanner != nil {
			filesSvc.SetScanner(scanner)
		}
		msgSvc.SetFiles(filesSvc)
		ts := httptest.NewServer(Handler(ctx, Deps{
			Pool: pool, Hub: hub, Log: slog.Default(),
			Identity:  identity.New(pool, permsSvc),
			Messaging: msgSvc,
			DM:        dm.New(pool),
			Files:     filesSvc,
		}))
		t.Cleanup(ts.Close)
		return ts, store
	}
	bootstrap := func(tsURL, slug string) (orgID, userID, channelID int64, token string) {
		t.Helper()
		var b struct {
			OrgID     int64  `json:"org_id"`
			UserID    int64  `json:"user_id"`
			ChannelID int64  `json:"channel_id"`
			Token     string `json:"token"`
		}
		postJSON(t, tsURL+"/api/v1/orgs/bootstrap", "", map[string]any{
			"org_slug": slug, "email": "a@" + slug + ".test", "password": "password123",
			"full_name": "Alice Chen",
		}, &b)
		return b.OrgID, b.UserID, b.ChannelID, b.Token
	}

	// --- scanner set: clean passes, marked file is quarantined + inert. ---
	tsA, storeA := newScanServer(virusScanner{})
	orgA, userA, chanA, tokA := bootstrap(tsA.URL, "scan")

	code, clean := uploadFile(t, tsA.URL, tokA, "clean.txt", "text/plain", "all good here")
	if code != http.StatusCreated {
		t.Fatalf("clean upload = %d, want 201", code)
	}
	var cleanStatus int16
	if err := pool.QueryRow(ctx, `SELECT scan_status FROM file WHERE id = $1`, clean.ID).Scan(&cleanStatus); err != nil {
		t.Fatalf("clean status: %v", err)
	}
	if cleanStatus != 1 {
		t.Fatalf("clean scan_status = %d, want 1", cleanStatus)
	}
	if resp, body := download(t, tsA.URL, tokA, clean.ID); resp.StatusCode != 200 || body != "all good here" {
		t.Fatalf("clean download = %d %q", resp.StatusCode, body)
	}

	if code, _ := uploadFile(t, tsA.URL, tokA, "bad.txt", "text/plain", "danger VIRUS inside"); code != http.StatusUnprocessableEntity {
		t.Fatalf("quarantined upload = %d, want 422", code)
	}
	var qid int64
	var qStatus int16
	if err := pool.QueryRow(ctx,
		`SELECT id, scan_status FROM file WHERE org_id = $1 AND name = 'bad.txt'`, orgA).Scan(&qid, &qStatus); err != nil {
		t.Fatalf("quarantined row: %v", err)
	}
	if qStatus != 2 {
		t.Fatalf("quarantined scan_status = %d, want 2", qStatus)
	}
	// The bytes ARE stored (evidence).
	var qKey string
	if err := pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, qid).Scan(&qKey); err != nil {
		t.Fatalf("q key: %v", err)
	}
	if rc, err := storeA.Open(ctx, qKey); err != nil {
		t.Fatalf("quarantined bytes should be stored as evidence: %v", err)
	} else {
		rc.Close()
	}
	// Every read path 404s: bearer download and signed-link mint.
	if resp, _ := download(t, tsA.URL, tokA, qid); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("quarantined bearer download = %d, want 404", resp.StatusCode)
	}
	if code := postJSONStatus(t, fmt.Sprintf("%s/api/v1/files/%d/link", tsA.URL, qid), tokA, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("quarantined signed-link mint = %d, want 404", code)
	}
	// A message link to it forms NO reference (the attach hook skips status 2,
	// even for the uploader).
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", tsA.URL, chanA), tokA,
		map[string]any{"content": fmt.Sprintf("see [x](/api/v1/files/%d)", qid)}, nil)
	var refs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM file_reference WHERE file_id = $1`, qid).Scan(&refs); err != nil {
		t.Fatalf("ref count: %v", err)
	}
	if refs != 0 {
		t.Fatalf("quarantined file gained %d references, want 0", refs)
	}
	// Avatar/emoji set is impossible — the upload path rejects first.
	pv := pngWith("harmless-looking but VIRUS inside")
	if code := putMultipartFile(t, "PUT", tsA.URL+"/api/v1/me/avatar", tokA, "a.png", pv); code != http.StatusUnprocessableEntity {
		t.Fatalf("avatar set with quarantined image = %d, want 422", code)
	}
	var avatarFID *int64
	if err := pool.QueryRow(ctx, `SELECT avatar_file_id FROM user_account WHERE id = $1`, userA).Scan(&avatarFID); err != nil {
		t.Fatalf("avatar fid: %v", err)
	}
	if avatarFID != nil {
		t.Fatalf("avatar_file_id set to %d despite quarantine, want nil", *avatarFID)
	}
	if code := putMultipartFile(t, "POST", tsA.URL+"/api/v1/emoji?name=danger", tokA, "e.png", pv); code != http.StatusUnprocessableEntity {
		t.Fatalf("emoji set with quarantined image = %d, want 422", code)
	}
	var emojiCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM custom_emoji WHERE org_id = $1`, orgA).Scan(&emojiCount); err != nil {
		t.Fatalf("emoji count: %v", err)
	}
	if emojiCount != 0 {
		t.Fatalf("custom_emoji rows = %d, want 0", emojiCount)
	}
	// GC treats the quarantined row like any unclaimed file: backdate it past
	// the grace window and it purges (bytes reclaimed).
	if _, err := pool.Exec(ctx,
		`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, qid); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	janitor := compliance.NewJanitor(pool, storeA, slog.Default())
	rep, err := janitor.SweepOnce(ctx, time.Now())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if rep.UnclaimedPurged != 1 {
		t.Fatalf("GC UnclaimedPurged = %d, want 1 (the backdated quarantined file)", rep.UnclaimedPurged)
	}
	var qDeleted bool
	if err := pool.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM file WHERE id = $1`, qid).Scan(&qDeleted); err != nil {
		t.Fatalf("q deleted: %v", err)
	}
	if !qDeleted {
		t.Fatal("quarantined file not purged by GC")
	}

	// --- scanner error fails closed: 500, no row, no blob. ---
	tsB, _ := newScanServer(downScanner{})
	orgB, _, _, tokB := bootstrap(tsB.URL, "scanerr")
	if code, _ := uploadFile(t, tsB.URL, tokB, "x.txt", "text/plain", "whatever"); code != http.StatusInternalServerError {
		t.Fatalf("scanner-error upload = %d, want 500", code)
	}
	var rowsB int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM file WHERE org_id = $1`, orgB).Scan(&rowsB); err != nil {
		t.Fatalf("rows B: %v", err)
	}
	if rowsB != 0 {
		t.Fatalf("scanner error left %d file rows, want 0 (fail closed)", rowsB)
	}

	// --- no scanner: status stays 0 (pending) and downloads work. ---
	tsC, _ := newScanServer(nil)
	_, _, _, tokC := bootstrap(tsC.URL, "noscan")
	codeC, fC := uploadFile(t, tsC.URL, tokC, "n.txt", "text/plain", "no scanner content")
	if codeC != http.StatusCreated {
		t.Fatalf("no-scanner upload = %d, want 201", codeC)
	}
	var stC int16
	if err := pool.QueryRow(ctx, `SELECT scan_status FROM file WHERE id = $1`, fC.ID).Scan(&stC); err != nil {
		t.Fatalf("no-scanner status: %v", err)
	}
	if stC != 0 {
		t.Fatalf("no-scanner scan_status = %d, want 0 (pending)", stC)
	}
	if resp, body := download(t, tsC.URL, tokC, fC.ID); resp.StatusCode != 200 || body != "no scanner content" {
		t.Fatalf("no-scanner download = %d %q", resp.StatusCode, body)
	}
}
