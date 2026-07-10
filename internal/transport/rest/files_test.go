package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
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
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

func uploadFile(t *testing.T, base, token, name, mime, content string) (int, files.File) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", name)
	_, _ = io.WriteString(fw, content)
	mw.Close()
	req, _ := http.NewRequest("POST", base+"/api/v1/files", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()
	var f files.File
	_ = jsonDecode(resp.Body, &f)
	return resp.StatusCode, f
}

func jsonDecode(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

func download(t *testing.T, base, token string, id int64) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/files/%d", base, id), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// TestFilesBlobSeam: upload → content-addressed blob on the fs driver with
// org dedup, download via the F-12 union-of-referencing-ACLs (attachment in
// a private channel opens the file to members and nobody else), and the
// forced-attachment/nosniff download headers.
func TestFilesBlobSeam(t *testing.T) {
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
		DM:        dm.New(pool),
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
		"org_slug": "fil", "email": "a@f2.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@f2.test", "Bob Ray", "bobfiltok")
	charlieTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"charlie@f2.test", "Charlie Kim", "charliefiltok")

	// Upload: content-addressed, event recorded.
	code, f := uploadFile(t, ts.URL, boot.Token, "plan.txt", "text/plain", "the secret roadmap")
	if code != http.StatusCreated || f.ID == 0 || f.Size != int64(len("the secret roadmap")) {
		t.Fatalf("upload = %d %+v", code, f)
	}
	// Same content again → same blob key (org dedup), distinct file row.
	_, f2 := uploadFile(t, ts.URL, boot.Token, "copy.txt", "text/plain", "the secret roadmap")
	var k1, k2 string
	_ = pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, f.ID).Scan(&k1)
	_ = pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, f2.ID).Scan(&k2)
	if k1 == "" || k1 != k2 || f.ID == f2.ID {
		t.Fatalf("dedup wrong: k1=%q k2=%q ids=%d/%d", k1, k2, f.ID, f2.ID)
	}

	// Unreferenced: only the uploader reads it; others get an oracle-free 404.
	resp, body := download(t, ts.URL, boot.Token, f.ID)
	if resp.StatusCode != 200 || body != "the secret roadmap" {
		t.Fatalf("uploader download = %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
		!strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment") {
		t.Fatalf("download headers unsafe: %v", resp.Header)
	}
	if resp, _ := download(t, ts.URL, bobTok, f.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unreferenced file visible to non-uploader: %d", resp.StatusCode)
	}

	// Attach in a PRIVATE channel: members gain access via the union rule,
	// non-members still see nothing.
	var priv struct {
		ChannelID int64 `json:"channel_id"`
	}
	postJSON(t, ts.URL+"/api/v1/channels", boot.Token,
		map[string]any{"name": "vault", "private": true}, &priv)
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_member (channel_id, user_id)
		SELECT $1, id FROM user_account WHERE org_id = $2 AND email = 'bob@f2.test'`,
		priv.ChannelID, boot.OrgID); err != nil {
		t.Fatalf("add bob to vault: %v", err)
	}
	var sent struct {
		MessageID int64 `json:"message_id"`
	}
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, priv.ChannelID), boot.Token,
		map[string]any{"content": fmt.Sprintf("attached: [plan](/api/v1/files/%d)", f.ID)}, &sent)
	var hasAttach bool
	_ = pool.QueryRow(ctx,
		`SELECT has_attachment FROM message WHERE id = $1`, sent.MessageID).Scan(&hasAttach)
	if !hasAttach {
		t.Fatal("has_attachment not set by the reference hook")
	}
	if resp, body := download(t, ts.URL, bobTok, f.ID); resp.StatusCode != 200 || body != "the secret roadmap" {
		t.Fatalf("member download via reference = %d", resp.StatusCode)
	}
	if resp, _ := download(t, ts.URL, charlieTok, f.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("non-member reads private attachment: %d", resp.StatusCode)
	}

	// A non-owner cannot smuggle someone ELSE'S unreferenced file into a
	// message to expose it: the attach silently skips, no reference is
	// created, and access stays closed.
	_, priv2 := uploadFile(t, ts.URL, charlieTok, "diary.txt", "text/plain", "charlies diary")
	postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID), bobTok,
		map[string]any{"content": fmt.Sprintf("steal: [x](/api/v1/files/%d)", priv2.ID)}, &sent)
	var refs int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM file_reference WHERE file_id = $1`, priv2.ID).Scan(&refs)
	if refs != 0 {
		t.Fatalf("smuggled reference created: %d", refs)
	}
	if resp, _ := download(t, ts.URL, bobTok, priv2.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("smuggler gained access: %d", resp.StatusCode)
	}

	// DM attachments follow participation.
	var opened dm.Summary
	postJSON(t, ts.URL+"/api/v1/dms", boot.Token, map[string]any{
		"user_ids": []int64{userIDByEmail(t, ctx, pool, boot.OrgID, "bob@f2.test")}}, &opened)
	_, df := uploadFile(t, ts.URL, boot.Token, "dm.txt", "text/plain", "dm payload")
	postJSON(t, fmt.Sprintf("%s/api/v1/threads/%d/messages", ts.URL, opened.RootThreadID), boot.Token,
		map[string]any{"content": fmt.Sprintf("[here](/api/v1/files/%d)", df.ID)}, nil)
	if resp, _ := download(t, ts.URL, bobTok, df.ID); resp.StatusCode != 200 {
		t.Fatalf("dm participant download = %d", resp.StatusCode)
	}
	if resp, _ := download(t, ts.URL, charlieTok, df.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("dm outsider download = %d", resp.StatusCode)
	}

	// The upload wrote its event.
	var n int
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM event_log WHERE org_id = $1 AND verb = 'file.uploaded'`,
		boot.OrgID).Scan(&n)
	if n < 3 {
		t.Fatalf("file.uploaded events = %d, want >= 3", n)
	}
}

func userIDByEmail(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID int64, email string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM user_account WHERE org_id = $1 AND email = $2`, orgID, email).Scan(&id); err != nil {
		t.Fatalf("user %s: %v", email, err)
	}
	return id
}
