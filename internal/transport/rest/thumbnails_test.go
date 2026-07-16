package rest

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// TestImageThumbnails: the P-18 slice end to end against real Postgres and the
// fs blob store — thumbnails are DERIVED blobs at files.ThumbKey, never file
// rows. Upload-time generation + lazy backfill; inline JPEG serving with the
// avatar-style safe headers; oracle-free 404 for non-images and no-ACL files;
// THE decompression-bomb cap (the load-bearing red/green); and GC that
// reclaims the thumb with the original while a dedup twin keeps it alive.
func TestImageThumbnails(t *testing.T) {
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
		Identity:   identity.New(pool, permsSvc),
		Messaging:  msgSvc,
		DM:         dm.New(pool),
		Files:      filesSvc,
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
		"org_slug": "thm", "email": "a@thm.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	bobTok := addChannelMember(t, ctx, pool, boot.OrgID, boot.ChannelID,
		"bob@thm.test", "Bob Ray", "bobthmtok")

	blobKey := func(id int64) string {
		t.Helper()
		var k string
		if err := pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, id).Scan(&k); err != nil {
			t.Fatalf("key of %d: %v", id, err)
		}
		return k
	}
	blobExists := func(key string) bool {
		t.Helper()
		rc, err := store.Open(ctx, key)
		if err != nil {
			return false
		}
		rc.Close()
		return true
	}
	thumbExists := func(id int64) bool { return blobExists(files.ThumbKey(blobKey(id))) }
	fileLive := func(id int64) bool {
		t.Helper()
		var live bool
		if err := pool.QueryRow(ctx,
			`SELECT deleted_at IS NULL FROM file WHERE id = $1`, id).Scan(&live); err != nil {
			t.Fatalf("liveness of %d: %v", id, err)
		}
		return live
	}
	backdateFile := func(id int64) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE file SET created_at = now() - interval '40 days' WHERE id = $1`, id); err != nil {
			t.Fatalf("backdate file %d: %v", id, err)
		}
	}
	getThumb := func(tok string, id int64) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/files/%d/thumbnail", ts.URL, id), nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("thumbnail GET: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, b
	}
	referenceFile := func(f files.File) {
		t.Helper()
		var sent struct {
			MessageID int64 `json:"message_id"`
		}
		postJSON(t, fmt.Sprintf("%s/api/v1/channels/%d/messages", ts.URL, boot.ChannelID),
			boot.Token, map[string]any{"content": fmt.Sprintf("see [pic](%s)", f.URL)}, &sent)
		if sent.MessageID == 0 {
			t.Fatalf("referencing message for file %d failed", f.ID)
		}
	}

	// ---- Upload PNG → inline JPEG thumbnail with the safe headers ----------
	code, pf := uploadFile(t, ts.URL, boot.Token, "photo.png", "image/png", string(makePNG(t, 960, 600)))
	if code != http.StatusCreated {
		t.Fatalf("upload = %d, want 201", code)
	}
	// Generation is synchronous in Upload (best-effort): the thumb exists
	// before any GET.
	if !thumbExists(pf.ID) {
		t.Fatal("upload did not generate a thumbnail for a PNG")
	}
	resp, body := getThumb(boot.Token, pf.ID)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("thumbnail = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Fatalf("Content-Type = %q, want image/jpeg", ct)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
		resp.Header.Get("Cache-Control") != "private, max-age=3600" ||
		resp.Header.Get("Content-Disposition") != "inline" {
		t.Fatalf("thumbnail headers not the avatar-style safe set: %v", resp.Header)
	}
	if resp.Header.Get("X-Image-Width") != "960" || resp.Header.Get("X-Image-Height") != "600" ||
		resp.Header.Get("X-Thumbnail-Width") != "480" || resp.Header.Get("X-Thumbnail-Height") != "300" {
		t.Fatalf("dimension headers wrong: %v", resp.Header)
	}
	if img, err := jpeg.Decode(bytes.NewReader(body)); err != nil {
		t.Fatalf("thumbnail body is not a JPEG: %v", err)
	} else if b := img.Bounds(); b.Dx() != 480 || b.Dy() != 300 {
		t.Fatalf("thumbnail dims = %dx%d, want 480x300", b.Dx(), b.Dy())
	}

	// ---- Lazy backfill: a file stored WITHOUT upload-time generation -------
	// StoreDocument (compliance-export/system artifacts) never pre-generates a
	// thumb; the first GET must backfill it (pre-P-18 uploads behave the same).
	actor := auth.Identity{OrgID: boot.OrgID, UserID: boot.UserID}
	df, err := filesSvc.StoreDocument(ctx, actor, "doc.png", "image/png", makePNG(t, 600, 240))
	if err != nil {
		t.Fatalf("StoreDocument: %v", err)
	}
	if thumbExists(df.ID) {
		t.Fatal("StoreDocument must not pre-generate a thumbnail")
	}
	if resp, _ := getThumb(boot.Token, df.ID); resp.StatusCode != http.StatusOK {
		t.Fatalf("lazy backfill thumbnail = %d, want 200", resp.StatusCode)
	}
	if !thumbExists(df.ID) {
		t.Fatal("lazy backfill did not persist the thumbnail")
	}

	// ---- Non-image → 404 (never generated, never inline) -------------------
	_, tf := uploadFile(t, ts.URL, boot.Token, "notes.txt", "text/plain", "just some plain text, not an image at all")
	if thumbExists(tf.ID) {
		t.Fatal("a text file must not get a thumbnail")
	}
	if resp, _ := getThumb(boot.Token, tf.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("non-image thumbnail = %d, want 404", resp.StatusCode)
	}

	// ---- No-ACL viewer → 404 (EXACTLY authorizeDownload, oracle-free) ------
	// pf is unreferenced, so only its uploader may read it; another member
	// gets the same 404 a nonexistent/foreign-org id would.
	if resp, _ := getThumb(bobTok, pf.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no-ACL thumbnail = %d, want 404", resp.StatusCode)
	}

	// ---- THE SECURITY PIN: decompression-bomb cap → 404 --------------------
	// A real, decodable PNG whose WIDTH exceeds the 12000-px per-dimension cap
	// (but with few total pixels, so it is cheap to build/decode — a gigapixel
	// square would OOM the process in the red state rather than fail cleanly).
	// With the cap, generation is refused at both upload and GET → 404.
	// RED/GREEN: delete the withinCaps guard in imaging.plan() and this image
	// decodes and serves a thumbnail, flipping this assertion from 404 to 200.
	_, bf := uploadFile(t, ts.URL, boot.Token, "bomb.png", "image/png", string(makePNG(t, 13000, 16)))
	// THE load-bearing assertion: the serve path refuses the over-cap image.
	if resp, _ := getThumb(boot.Token, bf.ID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("decompression-bomb thumbnail = %d, want 404 (the pixel cap)", resp.StatusCode)
	}
	// Belt and braces: it was never generated at upload either (best-effort skip).
	if thumbExists(bf.ID) {
		t.Fatal("an over-cap image must not generate a thumbnail at upload")
	}

	// ---- GC purge reclaims the thumb with the original ---------------------
	_, gf := uploadFile(t, ts.URL, boot.Token, "gc.png", "image/png", string(makePNG(t, 500, 300)))
	gfKey := blobKey(gf.ID)
	if !blobExists(gfKey) || !blobExists(files.ThumbKey(gfKey)) {
		t.Fatal("expected original + thumb blobs before GC")
	}
	backdateFile(gf.ID) // unreferenced, past the unclaimed grace
	if _, err := janitor.SweepOnce(ctx, time.Now()); err != nil {
		t.Fatalf("gc sweep: %v", err)
	}
	if fileLive(gf.ID) {
		t.Fatal("gc.png should be purged")
	}
	if blobExists(gfKey) {
		t.Fatal("original blob should be gone after purge")
	}
	if blobExists(files.ThumbKey(gfKey)) {
		t.Fatal("thumbnail blob should be reclaimed WITH the original")
	}

	// ---- Dedup twin keeps the shared thumb alive ---------------------------
	// Two files with identical content share one blob key AND one thumb key.
	// Purging the unreferenced twin's row must NOT delete the shared blobs
	// while the referenced twin lives (the existing content-addressed twin
	// rule that gates the blob delete gates the thumb delete too).
	twin := makePNG(t, 640, 400)
	_, c1 := uploadFile(t, ts.URL, boot.Token, "twin1.png", "image/png", string(twin))
	_, c2 := uploadFile(t, ts.URL, boot.Token, "twin2.png", "image/png", string(twin))
	twinKey := blobKey(c1.ID)
	if twinKey != blobKey(c2.ID) {
		t.Fatalf("twins should share a key: %q vs %q", twinKey, blobKey(c2.ID))
	}
	referenceFile(c1) // c1 stays live
	backdateFile(c2.ID)
	if _, err := janitor.SweepOnce(ctx, time.Now()); err != nil {
		t.Fatalf("twin sweep: %v", err)
	}
	if fileLive(c2.ID) {
		t.Fatal("c2 row should be purged")
	}
	if !fileLive(c1.ID) {
		t.Fatal("c1 must survive (referenced)")
	}
	if !blobExists(twinKey) {
		t.Fatal("shared original blob must survive its live twin")
	}
	if !blobExists(files.ThumbKey(twinKey)) {
		t.Fatal("shared thumbnail must survive its live twin")
	}
	if resp, _ := getThumb(boot.Token, c1.ID); resp.StatusCode != http.StatusOK {
		t.Fatalf("live twin thumbnail = %d, want 200", resp.StatusCode)
	}
}

// makePNG encodes a deterministic RGBA gradient as PNG: identical dimensions
// yield identical bytes (so the twin case dedups to one content-addressed
// key), and every distinct size is distinct content.
func makePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.SetRGBA(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png %dx%d: %v", w, h, err)
	}
	return buf.Bytes()
}
