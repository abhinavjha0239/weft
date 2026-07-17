package rest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
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
		resp.Header.Get("Vary") != "Authorization" ||
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

// hardenedStore wraps a blob.Store with per-key open counting, an optional
// slow set (opens of those keys sleep 150ms and track peak concurrency — the
// render-semaphore probe), and per-key failure injection (the outage probe).
type hardenedStore struct {
	blob.Store
	mu       sync.Mutex
	opens    map[string]int
	slow     map[string]bool
	fail     map[string]error
	inFlight int
	maxIn    int
}

func newHardenedStore(s blob.Store) *hardenedStore {
	return &hardenedStore{Store: s, opens: map[string]int{},
		slow: map[string]bool{}, fail: map[string]error{}}
}

func (h *hardenedStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	h.mu.Lock()
	h.opens[key]++
	slow, ferr := h.slow[key], h.fail[key]
	if slow {
		h.inFlight++
		if h.inFlight > h.maxIn {
			h.maxIn = h.inFlight
		}
	}
	h.mu.Unlock()
	if slow {
		time.Sleep(150 * time.Millisecond)
		h.mu.Lock()
		h.inFlight--
		h.mu.Unlock()
	}
	if ferr != nil {
		return nil, ferr
	}
	return h.Store.Open(ctx, key)
}

func (h *hardenedStore) openCount(keys ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, k := range keys {
		n += h.opens[k]
	}
	return n
}

// logCapture is a minimal slog.Handler recording message lines.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, r.Message)
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }
func (c *logCapture) count(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, l := range c.lines {
		if l == msg {
			n++
		}
	}
	return n
}

// TestThumbnailDoSHardening: the render lane under abuse. A corrupt image
// UNDER the bomb caps passes the header check and only fails at full decode —
// pre-hardening, every GET re-paid that decode (~4 B/pixel alloc) with
// unbounded concurrency and no memory of the failure. Pins: (1) concurrent
// first-views hold at most renderConcurrency originals open (the semaphore is
// acquired before the blob open); (2) a failed render is remembered — repeat
// GETs are 404s with ZERO store IO; (3) a warm hit never re-opens the
// original (dimensions ride the per-key memo); (4) a blob OUTAGE logs a
// warning while keeping the oracle-free 404, is never poisoned into the
// negative cache, and recovers immediately; an ABSENT blob stays silent.
func TestThumbnailDoSHardening(t *testing.T) {
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

	fsStore, err := blob.Open("fs", t.TempDir())
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	store := newHardenedStore(fsStore)
	capture := &logCapture{}
	hub := gateway.NewHub(pool, slog.Default())
	go hub.Run(ctx)
	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	filesSvc := files.New(pool, store)
	filesSvc.SetLogger(slog.New(capture))
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

	var boot struct {
		OrgID     int64  `json:"org_id"`
		UserID    int64  `json:"user_id"`
		ChannelID int64  `json:"channel_id"`
		Token     string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "dos", "email": "a@dos.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)
	actor := auth.Identity{OrgID: boot.OrgID, UserID: boot.UserID}

	blobKey := func(id int64) string {
		t.Helper()
		var k string
		if err := pool.QueryRow(ctx, `SELECT storage_key FROM file WHERE id = $1`, id).Scan(&k); err != nil {
			t.Fatalf("key of %d: %v", id, err)
		}
		return k
	}
	// statusOf is goroutine-safe (no t.Fatalf): errors surface as -1.
	statusOf := func(tok string, id int64) int {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/files/%d/thumbnail", ts.URL, id), nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return -1
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	// ---- Corrupt-under-caps wave: semaphore + negative cache ---------------
	// StoreDocument never renders at upload, so the FIRST GET is the render —
	// the lane the semaphore and the failure memo protect. Distinct garbage
	// makes distinct content-addressed keys.
	const herd = 6
	ids := make([]int64, herd)
	keys := make([]string, herd)
	for i := range ids {
		df, err := filesSvc.StoreDocument(ctx, actor, fmt.Sprintf("bad%d.png", i),
			"image/png", corruptPNG(t, 4000, 4000, byte(i)))
		if err != nil {
			t.Fatalf("store corrupt doc %d: %v", i, err)
		}
		ids[i] = df.ID
		keys[i] = blobKey(df.ID)
		store.mu.Lock()
		store.slow[keys[i]] = true
		store.mu.Unlock()
	}
	statuses := make(chan int, herd)
	for _, id := range ids {
		go func(id int64) { statuses <- statusOf(boot.Token, id) }(id)
	}
	for i := 0; i < herd; i++ {
		if code := <-statuses; code != http.StatusNotFound {
			t.Fatalf("corrupt thumbnail = %d, want 404", code)
		}
	}
	// SEMAPHORE PIN: the render lane holds at most renderConcurrency originals
	// open at once — the semaphore is acquired BEFORE the blob open, so peak
	// concurrent opens of the slow keys measures exactly the bound. RED/GREEN:
	// remove the acquire in renderThumbFrom and the herd opens ~all 6 at once.
	store.mu.Lock()
	maxIn := store.maxIn
	store.mu.Unlock()
	if maxIn > 2 {
		t.Fatalf("peak concurrent renders = %d, want <= 2 (the render semaphore)", maxIn)
	}
	// NEGATIVE-CACHE PIN: the failures are remembered — a second sweep of GETs
	// is all 404s with ZERO further store IO on any key (original OR thumb).
	// RED/GREEN: drop the thumbRememberFailure call on the imaging-error path
	// and these opens climb again (one full decode per GET, the DoS).
	thumbKeys := make([]string, herd)
	for i, k := range keys {
		thumbKeys[i] = files.ThumbKey(k)
	}
	before := store.openCount(append(append([]string{}, keys...), thumbKeys...)...)
	for _, id := range ids {
		if code := statusOf(boot.Token, id); code != http.StatusNotFound {
			t.Fatalf("re-probe = %d, want 404", code)
		}
	}
	if after := store.openCount(append(append([]string{}, keys...), thumbKeys...)...); after != before {
		t.Fatalf("re-probing corrupt files touched the store: opens %d → %d, want unchanged", before, after)
	}

	// ---- Warm-hit memo: dimensions without re-opening the original ---------
	code, gf := uploadFile(t, ts.URL, boot.Token, "good.png", "image/png", string(makePNG(t, 800, 500)))
	if code != http.StatusCreated {
		t.Fatalf("upload = %d", code)
	}
	goodKey := blobKey(gf.ID)
	for i := 0; i < 2; i++ {
		if code := statusOf(boot.Token, gf.ID); code != http.StatusOK {
			t.Fatalf("warm thumbnail GET %d = %d, want 200", i, code)
		}
	}
	// WARM PIN: the upload render memoized the dimensions, so warm GETs open
	// ONLY the thumb blob — the original is never touched on the serve path.
	// RED/GREEN: drop the thumbRemember call on the success path and every
	// warm GET re-describes the original (opens climb to one per GET).
	if n := store.openCount(goodKey); n != 0 {
		t.Fatalf("warm serves opened the original %d times, want 0 (the dimension memo)", n)
	}
	// A FRESH service over the same store (a restart: empty memo, thumb blob
	// present) must still serve via the bounded describe fallback — and the
	// dimensions must match what the memo would have said.
	fresh := files.New(pool, store)
	fresh.SetLogger(slog.New(capture))
	meta, rc, err := fresh.OpenThumbnail(ctx, actor, gf.ID)
	if err != nil {
		t.Fatalf("fresh-process warm thumbnail: %v", err)
	}
	rc.Close()
	if meta.SrcW != 800 || meta.SrcH != 500 {
		t.Fatalf("fresh-process meta = %+v, want 800x500 source", meta)
	}

	// ---- Outage vs absent: loud Warn + no poison vs silent 404 -------------
	of, err := filesSvc.StoreDocument(ctx, actor, "outage.png", "image/png", makePNG(t, 300, 200))
	if err != nil {
		t.Fatalf("store outage doc: %v", err)
	}
	outKey := blobKey(of.ID)
	store.mu.Lock()
	store.fail[outKey] = errors.New("simulated blob outage")
	store.mu.Unlock()
	const outageMsg = "thumbnail: original blob open failed"
	if code := statusOf(boot.Token, of.ID); code != http.StatusNotFound {
		t.Fatalf("outage thumbnail = %d, want 404 (mask holds)", code)
	}
	if n := capture.count(outageMsg); n != 1 {
		t.Fatalf("outage warnings = %d, want 1 (operators must hear an outage)", n)
	}
	// An open failure is NOT negative-cached (no decode was spent): once the
	// store recovers, the very next GET renders and serves.
	store.mu.Lock()
	delete(store.fail, outKey)
	store.mu.Unlock()
	if code := statusOf(boot.Token, of.ID); code != http.StatusOK {
		t.Fatalf("post-outage thumbnail = %d, want 200 (outages must not poison)", code)
	}
	// ABSENT original (purged, never written) stays a SILENT 404: delete the
	// blob under a fresh doc and probe — no new outage warning.
	af, err := filesSvc.StoreDocument(ctx, actor, "absent.png", "image/png", makePNG(t, 301, 200))
	if err != nil {
		t.Fatalf("store absent doc: %v", err)
	}
	if err := fsStore.Delete(ctx, blobKey(af.ID)); err != nil {
		t.Fatalf("delete absent blob: %v", err)
	}
	if code := statusOf(boot.Token, af.ID); code != http.StatusNotFound {
		t.Fatalf("absent thumbnail = %d, want 404", code)
	}
	if n := capture.count(outageMsg); n != 1 {
		t.Fatalf("outage warnings after absent probe = %d, want still 1 (absent is silent)", n)
	}
}

// corruptPNG hand-builds a PNG whose IHDR declares w×h (under the bomb caps)
// but whose IDAT is garbage: image.DecodeConfig succeeds, the full decode
// allocates the pixel buffer and then FAILS — the expensive-failure shape the
// DoS hardening exists for. seed varies the garbage so each call is distinct
// content-addressed bytes.
func corruptPNG(t *testing.T, w, h int, seed byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'})
	chunk := func(typ string, data []byte) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(data)))
		buf.Write(n[:])
		buf.WriteString(typ)
		buf.Write(data)
		crc := crc32.NewIEEE()
		_, _ = crc.Write([]byte(typ))
		_, _ = crc.Write(data)
		var c [4]byte
		binary.BigEndian.PutUint32(c[:], crc.Sum32())
		buf.Write(c[:])
	}
	ihdr := make([]byte, 13)
	binary.BigEndian.PutUint32(ihdr[0:4], uint32(w))
	binary.BigEndian.PutUint32(ihdr[4:8], uint32(h))
	ihdr[8] = 8 // bit depth
	ihdr[9] = 6 // color type: RGBA
	chunk("IHDR", ihdr)
	chunk("IDAT", []byte{seed, 0xde, 0xad, 0xbe, 0xef, ^seed})
	chunk("IEND", nil)
	return buf.Bytes()
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
