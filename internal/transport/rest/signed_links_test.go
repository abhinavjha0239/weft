package rest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

const testSigningSecret = "s3cr3t-signing-key-for-tests"

// signedURL builds the capability path the way the server does, so the test
// can forge expired / wrong-org / tampered variants with the known secret.
func signedURL(fileID, exp, orgID int64) string {
	h := hmac.New(sha256.New, []byte(testSigningSecret))
	fmt.Fprintf(h, "%d|%d|%d", fileID, exp, orgID)
	sig := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("/api/v1/files/%d?sig=%s&exp=%d&org=%d", fileID, sig, exp, orgID)
}

// forgedURL is signedURL with the WRONG secret: a well-formed, correct-length
// hex sig over the right (fileID, exp, org) that simply does not verify. Every
// other guard (org > 0, fresh exp, the row exists in this org) passes, so ONLY
// the HMAC comparison can reject it — this is what proves signature
// verification is load-bearing (neuter the guard and this case goes 200).
func forgedURL(fileID, exp, orgID int64) string {
	h := hmac.New(sha256.New, []byte("not-the-server-secret"))
	fmt.Fprintf(h, "%d|%d|%d", fileID, exp, orgID)
	sig := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("/api/v1/files/%d?sig=%s&exp=%d&org=%d", fileID, sig, exp, orgID)
}

func fetchNoAuth(t *testing.T, base, path string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest("GET", base+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestSignedLinks: P-07 signed download links — mint under the union ACL,
// fetch with no Authorization header, constant-time HMAC verification with a
// 30s expiry leeway, org-pinning (a wrong-org link 404s via the scoped load),
// a 404 for a file GC'd after minting, and the config-missing 500.
func TestSignedLinks(t *testing.T) {
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
	filesSvc := files.New(pool, store) // signing secret left unset for now
	ts := httptest.NewServer(Handler(ctx, Deps{
		Pool: pool, Hub: hub, Log: slog.Default(),
		Identity:  identity.New(pool, permsSvc),
		Messaging: messaging.New(pool, permsSvc),
		Files:     filesSvc,
	}))
	defer ts.Close()

	var boot struct {
		OrgID  int64  `json:"org_id"`
		UserID int64  `json:"user_id"`
		Token  string `json:"token"`
	}
	postJSON(t, ts.URL+"/api/v1/orgs/bootstrap", "", map[string]any{
		"org_slug": "sig", "email": "a@sig.test", "password": "password123",
		"full_name": "Alice Chen",
	}, &boot)

	const content = "the signed payload"
	code, f := uploadFile(t, ts.URL, boot.Token, "doc.txt", "text/plain", content)
	if code != http.StatusCreated || f.ID == 0 {
		t.Fatalf("upload = %d %+v", code, f)
	}
	linkURL := fmt.Sprintf("%s/api/v1/files/%d/link", ts.URL, f.ID)

	// Config-missing: no signing secret set → a clear 500.
	{
		req, _ := http.NewRequest("POST", linkURL, nil)
		req.Header.Set("Authorization", "Bearer "+boot.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("link (no secret): %v", err)
		}
		var body struct {
			Error string `json:"error"`
		}
		_ = jsonDecode(resp.Body, &body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError || body.Error != "signing secret not configured" {
			t.Fatalf("link without secret = %d %q, want 500 + clear message", resp.StatusCode, body.Error)
		}
	}

	// Wire the secret (weftd does this from config); minting now works.
	filesSvc.SetSigningSecret(testSigningSecret)
	var link files.SignedLinkResult
	{
		req, _ := http.NewRequest("POST", linkURL, nil)
		req.Header.Set("Authorization", "Bearer "+boot.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("mint = %d, want 200", resp.StatusCode)
		}
		_ = jsonDecode(resp.Body, &link)
		resp.Body.Close()
	}
	if link.URL == "" || link.ExpiresAt <= time.Now().Unix() {
		t.Fatalf("mint result = %+v", link)
	}

	// Fetch the capability URL with NO Authorization header → the bytes.
	if code, body := fetchNoAuth(t, ts.URL, link.URL); code != 200 || string(body) != content {
		t.Fatalf("signed fetch = %d %q, want 200 + %q", code, body, content)
	}

	// Forged signature → 401. A well-formed sig computed with the WRONG
	// secret over the RIGHT (file, exp, org): org > 0, exp fresh, the row
	// exists — so only the HMAC comparison can reject it. (Neuter that guard
	// and this case returns 200: it is the one test that pins signature
	// verification itself, distinct from the org/expiry/scope guards below.)
	if code, _ := fetchNoAuth(t, ts.URL, forgedURL(f.ID, time.Now().Add(5*time.Minute).Unix(), boot.OrgID)); code != http.StatusUnauthorized {
		t.Fatalf("forged sig (wrong secret) = %d, want 401", code)
	}
	// A one-nibble flip INSIDE the real sig (not the trailing org param) is
	// likewise rejected — guards against a tamper that leaves org/exp intact.
	sigStart := strings.Index(link.URL, "sig=") + 4
	flipped := []byte(link.URL)
	if flipped[sigStart] == '0' {
		flipped[sigStart] = '1'
	} else {
		flipped[sigStart] = '0'
	}
	if code, _ := fetchNoAuth(t, ts.URL, string(flipped)); code != http.StatusUnauthorized {
		t.Fatalf("flipped-sig = %d, want 401", code)
	}

	// Expiry: 10s past is inside the 30s leeway (200); 60s past is expired (401).
	if code, body := fetchNoAuth(t, ts.URL, signedURL(f.ID, time.Now().Add(-10*time.Second).Unix(), boot.OrgID)); code != 200 || string(body) != content {
		t.Fatalf("within-leeway = %d, want 200", code)
	}
	if code, _ := fetchNoAuth(t, ts.URL, signedURL(f.ID, time.Now().Add(-60*time.Second).Unix(), boot.OrgID)); code != http.StatusUnauthorized {
		t.Fatalf("expired = %d, want 401", code)
	}

	// Org-pinned: a validly-signed link for the WRONG org 404s — the
	// org-scoped row load misses (a leaked link cannot cross orgs).
	exp := time.Now().Add(10 * time.Minute).Unix()
	if code, _ := fetchNoAuth(t, ts.URL, signedURL(f.ID, exp, boot.OrgID+999)); code != http.StatusNotFound {
		t.Fatalf("foreign-org sig = %d, want 404", code)
	}

	// No sig and no token → the bearer path 401s; a valid bearer still works.
	if code, _ := fetchNoAuth(t, ts.URL, fmt.Sprintf("/api/v1/files/%d", f.ID)); code != http.StatusUnauthorized {
		t.Fatalf("no auth, no sig = %d, want 401", code)
	}
	if resp, body := download(t, ts.URL, boot.Token, f.ID); resp.StatusCode != 200 || body != content {
		t.Fatalf("bearer download = %d %q", resp.StatusCode, body)
	}

	// A file GC'd (deleted) after minting → 404 even with a valid signature.
	if _, err := pool.Exec(ctx, `UPDATE file SET deleted_at = now() WHERE id = $1`, f.ID); err != nil {
		t.Fatalf("soft-delete: %v", err)
	}
	if code, _ := fetchNoAuth(t, ts.URL, signedURL(f.ID, exp, boot.OrgID)); code != http.StatusNotFound {
		t.Fatalf("signed fetch of deleted file = %d, want 404", code)
	}
}
