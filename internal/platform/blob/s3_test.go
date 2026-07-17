package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
)

// TestS3RoundTrip exercises the S3 driver against a real S3-compatible
// endpoint (MinIO/R2/AWS). It SKIPS unless TEST_S3_ENDPOINT is set — CI has no
// MinIO service yet (recorded gap). The fs-driver suite proves the seam
// contract regardless; this proves the S3 implementation of it when wired.
func TestS3RoundTrip(t *testing.T) {
	endpoint := os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_S3_ENDPOINT not set; skipping S3 integration test")
	}
	bucket := os.Getenv("TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "weft-test"
	}
	region := os.Getenv("TEST_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	ctx := context.Background()
	store, err := NewS3(ctx, S3Config{Bucket: bucket, Region: region, Endpoint: endpoint, Prefix: "blobtest"})
	if err != nil {
		t.Fatalf("new s3: %v", err)
	}

	key := "ab/cd/abcd1234roundtrip"
	content := []byte("hello from the s3 seam")
	if err := store.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Idempotent re-put of identical content-addressed bytes.
	if err := store.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("re-put: %v", err)
	}
	rc, err := store.Open(ctx, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("round-trip mismatch: %q", got)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Deleting a missing key is a no-op.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("delete missing (should tolerate): %v", err)
	}
	// Opening a missing key satisfies the Store contract's fs.ErrNotExist,
	// so callers can tell absent from outage (the fs driver gets this from
	// os.Open natively).
	if _, err := store.Open(ctx, key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open missing = %v, want fs.ErrNotExist", err)
	}
}
