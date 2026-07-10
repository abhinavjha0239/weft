// Package blob is the storage seam (ADR-012): the core backend speaks ONLY
// this interface — which backend holds the bytes is an operator choice, not
// a code change. Keys are backend-relative (file.storage_key); the domain
// layer derives them content-addressed, so every implementation gets
// idempotent puts and org-level dedup for free.
//
// Adding a backend = one file implementing Store + one case in Open.
// Planned drivers ride the same seam: s3 (AWS + any S3-compatible: MinIO,
// R2, GCS in interop mode), gcs native, azure.
package blob

import (
	"context"
	"fmt"
	"io"
)

type Store interface {
	// Put writes the blob under key. Implementations must be atomic per
	// key: a concurrent reader never sees a partial object. Writing a key
	// that already exists is a success no-op (keys are content-addressed).
	Put(ctx context.Context, key string, r io.Reader) error
	// Open streams the blob; the caller closes it.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete removes the blob; deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// Open constructs the configured driver.
//
//	driver "fs": dsn is the data directory (bare metal / any mounted volume).
//	driver "s3"|"gcs"|"azure": reserved — implement Store, add a case here.
func Open(driver, dsn string) (Store, error) {
	switch driver {
	case "", "fs":
		if dsn == "" {
			return nil, fmt.Errorf("blob: fs driver needs a data directory")
		}
		return NewFS(dsn)
	default:
		return nil, fmt.Errorf("blob: unknown driver %q (implement blob.Store and register it here)", driver)
	}
}
