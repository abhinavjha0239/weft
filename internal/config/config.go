// Package config reads server configuration from the environment. Every
// variable is prefixed with brand.EnvPrefix (docs/BRANDING.md).
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/brand"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	// Blob storage seam: driver + location. fs (default) is bare metal /
	// any mounted volume; s3/gcs/azure are drop-in Store implementations.
	BlobDriver string
	BlobDir    string
	// GC windows (days). Unclaimed: never-referenced uploads (Zulip ships
	// 5 weeks — drafts hold uploads silently). DeadRef: files whose last
	// referencing message was deleted (Zulip's 30-day vacuum delay).
	GCUnclaimedDays int
	GCDeadRefDays   int
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: os.Getenv(brand.EnvPrefix + "DATABASE_URL"),
		ListenAddr:  os.Getenv(brand.EnvPrefix + "LISTEN_ADDR"),
		BlobDriver:  os.Getenv(brand.EnvPrefix + "BLOB_DRIVER"),
		BlobDir:     os.Getenv(brand.EnvPrefix + "BLOB_DIR"),
	}
	if c.BlobDir == "" {
		c.BlobDir = "./data/blobs"
	}
	c.GCUnclaimedDays = envDays("GC_UNCLAIMED_DAYS", 35)
	c.GCDeadRefDays = envDays("GC_DEAD_REF_DAYS", 30)
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("%sDATABASE_URL is required", brand.EnvPrefix)
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":8443"
	}
	return c, nil
}

// envDays reads a positive day count, falling back on absent or bad values.
func envDays(name string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(brand.EnvPrefix + name))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}
