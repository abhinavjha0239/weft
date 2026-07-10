// Package config reads server configuration from the environment. Every
// variable is prefixed with brand.EnvPrefix (docs/BRANDING.md).
package config

import (
	"fmt"
	"os"

	"github.com/abhinavjha0239/weft/internal/brand"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	// Blob storage seam: driver + location. fs (default) is bare metal /
	// any mounted volume; s3/gcs/azure are drop-in Store implementations.
	BlobDriver string
	BlobDir    string
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
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("%sDATABASE_URL is required", brand.EnvPrefix)
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":8443"
	}
	return c, nil
}
