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
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: os.Getenv(brand.EnvPrefix + "DATABASE_URL"),
		ListenAddr:  os.Getenv(brand.EnvPrefix + "LISTEN_ADDR"),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("%sDATABASE_URL is required", brand.EnvPrefix)
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":8443"
	}
	return c, nil
}
