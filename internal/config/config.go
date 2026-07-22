// Package config reads server configuration from the environment. Every
// variable is prefixed with brand.EnvPrefix (docs/BRANDING.md).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/abhinavjha0239/weft/internal/brand"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	// Blob storage seam: driver + location. fs (default) is bare metal /
	// any mounted volume; s3/gcs/azure are drop-in Store implementations.
	BlobDriver string
	BlobDir    string
	// S3 driver (P-07, when BlobDriver=s3). Credentials come from the AWS SDK
	// default chain, never these keys. Endpoint/Prefix are optional (MinIO/R2).
	S3Bucket   string
	S3Region   string
	S3Endpoint string
	S3Prefix   string
	// GC windows (days). Unclaimed: never-referenced uploads (Zulip ships
	// 5 weeks — drafts hold uploads silently). DeadRef: files whose last
	// referencing message was deleted (Zulip's 30-day vacuum delay).
	GCUnclaimedDays int
	GCDeadRefDays   int
	// VacuumRestoreDays: how long a retention-vacuumed message stays
	// recoverable (soft-tombstoned) before the purge lane removes it (P-17).
	GCVacuumRestoreDays int
	// Outbound mail seam: "log" (default, never sends) or "smtp".
	MailDriver string
	SMTPAddr   string
	SMTPFrom   string
	SMTPUser   string
	SMTPPass   string
	// SigningSecret keys signed download links (P-07) and unsubscribe MACs
	// (P-20). Empty = link minting refuses with a clear config error and the
	// unsubscribe endpoints 404; both features are opt-in per operator.
	SigningSecret string
	// BaseURL is the public origin used to build absolute links in email
	// (the one-click unsubscribe URL). Trailing slash trimmed.
	BaseURL string
	// Web Push (P-21): the VAPID key pair (base64url raw P-256 keys) and the
	// contact subject (mailto:/https:). All three unset → the push lane is a
	// structural no-op and the subscribe API 409s (the mail log-driver spirit:
	// dev/CI never need keys). `weftd gen-vapid-keys` prints a fresh pair.
	VAPIDPublicKey  string
	VAPIDPrivateKey string
	PushSubject     string
	// Metrics seam (S0): "" / "noop" (default, zero cost) or "expvar" (stdlib,
	// served at /debug/vars). Picking expvar opts the operator into exposing
	// that ops endpoint — the default stays off.
	MetricsDriver string
	// Presence plane seam (S5): "pg" (default) shares presence across a cell's
	// gateway nodes via the UNLOGGED presence table + LISTEN/NOTIFY, so any
	// node sees org-wide presence; "local" keeps it in-process (single-node —
	// no cross-node view, no presence DB writes). The multi-node target wants
	// pg; a single-node operator may opt down to local.
	PresenceDriver string
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
	c.S3Bucket = os.Getenv(brand.EnvPrefix + "S3_BUCKET")
	c.S3Region = os.Getenv(brand.EnvPrefix + "S3_REGION")
	c.S3Endpoint = os.Getenv(brand.EnvPrefix + "S3_ENDPOINT")
	c.S3Prefix = os.Getenv(brand.EnvPrefix + "S3_PREFIX")
	c.GCUnclaimedDays = envDays("GC_UNCLAIMED_DAYS", 35)
	c.GCDeadRefDays = envDays("GC_DEAD_REF_DAYS", 30)
	c.GCVacuumRestoreDays = envDays("GC_VACUUM_RESTORE_DAYS", 30)
	c.SigningSecret = os.Getenv(brand.EnvPrefix + "SIGNING_SECRET")
	c.BaseURL = strings.TrimRight(os.Getenv(brand.EnvPrefix+"BASE_URL"), "/")
	if c.BaseURL == "" {
		c.BaseURL = "http://localhost:8080"
	}
	c.MailDriver = os.Getenv(brand.EnvPrefix + "MAIL_DRIVER")
	c.SMTPAddr = os.Getenv(brand.EnvPrefix + "SMTP_ADDR")
	c.SMTPFrom = os.Getenv(brand.EnvPrefix + "SMTP_FROM")
	c.SMTPUser = os.Getenv(brand.EnvPrefix + "SMTP_USER")
	c.SMTPPass = os.Getenv(brand.EnvPrefix + "SMTP_PASS")
	c.VAPIDPublicKey = os.Getenv(brand.EnvPrefix + "VAPID_PUBLIC_KEY")
	c.VAPIDPrivateKey = os.Getenv(brand.EnvPrefix + "VAPID_PRIVATE_KEY")
	c.PushSubject = os.Getenv(brand.EnvPrefix + "PUSH_SUBJECT")
	c.MetricsDriver = os.Getenv(brand.EnvPrefix + "METRICS_DRIVER")
	c.PresenceDriver = os.Getenv(brand.EnvPrefix + "PRESENCE_DRIVER")
	if c.PresenceDriver == "" {
		c.PresenceDriver = "pg"
	}
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
