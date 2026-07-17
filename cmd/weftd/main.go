// Command weftd is the server binary.
package main

import (
	"context"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abhinavjha0239/weft/internal/brand"
	"github.com/abhinavjha0239/weft/internal/config"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/importer"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
	"github.com/abhinavjha0239/weft/internal/platform/mail"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
	"github.com/abhinavjha0239/weft/internal/platform/webpush"
	"github.com/abhinavjha0239/weft/internal/transport/rest"
	"github.com/abhinavjha0239/weft/internal/webui"
	"github.com/abhinavjha0239/weft/migrations"
)

// version is stamped by the build (-ldflags "-X main.version=...").
var version = "0.0.1-dev"

func main() {
	cmd := "help"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "version":
		fmt.Printf("%s %s\n", brand.Slug, version)
	case "migrate":
		run(func(ctx context.Context, cfg config.Config) error {
			pool, err := db.Connect(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			return db.Migrate(ctx, pool, migrations.FS)
		})
	case "serve":
		run(serve)
	case "import-zulip":
		run(importZulip)
	case "gen-vapid-keys":
		genVAPIDKeys()
	default:
		fmt.Printf("%s v%s — %s\n", brand.Name, version, brand.Tagline)
		fmt.Println("usage: serve | migrate | import-zulip | gen-vapid-keys | version")
	}
}

// genVAPIDKeys prints a fresh Web Push VAPID key pair (P-21) as the two env
// assignments an operator drops into their config. No DB or config needed —
// it is a pure key generator, like an ssh-keygen for push.
func genVAPIDKeys() {
	pub, priv, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		slog.Error("gen-vapid-keys", "err", err)
		os.Exit(1)
	}
	fmt.Printf("%sVAPID_PUBLIC_KEY=%s\n", brand.EnvPrefix, pub)
	fmt.Printf("%sVAPID_PRIVATE_KEY=%s\n", brand.EnvPrefix, priv)
}

func run(fn func(context.Context, config.Config) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err == nil {
		err = fn(ctx, cfg)
	}
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// importZulip: weftd import-zulip -org <slug> -dir <unpacked-export> [-dry-run]
// openBlob picks the storage driver from config (P-07): s3 needs its own
// multi-arg constructor (bucket/region/endpoint/prefix), everything else is
// blob.Open's single data-directory dsn. Operators swap backends by env alone.
func openBlob(ctx context.Context, cfg config.Config) (blob.Store, error) {
	if cfg.BlobDriver == "s3" {
		return blob.NewS3(ctx, blob.S3Config{
			Bucket:   cfg.S3Bucket,
			Region:   cfg.S3Region,
			Endpoint: cfg.S3Endpoint,
			Prefix:   cfg.S3Prefix,
		})
	}
	return blob.Open(cfg.BlobDriver, cfg.BlobDir)
}

func importZulip(ctx context.Context, cfg config.Config) error {
	fs := flag.NewFlagSet("import-zulip", flag.ExitOnError)
	orgSlug := fs.String("org", "", "target org slug (must exist)")
	dir := fs.String("dir", "", "unpacked Zulip export directory")
	dryRun := fs.Bool("dry-run", false, "report fidelity without writing")
	_ = fs.Parse(os.Args[2:])
	if *orgSlug == "" || *dir == "" {
		return fmt.Errorf("import-zulip: -org and -dir are required")
	}
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		return err
	}
	store, err := openBlob(ctx, cfg)
	if err != nil {
		return err
	}
	var orgID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM org WHERE slug = $1`, *orgSlug).Scan(&orgID); err != nil {
		return fmt.Errorf("org %q not found: %w", *orgSlug, err)
	}
	rep, err := importer.New(pool, store).Run(ctx, orgID, *dir, *dryRun)
	if err != nil {
		return err
	}
	if !*dryRun {
		// The import ENQUEUED its closure rebuild (S2 — never in the import
		// tx). This process is already off any request path, so drain the
		// queue before returning: imported users resolve permissions the
		// moment the command exits.
		if _, err := perms.NewRebuildWorker(pool, perms.New(pool), slog.Default()).RunOnce(ctx); err != nil {
			return err
		}
	}
	out, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(out))
	return nil
}

func serve(ctx context.Context, cfg config.Config) error {
	log := slog.Default()
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		return err
	}

	store, err := openBlob(ctx, cfg)
	if err != nil {
		return err
	}
	// Metrics seam (S0): config-picked driver, Nop by default. Wired into the
	// four instrumented hot sites below so consumer lag and fan-out cost are
	// observable at /debug/vars when the operator picks the expvar driver.
	reg, err := metrics.Open(cfg.MetricsDriver)
	if err != nil {
		return err
	}
	hub := gateway.NewHub(pool, log)
	hub.SetMetrics(reg)
	go hub.Run(ctx)
	notifRunner := notification.NewRunner(pool, hub, log)
	notifRunner.SetMetrics(reg)
	go notifRunner.Run(ctx)
	sender, err := mail.Open(cfg.MailDriver, cfg.SMTPAddr, cfg.SMTPFrom,
		cfg.SMTPUser, cfg.SMTPPass, log)
	if err != nil {
		return err
	}
	emailWorker := notification.NewEmailWorker(pool, sender, log)
	emailWorker.SetUnsubscribe(cfg.BaseURL, cfg.SigningSecret)
	go emailWorker.Run(ctx)
	janitor := compliance.NewJanitor(pool, store, log)
	janitor.UnclaimedGrace = time.Duration(cfg.GCUnclaimedDays) * 24 * time.Hour
	janitor.DeadRefWindow = time.Duration(cfg.GCDeadRefDays) * 24 * time.Hour
	janitor.VacuumRestoreWindow = time.Duration(cfg.GCVacuumRestoreDays) * 24 * time.Hour
	go janitor.Run(ctx)

	notifSvc := notification.New(pool)
	notifSvc.SetFanout(hub)
	notifSvc.SetUnsubscribe(cfg.SigningSecret)
	// P-21: Web Push. Configured VAPID keys wake the medium — the subscription
	// API accepts registrations and the lane delivers through the SAME
	// SSRF-guarded egress client as link previews and webhooks (endpoints are
	// user-registered URLs). Any of the three set means push is intended, so a
	// malformed/half-set trio fails fast rather than silently disabling; all
	// unset → push stays a structural no-op (subscribe 409s). No test options
	// on the egress client (production never allows loopback).
	if cfg.VAPIDPublicKey != "" || cfg.VAPIDPrivateKey != "" || cfg.PushSubject != "" {
		pushSender, err := webpush.NewSender(cfg.VAPIDPublicKey, cfg.VAPIDPrivateKey, cfg.PushSubject)
		if err != nil {
			return err
		}
		notifSvc.SetPush(pushSender)
		pushWorker := notification.NewPushWorker(pool, pushSender,
			egress.New(egress.Options{UserAgent: brand.Name + "Bot/1.0 (+push)"}), log)
		go pushWorker.Run(ctx)
	}
	permsSvc := perms.New(pool)
	permsSvc.SetMetrics(reg)
	// S2: the async closure-rebuild lane — bulk imports enqueue, this worker
	// recomputes behind the version fence (readers stay on the old closure
	// until the atomic flip).
	go perms.NewRebuildWorker(pool, permsSvc, log).Run(ctx)
	identitySvc := identity.New(pool, permsSvc)
	identitySvc.SetMailer(sender) // P-35 password-reset mail via the mail seam
	// P-30: OIDC discovery, JWKS, and token exchange ride the SSRF-guarded
	// egress client — the ONLY path the IdP endpoints may be dialed. No test
	// options (production wiring). baseURL builds the absolute redirect_uri.
	identitySvc.SetOIDC(egress.New(egress.Options{
		UserAgent: brand.Name + "Bot/1.0 (+oidc)",
	}), cfg.BaseURL)
	msgSvc := messaging.New(pool, permsSvc)
	// F-17: level/follow settings changes patch the notification candidate
	// set in the same tx as the setting write (the SetFiles seam pattern).
	msgSvc.SetDeliverability(notification.NewDeliverability(pool, log))
	msgSvc.SetLogger(log) // S6 unread-counter reconcile Warn-logs here
	// S6: the O(1) unread counter rides the notification consumer's per-message
	// pass and its slow reconcile ticker (messaging owns the counter table).
	notifRunner.SetUnread(msgSvc)
	autoSvc := automation.New(pool, permsSvc)
	autoSvc.SetMessaging(msgSvc) // the slash-command channel-send gate
	autoRunner := automation.NewRunner(pool, msgSvc, permsSvc, notifSvc, log)
	// P-24: the delivery lane's outbound webhook calls ride the SSRF-guarded
	// egress client — the ONLY path an http_request step may dial. No test
	// options here (production wiring never allows loopback).
	autoRunner.SetEgress(egress.New(egress.Options{
		UserAgent: brand.Name + "Bot/1.0 (+webhook)",
	}))
	go autoRunner.Run(ctx)
	go msgSvc.RunScheduledLoop(ctx, log)
	filesSvc := files.New(pool, store)
	filesSvc.SetSigningSecret(cfg.SigningSecret)
	filesSvc.SetPerms(permsSvc) // P-19 storage-quota admin gate (no scanner wired — no real driver yet)
	filesSvc.SetLogger(log)     // P-18 best-effort thumbnail generation logs here
	msgSvc.SetFiles(filesSvc)
	complianceSvc := compliance.New(pool, permsSvc)
	complianceSvc.SetLogger(log)
	complianceSvc.SetFiles(filesSvc)
	go complianceSvc.RunExportLoop(ctx)
	// P-15: link previews fetch through the SSRF-guarded egress client —
	// the ONLY path attacker-chosen URLs may ride. No test options here.
	unfurlSvc := unfurl.New(pool, egress.New(egress.Options{
		UserAgent: brand.Name + "Bot/1.0 (+link-preview)",
	}))
	unfurlSvc.SetPerms(permsSvc)
	if u, err := url.Parse(cfg.BaseURL); err == nil {
		unfurlSvc.SetBaseHost(u.Host)
	}
	go unfurl.NewRunner(pool, unfurlSvc, log).Run(ctx)
	apiHandler := rest.Handler(ctx, rest.Deps{
		Pool: pool, Hub: hub, Log: log,
		Identity:      identitySvc,
		Messaging:     msgSvc,
		Worktrack:     worktrack.New(pool, permsSvc, msgSvc),
		DM:            dm.New(pool),
		Notifications: notifSvc,
		Files:         filesSvc,
		Compliance:    complianceSvc,
		Automations:   autoSvc,
		Unfurl:        unfurlSvc,
	})
	ui, err := webui.Handler()
	if err != nil {
		return err
	}
	root := http.NewServeMux()
	root.Handle("/api/", apiHandler)
	// The expvar driver publishes to /debug/vars; mount it only when that driver
	// is active so the default (noop) server exposes no ops endpoint.
	if cfg.MetricsDriver == "expvar" {
		root.Handle("/debug/vars", expvar.Handler())
	}
	root.Handle("/", ui)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           root,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Info("listening", "addr", cfg.ListenAddr, "version", version)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
