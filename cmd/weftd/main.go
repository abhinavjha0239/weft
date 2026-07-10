// Command weftd is the server binary.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
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
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
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
	default:
		fmt.Printf("%s v%s — %s\n", brand.Name, version, brand.Tagline)
		fmt.Println("usage: serve | migrate | import-zulip | version")
	}
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
	store, err := blob.Open(cfg.BlobDriver, cfg.BlobDir)
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

	store, err := blob.Open(cfg.BlobDriver, cfg.BlobDir)
	if err != nil {
		return err
	}
	hub := gateway.NewHub(pool, log)
	go hub.Run(ctx)
	go notification.NewRunner(pool, hub, log).Run(ctx)
	janitor := compliance.NewJanitor(pool, store, log)
	janitor.UnclaimedGrace = time.Duration(cfg.GCUnclaimedDays) * 24 * time.Hour
	janitor.DeadRefWindow = time.Duration(cfg.GCDeadRefDays) * 24 * time.Hour
	go janitor.Run(ctx)

	permsSvc := perms.New(pool)
	msgSvc := messaging.New(pool, permsSvc)
	go automation.NewRunner(pool, msgSvc, log).Run(ctx)
	filesSvc := files.New(pool, store)
	msgSvc.SetFiles(filesSvc)
	apiHandler := rest.Handler(ctx, rest.Deps{
		Pool: pool, Hub: hub, Log: log,
		Identity:      identity.New(pool, permsSvc),
		Messaging:     msgSvc,
		Worktrack:     worktrack.New(pool, permsSvc, msgSvc),
		DM:            dm.New(pool),
		Notifications: notification.New(pool),
		Files:         filesSvc,
		Compliance:    compliance.New(pool, permsSvc),
		Automations:   automation.New(pool, permsSvc),
	})
	ui, err := webui.Handler()
	if err != nil {
		return err
	}
	root := http.NewServeMux()
	root.Handle("/api/", apiHandler)
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
