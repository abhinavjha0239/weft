// Command weftd is the server binary.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/abhinavjha0239/weft/internal/brand"
	"github.com/abhinavjha0239/weft/internal/config"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/server"
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
			return db.Migrate(ctx, pool, "migrations")
		})
	case "serve":
		run(serve)
	default:
		fmt.Printf("%s v%s — %s\n", brand.Name, version, brand.Tagline)
		fmt.Println("usage: serve | migrate | version")
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

func serve(ctx context.Context, cfg config.Config) error {
	log := slog.Default()
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool, "migrations"); err != nil {
		return err
	}

	hub := gateway.NewHub(pool, log)
	go hub.Run(ctx)

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: server.New(pool, hub).Handler(),
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
