// Package db owns the connection pool and migration runner.
package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect returns a pool with the write-side session defaults from the scale
// contract (docs/SCHEMA.md): write transactions must stay short because the
// event-log consumer gate is bounded by the oldest in-flight transaction.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db: parse url: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000" // 10s
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	return pool, nil
}

// Migrate applies migrations/*.sql in filename order, tracking applied files
// in schema_migrations. Files are append-only once released.
func Migrate(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("db: migrations table: %w", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "0*.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		name := filepath.Base(f)
		var done bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = $1)`,
			name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("db: apply %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations (filename) VALUES ($1)`, name); err != nil {
			return err
		}
	}
	return nil
}
