// Package db owns the connection pool and migration runner.
package db

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
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
	// pgx defaults MaxConns to max(4, numCPU) — far too low for a server
	// fronting many concurrent tenants (load testing showed pool-wait
	// dominating latency at the default). Set a sane floor; operators raise it
	// per cell via ?pool_max_conns=N in the URL. One Postgres tops out in the
	// low hundreds of active connections — beyond that a cell adds PgBouncer
	// or splits (scale contract).
	if cfg.MaxConns < 25 {
		cfg.MaxConns = 25
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	return pool, nil
}

// WithTx owns the transaction lifecycle (ARCHITECTURE.md §2): services run
// their write path inside fn; commit happens iff fn returns nil. Transactions
// must stay short (scale contract — the event-log xmin gate).
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: commit: %w", err)
	}
	return nil
}

// Migrate applies 0*.sql from fsys in filename order, tracking applied files
// in schema_migrations. Files are append-only once released. Callers pass the
// embedded migrations.FS, so the runner never depends on the working
// directory; an empty fsys is an error, never a silent no-op.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("db: migrations table: %w", err)
	}
	files, err := fs.Glob(fsys, "0*.sql")
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("db: no migration files in the provided filesystem")
	}
	sort.Strings(files)
	for _, name := range files {
		var done bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = $1)`,
			name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := fs.ReadFile(fsys, name)
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
