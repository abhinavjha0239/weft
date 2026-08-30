package eventlog

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// Feed is the consumer contract — the shape EVERY consumer codes against, and
// the reason a delivery-mechanism swap is not a call-site change. It is exactly
// the surface *Consumer has always had; the interface only names it so a second
// driver can implement it (S4).
//
// Contract, identical for every driver:
//   - Poll returns the next batch of COMMITTED events for one org, in the order
//     the consumer must process them, and never returns an event whose
//     transaction has not committed.
//   - An empty batch means "nothing to do right now", never "you are done
//     forever" — callers poll again (or wait for the NOTIFY wake).
//   - Ack durably advances the cursor. Delivery is AT-LEAST-ONCE: a crash
//     between processing and Ack replays, so consumers must be idempotent
//     (dedupe keys / ON CONFLICT claims — the outbox rule already requires it).
//   - Lag reports the org's undelivered backlog — committed and not yet acked —
//     and publishes it as the consumer_lag{consumer,org} gauge. It is measured
//     WITHOUT the driver's own delivery gate, on purpose: a mechanism that has
//     stopped delivering must SHOW as lag, never hide behind its own horizon.
//     The unit is a driver detail (the xmin driver returns an id delta against
//     last_id, the logical driver an exact entry count against lsn); "0 means
//     caught up, and it rises when this consumer falls behind" is the contract.
type Feed interface {
	Poll(ctx context.Context, orgID int64) ([]Row, error)
	Ack(ctx context.Context, orgID, lastID int64) error
	Lag(ctx context.Context, orgID int64) (int64, error)
	SetMetrics(reg metrics.Registry)
}

// Source mints named Feeds — the driver seam (the platform/blob.Store,
// mail.Sender, presence.Plane and metrics.Registry pattern): the consumers
// speak only Feed, and WHICH mechanism carries events to them is an operator
// choice, not a code change.
//
// Two drivers ship:
//
//   - xmin (default): the polling consumer gated on
//     pg_snapshot_xmin(pg_current_snapshot()). No replication slot, no
//     wal_level=logical, nothing for a small install or CI to configure — but
//     the gate is DATABASE-GLOBAL (see Consumer's scale contract).
//   - logical: a logical-decoding reader over a replication slot. WAL order IS
//     commit order, so there is no gate to stall behind and no crossing hazard.
//     Costs a slot (retains WAL) and wal_level=logical.
//
// An unknown driver is a config error, never a silent fallback.
type Source interface {
	// Consumer returns the named durable feed. Names are stable identifiers
	// ("notifications", "automations", "unfurl") — renaming one starts its
	// cursor over.
	Consumer(name string, batchSize int) Feed
	// Tail returns the driver's CURSOR-FREE reader, for a consumer that holds
	// no durable state and must not share a cursor with other nodes — the
	// gateway (P-45). See Tail.
	Tail() Tail
	// Run drives whatever background work the driver needs and blocks until ctx
	// ends. The xmin driver has none and returns immediately; the logical driver
	// runs its WAL reader here.
	Run(ctx context.Context)
	// SetMetrics wires an observability registry (S0). Optional — default Nop.
	SetMetrics(reg metrics.Registry)
}

// Open selects the feed driver by name. `pool` is the ordinary connection pool;
// the logical driver derives its own replication connection from it.
func Open(driver string, pool *pgxpool.Pool, log *slog.Logger, opts LogicalOptions) (Source, error) {
	switch driver {
	case "", "xmin":
		return &xminSource{pool: pool}, nil
	case "logical":
		if pool == nil {
			return nil, fmt.Errorf("eventlog: logical driver needs a database pool")
		}
		if log == nil {
			log = slog.Default()
		}
		return NewLogicalSource(pool, log, opts)
	default:
		return nil, fmt.Errorf("eventlog: unknown feed driver %q (implement Source and register it here)", driver)
	}
}

// xminSource is the default driver: every Consumer it mints is the existing
// xmin-gated poller, byte-for-byte the pre-S4 behaviour.
type xminSource struct {
	pool *pgxpool.Pool
	reg  metrics.Registry
}

func (s *xminSource) Consumer(name string, batchSize int) Feed {
	c := NewConsumer(s.pool, name, batchSize)
	if s.reg != nil {
		c.SetMetrics(s.reg)
	}
	return c
}

// Run is a no-op: the xmin driver is pull-only, driven by its callers' loops.
func (s *xminSource) Run(context.Context) {}

func (s *xminSource) SetMetrics(reg metrics.Registry) { s.reg = reg }
