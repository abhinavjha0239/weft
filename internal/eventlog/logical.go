package eventlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// The logical-decoding feed (S4, ADR-003 F-1).
//
// WHY IT EXISTS. The default driver gates every scan on
// `txid < pg_snapshot_xmin(pg_current_snapshot())`, which is DATABASE-GLOBAL:
// one long write transaction anywhere holds the horizon down and delivery
// stalls for every org and every consumer. That is the latency story. The
// correctness story is worse, and is the real reason this driver exists:
//
//	txid is stamped at a transaction's FIRST write; the event id is stamped at
//	APPEND. So transaction A can hold the LOWER txid and the HIGHER event id.
//	If A commits while B (higher txid, lower event id) is still in flight, the
//	gate — which only asks "is this row's txid below the oldest running txid?"
//	— admits A's higher id. The cursor advances past B's lower id. When B
//	finally commits, its event is BELOW the cursor and is never delivered, and
//	nothing notices, because the cursor sequence already passed it.
//
// WAL order is COMMIT order, so a feed built on logical decoding cannot have
// that crossing: B's commit record simply arrives after A's, and the position
// that advances is an LSN, not an id. No gate is needed, because uncommitted
// rows never appear in the stream at all.
//
// WHAT IS DECODED. The reader decodes the WAL for the COMMIT ORDER OF EVENT
// IDS (the id and org_id columns of each event_log INSERT, plus the commit
// LSN) and materialises the row bodies through the normal pgx path, ungated —
// every id the WAL produced is committed by construction. That is deliberate:
// the only values hand-parsed out of pgoutput's text tuples are two BIGINTs,
// while jsonb, timestamptz and the nullable columns keep going through pgx's
// type system instead of a hand-rolled text decoder whose DateStyle/TimeZone
// assumptions would be a silent corruption surface. Decoding whole tuples in
// the WAL to skip the body read is a recorded optimisation, not a correctness
// requirement.
//
// OPERATIONS. The slot and publication are a deploy-time OPERATOR step
// (docs/ARCHITECTURE.md §6 runbook), never a migration: a slot is per-cell
// physical state that RETAINS WAL, so a dead consumer can fill the disk. That
// risk is instrumented (eventlog_slot_lag_bytes) and has a documented
// drop-and-resync policy. A slot also allows exactly ONE active reader, so the
// feed is single-reader per cell: a second node blocks until the holder dies
// and its consumers report ErrFeedNotReady rather than silently stalling.

const (
	// DefaultSlot / DefaultPublication are the per-cell logical-decoding
	// objects. Neutral names (no brand token — docs/BRANDING.md) because an
	// operator types them into psql and a rebrand must never orphan a slot.
	DefaultSlot        = "eventlog_feed"
	DefaultPublication = "eventlog_pub"

	// standbyInterval bounds how long the reader may go without telling the
	// server where it is. It must stay well under wal_sender_timeout (60s by
	// default) or the walsender drops the connection.
	standbyInterval = 10 * time.Second

	// maxIndexEntries bounds the per-org commit-order window held in memory.
	// Beyond it the oldest entries are evicted and a consumer whose cursor
	// predates the eviction gets a LOUD ErrCursorTooOld instead of a silent
	// skip — F-2 tolerates loss only when it is detectable. The alternative
	// (stop reading the WAL as backpressure) was rejected: it would let one
	// wedged consumer stall delivery for every OTHER org, which is the exact
	// global-stall shape this driver exists to remove.
	maxIndexEntries = 100_000
)

var (
	// ErrFeedNotReady means the WAL reader is not streaming yet (starting up,
	// or another node holds the single per-cell slot). Poll again.
	ErrFeedNotReady = errors.New("eventlog: logical feed not ready")
	// ErrCursorTooOld means this consumer fell further behind than the
	// in-memory commit-order window, so its position can no longer be served.
	// The runbook's remedy is drop-and-resync (reset the cursor).
	ErrCursorTooOld = errors.New("eventlog: consumer fell behind the logical feed window")
)

// LogicalOptions configures the logical driver. The zero value is the
// documented default (DefaultSlot/DefaultPublication, replication connection
// derived from the pool).
type LogicalOptions struct {
	Slot        string
	Publication string
	// ConnString overrides the replication connection string. Empty derives it
	// from the pool, which is what production wants: same host, same
	// credentials, same TLS.
	ConnString string
}

// LogicalSource is the logical-decoding driver. One per process: it owns the
// replication connection and the per-org commit-order index every Feed it
// mints reads from.
type LogicalSource struct {
	pool        *pgxpool.Pool
	log         *slog.Logger
	slot        string
	publication string
	// connString is set ONLY when the operator overrode it; otherwise the
	// replication connection is cloned from the pool's own pgconn.Config.
	connString string

	mu    sync.Mutex
	orgs  map[int64]*orgIndex
	head  pglogrepl.LSN // the newest commit LSN the reader has processed
	ready bool
	// wakes are the push subscribers (AddWake): the reader calls them with an
	// org id the moment that org's commit is DECODED, i.e. the first instant
	// its events can be read back. See Tail.OnWake for why a NOTIFY wake is
	// not enough under this driver.
	wakes []func(orgID int64)

	slotLag metrics.Gauge
	evicted metrics.Counter
	decoded metrics.Counter
}

// entry is one committed event in commit order: the id, and the LSN of the
// COMMIT that made it visible. Entries with equal LSN come from one
// transaction and are held in append order.
type entry struct {
	lsn pglogrepl.LSN
	id  int64
}

// splicepoint is a pending bootstrap hand-off: once the caller acks lastID
// (the end of the history walk), head becomes the consumer's LSN and it joins
// the live lane.
type splicepoint struct {
	head   pglogrepl.LSN
	lastID int64
}

// pendingInsert is one decoded event_log INSERT, held until its COMMIT.
type pendingInsert struct{ org, id int64 }

type orgIndex struct {
	entries []entry
	// evictedTo is the highest LSN dropped by the memory bound. A cursor at or
	// below it can no longer be served from this index.
	evictedTo pglogrepl.LSN
}

// NewLogicalSource builds the driver. It does NOT create the slot or the
// publication (operator step) and does not connect until Run.
func NewLogicalSource(pool *pgxpool.Pool, log *slog.Logger, opts LogicalOptions) (*LogicalSource, error) {
	if pool == nil {
		return nil, fmt.Errorf("eventlog: logical driver needs a database pool")
	}
	if log == nil {
		log = slog.Default()
	}
	s := &LogicalSource{
		pool:        pool,
		log:         log,
		slot:        orDefault(opts.Slot, DefaultSlot),
		publication: orDefault(opts.Publication, DefaultPublication),
		connString:  opts.ConnString,
		orgs:        map[int64]*orgIndex{},
	}
	s.SetMetrics(metrics.Nop())
	return s, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// SetMetrics wires the S0 registry. eventlog_slot_lag_bytes is the one an
// operator must alarm on: it is the WAL this cell's slot is RETAINING because
// some consumer has not acked, i.e. the disk-fill risk, in bytes.
func (s *LogicalSource) SetMetrics(reg metrics.Registry) {
	s.slotLag = reg.Gauge("eventlog_slot_lag_bytes", "slot")
	s.evicted = reg.Counter("eventlog_feed_evicted_total", "org")
	s.decoded = reg.Counter("eventlog_feed_events_total")
}

// Consumer mints a named feed backed by this reader.
func (s *LogicalSource) Consumer(name string, batchSize int) Feed {
	if batchSize <= 0 {
		batchSize = 500
	}
	c := &logicalConsumer{src: s, name: name, batch: batchSize,
		pending: map[int64][]entry{}, splice: map[int64]splicepoint{}}
	c.SetMetrics(metrics.Nop())
	return c
}

// Run streams the WAL until ctx ends, restarting on error. A failure to hold
// the slot (another node has it) is a normal condition, not a crash.
func (s *LogicalSource) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := s.stream(ctx); err != nil && ctx.Err() == nil {
			s.setReady(false)
			s.log.Warn("eventlog: logical feed restarting", "slot", s.slot, "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	s.setReady(false)
}

// Head is the newest commit LSN the reader has processed. It is the splice
// point a bootstrapping consumer stamps (see logicalConsumer.Poll).
func (s *LogicalSource) Head() (pglogrepl.LSN, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, s.ready
}

// WaitReady blocks until the reader is streaming or ctx ends. Wiring calls it
// so a consumer's first Poll does not race the reader's startup.
func (s *LogicalSource) WaitReady(ctx context.Context) error {
	for {
		if _, ok := s.Head(); ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// WaitFor blocks until the reader has decoded the given event id for the org.
// It is the synchronisation point for tests and for an operator checking that
// the feed is live; production consumers never need it (they are woken by
// NOTIFY and poll).
func (s *LogicalSource) WaitFor(ctx context.Context, orgID, id int64) error {
	for {
		s.mu.Lock()
		idx := s.orgs[orgID]
		found := false
		if idx != nil {
			for i := len(idx.entries) - 1; i >= 0; i-- {
				if idx.entries[i].id == id {
					found = true
					break
				}
			}
		}
		s.mu.Unlock()
		if found {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("eventlog: waiting for event %d in org %d: %w", id, orgID, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// AddWake registers a push subscriber. Every registered callback is invoked
// (subscribers ACCUMULATE — a second caller never silently displaces the
// first), on the reader's own goroutine, so a callback must not block: the
// gateway's is a map lookup plus a non-blocking channel send.
func (s *LogicalSource) AddWake(fn func(orgID int64)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	s.wakes = append(s.wakes, fn)
	s.mu.Unlock()
}

func (s *LogicalSource) setReady(v bool) {
	s.mu.Lock()
	s.ready = v
	s.mu.Unlock()
}

// stream holds the replication connection for one streaming session.
func (s *LogicalSource) stream(ctx context.Context) error {
	cfg, err := s.replicationConfig()
	if err != nil {
		return err
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("eventlog: replication connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	start, err := s.slotPosition(ctx)
	if err != nil {
		return err
	}
	// proto_version 1: the server streams only COMMITTED transactions, in
	// commit order. Version 2's in-progress streaming is exactly what this feed
	// must NOT have — an uncommitted row must never reach a consumer.
	err = pglogrepl.StartReplication(ctx, conn, s.slot, start,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + s.publication + "'",
		}})
	if err != nil {
		return fmt.Errorf("eventlog: start replication on slot %q: %w", s.slot, err)
	}
	s.mu.Lock()
	if start > s.head {
		s.head = start
	}
	s.ready = true
	s.mu.Unlock()
	s.log.Info("eventlog: logical feed streaming",
		"slot", s.slot, "publication", s.publication, "from", start.String())

	return s.consume(ctx, conn)
}

// replicationConfig builds the walsender connection. It CLONES the pool's own
// pgconn.Config rather than re-parsing a connection string: a pool URL legally
// carries pgxpool settings (pool_max_conns and friends) that pgconn does not
// know, and re-parsing would forward them to the server as unknown runtime
// parameters and fail the connection. Only an explicit operator override is
// parsed from text.
func (s *LogicalSource) replicationConfig() (*pgconn.Config, error) {
	var cfg *pgconn.Config
	if s.connString != "" {
		parsed, err := pgconn.ParseConfig(s.connString)
		if err != nil {
			return nil, fmt.Errorf("eventlog: parse replication conn string: %w", err)
		}
		cfg = parsed
	} else {
		clone := s.pool.Config().ConnConfig.Config // value copy
		cfg = &clone
	}
	// RuntimeParams is shared with the pool's config when cloned — never mutate
	// it in place.
	params := make(map[string]string, len(cfg.RuntimeParams)+1)
	for k, v := range cfg.RuntimeParams {
		// The write-path session defaults (db.Connect) have no meaning on a
		// walsender connection, which runs no user transactions.
		if k == "idle_in_transaction_session_timeout" {
			continue
		}
		params[k] = v
	}
	// `replication=database` is what turns this into a walsender session.
	params["replication"] = "database"
	cfg.RuntimeParams = params
	// A cloned pool config carries pgxpool's own hooks; a raw pgconn dial must
	// not run them.
	cfg.OnNotice = nil
	cfg.OnNotification = nil
	return cfg, nil
}

// slotPosition reads where the slot will resume, and fails loudly (with the
// runbook's SQL) when the slot does not exist — never silently falling back to
// the xmin poller, which would hide a misconfigured cell.
func (s *LogicalSource) slotPosition(ctx context.Context) (pglogrepl.LSN, error) {
	var confirmed *string
	err := s.pool.QueryRow(ctx, `
		SELECT confirmed_flush_lsn::text
		FROM pg_replication_slots
		WHERE slot_name = $1 AND plugin = 'pgoutput'`, s.slot).Scan(&confirmed)
	if err != nil {
		return 0, fmt.Errorf("eventlog: replication slot %q not found — create it with "+
			"SELECT pg_create_logical_replication_slot('%s','pgoutput') and "+
			"CREATE PUBLICATION %s FOR TABLE event_log (docs/ARCHITECTURE.md runbook): %w",
			s.slot, s.slot, s.publication, err)
	}
	if confirmed == nil {
		return 0, nil
	}
	return pglogrepl.ParseLSN(*confirmed)
}

// consume is the receive loop: dispatch keepalives vs WAL data, decode, and
// keep the server told where we are.
func (s *LogicalSource) consume(ctx context.Context, conn *pgconn.PgConn) error {
	// relations maps a pgoutput relation id to the column index of id/org_id.
	// pgoutput re-sends the Relation message whenever the schema changes, so
	// the mapping is never assumed, always learned.
	type relCols struct{ idCol, orgCol int }
	relations := map[uint32]relCols{}
	// Inserts buffered until their COMMIT arrives. Until then they do not
	// exist for any consumer — that is the guarantee this driver is for.
	var txn []pendingInsert
	nextStandby := time.Now().Add(standbyInterval)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if time.Now().After(nextStandby) {
			if err := s.sendStandby(ctx, conn); err != nil {
				return err
			}
			nextStandby = time.Now().Add(standbyInterval)
		}
		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		raw, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline reached: loop round and send a standby update
			}
			return fmt.Errorf("eventlog: receive: %w", err)
		}
		cd, ok := raw.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue // non-CopyData (or empty) on a streaming connection
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("eventlog: parse keepalive: %w", err)
			}
			if ka.ReplyRequested {
				if err := s.sendStandby(ctx, conn); err != nil {
					return err
				}
				nextStandby = time.Now().Add(standbyInterval)
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("eventlog: parse xlogdata: %w", err)
			}
			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("eventlog: parse logical message: %w", err)
			}
			switch m := msg.(type) {
			case *pglogrepl.RelationMessage:
				if !isEventLogRelation(m.RelationName) {
					continue
				}
				cols := relCols{idCol: -1, orgCol: -1}
				for i, c := range m.Columns {
					switch c.Name {
					case "id":
						cols.idCol = i
					case "org_id":
						cols.orgCol = i
					}
				}
				if cols.idCol < 0 || cols.orgCol < 0 {
					return fmt.Errorf("eventlog: relation %q has no id/org_id column", m.RelationName)
				}
				relations[m.RelationID] = cols
			case *pglogrepl.BeginMessage:
				txn = txn[:0]
			case *pglogrepl.InsertMessage:
				cols, known := relations[m.RelationID]
				if !known || m.Tuple == nil {
					continue // some other published table, or a tuple-less insert
				}
				id, org, err := tupleIDs(m.Tuple, cols.idCol, cols.orgCol)
				if err != nil {
					return err
				}
				txn = append(txn, pendingInsert{org: org, id: id})
			case *pglogrepl.CommitMessage:
				s.applyCommit(m.CommitLSN, txn)
				txn = txn[:0]
			}
		}
	}
}

// applyCommit publishes one transaction's events, in append order, into their
// orgs' commit-order indexes stamped with the COMMIT LSN. Until this runs the
// events do not exist for any consumer — which is the whole guarantee.
func (s *LogicalSource) applyCommit(commitLSN pglogrepl.LSN, txn []pendingInsert) {
	s.mu.Lock()
	if commitLSN > s.head {
		s.head = commitLSN
	}
	evictions := map[int64]int{}
	// touched is built only when someone is subscribed, so the pull-only case
	// (no gateway on this driver) allocates nothing extra per commit.
	var touched []int64
	pushing := len(s.wakes) > 0
	for _, ins := range txn {
		orgID := ins.org
		idx := s.orgs[orgID]
		if idx == nil {
			idx = &orgIndex{}
			s.orgs[orgID] = idx
		}
		idx.entries = append(idx.entries, entry{lsn: commitLSN, id: ins.id})
		if over := len(idx.entries) - maxIndexEntries; over > 0 {
			idx.evictedTo = idx.entries[over-1].lsn
			idx.entries = append(idx.entries[:0], idx.entries[over:]...)
			evictions[orgID] += over
		}
		if pushing && !containsID(touched, orgID) {
			touched = append(touched, orgID)
		}
	}
	n := len(txn)
	wakes := s.wakes
	s.mu.Unlock()

	s.decoded.Add(float64(n))
	for orgID, count := range evictions {
		s.evicted.Add(float64(count), strconv.FormatInt(orgID, 10))
		s.log.Warn("eventlog: logical feed window overflowed — a consumer is far behind",
			"org", orgID, "evicted", count, "window", maxIndexEntries)
	}
	// The push wake fires AFTER the index is published, so a subscriber that
	// reads synchronously always finds the events it was woken for. Outside
	// the lock, because a callback must never be able to deadlock the reader.
	for _, orgID := range touched {
		for _, fn := range wakes {
			fn(orgID)
		}
	}
}

func containsID(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// after returns up to `limit` entries committed strictly after `cursor`, cut
// on a COMMIT BOUNDARY: a transaction's events are never split across batches,
// so acking a batch's last id always covers a whole commit (limit is therefore
// a floor-then-finish-the-commit bound, not a hard cap).
func (s *LogicalSource) after(orgID int64, cursor pglogrepl.LSN, limit int) ([]entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, ErrFeedNotReady
	}
	idx := s.orgs[orgID]
	if idx == nil {
		return nil, nil
	}
	if cursor < idx.evictedTo {
		return nil, fmt.Errorf("%w: org %d cursor %s is older than the retained window (%s)",
			ErrCursorTooOld, orgID, cursor, idx.evictedTo)
	}
	first := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].lsn > cursor
	})
	end := first
	for end < len(idx.entries) {
		if end-first >= limit && idx.entries[end].lsn != idx.entries[end-1].lsn {
			break
		}
		end++
	}
	out := make([]entry, end-first)
	copy(out, idx.entries[first:end])
	return out, nil
}

// backlog counts the org's entries committed after cursor — the true
// undelivered count, with no horizon to hide behind.
func (s *LogicalSource) backlog(orgID int64, cursor pglogrepl.LSN) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return 0, ErrFeedNotReady
	}
	idx := s.orgs[orgID]
	if idx == nil {
		return 0, nil
	}
	first := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].lsn > cursor
	})
	return int64(len(idx.entries) - first), nil
}

// sendStandby tells the server how far it may recycle WAL. The reported
// position is the MINIMUM durable cursor across every consumer, never the
// reader's head: the slot must retain what the SLOWEST consumer still needs,
// or a restart would lose events that were decoded but not processed. It is
// also why a wedged consumer shows up as slot lag (and disk) — the metric and
// the runbook exist for exactly that.
func (s *LogicalSource) sendStandby(ctx context.Context, conn *pgconn.PgConn) error {
	safe, err := s.safeFlushLSN(ctx)
	if err != nil {
		return err
	}
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: safe}); err != nil {
		return fmt.Errorf("eventlog: standby status: %w", err)
	}
	s.publishSlotLag(ctx)
	return nil
}

func (s *LogicalSource) safeFlushLSN(ctx context.Context) (pglogrepl.LSN, error) {
	head, _ := s.Head()
	// The one query in this driver that is NOT org-pinned, and deliberately:
	// the slot is per-CELL physical state (one WAL, one retention floor), so
	// the floor must cover every org on the cell. This is not cross-org logical
	// coordination — no org's DELIVERY depends on another's (that is the whole
	// point of the slice), only the cell's WAL recycling does, exactly as the
	// cell model allows (docs/SCHEMA.md).
	var minLSN *string
	err := s.pool.QueryRow(ctx, `
		SELECT min(lsn)::text FROM event_consumer_cursor WHERE lsn IS NOT NULL`).Scan(&minLSN)
	if err != nil {
		return 0, fmt.Errorf("eventlog: safe flush lsn: %w", err)
	}
	if minLSN == nil {
		// No consumer has a durable LSN yet: nothing needs retained WAL (a
		// consumer with no LSN bootstraps from the table), so the head is safe.
		return head, nil
	}
	lsn, err := pglogrepl.ParseLSN(*minLSN)
	if err != nil {
		return 0, fmt.Errorf("eventlog: parse cursor lsn %q: %w", *minLSN, err)
	}
	if lsn > head {
		return head, nil
	}
	return lsn, nil
}

// publishSlotLag samples the WAL this slot is retaining. Best-effort: a
// sampling failure must never break the feed.
func (s *LogicalSource) publishSlotLag(ctx context.Context) {
	var lag *int64
	err := s.pool.QueryRow(ctx, `
		SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, s.slot).Scan(&lag)
	if err != nil {
		s.log.Debug("eventlog: slot lag sample failed", "slot", s.slot, "err", err)
		return
	}
	if lag != nil {
		s.slotLag.Set(float64(*lag), s.slot)
	}
}

// isEventLogRelation accepts the partitioned parent AND its leaf partitions,
// so the feed works whether the operator created the publication with
// publish_via_partition_root or without it.
func isEventLogRelation(name string) bool {
	return name == "event_log" || strings.HasPrefix(name, "event_log_")
}

// tupleIDs pulls the two BIGINT columns the feed needs out of a pgoutput text
// tuple. Everything else in the row is read back through pgx.
func tupleIDs(t *pglogrepl.TupleData, idCol, orgCol int) (id, org int64, err error) {
	if idCol >= len(t.Columns) || orgCol >= len(t.Columns) {
		return 0, 0, fmt.Errorf("eventlog: tuple has %d columns, need id@%d org@%d",
			len(t.Columns), idCol, orgCol)
	}
	id, err = tupleInt(t.Columns[idCol], "id")
	if err != nil {
		return 0, 0, err
	}
	org, err = tupleInt(t.Columns[orgCol], "org_id")
	if err != nil {
		return 0, 0, err
	}
	return id, org, nil
}

func tupleInt(c *pglogrepl.TupleDataColumn, name string) (int64, error) {
	if c.DataType != pglogrepl.TupleDataTypeText {
		return 0, fmt.Errorf("eventlog: %s arrived as tuple type %q, want text", name, c.DataType)
	}
	v, err := strconv.ParseInt(string(c.Data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("eventlog: %s %q: %w", name, c.Data, err)
	}
	return v, nil
}

// logicalConsumer is the Feed backed by the WAL reader. It is a sibling of
// Consumer, not a replacement: same Poll/Ack/Lag surface, LSN-based position.
type logicalConsumer struct {
	src   *LogicalSource
	name  string
	batch int

	mu sync.Mutex
	// pending remembers, per org, the (lsn,id) entries handed to the caller but
	// not yet acked, so Ack can translate an event id back into the LSN to
	// persist. Cleared as the cursor advances past them.
	pending map[int64][]entry
	// splice holds the bootstrap hand-off, per org, until the caller acks the
	// final history batch. Stamping the LSN at Poll time instead would lose
	// those rows on a crash before Ack: the cursor would come back LIVE at a
	// head above events the consumer never processed.
	splice map[int64]splicepoint

	events metrics.Counter
	lag    metrics.Gauge
}

// OnWake rides the WAL reader, exactly as logicalTail.OnWake does: the
// callback fires when a commit has been DECODED, which is the first instant
// Poll can read its events back. Every registered consumer gets its own
// subscription (AddWake accumulates), so wiring a second, third or fourth
// consumer cannot silently displace the first.
func (c *logicalConsumer) OnWake(fn func(orgID int64)) { c.src.AddWake(fn) }

func (c *logicalConsumer) SetMetrics(reg metrics.Registry) {
	c.events = reg.Counter("fanout_events_total", "consumer")
	c.lag = reg.Gauge("consumer_lag", "consumer", "org")
}

// Poll returns the org's next events in COMMIT order.
//
// Two lanes, because a consumer that has never run under this driver must
// still replay history (ADR-003 E2 — a fresh consumer name starts from 0), and
// history predates the slot:
//
//   - BOOTSTRAP (cursor lsn IS NULL): read the table in id order after
//     last_id, UNGATED. The splice is safe because the head LSN is sampled
//     BEFORE the read: anything committed at or below that head is visible to
//     the read, and anything an in-flight transaction commits afterwards has a
//     commit LSN above it and therefore arrives on the live lane. When the
//     table read runs dry the consumer stamps that head as its LSN and is live
//     from then on. Overlap at the splice re-delivers, never skips —
//     at-least-once is the contract.
//   - LIVE: serve the in-memory commit-order index after the cursor LSN.
func (c *logicalConsumer) Poll(ctx context.Context, orgID int64) ([]Row, error) {
	cursor, lastID, live, err := c.loadCursor(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if !live {
		return c.bootstrap(ctx, orgID, lastID)
	}
	ents, err := c.src.after(orgID, cursor, c.batch)
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(ents))
	for i, e := range ents {
		ids[i] = e.id
	}
	byID, err := c.src.readRows(ctx, orgID, ids)
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0, len(ents))
	kept := make([]entry, 0, len(ents))
	for _, e := range ents {
		r, ok := byID[e.id]
		if !ok {
			// The row is gone (a retention partition drop between decode and
			// read). Skip it, but keep its entry so the cursor can still
			// advance past the commit.
			kept = append(kept, e)
			continue
		}
		out = append(out, r)
		kept = append(kept, e)
	}
	c.mu.Lock()
	c.pending[orgID] = kept
	c.mu.Unlock()
	if len(out) == 0 {
		// Every row in the window vanished: advance past them so the consumer
		// does not spin on a dead window.
		if err := c.Ack(ctx, orgID, kept[len(kept)-1].id); err != nil {
			return nil, err
		}
		return nil, nil
	}
	c.events.Add(float64(len(out)), c.name)
	return out, nil
}

// bootstrap is the history lane described on Poll.
func (c *logicalConsumer) bootstrap(ctx context.Context, orgID, lastID int64) ([]Row, error) {
	head, ready := c.src.Head()
	if !ready {
		return nil, ErrFeedNotReady
	}
	out, err := c.src.historyRows(ctx, orgID, lastID, c.batch)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.pending[orgID] = nil
	delete(c.splice, orgID)
	c.mu.Unlock()
	if len(out) == c.batch {
		// More history to walk: stay in the bootstrap lane, cursor by id only.
		c.events.Add(float64(len(out)), c.name)
		return out, nil
	}
	// History exhausted: hand off to the live lane at the head sampled BEFORE
	// the read. Nothing handed out means nothing can be lost, so stamp now —
	// otherwise the consumer would never leave the bootstrap lane. With rows in
	// hand, the stamp waits for their Ack.
	if len(out) == 0 {
		return nil, c.writeCursor(ctx, orgID, lastID, &head)
	}
	c.mu.Lock()
	c.splice[orgID] = splicepoint{head: head, lastID: out[len(out)-1].ID}
	c.mu.Unlock()
	c.events.Add(float64(len(out)), c.name)
	return out, nil
}

func (c *logicalConsumer) pool() *pgxpool.Pool { return c.src.pool }

// readRows materialises decoded ids. It lives on the SOURCE, not the consumer,
// because both readers of the commit-order index need it (the durable
// logicalConsumer and the ephemeral logicalTail) and one body read is the only
// way they cannot drift.
//
// No xmin gate, deliberately: every id here came out of the WAL, so its
// transaction has committed. That absence IS the fix.
func (s *LogicalSource) readRows(ctx context.Context, orgID int64, ids []int64) (map[int64]Row, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.org_id, e.workspace_id, e.actor_kind, e.actor_id,
		       e.entity_type, e.entity_id, e.verb, e.payload, e.hint,
		       e.occurred_at, e.recorded_at, e.origin
		FROM event_log e
		WHERE e.org_id = $1 AND e.id = ANY($2::bigint[])`, orgID, ids)
	if err != nil {
		return nil, fmt.Errorf("eventlog: logical read: %w", err)
	}
	list, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]Row, len(list))
	for _, r := range list {
		byID[r.ID] = r
	}
	return byID, nil
}

// historyRows is the UNGATED id-ordered walk of an org's log — the read behind
// both history lanes: the durable consumer's bootstrap (replay from a NULL
// LSN) and the gateway's resume replay. One query, so "what history means"
// cannot differ between them.
func (s *LogicalSource) historyRows(ctx context.Context, orgID, afterID int64, limit int) ([]Row, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.org_id, e.workspace_id, e.actor_kind, e.actor_id,
		       e.entity_type, e.entity_id, e.verb, e.payload, e.hint,
		       e.occurred_at, e.recorded_at, e.origin
		FROM event_log e
		WHERE e.org_id = $1 AND e.id > $2
		ORDER BY e.id
		LIMIT $3`, orgID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("eventlog: logical history: %w", err)
	}
	return scanRows(rows)
}

// Ack advances the durable cursor. The event id is what the caller has (the
// Feed contract), so Ack translates it back to the commit LSN of the batch it
// came from. Batches end on commit boundaries, so an ack at a batch's last id
// covers that whole commit; an ack in the MIDDLE of a commit advances only to
// the previous commit, never past events the caller has not seen.
func (c *logicalConsumer) Ack(ctx context.Context, orgID, lastID int64) error {
	c.mu.Lock()
	pend := c.pending[orgID]
	sp, splicing := c.splice[orgID]
	c.mu.Unlock()

	var lsn *pglogrepl.LSN
	pos := -1
	for i := len(pend) - 1; i >= 0; i-- {
		if pend[i].id == lastID {
			pos = i
			break
		}
	}
	if pos >= 0 {
		at := pend[pos].lsn
		if pos+1 < len(pend) && pend[pos+1].lsn == at {
			// Mid-commit ack: only whole commits below `at` are safe.
			for i := pos; i >= 0; i-- {
				if pend[i].lsn != at {
					v := pend[i].lsn
					lsn = &v
					break
				}
			}
		} else {
			v := at
			lsn = &v
		}
	}
	// Bootstrap hand-off: the history walk is only spliced onto the live lane
	// once its FINAL batch is acked, and only by an ack that covers all of it.
	if lsn == nil && splicing && lastID >= sp.lastID {
		v := sp.head
		lsn = &v
		c.mu.Lock()
		delete(c.splice, orgID)
		c.mu.Unlock()
	}
	if err := c.writeCursor(ctx, orgID, lastID, lsn); err != nil {
		return err
	}
	if lsn != nil {
		c.mu.Lock()
		rest := pend[:0]
		for _, e := range pend {
			if e.lsn > *lsn {
				rest = append(rest, e)
			}
		}
		c.pending[orgID] = rest
		c.mu.Unlock()
	}
	return nil
}

// writeCursor persists (last_id, lsn). Both advance monotonically: a stale ack
// never rewinds either, which is the same guarantee the xmin driver gives.
// A nil lsn leaves the stored LSN untouched (the bootstrap lane's id-only
// progress, and the safe fallback when an ack cannot be located).
func (c *logicalConsumer) writeCursor(ctx context.Context, orgID, lastID int64, lsn *pglogrepl.LSN) error {
	var text *string
	if lsn != nil {
		s := lsn.String()
		text = &s
	}
	_, err := c.pool().Exec(ctx, `
		INSERT INTO event_consumer_cursor (consumer, org_id, last_id, lsn, updated_at)
		VALUES ($1, $2, $3, $4::pg_lsn, now())
		ON CONFLICT (consumer, org_id) DO UPDATE
		SET last_id = GREATEST(event_consumer_cursor.last_id, EXCLUDED.last_id),
		    lsn = GREATEST(COALESCE(event_consumer_cursor.lsn, '0/0'::pg_lsn),
		                   COALESCE(EXCLUDED.lsn, event_consumer_cursor.lsn, '0/0'::pg_lsn)),
		    updated_at = now()`,
		c.name, orgID, lastID, text)
	if err != nil {
		return fmt.Errorf("eventlog: logical ack: %w", err)
	}
	return nil
}

// loadCursor returns the durable position. `live` is false while the consumer
// has no LSN yet — the bootstrap lane.
func (c *logicalConsumer) loadCursor(ctx context.Context, orgID int64) (
	cursor pglogrepl.LSN, lastID int64, live bool, err error) {
	var text *string
	err = c.pool().QueryRow(ctx, `
		SELECT last_id, lsn::text FROM event_consumer_cursor
		WHERE consumer = $1 AND org_id = $2`, c.name, orgID).Scan(&lastID, &text)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("eventlog: logical cursor: %w", err)
	}
	if text == nil || *text == "" || *text == "0/0" {
		return 0, lastID, false, nil
	}
	cursor, err = pglogrepl.ParseLSN(*text)
	if err != nil {
		return 0, 0, false, fmt.Errorf("eventlog: parse cursor lsn %q: %w", *text, err)
	}
	return cursor, lastID, true, nil
}

// Lag reports the org's TRUE undelivered backlog and publishes the
// consumer_lag{consumer,org} gauge.
//
// Note the difference from the xmin driver, which is not cosmetic: that Lag
// counts only events BELOW the global xmin horizon, so during exactly the
// stall it is supposed to expose — a long transaction elsewhere holding the
// horizon down — it reads 0 while events pile up undelivered. Here the number
// is the count of decoded, committed, unacked events, so a stall cannot hide
// in it.
func (c *logicalConsumer) Lag(ctx context.Context, orgID int64) (int64, error) {
	if _, ready := c.src.Head(); !ready {
		// A number sourced from a dead reader would look healthy. Say so instead.
		return 0, ErrFeedNotReady
	}
	cursor, lastID, live, err := c.loadCursor(ctx, orgID)
	if err != nil {
		return 0, err
	}
	var lag int64
	if live {
		lag, err = c.src.backlog(orgID, cursor)
		if err != nil {
			return 0, err
		}
	} else {
		// Bootstrap lane: the backlog is whatever history is still unread.
		err = c.pool().QueryRow(ctx, `
			SELECT count(*) FROM event_log WHERE org_id = $1 AND id > $2`,
			orgID, lastID).Scan(&lag)
		if err != nil {
			return 0, fmt.Errorf("eventlog: logical lag: %w", err)
		}
	}
	c.lag.Set(float64(lag), c.name, strconv.FormatInt(orgID, 10))
	return lag, nil
}

// scanRows is the shared Row scanner for both lanes (and the same column order
// the xmin Consumer uses — one shape, no drift).
func scanRows(rows pgx.Rows) ([]Row, error) {
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		var actorKind, entityType int16
		if err := rows.Scan(&r.ID, &r.OrgID, &r.WorkspaceID, &actorKind,
			&r.ActorID, &entityType, &r.EntityID, &r.Verb, &r.Payload,
			&r.Hint, &r.OccurredAt, &r.RecordedAt, &r.Origin); err != nil {
			return nil, fmt.Errorf("eventlog: scan: %w", err)
		}
		r.ActorKind = enum.ActorKind(actorKind)
		r.EntityType = enum.EntityType(entityType)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ProvisionLogical creates the replication slot and publication the logical
// driver needs, idempotently. It is EXACTLY what the runbook's SQL does,
// exposed so an operator tool and the tests can call it — `weftd serve` never
// does. A slot retains WAL, so creating one is a deliberate operator decision
// with a disk cost, not a side effect of starting a process (which is also why
// it is not a migration: migrations run on every install, including the ones
// that never enable this driver).
func ProvisionLogical(ctx context.Context, pool *pgxpool.Pool, opts LogicalOptions) error {
	slot := orDefault(opts.Slot, DefaultSlot)
	pub := orDefault(opts.Publication, DefaultPublication)
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, pub).Scan(&exists); err != nil {
		return fmt.Errorf("eventlog: publication lookup: %w", err)
	}
	if !exists {
		// publish_via_partition_root keeps the stream on the parent relation as
		// event_log's id-range partitions come and go (ADR-003 E6); the reader
		// accepts the leaf names too, so a publication made without it works.
		if _, err := pool.Exec(ctx, `CREATE PUBLICATION `+pgx.Identifier{pub}.Sanitize()+
			` FOR TABLE event_log WITH (publish_via_partition_root = true)`); err != nil {
			return fmt.Errorf("eventlog: create publication %q: %w", pub, err)
		}
	}
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot).Scan(&exists); err != nil {
		return fmt.Errorf("eventlog: slot lookup: %w", err)
	}
	if !exists {
		if _, err := pool.Exec(ctx,
			`SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, slot); err != nil {
			return fmt.Errorf("eventlog: create replication slot %q "+
				"(needs wal_level=logical): %w", slot, err)
		}
	}
	return nil
}

// DropLogical removes the slot and publication. It is the second half of the
// runbook's drop-and-resync remedy for a stuck slot: dropping the slot frees
// the retained WAL immediately, and the consumers resync by resetting their
// cursors (which sends them back through the bootstrap lane).
func DropLogical(ctx context.Context, pool *pgxpool.Pool, opts LogicalOptions) error {
	slot := orDefault(opts.Slot, DefaultSlot)
	pub := orDefault(opts.Publication, DefaultPublication)
	if _, err := pool.Exec(ctx, `
		SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
		WHERE slot_name = $1 AND active = false`, slot); err != nil {
		return fmt.Errorf("eventlog: drop replication slot %q: %w", slot, err)
	}
	if _, err := pool.Exec(ctx,
		`DROP PUBLICATION IF EXISTS `+pgx.Identifier{pub}.Sanitize()); err != nil {
		return fmt.Errorf("eventlog: drop publication %q: %w", pub, err)
	}
	return nil
}
