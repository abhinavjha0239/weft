package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestClientPassedIsOrderingAware pins the skip that decides, per row, between
// a silent DROP and a duplicate — the P-45 change, at the predicate.
//
// It is a pure-logic unit test on purpose: the end-to-end proof that a
// late-committing lower id reaches a live socket lives in
// TestGatewayOnLogicalFeed against a real wal_level=logical Postgres, but the
// BOUNDS of the window (what happens when it overflows, that the cursor stays
// monotone, that the ordered path allocates nothing) cannot be forced through
// a real feed at reasonable cost, and an unbounded claim about them would be
// unbacked.
//
// RED: make passed() return `id <= c.lastID` regardless of `ordered` — the
// pre-P-45 rule — and the "late-committing lower id was skipped" assert fires.
func TestClientPassedIsOrderingAware(t *testing.T) {
	// ID-MONOTONE feed (the default xmin driver): the cursor is the exact,
	// unbounded answer, and no window is allocated at all — 100k live
	// connections must not pay for a dedupe structure they cannot need.
	ordered := &client{}
	ordered.pass(10, true)
	if ordered.recent != nil {
		t.Fatal("the id-monotone path allocated a dedupe window; the default driver must pay nothing")
	}
	if ordered.lastID != 10 {
		t.Fatalf("cursor = %d, want 10", ordered.lastID)
	}
	if !ordered.passed(10, true) {
		t.Fatal("the id at the cursor was treated as new: it would be delivered twice")
	}
	if !ordered.passed(4, true) {
		t.Fatal("an id below the cursor was treated as new on an id-monotone feed")
	}
	if ordered.passed(11, true) {
		t.Fatal("an id above the cursor was treated as already delivered: a DROP")
	}

	// COMMIT-ORDERED feed (logical): a lower id can arrive after a higher one,
	// so the cursor cannot answer. What was never sent is NEW.
	crossing := &client{}
	crossing.pass(10, false)
	if crossing.passed(4, false) {
		t.Fatal("a late-committing lower id (4) was skipped below the cursor (10): " +
			"that is exactly the undetectable loss this slice removes")
	}
	if !crossing.passed(10, false) {
		t.Fatal("a re-fanned id was treated as new: the hand-off would duplicate every overlapping row")
	}
	if crossing.passed(11, false) {
		t.Fatal("an id above the cursor was treated as already delivered: a DROP")
	}

	// The resume cursor stays MONOTONE even though delivery order is not: it
	// is the checkpoint seq and the resume floor, and a client's last_id must
	// never go backwards.
	crossing.pass(4, false)
	if crossing.lastID != 10 {
		t.Fatalf("cursor fell back to %d after delivering a lower id; a resume cursor must be monotone",
			crossing.lastID)
	}
	if !crossing.passed(4, false) {
		t.Fatal("an id just delivered was not remembered: the hand-off would duplicate it")
	}

	// The window is BOUNDED, and the bound's failure mode is the documented
	// one: past recentWindow ids the oldest answers "not handled", i.e.
	// DELIVER — a duplicate seq, never a drop.
	bounded := &client{}
	const base = int64(1000)
	bounded.pass(base, false)
	for i := 1; i <= recentWindow; i++ {
		bounded.pass(base+int64(i)*2, false)
	}
	if bounded.passed(base, false) {
		t.Fatal("an id evicted from the window was reported as delivered: eviction must " +
			"degrade to a duplicate, never to a silent skip")
	}
	if !bounded.passed(base+2, false) {
		t.Fatalf("the window does not retain %d entries", recentWindow)
	}
}

// TestMulticastFeedErrors pins how the per-org reader answers the two errors
// only a streaming driver can raise. Both are about NOT wedging silently:
// not-ready is a normal startup condition, and a position that fell out of the
// driver's bounded commit-order window must send the org's clients back
// through the resume lane instead of failing the same read forever.
//
// RED: delete the ErrCursorTooOld branch in multicast — the connection is
// never cancelled and the assert below fires.
func TestMulticastFeedErrors(t *testing.T) {
	notReady := newStubHub(t, &stubTail{err: eventlog.ErrFeedNotReady})
	more, err := notReady.hub.multicast(context.Background(), notReady.shard)
	if err != nil || more {
		t.Fatalf("a not-yet-streaming feed produced err=%v more=%v, want a quiet no-op", err, more)
	}
	select {
	case <-notReady.connCtx.Done():
		t.Fatal("a feed that has not started streaming dropped live connections")
	default:
	}

	tooOld := newStubHub(t, &stubTail{err: eventlog.ErrCursorTooOld})
	more, err = tooOld.hub.multicast(context.Background(), tooOld.shard)
	if err != nil || more {
		t.Fatalf("ErrCursorTooOld produced err=%v more=%v, want it handled in place", err, more)
	}
	select {
	case <-tooOld.connCtx.Done():
	default:
		t.Fatal("a shard whose position fell out of the feed's window kept its connections: " +
			"its live lane then fails the same read forever, undetectably")
	}
}

// TestFannedRowCarriesACLColumns pins that the columns the READ ACL gates on
// survive the trip from the feed driver onto the row the gateway fans.
//
// It is here because both ways of getting this wrong are SILENT. entity_type
// classifies a container-less event as space-scoped; left at zero it is not,
// so scoped events fall back to the org-wide fan — a leak. boundaryAt is
// compared against channel_member.history_from; left at zero it is before every
// floor, so a protected-history member silently stops receiving. Neither is a
// compile error and neither shows up in a delivery-count assert, so the
// structure is one construction site (fanRows) and this is the pin on it.
//
// boundaryAt is the EARLIER of occurred_at and recorded_at, and this pins both
// directions of that rule — the import shape (recorded later, so the backdated
// domain time must win) and the clock-skew shape (recorded earlier, so an app
// clock running ahead must not open the floor).
//
// RED: drop either field from fanRows, or take OccurredAt alone instead of the
// earlier of the two — the matching assert names it.
func TestFannedRowCarriesACLColumns(t *testing.T) {
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// recorded_at LATER than occurred_at is the IMPORT shape (E3 backdates the
	// domain time while ingest stamps the row today), so LEAST must pick
	// occurred_at — judging recorded_at alone would stream a member the
	// pre-join history a backfill just wrote.
	row := eventlog.Row{
		ID: 41, Verb: "workitem.updated", Payload: json.RawMessage(`{"a":1}`),
		OccurredAt: when, RecordedAt: when.Add(time.Hour),
		EntityType: enum.EntityWorkItem,
	}

	// The helper itself.
	got := fanRows([]eventlog.Row{row})
	if len(got) != 1 {
		t.Fatalf("fanRows produced %d rows, want 1", len(got))
	}
	if !got[0].boundaryAt.Equal(when) {
		t.Fatalf("boundaryAt = %v, want %v: a zero domain time is BEFORE every "+
			"protected-history floor, so members silently stop receiving", got[0].boundaryAt, when)
	}
	// The other direction of LEAST: an app clock running AHEAD of the DB would
	// put an event that raced a join on the delivered side of a boundary REST
	// hides, so the EARLIER of the two must win here too. Taking OccurredAt
	// alone reinstates that leak (gateway_acl_test.go break 17).
	earlier := when.Add(-time.Hour)
	skewed := fanRows([]eventlog.Row{{ID: 42, OccurredAt: when, RecordedAt: earlier}})
	if !skewed[0].boundaryAt.Equal(earlier) {
		t.Fatalf("boundaryAt = %v, want the EARLIER %v: the floor must be judged on "+
			"LEAST(occurred_at, recorded_at), not on the app clock alone",
			skewed[0].boundaryAt, earlier)
	}
	if got[0].entityType != enum.EntityWorkItem {
		t.Fatalf("entityType = %v, want %v: a zero entity type is not space-scoped, "+
			"so scoped events silently fall back to the org-wide fan",
			got[0].entityType, enum.EntityWorkItem)
	}

	// And the LIVE lane, end to end through multicast: the batch that actually
	// reaches a connection's feed, not just the helper in isolation.
	sh := newStubHub(t, &stubTail{rows: []eventlog.Row{row}})
	if _, err := sh.hub.multicast(context.Background(), sh.shard); err != nil {
		t.Fatalf("multicast: %v", err)
	}
	var fanned []eventRow
	select {
	case fanned = <-sh.conn.feed:
	default:
		t.Fatal("the multicast reader fanned nothing")
	}
	if len(fanned) != 1 || !fanned[0].boundaryAt.Equal(when) ||
		fanned[0].entityType != enum.EntityWorkItem {
		t.Fatalf("the live lane fanned %+v; the ACL columns did not survive the fan", fanned)
	}
	if fanned[0].enc == nil {
		t.Fatal("the live lane lost its marshal-once bytes")
	}
}

type stubHub struct {
	hub     *Hub
	shard   *orgShard
	conn    *client
	connCtx context.Context
}

// newStubHub builds a hub reading from the given stub feed. The stub stands in
// for the DRIVER, never for the behaviour under proof: commit-order delivery is
// proved end to end against a real logical-decoding Postgres
// (TestGatewayOnLogicalFeed), and the paths below have no reachable real-feed
// trigger at test cost (the driver's window is 100k entries).
func newStubHub(t *testing.T, tail *stubTail) *stubHub {
	t.Helper()
	h := NewHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.SetSource(&stubSource{tail: tail})
	if h.ordered {
		t.Fatal("SetSource did not adopt the driver's ordering guarantee")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &client{cancel: cancel, live: true, feed: make(chan []eventRow, feedBuffer)}
	sh := &orgShard{
		orgID: 7, conns: map[*client]struct{}{c: {}},
		userConns: map[int64]*userPresence{}, wake: make(chan struct{}, 1),
	}
	c.shard = sh
	return &stubHub{hub: h, shard: sh, conn: c, connCtx: ctx}
}

type stubSource struct{ tail eventlog.Tail }

func (s *stubSource) Consumer(string, int) eventlog.Feed { return nil }
func (s *stubSource) Tail() eventlog.Tail                { return s.tail }
func (s *stubSource) Run(context.Context)                {}
func (s *stubSource) SetMetrics(metrics.Registry)        {}

type stubTail struct {
	err  error
	rows []eventlog.Row
}

func (t *stubTail) Ordered() bool      { return false }
func (t *stubTail) OnWake(func(int64)) {}

func (t *stubTail) Head(context.Context, int64) (eventlog.Position, int64, error) {
	return eventlog.Position{}, 0, t.err
}

func (t *stubTail) Next(_ context.Context, _ int64, pos eventlog.Position, _ int) ([]eventlog.Row, eventlog.Position, error) {
	if t.err != nil {
		return nil, pos, t.err
	}
	return t.rows, pos, nil
}

func (t *stubTail) History(context.Context, int64, int64, int) ([]eventlog.Row, error) {
	return t.rows, t.err
}
