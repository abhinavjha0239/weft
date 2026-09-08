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

// TestSpaceViewFailsClosed pins the fail-closed defaults of the SHARED space
// view, which is where hoisting the set to the org shard could most easily have
// reopened the hole #132 closed.
//
// It is a pure-logic unit test because the states it pins are ones a real
// connection passes through for microseconds, or reaches only when the database
// says no: registered-before-its-org's-view-loaded, and load-failed. Both must
// WITHHOLD every space-scoped event rather than fall through to the org-wide
// branch. Nothing here is mocked — spaceView and client.filter are the real
// types, and the integration proof that a guest holds exactly this state lives
// in TestGatewaySharedSpaceSet against real Postgres and real sockets.
//
// RED: make spaceView.visible answer true on a nil receiver; make
// spaceView.itemSecurity answer false on one; make it answer false always —
// each has its own assert below, and the nil-itemSecurity one is asserted on
// the method rather than on a delivery outcome precisely because the space gate
// already withholds there, so a delivery assert could not go red.
func TestSpaceViewFailsClosed(t *testing.T) {
	const space, other = int64(7), int64(8)
	item := eventRow{verb: "workitem.created", entityType: enum.EntityWorkItem,
		payload: json.RawMessage(`{"space_id":7,"item_id":1}`)}
	otherItem := eventRow{verb: "workitem.created", entityType: enum.EntityWorkItem,
		payload: json.RawMessage(`{"space_id":8,"item_id":2}`)}
	sprint := eventRow{verb: "sprint.created", entityType: enum.EntitySprint,
		payload: json.RawMessage(`{"space_id":7,"sprint_id":1}`)}
	orgWide := eventRow{verb: "emoji.created", entityType: enum.EntityEmoji,
		payload: json.RawMessage(`{"name":"party"}`)}

	// NO VIEW. One state, three ways in: a connection registered before its
	// org's view loaded, a connection whose load failed, and every GUEST —
	// which is the whole reason the guest restriction could move out of the SQL
	// without adding a role branch to filter.
	blind := &client{}
	if blind.filter(item).deliver || blind.filter(sprint).deliver {
		t.Fatal("a connection with no space view delivered a space-scoped event: " +
			"not-yet-loaded must WITHHOLD, never fall through to the org-wide fan")
	}
	if !blind.filter(orgWide).deliver {
		t.Fatal("a connection with no space view stopped delivering a genuinely " +
			"org-wide event; the withholding must be no wider than the space gate")
	}

	// LOADED, no item security: the positive anchor. Without it every negative
	// above would be satisfied by a filter that simply delivered nothing.
	open := &client{spaces: &spaceView{spaces: map[int64]bool{space: true}}}
	if !open.filter(item).deliver || !open.filter(sprint).deliver {
		t.Fatal("a loaded view withheld events for a Space it contains")
	}
	if open.filter(otherItem).deliver {
		t.Fatalf("an event for Space %d was delivered against a view that does not contain it", other)
	}

	// LOADED with item security: work items are withheld wholesale (there is no
	// evaluator for visibility_scope.rule) while space events that cannot carry
	// a scope keep flowing — the blackout stays as narrow as the hook.
	secured := &client{spaces: &spaceView{
		spaces: map[int64]bool{space: true}, itemSecurityActive: true}}
	if secured.filter(item).deliver {
		t.Fatal("a work-item event was delivered while the org defines a visibility_scope")
	}
	if !secured.filter(sprint).deliver {
		t.Fatal("item security blacked out a NON-work-item space event")
	}

	// The refresh signal is the announced Space ID, not a bare flag: that is
	// what lets the org load its view ONCE per new Space instead of once per
	// connection, and what keeps a resume lane replaying old space.created
	// events from loading at all.
	created := eventRow{verb: "space.created", entityType: enum.EntitySpace,
		payload: json.RawMessage(`{"space_id":7}`)}
	if got := blind.filter(created).newSpace; got != space {
		t.Fatalf("space.created reported newSpace=%d, want %d: the org would reload "+
			"its view on every announcement, or never", got, space)
	}
	if blind.filter(created).deliver {
		t.Fatal("space.created was delivered to a connection whose view does not yet " +
			"contain it; the envelope is decided against the PRE-refresh view")
	}

	// visible doubles as the single-flight test the space.created refresh uses
	// (Hub.refreshSpaceView): a view that already holds the announced Space is
	// accepted as-is — which is what makes ONE space.created cost ONE query for
	// the whole org — and one that does not must force the reload.
	var none *spaceView
	if none.visible(space) {
		t.Fatal("a nil view accepted a Space: the org would never issue the load and " +
			"every connection would withhold forever")
	}
	// itemSecurity on a nil view is DEFENCE IN DEPTH, not a path filter can
	// reach today — visible() withholds first — so it is asserted DIRECTLY
	// rather than through a delivery outcome that would pass whatever this
	// answered. It is the structural replacement for client's old
	// `itemSecurityActive: true` seed, and what stops a future reordering of
	// the two gates from turning "no view" into "deliver every work item".
	if !none.itemSecurity() {
		t.Fatal("a nil space view reported item security INACTIVE; a view that cannot " +
			"answer must withhold work-item events, not wave them through")
	}
	loaded := &spaceView{spaces: map[int64]bool{space: true}}
	if !loaded.visible(space) {
		t.Fatal("a loaded view rejected a Space it holds: the reload would run per connection")
	}
	if loaded.visible(other) {
		t.Fatalf("a view without Space %d accepted it: a new Space would never be picked up", other)
	}
}

// TestSpaceViewLivesOnTheOrgShard pins WHERE the shared space view is kept.
// One org's set must never be readable through another org's shard, and the
// only structural guarantee of that is that the view is shard state — a hub
// field, a package-level memo or a map keyed by anything but the org would all
// compile and would all hand org A's Spaces to org B.
//
// It exists because the end-to-end two-org subtest in TestGatewaySharedSpaceSet
// CANNOT reproduce that shape deterministically: the connect-time freshness
// rule makes every registering connection load through its OWN shard, and Space
// ids are globally unique so the space.created refresh always reloads too. So
// the behavioural claim (org two's item security must not reach org one) is
// pinned there, and the placement claim is pinned here.
//
// No pool is needed and none is used: the predicate accepts whatever the shard
// already holds, so orgSpaceView answers from state and never queries. The
// nil pool is itself part of the pin — a version that reached the database here
// would panic rather than pass.
//
// RED: cache the view on the Hub (or anywhere else shared) instead of the
// orgShard — the first assert fires with "(<nil>, <nil>)", i.e. the view is no
// longer where the org's own shard can find it.
func TestSpaceViewLivesOnTheOrgShard(t *testing.T) {
	h := NewHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a := &orgShard{orgID: 1}
	b := &orgShard{orgID: 2}
	viewA := &spaceView{spaces: map[int64]bool{11: true}}
	a.spaces.Store(viewA)
	anything := func(*spaceView) bool { return true }

	got, err := h.orgSpaceView(context.Background(), a, anything)
	if err != nil || got != viewA {
		t.Fatalf("a view stored on an org's own shard read back as (%v, %v); the shared "+
			"view must live on the shard, not anywhere hub-wide", got, err)
	}
	got, err = h.orgSpaceView(context.Background(), b, anything)
	if err != nil || got != nil {
		t.Fatalf("a shard holding no space view answered with %v (%v): one org's Space "+
			"set must never be reachable through another org's shard", got, err)
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
