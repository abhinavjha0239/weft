package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// TestCatchupSharesOneReadPerCohort is the constancy pin: a reconnect cohort's
// catch-up reads must not scale with the number of connections in it.
//
// It measures at TWO cohort sizes and compares, because a smallness assert
// ("fewer than five reads") passes on a small fixture whatever the code does.
// What must hold is that quadrupling the cohort does not multiply the reads.
//
// The count is taken at the DRIVER SEAM (eventlog.Tail.History) because that is
// exactly the quantity this slice reduces — one round trip to event_log per
// call — and the stub answers precisely as both real drivers do (`id > afterID
// ORDER BY id LIMIT n`), so the block's from/end/full bookkeeping is exercised
// against the real contract. Everything above the seam is the real thing: the
// real pump, the real catchUp, the real deliverShared, the real filter, and a
// real WebSocket per connection. The end-to-end proof over real Postgres, with
// real reconnects, is TestGatewayReconnectStorm.
//
// RED: restore the per-connection read (delete the shared-block lookups from
// catchUp, or any one of blockServes' three conditions so nothing is ever
// served from a block) — the read count tracks the cohort size and the
// comparison below names both numbers.
func TestCatchupSharesOneReadPerCohort(t *testing.T) {
	// One org, 400 events, and a cohort resuming from cursors the F-2
	// checkpoint has converged — the MEASURED shape of a node restart (see
	// catchup.go): a 20-event gap opened while the node was down, and the
	// cohort's cursors sit in three neighbouring visibility classes at the far
	// side of it. Each connection is checked to have received its OWN suffix as
	// well, so the read count cannot fall by under-delivering.
	const head, gap, classes = 400, 20, 3
	reads := func(cohort int) int {
		f := newCohortFixture(t, head)
		arrived := time.Now()
		for i := 0; i < cohort; i++ {
			from := int64(head - gap - i%classes)
			c := f.client(t, int64(i+1), from, arrived)
			f.pump(t, c)
			want := make([]int64, 0, head-int(from))
			for id := from + 1; id <= head; id++ {
				want = append(want, id)
			}
			f.expect(t, c, want)
		}
		return f.tail.calls()
	}
	const small, large = 4, 16
	smallReads, largeReads := reads(small), reads(large)
	if largeReads > 2*smallReads {
		t.Fatalf("catch-up reads scaled with the cohort: %d connections cost %d reads, "+
			"%d connections cost %d — a reconnect storm must cost O(cohort reads), "+
			"not O(connections)", small, smallReads, large, largeReads)
	}
	if largeReads >= large {
		t.Fatalf("a %d-connection cohort issued %d catch-up reads; want substantially fewer than %d",
			large, largeReads, large)
	}
	if smallReads < 1 {
		t.Fatal("the cohort issued NO catch-up read; the resume gap would never be replayed")
	}
	t.Logf("cohort catch-up reads: %d connections → %d, %d connections → %d",
		small, smallReads, large, largeReads)
}

// TestCatchupFarBehindResumerReadsAlone pins both halves of "sharing a read must
// not turn into sharing a cursor", on the outlier the premise predicts: a
// connection that was offline across checkpoints.
//
// Its cursor is BELOW the cohort's shared block, so the block does not contain
// the events between them — serving it from that block is the one way sharing
// could become LOSS. And its own lower, wider read must not drag the cohort
// backwards either: a block that does not reach past a connection's cursor has
// nothing for it.
//
// RED: delete `b.from <= c.lastID` from blockServes — the far-behind connection
// is served the cohort's block and the assert below names the events it never
// received.
func TestCatchupFarBehindResumerReadsAlone(t *testing.T) {
	const head = 300
	f := newCohortFixture(t, head)
	arrived := time.Now()

	cohort := f.client(t, 1, head-2, arrived)
	f.pump(t, cohort)
	if got := f.tail.calls(); got != 1 {
		t.Fatalf("the first resumer issued %d reads, want 1", got)
	}
	f.expect(t, cohort, []int64{head - 1, head})

	stale := f.client(t, 2, head-250, arrived)
	f.pump(t, stale)
	if f.tail.calls() == 1 {
		t.Fatal("a resumer 250 events behind was served from the cohort's block, which " +
			"starts above its cursor: every event between them is LOST")
	}
	want := make([]int64, 0, 250)
	for id := int64(head - 249); id <= head; id++ {
		want = append(want, id)
	}
	f.expect(t, stale, want)

	// The cohort is not dragged back by it: a connection at the cohort's cursor
	// must not be answered by the stale resumer's block, which ends below it.
	third := f.client(t, 3, head-1, arrived)
	f.pump(t, third)
	f.expect(t, third, []int64{head})
}

// TestCatchupBlockNeverStalerThanTheConnection pins the freshness rule, which is
// what keeps a shared read from quietly widening the default driver's
// visibility gate: a resume read is txid-gated under xmin, so a row whose
// transaction was in flight at the read instant is skipped and the cursor moves
// past it — and a connection served from an OLD block would inherit a gate its
// own read would have cleared.
//
// The rule is that the read must have STARTED after the connection's request
// arrived: attachSpaceView's rule for the shared space view. Its second effect
// is that the sharing engages only in a STORM — a lone reconnect finds nothing
// read after it arrived and reads exactly as it did before this slice.
//
// RED: delete `b.startedAt.After(c.arrivedAt)` from blockServes — the
// late-arriving connection is served from the earlier read, the read count
// stays at 1 and the assert fires.
func TestCatchupBlockNeverStalerThanTheConnection(t *testing.T) {
	const head = 100
	f := newCohortFixture(t, head)

	early := f.client(t, 1, head-3, time.Now())
	f.pump(t, early)
	if got := f.tail.calls(); got != 1 {
		t.Fatalf("first read count = %d, want 1", got)
	}

	// A connection whose request arrived AFTER that read must not be served
	// from it, however well it fits.
	late := f.client(t, 2, head-3, time.Now().Add(time.Millisecond))
	if blockServes(f.shard.catchup.Load(), late) {
		t.Fatal("a block read BEFORE the connection arrived was accepted: a shared read " +
			"must never be staler than one the connection could have issued itself")
	}
	f.pump(t, late)
	if got := f.tail.calls(); got != 2 {
		t.Fatalf("read count = %d after a late arrival; it must read for itself (want 2)", got)
	}
	f.expect(t, late, []int64{head - 2, head - 1, head})
}

// TestCatchupServesEachConnectionItsOwnSuffix pins that a shared block is SLICED
// per connection rather than handed over whole.
//
// The suffix is what makes a shared read identical to the read the connection
// would have issued (History is exclusive of the cursor). Handing over the whole
// block would re-send up to batchLimit events the connection already had — and
// under a COMMIT-ORDERED feed client.passed cannot catch them, because a
// resuming connection's recently-sent window is empty, so the duplicate the
// protocol bounds to the live/resume hand-off would become a block-wide burst.
// The feed here is therefore the commit-ordered one, where the mistake is
// visible.
//
// RED: return b.rows instead of blockAfter(b, c.lastID) in catchUp — the assert
// names the already-seen ids that came back.
func TestCatchupServesEachConnectionItsOwnSuffix(t *testing.T) {
	const head = 50
	f := newCommitOrderedFixture(t, head)
	arrived := time.Now()

	leader := f.client(t, 1, head-10, arrived)
	f.pump(t, leader)

	follower := f.client(t, 2, head-2, arrived)
	f.pump(t, follower)
	if got := f.tail.calls(); got != 1 {
		t.Fatalf("the follower did not share the leader's read (%d reads)", got)
	}
	f.expect(t, follower, []int64{head - 1, head})
	if follower.lastID != head {
		t.Fatalf("follower cursor = %d, want %d", follower.lastID, head)
	}
}

// TestCatchupBlockMustAdvanceTheCursor pins the PROGRESS half of blockServes,
// which is what makes the resume lane terminate without a second guard.
//
// A block that reaches only to a connection's cursor has nothing for it.
// Serving one gives a pump that delivers nothing AND reads nothing — and since
// resume re-arms the pump whenever the org head has moved past the connection
// (the unchanged sh.mu hand-off), a pump that cannot make progress spins on
// that re-arm instead of catching up. Every accepted block therefore has to
// move the cursor strictly forward.
//
// RED: relax `b.end > c.lastID` to `>=` in blockServes — the connection is
// answered by a block that ends at its cursor, issues no read at all, and the
// assert below fires.
func TestCatchupBlockMustAdvanceTheCursor(t *testing.T) {
	const head = 10
	f := newCohortFixture(t, head)
	// The org's shared block stops exactly at this connection's cursor.
	rows := fanRows(syntheticRows(5))
	f.shard.catchup.Store(&catchupBlock{
		from: 0, end: 5, rows: rows, startedAt: time.Now().Add(time.Millisecond)})

	c := f.client(t, 1, 5, time.Now())
	f.pump(t, c)
	if got := f.tail.calls(); got != 1 {
		t.Fatalf("pump issued %d reads when the org's block ends at its cursor, want 1: "+
			"a pump answered by a block with nothing in it delivers nothing and reads "+
			"nothing, so the head re-arm spins", got)
	}
	f.expect(t, c, []int64{6, 7, 8, 9, 10})
}

// TestCatchupBlockIsDroppedWhenItCanServeNobody pins the memory bound. The
// freshness rule means a block's useful life is the milliseconds the cohort
// takes to drain it; keeping one until the org's next resume would pin up to
// batchLimit event payloads per shard for as long as the org stays connected.
//
// It runs through the SWEEP, with the clock injected, because the release is
// only real if it is wired: the 5s shard walk is the only thing that calls it.
//
// RED: delete the dropStaleCatchup call from sweepOnce, or the age test inside
// dropStaleCatchup — the block stays resident and the second assert fires.
func TestCatchupBlockIsDroppedWhenItCanServeNobody(t *testing.T) {
	f := newCohortFixture(t, 10)
	now := time.Now()
	fresh := &catchupBlock{from: 1, end: 2, startedAt: now}
	f.shard.catchup.Store(fresh)

	f.hub.sweepOnce(now.Add(sweepInterval / 2))
	if f.shard.catchup.Load() != fresh {
		t.Fatal("a block younger than one sweep interval was dropped while the cohort " +
			"that produced it may still be draining it")
	}
	f.hub.sweepOnce(now.Add(2 * sweepInterval))
	if got := f.shard.catchup.Load(); got != nil {
		t.Fatalf("a block no connection can be served from is still resident (%+v): a "+
			"shard's worth of event payloads would be pinned for the org's lifetime", got)
	}
}

// TestCatchupRejectsAForeignOrgsShard pins the cell-isolation guard at the one
// hand-off that shares raw event ROWS. register makes shard.orgID and the
// connection's org identical, so this is defence in depth on the most sensitive
// shared state in the package — the assertion syncSpaceView already makes for
// the space view, on a heavier payload.
//
// RED: delete the org check at the top of catchUp — the read runs against the
// connection's org and publishes its rows on ANOTHER org's shard, where that
// org's connections can be served from them.
func TestCatchupRejectsAForeignOrgsShard(t *testing.T) {
	f := newCohortFixture(t, 10)
	other := &orgShard{orgID: 8}
	c := &client{id: auth.Identity{OrgID: 7, UserID: 1}, shard: other,
		arrivedAt: time.Now()}
	if _, _, err := f.hub.catchUp(context.Background(), c); err == nil {
		t.Fatal("a connection was served a catch-up read through another org's shard; " +
			"its rows would be published where another org's connections can take them")
	}
	if got := f.tail.calls(); got != 0 {
		t.Fatalf("the foreign-shard read still reached the driver (%d calls)", got)
	}
	if other.catchup.Load() != nil {
		t.Fatal("one org's event rows were published on another org's shard")
	}
}

// --- harness ------------------------------------------------------------

// cohortFixture is one org's hub with a counting feed and a WebSocket sink, so
// every connection below runs the REAL resume lane (pump → catchUp →
// deliverShared → filter → a real socket write) and only the DRIVER — the thing
// whose calls are being counted — stands in for Postgres.
type cohortFixture struct {
	hub   *Hub
	shard *orgShard
	tail  *countingTail
	sink  *wsSink
	// delivered is the hub's own delivery counter, which is what makes the
	// frame assertions exact: pump returns only after every write has been
	// made, so the counter says precisely how many frames to read back and an
	// over-delivery cannot hide behind a short read.
	delivered *countingRegistry
}

// newCohortFixture builds the fixture on the DEFAULT (id-monotone) feed.
func newCohortFixture(t *testing.T, head int64) *cohortFixture {
	return newFeedFixture(t, head, true)
}

// newCommitOrderedFixture builds it on the COMMIT-ordered feed, where
// client.passed consults the recently-sent window instead of the cursor — the
// feed on which a mistake about what a connection has already had is visible
// rather than absorbed by the skip.
func newCommitOrderedFixture(t *testing.T, head int64) *cohortFixture {
	return newFeedFixture(t, head, false)
}

func newFeedFixture(t *testing.T, head int64, ordered bool) *cohortFixture {
	t.Helper()
	reg := &countingRegistry{counts: map[string]float64{}}
	h := NewHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.SetMetrics(reg)
	tail := &countingTail{rows: syntheticRows(head), ordered: ordered}
	h.SetSource(&stubSource{tail: tail})
	sh := &orgShard{orgID: 7, conns: map[*client]struct{}{},
		userConns: map[int64]*userPresence{}, wake: make(chan struct{}, 1)}
	h.orgs[7] = sh
	return &cohortFixture{hub: h, shard: sh, tail: tail, sink: newWSSink(t), delivered: reg}
}

// client builds one resuming connection on a real socket.
func (f *cohortFixture) client(t *testing.T, userID, lastID int64, arrivedAt time.Time) *client {
	t.Helper()
	return &client{
		id: auth.Identity{OrgID: 7, UserID: userID}, shard: f.shard,
		lastID: lastID, arrivedAt: arrivedAt, conn: f.sink.dial(t, userID),
	}
}

// pump drains one connection's whole catch-up through the real resume lane,
// with the delivery counter zeroed first so expect below reads exactly this
// connection's frames.
func (f *cohortFixture) pump(t *testing.T, c *client) {
	t.Helper()
	f.delivered.reset("fanout_deliveries_total")
	if err := f.hub.pump(context.Background(), c); err != nil {
		t.Fatalf("pump: %v", err)
	}
}

// expect reads back exactly the frames the hub says it delivered to c and
// asserts their seqs, in order.
func (f *cohortFixture) expect(t *testing.T, c *client, want []int64) {
	t.Helper()
	n := int(f.delivered.value("fanout_deliveries_total"))
	if n != len(want) {
		t.Fatalf("connection %d was delivered %d events, want %d (%s)",
			c.id.UserID, n, len(want), summarise(want))
	}
	got := f.sink.seqs(t, c.id.UserID, n)
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("connection %d received %s, want %s",
				c.id.UserID, summarise(got), summarise(want))
		}
	}
}

// wsSink is a WebSocket server that records what the hub writes, per
// connection: the connections under test are real sockets, so deliverShared and
// sendRaw run unmodified.
type wsSink struct {
	srv    *httptest.Server
	mu     sync.Mutex
	frames map[int64]chan Envelope
}

func newWSSink(t *testing.T) *wsSink {
	t.Helper()
	s := &wsSink{frames: map[int64]chan Envelope{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(r.URL.Query().Get("c"), 10, 64)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var e Envelope
			if json.Unmarshal(data, &e) == nil {
				s.channel(id) <- e
			}
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *wsSink) channel(id int64) chan Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frames[id] == nil {
		s.frames[id] = make(chan Envelope, 1024)
	}
	return s.frames[id]
}

func (s *wsSink) dial(t *testing.T, id int64) *websocket.Conn {
	t.Helper()
	s.channel(id) // exists before the first frame can land
	u := strings.Replace(s.srv.URL, "http://", "ws://", 1) + "?c=" + strconv.FormatInt(id, 10)
	conn, _, err := websocket.Dial(context.Background(), u, nil)
	if err != nil {
		t.Fatalf("sink dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// seqs reads back n frames for one connection. n comes from the hub's own
// delivery counter, so this never waits for a frame that was not sent.
func (s *wsSink) seqs(t *testing.T, id int64, n int) []int64 {
	t.Helper()
	ch := s.channel(id)
	out := make([]int64, 0, n)
	deadline := time.After(8 * time.Second)
	for len(out) < n {
		select {
		case e := <-ch:
			out = append(out, e.Seq)
		case <-deadline:
			t.Fatalf("connection %d: read back %d of %d delivered frames", id, len(out), n)
		}
	}
	return out
}

// syntheticRows is an org's event log: ids 1..head, org-wide (no container), so
// every connection's filter delivers all of them and the quantity under test is
// the READ count and nothing else.
func syntheticRows(head int64) []eventlog.Row {
	now := time.Now()
	out := make([]eventlog.Row, 0, head)
	for id := int64(1); id <= head; id++ {
		out = append(out, eventlog.Row{
			ID: id, Verb: "message.created", OccurredAt: now, RecordedAt: now,
			Payload: json.RawMessage(fmt.Sprintf(`{"message_id":%d}`, id)),
		})
	}
	return out
}

// countingTail is the driver seam with a call counter. History answers exactly
// as both real drivers do — `id > afterID ORDER BY id LIMIT n` — so a shared
// block is built from the same shape of batch Postgres returns.
type countingTail struct {
	mu      sync.Mutex
	n       int
	rows    []eventlog.Row
	ordered bool
}

func (t *countingTail) calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

func (t *countingTail) Ordered() bool      { return t.ordered }
func (t *countingTail) OnWake(func(int64)) {}

func (t *countingTail) Head(context.Context, int64) (eventlog.Position, int64, error) {
	return eventlog.Position{}, int64(len(t.rows)), nil
}

func (t *countingTail) Next(_ context.Context, _ int64, pos eventlog.Position, _ int) ([]eventlog.Row, eventlog.Position, error) {
	return nil, pos, nil
}

func (t *countingTail) History(_ context.Context, _ int64, afterID int64, limit int) ([]eventlog.Row, error) {
	t.mu.Lock()
	t.n++
	t.mu.Unlock()
	var out []eventlog.Row
	for _, r := range t.rows {
		if r.ID > afterID {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// countingRegistry is a local metrics.Registry: process-global expvar series
// would be shared with every other test in the package, and these assertions
// are exact.
type countingRegistry struct {
	mu     sync.Mutex
	counts map[string]float64
}

func (r *countingRegistry) Counter(name string, _ ...string) metrics.Counter {
	return &countingCounter{reg: r, name: name}
}

func (r *countingRegistry) Gauge(string, ...string) metrics.Gauge { return nopGauge{} }

func (r *countingRegistry) value(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name]
}

func (r *countingRegistry) reset(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.counts, name)
}

type countingCounter struct {
	reg  *countingRegistry
	name string
}

func (c *countingCounter) Add(delta float64, _ ...string) {
	c.reg.mu.Lock()
	defer c.reg.mu.Unlock()
	c.reg.counts[c.name] += delta
}

type nopGauge struct{}

func (nopGauge) Set(float64, ...string) {}

// summarise renders an id list compactly enough to read in a failure message.
func summarise(ids []int64) string {
	if len(ids) <= 8 {
		return fmt.Sprint(ids)
	}
	return fmt.Sprintf("%v … %v (%d ids)", ids[:4], ids[len(ids)-4:], len(ids))
}
