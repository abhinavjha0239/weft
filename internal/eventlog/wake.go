package eventlog

import (
	"context"
	"sync"
)

// WakeQueue is the CONSUMER-SIDE half of the driver push wake — the thing a
// cursor-tracked consumer points its Feed.OnWake callback at.
//
// It exists because the two ends of a push wake have incompatible constraints.
// The DRIVER's end must never block: OnWake fires on the WAL reader's own
// goroutine, so a callback that waits on a database round trip stalls decoding
// for every org on the cell. The CONSUMER's end is a database pass that can
// take arbitrarily long. A non-blocking, coalescing hand-off is the only shape
// that satisfies both, and every consumer that takes the wake needs the same
// one — so it lives here rather than three times over in the runners.
//
// Semantics:
//
//   - Signal never blocks and never fails. Repeated signals for one org, and
//     signals that arrive while that org's pass is already running, collapse
//     into ONE further pass: the pending set holds org IDS, not events, so a
//     burst costs one map write each and one extra pass in total. That is the
//     property that keeps a hot org from turning the wake into a busy loop.
//   - Run drains the pending set on ONE goroutine, one org at a time, and
//     parks on a channel when the set is empty. It cannot spin: the signal
//     channel is buffered 1, so at most one no-work wakeup can trail a burst
//     (the token that outlived the set), and the loop parks again after it.
//   - A signal is a HINT, never a delivery. Nothing here is durable: a signal
//     that arrives while no Run is draining, or that Run abandons at shutdown,
//     is simply forgotten. That is SAFE — and must stay safe — because the
//     consumer's real position is event_consumer_cursor and its fallback
//     sweep re-polls every org regardless. Losing every wake costs LATENCY
//     (one sweep interval) and nothing else; no consumer may ever come to
//     depend on a wake for correctness.
type WakeQueue struct {
	mu      sync.Mutex
	pending map[int64]struct{}
	// sig is buffered 1 and carries only "the set is non-empty", never the
	// orgs themselves, so a signal storm cannot grow a backlog in it.
	sig chan struct{}
}

// NewWakeQueue builds an empty queue. Cheap: consumers keep one for the
// process lifetime, created at construction so SetSource and Run can wire it
// in either order.
func NewWakeQueue() *WakeQueue {
	return &WakeQueue{pending: map[int64]struct{}{}, sig: make(chan struct{}, 1)}
}

// Signal marks orgID as having newly READABLE events. Safe from any goroutine,
// including a driver's WAL reader: one uncontended mutex, one map write, one
// non-blocking send.
func (q *WakeQueue) Signal(orgID int64) {
	q.mu.Lock()
	q.pending[orgID] = struct{}{}
	q.mu.Unlock()
	select {
	case q.sig <- struct{}{}:
	default:
		// A drain is already scheduled and the set above already carries this
		// org, so the dropped token loses nothing: Signal always publishes to
		// the set BEFORE it tries to send, and Run always re-reads the set
		// after it takes a token.
	}
}

// Run drains signalled orgs through pass until ctx ends. ONE goroutine, so the
// wake can never overlap two passes for the same org with itself; the fallback
// sweep is a separate caller and may overlap, which is already true today and
// is safe for the same reason it has always been — these consumers are
// at-least-once with cursor-tracked, idempotent effects.
func (q *WakeQueue) Run(ctx context.Context, pass func(context.Context, int64)) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.sig:
		}
		for {
			orgID, ok := q.take()
			if !ok {
				break
			}
			pass(ctx, orgID)
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// take removes one pending org, or reports the set empty. The choice is map
// iteration order, i.e. arbitrary, deliberately: an org that signals more
// often must not be able to starve one that signals rarely.
func (q *WakeQueue) take() (int64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for orgID := range q.pending {
		delete(q.pending, orgID)
		return orgID, true
	}
	return 0, false
}
