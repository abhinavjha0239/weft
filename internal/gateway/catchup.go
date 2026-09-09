package gateway

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// The RESUME lane's read, shared across a reconnect cohort.
//
// S3 hoisted the STEADY-STATE read to one per org per batch (see runReader).
// The resume lane stayed per connection, which is right for an occasional
// reconnect and wrong for a reconnect STORM: a gateway node restart drops every
// connection it held and they all come back at once, so at the 100k target the
// read S3 removed from steady state returns in a burst at every deploy.
//
// The premise, and it was MEASURED before this was built rather than assumed:
// connections that reconnect together resume from cursors that CLUSTER, so one
// read can serve the cohort. Two mechanisms put them there, and only the second
// is strong enough to rely on:
//
//   - Delivery equalises the SERVER's cursor. deliverShared advances past
//     ACL-FILTERED rows too (seq gaps are expected, F-2), so every live
//     connection in an org holds the same lastID. But a CLIENT resumes from the
//     last seq it was DELIVERED, and that is per-visibility-class: in the
//     fixture (12 connections, four channels, skewed traffic) the six members of
//     the busy channel all resumed from 28, the three on the medium channel from
//     33, the two on the quiet one from 34 — four distinct cursors for twelve
//     connections — and the member of a SILENT channel sat 34 events behind the
//     org head. Clustering by cohort alone would therefore have been a guess.
//   - The F-2 CHECKPOINT closes it. Every checkpointInterval the hub sends each
//     connection `Seq: c.lastID` — the ORG-wide cursor, gaps included — and a
//     client tracks it (the reference client's `if (e.seq) S.lastSeq = e.seq`).
//     Re-running the same fixture with the interval shortened collapsed all 12
//     cursors onto the org head exactly: spread 34 → 0, silent-channel member
//     included. So a cohort's spread is bounded by ONE checkpoint interval of
//     org events, whatever each connection can see.
//
// A cohort therefore shares a read, and a genuinely far-behind resumer (offline
// across checkpoints) reads ALONE rather than widening anyone else's read —
// blockServes is what decides which, per connection.
//
// What is NOT shared is the cursor, the ACL or the hand-off. A block is a
// read-only set of ROWS; every connection slices it to its own suffix, runs its
// OWN client.filter over it (the property that makes a shared read safe — the
// multicast lane's rule, applied to the resume lane), advances its OWN cursor,
// and reaches the live lane through the unchanged sh.mu hand-off in resume.

// catchupBlock is ONE resume read, kept on the org shard so the rest of a
// reconnect cohort can be served from it instead of each running the same
// query. Immutable once published (the spaceView discipline): a connection
// reading it needs no lock, and the atomic Store/Load that publishes it is what
// orders the rows against the readers.
type catchupBlock struct {
	// from is the cursor the read started from, EXCLUSIVE — so the block holds
	// every row the driver returned in (from, end]. It is the no-gap test: a
	// connection may be served only if its own cursor is at or above it.
	from int64
	// end is the highest id in rows. It is the progress test: a block that does
	// not reach past a connection's cursor has nothing for it, and serving one
	// would spin the pump.
	end int64
	// rows is the read's fanned rows in EVENT-ID order (the Tail.History
	// contract, which both drivers implement with ORDER BY e.id) — the ordering
	// blockAfter's search and `end` above both rest on.
	rows []eventRow
	// full records that the read hit batchLimit, i.e. there is more behind it.
	// It is the pump's loop condition and belongs to the READ, not to the
	// per-connection suffix: a block that returned 200 rows means "keep going"
	// even for a connection only three of them were new to.
	full bool
	// startedAt is when the READ began — the freshness anchor, and the whole
	// reason this memo cannot quietly widen the xmin driver's visibility gate.
	// See blockServes.
	startedAt time.Time
}

// blockServes reports whether b can stand in for c's own catch-up read. Written
// as a function on a possibly-nil block so "no block yet" resolves through the
// same predicate as "a block that does not fit" — the spaceView nil-receiver
// discipline.
//
// Three conditions, each load-bearing:
//
//   - FRESHNESS: the read must have STARTED after this connection's request
//     arrived. This is attachSpaceView's rule for the shared space view, and it
//     is here for a sharper reason. Under the default (xmin) driver a resume
//     read is visibility-gated, so a row whose transaction was still in flight
//     at the read instant is skipped and the cursor moves past it — pre-existing,
//     documented, and the reason the logical driver exists. Sharing a read means
//     sharing that gate, so a memo without this rule would let a connection
//     inherit an arbitrarily OLD snapshot and skip rows its own read would have
//     seen. Anchored on arrival, a shared read is never staler than a read the
//     connection could have issued when it connected. It also means the sharing
//     ENGAGES ONLY IN A STORM: a lone reconnect finds nothing read after it
//     arrived and reads exactly as it did before this slice.
//   - NO GAP: b.from <= c.lastID, so (c.lastID, b.end] is entirely inside
//     (b.from, b.end]. A block that starts ABOVE the connection's cursor is
//     missing events it has not seen — the one way sharing could turn into
//     LOSS, and the case a far-behind resumer hits, which is why it reads alone.
//   - PROGRESS: b.end > c.lastID, so an accepted block always advances the
//     cursor. That is what makes pump terminate without a second guard: every
//     served block strictly moves lastID, so the loop cannot be handed the same
//     block twice, and a re-armed pump can never be answered by the block it
//     just drained.
func blockServes(b *catchupBlock, c *client) bool {
	return b != nil &&
		b.startedAt.After(c.arrivedAt) &&
		b.from <= c.lastID &&
		b.end > c.lastID
}

// blockAfter is the per-connection SLICE of a shared block: the rows strictly
// above lastID, which is exactly what this connection's own History(afterID)
// would have returned. The suffix is taken rather than left to client.passed
// because passed answers per DRIVER — under a commit-ordered feed it consults
// the recently-sent window, which a resuming connection has not filled, so
// handing it the whole block would re-send up to batchLimit events it already
// had. The protocol allows a duplicate at the live/resume hand-off; it should
// not have to allow a block-wide burst.
//
// Binary search over the id-ordered rows, so the slice is a header, not a copy:
// the cohort shares one backing array.
func blockAfter(b *catchupBlock, lastID int64) []eventRow {
	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].id > lastID })
	return b.rows[i:]
}

// catchUp returns c's next catch-up batch — from the org's shared block when
// one can stand in for c's own read, else from a fresh read that is PUBLISHED
// for the rest of the cohort. `more` is the read's own "there is more behind
// this" (batchLimit reached), which is what pump loops on.
//
// The single-flight is orgSpaceView's, for the same reason: N connections woken
// by the same restart all miss, all reach for the lock, and the winner's read
// answers every loser at the re-check inside it — so a storm collapses to about
// one read per read-latency instead of one per connection. The lock is NOT
// sh.mu: this does a pool round trip, and holding the fan lock across it would
// stall the org's whole multicast (S3's rule).
func (h *Hub) catchUp(ctx context.Context, c *client) (batch []eventRow, more bool, err error) {
	sh := c.shard
	// Shared state is where cross-org bugs live, and this shares ROWS. The
	// read below is pinned to c.id.OrgID while the block is published on
	// c.shard, so the two must be the same org (register guarantees it; this is
	// the syncSpaceView assertion, on the more sensitive payload).
	if sh.orgID != c.id.OrgID {
		return nil, false, fmt.Errorf("gateway: shard org %d serving connection org %d",
			sh.orgID, c.id.OrgID)
	}
	if b := sh.catchup.Load(); blockServes(b, c) {
		return blockAfter(b, c.lastID), b.full, nil
	}
	sh.catchupMu.Lock()
	defer sh.catchupMu.Unlock()
	if b := sh.catchup.Load(); blockServes(b, c) {
		return blockAfter(b, c.lastID), b.full, nil // another resumer read it while we waited
	}
	// One catch-up read for this connection's resume gap — and, from here on,
	// for every connection of this org that was already waiting on one. Both
	// counters rise: pumpQueries is the S3 series (one read per org per batch
	// plus the resume lane), resumeReads isolates THIS lane so a storm's cost
	// is measurable on its own (docs/PERF.md).
	h.pumpQueries.Add(1)
	h.resumeReads.Add(1)
	startedAt := time.Now()
	rows, err := h.tail.History(ctx, c.id.OrgID, c.lastID, batchLimit)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		// Nothing to publish: an empty block could serve nobody (it fails the
		// progress test by construction) and would only evict a usable one.
		return nil, false, nil
	}
	b := &catchupBlock{
		from: c.lastID, end: rows[len(rows)-1].ID, rows: fanRows(rows),
		full: len(rows) >= batchLimit, startedAt: startedAt,
	}
	// Marshal each row's Envelope ONCE for the cohort, exactly as the multicast
	// reader does for the live batch (#119): the bytes are identical for every
	// connection in the org, so a shared read is a shared ENCODE too and a storm
	// costs O(gap) marshals rather than O(connections x gap). Rows a connection
	// filters out are never sent, so this only ever pays for what the cohort as
	// a whole may see.
	for i := range b.rows {
		b.rows[i].enc, _ = h.encodeEnvelope(Envelope{
			Seq: b.rows[i].id, Type: b.rows[i].verb, OrgID: sh.orgID,
			Payload: b.rows[i].payload,
		})
	}
	sh.catchup.Store(b)
	return b.rows, b.full, nil
}

// dropStaleCatchup releases a shared block no connection can be served from any
// more. The freshness rule means a block's useful life is only as long as the
// connections that were ALREADY waiting when it was read take to drain it —
// milliseconds — so retaining one until the org's next resume would hold up to
// batchLimit event payloads per shard for as long as the org stays connected.
// The 5s sweep already walks every shard, so it is what lets go: retention is
// bounded to about two sweep intervals after the storm that created it.
//
// CompareAndSwap rather than Store, so a block published between the load and
// the drop is never thrown away.
func (sh *orgShard) dropStaleCatchup(now time.Time) {
	if b := sh.catchup.Load(); b != nil && now.Sub(b.startedAt) > sweepInterval {
		sh.catchup.CompareAndSwap(b, nil)
	}
}
