package notification

import (
	"encoding/json"

	"github.com/abhinavjha0239/weft/internal/eventlog"
)

// hintPayload is the F-17b coalescing stamp a bulk producer sets on the event
// hint (the reserved field, ADR-002 P1): every item event of ONE bulk
// operation carries the same batch_id, and the materializer folds a
// recipient's N item rows into ONE digest row per (user, kind, batch).
// Producer (BatchHintForEvent) and consumer (Runner.processEvent) share this
// one struct, so the wire shape cannot drift apart.
type hintPayload struct {
	BatchID *int64 `json:"batch_id"`
}

// BatchHintForEvent mints the F-17b batch stamp for ONE bulk operation —
// derived from the id of the event that TRIGGERED it — and renders it into
// the eventlog.Event.Hint shape the materializer reads. Bulk producers (a
// sprint close, an automation sweep, the retention vacuum) append their
// operation event first and stamp every item event of that operation with
// the hint this returns.
//
// It is the blessed path because the contract it satisfies is otherwise
// unenforceable: A BATCH ID MUST BE MINTED FRESH PER BULK OPERATION. The
// batch dedupe key notification_batch_dedupe_key (user_id, kind, batch_id)
// in migration 0021 is UNIQUE FOREVER and carries no time component, and the
// digest insert conflicts with an open DO NOTHING. So a producer that
// stamped a STABLE entity id — the automation rule id, the sprint id, the
// retention policy id, the obvious thing to reach for — would deliver its
// FIRST sweep and then permanently mint ZERO notifications for every
// (user, kind) pair it had already delivered to: silently, with no error,
// and with no net, because the hourly F-17 reconcile repairs the
// deliverability set, never notification rows. Event ids are monotone and
// never reused, so deriving the batch id from the triggering event makes
// freshness STRUCTURAL — the same sweep re-run tomorrow appends a new
// trigger event and therefore gets a new batch.
//
// A non-positive id yields a nil hint (no trigger event, no batch), which
// degrades to the unbatched per-item path: N rows, visibly, rather than a
// poisoned key that mints none.
func BatchHintForEvent(triggerEventID int64) json.RawMessage {
	if triggerEventID <= 0 {
		return nil
	}
	return eventlog.MustPayload(hintPayload{BatchID: &triggerEventID})
}
