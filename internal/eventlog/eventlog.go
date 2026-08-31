// Package eventlog is the write and read library for the event log — the
// spine every subsystem consumes (ADR-003, red-team F-1/F-4).
//
// Writers call Append inside the SAME transaction as their domain write (the
// outbox rule: no dual-write). Consumers use Consumer, which enforces the F-1
// commit-order gate — a scan never passes an id whose transaction was still
// in flight, so no committed event can ever be skipped.
package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/enum"
)

// NotifyChannel is the LISTEN/NOTIFY channel that wakes consumers.
const NotifyChannel = "event_log"

// MessageCreatedAtKey is THE payload key for the one piece of ACL input an
// event cannot derive from itself: the created_at of the message the event is
// ABOUT. Every producer of an event about an ALREADY-EXISTING message carries
// it under this key; it is defined here, once, because four packages
// (messaging, unfurl, compliance, and the gateway that reads it) must spell it
// identically for the read ACL to hold.
//
// Why it exists. The ADR-008 C-2 protected-history floor asks "may this
// connection see this?", and REST answers on the MESSAGE's time
// (messaging.Get's `m.created_at < cm2.history_from`). An event about a
// message carries its OWN time, which says nothing about its subject: a
// post-join edit / reaction / pin / move / delete of a PRE-join message clears
// a floor REST does not, handing a protected-history member the id of a message
// REST 404s for — an existence oracle over pre-join history. Resolving it in
// the gateway would cost a query per event PER CONNECTION and break the
// O(1)-per-event fan-out invariant, so the PRODUCER carries the timestamp and
// the filter stays in memory.
//
// Contract. The value is the message row's created_at, unmodified (a move does
// not change it, and REST compares that exact column). The field is ADDITIVE —
// payloads are wire contracts, so no key changes and no verb renames — and the
// gateway takes the EARLIER of it and the event's own boundary. Hence a
// producer that omits it is judged exactly as before, and one that carries it
// can only ever withhold MORE: absence never opens the gate.
//
// message.created deliberately does NOT carry it: its event and its message are
// written by the same transaction, so its boundary already equals (importers) or
// precedes (live sends) the created_at it would name.
const MessageCreatedAtKey = "message_created_at"

// MustPayload marshals a payload map; keys and shapes are wire contract.
// Panics only on unmarshalable input, which is a programming error.
func MustPayload(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("eventlog: marshal payload: %v", err))
	}
	return b
}

// Event is one entry to append. Payload carries structural deltas plus
// references to content revisions — never the only copy of user content
// (F-4 payload indirection; erasure = scrub the referenced revisions).
type Event struct {
	OrgID       int64
	WorkspaceID *int64
	ActorKind   enum.ActorKind
	ActorID     *int64
	EntityType  enum.EntityType
	EntityID    int64
	Verb        string
	Payload     json.RawMessage
	// Hint is the reserved consumer side-channel (ADR-002 P1): delivery
	// advice, never domain truth. Its only reader today is the notification
	// materializer's F-17b batch stamp — {"batch_id": N} folds one bulk
	// operation's N item events into ONE digest row per recipient. Mint it
	// with notification.BatchHintForEvent and nothing else: the batch dedupe
	// key is UNIQUE FOREVER with no time component, so stamping a stable
	// entity id (a rule id, a sprint id) delivers the first sweep and then
	// permanently suppresses that (user, kind) pair, silently.
	Hint json.RawMessage
	// OccurredAt is domain time; importers backdate it (ADR-003 E3).
	// Zero value means now().
	OccurredAt time.Time
	// Origin must be receiver-stamped, never taken from the wire (F-11).
	Origin json.RawMessage
}

// Append inserts the event within the caller's transaction and schedules a
// commit-time wake-up so consumers do not have to poll blind.
//
// The wake rides the INSERT's RETURNING clause (event_log_wake, migration
// 0024): ONE statement, and at most ONE pg_notify per (transaction, org) no
// matter how many events the transaction appends — a transaction writing N
// events used to pay N extra round trips (S4 NOTIFY coalescing). NOTIFY is
// transactional, so it is delivered only on commit and a waking consumer
// always finds the row. Under the logical-decoding feed the notification is a
// wake HINT only (the reader is pushed by the WAL); under the default xmin
// poller it is still the primary wake path.
func Append(ctx context.Context, tx pgx.Tx, e Event) (int64, error) {
	if e.Verb == "" {
		return 0, fmt.Errorf("eventlog: empty verb")
	}
	occurred := e.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	if e.Payload == nil {
		e.Payload = json.RawMessage(`{}`)
	}
	var id int64
	// wakeSent reports whether THIS append issued the pg_notify — false once an
	// earlier append in the same transaction already signalled this org. Append
	// does not surface it (no caller has a use for it), but the RETURNING column
	// must be scanned, and it is what makes the coalescing directly assertable.
	var wakeSent bool
	err := tx.QueryRow(ctx, `
		INSERT INTO event_log
			(org_id, workspace_id, actor_kind, actor_id, entity_type,
			 entity_id, verb, payload, hint, occurred_at, origin)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id, event_log_wake($12, org_id)`,
		e.OrgID, e.WorkspaceID, int16(e.ActorKind), e.ActorID,
		int16(e.EntityType), e.EntityID, e.Verb, e.Payload, e.Hint,
		occurred, e.Origin, NotifyChannel,
	).Scan(&id, &wakeSent)
	if err != nil {
		return 0, fmt.Errorf("eventlog: append: %w", err)
	}
	return id, nil
}

// Row is one consumed entry.
type Row struct {
	ID          int64
	OrgID       int64
	WorkspaceID *int64
	ActorKind   enum.ActorKind
	ActorID     *int64
	EntityType  enum.EntityType
	EntityID    int64
	Verb        string
	Payload     json.RawMessage
	Hint        json.RawMessage
	OccurredAt  time.Time
	RecordedAt  time.Time
	Origin      json.RawMessage
}
