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
// commit-time NOTIFY so consumers wake without polling.
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
	err := tx.QueryRow(ctx, `
		INSERT INTO event_log
			(org_id, workspace_id, actor_kind, actor_id, entity_type,
			 entity_id, verb, payload, hint, occurred_at, origin)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id`,
		e.OrgID, e.WorkspaceID, int16(e.ActorKind), e.ActorID,
		int16(e.EntityType), e.EntityID, e.Verb, e.Payload, e.Hint,
		occurred, e.Origin,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("eventlog: append: %w", err)
	}
	// NOTIFY is transactional: delivered only on commit, so a waking consumer
	// always finds the row (once past the txid gate).
	if _, err := tx.Exec(ctx,
		`SELECT pg_notify($1, $2)`, NotifyChannel, fmt.Sprint(e.OrgID)); err != nil {
		return 0, fmt.Errorf("eventlog: notify: %w", err)
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
