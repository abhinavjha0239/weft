package rest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// drainConsumer drives process until the named event-log consumer's cursor
// has passed every committed event for the org. One synchronous ProcessOrg
// pass can legitimately come back EMPTY while events are pending: Poll's
// txid gate (eventlog.Consumer, the F-1 skip guard) holds back any row newer
// than the oldest write transaction still in flight ANYWHERE in the database
// — the gateway hub acking its own cursor mid-poll is enough — so "poll
// again" is part of the consumer contract, and a test that asserts after a
// single pass flakes exactly the way TestAlertWords did in CI. Draining to
// lag 0 before asserting is the fix; the deadline turns a stuck consumer
// into a clear failure instead of a confusing assertion diff.
func drainConsumer(t *testing.T, ctx context.Context, pool *pgxpool.Pool, consumer string, orgID int64, process func(context.Context, int64) error) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		if err := process(ctx, orgID); err != nil {
			t.Fatalf("drain %s: %v", consumer, err)
		}
		var lag int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM event_log e
			WHERE e.org_id = $1 AND e.id > COALESCE((
			  SELECT last_id FROM event_consumer_cursor
			  WHERE consumer = $2 AND org_id = $1), 0)`,
			orgID, consumer).Scan(&lag); err != nil {
			t.Fatalf("drain %s lag: %v", consumer, err)
		}
		if lag == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s consumer never caught up", consumer)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
