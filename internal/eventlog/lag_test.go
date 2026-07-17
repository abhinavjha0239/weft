package eventlog

import (
	"context"
	"expvar"
	"strconv"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/metrics"
)

// lagGauge reads the published consumer_lag{consumer,org} series; -1 if absent.
func lagGauge(t *testing.T, consumer string, orgID int64) float64 {
	t.Helper()
	v := expvar.Get("consumer_lag")
	if v == nil {
		t.Fatal("consumer_lag not published")
	}
	m, ok := v.(*expvar.Map)
	if !ok {
		t.Fatalf("consumer_lag is %T, want *expvar.Map", v)
	}
	kv := m.Get("consumer=" + consumer + ",org=" + strconv.FormatInt(orgID, 10))
	if kv == nil {
		return -1
	}
	return kv.(*expvar.Float).Value()
}

// TestConsumerLagMetric is the S0 proof that the instrument reads reality: the
// consumer_lag gauge rises by EXACTLY the un-consumed backlog and falls to 0
// once the cursor catches up. Lag is THE health signal of the whole design, so
// this pins that it tracks the real event_log / cursor delta.
func TestConsumerLagMetric(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	orgID := seedOrg(t, pool)

	consumer := NewConsumer(pool, "lagtest", 100)
	consumer.SetMetrics(metrics.NewExpvar())

	// Nothing appended, no cursor row → zero lag.
	lag, err := consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (empty): %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty lag = %d, want 0", lag)
	}
	if got := lagGauge(t, "lagtest", orgID); got != 0 {
		t.Fatalf("consumer_lag gauge (empty) = %v, want 0", got)
	}

	// Append a backlog and DO NOT consume: lag must rise by exactly the count.
	const backlog = 5
	var lastID int64
	for i := 0; i < backlog; i++ {
		lastID = appendInTx(t, pool, orgID, "thing.happened")
	}
	lag, err = consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (backlog): %v", err)
	}
	if lag != backlog {
		t.Fatalf("backlog lag = %d, want %d", lag, backlog)
	}
	if got := lagGauge(t, "lagtest", orgID); got != backlog {
		t.Fatalf("consumer_lag gauge (backlog) = %v, want %d", got, backlog)
	}

	// Consume the whole backlog and advance the cursor: lag falls to 0.
	rows, err := consumer.Poll(ctx, orgID)
	if err != nil || len(rows) != backlog {
		t.Fatalf("poll: err=%v rows=%d want %d", err, len(rows), backlog)
	}
	if err := consumer.Ack(ctx, orgID, lastID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	lag, err = consumer.Lag(ctx, orgID)
	if err != nil {
		t.Fatalf("lag (drained): %v", err)
	}
	if lag != 0 {
		t.Fatalf("drained lag = %d, want 0", lag)
	}
	if got := lagGauge(t, "lagtest", orgID); got != 0 {
		t.Fatalf("consumer_lag gauge (drained) = %v, want 0", got)
	}
}
