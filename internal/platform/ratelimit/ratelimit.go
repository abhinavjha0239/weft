// Package ratelimit: per-key token buckets with bounded memory.
//
// Scale notes (docs/SCHEMA.md contract): limits are PER NODE — at fleet scale
// each node protects itself and the aggregate limit is nodes × limit, which
// is the standard first tier (a shared limiter is a cell-local concern later,
// never cross-org state). The key map is bounded by a janitor that evicts
// buckets idle longer than the refill horizon — no unbounded growth from
// key churn (e.g. IP scans).
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type entry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

type Limiter struct {
	mu    sync.Mutex
	keys  map[string]*entry
	rate  rate.Limit
	burst int
}

// New creates a limiter allowing r events/second with the given burst.
func New(r float64, burst int) *Limiter {
	return &Limiter{keys: make(map[string]*entry), rate: rate.Limit(r), burst: burst}
}

// Allow reports whether the key may proceed now.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	e, ok := l.keys[key]
	if !ok {
		e = &entry{lim: rate.NewLimiter(l.rate, l.burst)}
		l.keys[key] = e
	}
	e.lastSeen = time.Now()
	l.mu.Unlock()
	return e.lim.Allow()
}

// Janitor evicts idle buckets until ctx ends. Idle = untouched for longer
// than it takes a bucket to fully refill (evicting it loses nothing).
func (l *Limiter) Janitor(ctx context.Context, every time.Duration) {
	idle := time.Duration(float64(l.burst)/float64(l.rate)*float64(time.Second)) + time.Minute
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			l.mu.Lock()
			for k, e := range l.keys {
				if now.Sub(e.lastSeen) > idle {
					delete(l.keys, k)
				}
			}
			l.mu.Unlock()
		}
	}
}
