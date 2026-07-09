package loadtest

import (
	"sync/atomic"
	"time"
)

// histogram is a lock-free 1ms-resolution latency recorder: buckets 0..999 ms
// plus an overflow bucket. Chat latencies are ms-scale, so 1ms resolution to
// 1s with an overflow tail is accurate where it matters and O(1) memory (no
// per-sample retention — important at extreme volume).
type histogram struct {
	counts [1001]int64 // [1000] = overflow (>= 1s)
	total  int64
	maxNS  int64
}

func (h *histogram) record(d time.Duration) {
	ms := int(d / time.Millisecond)
	if ms > 999 {
		ms = 1000
	}
	if ms < 0 {
		ms = 0
	}
	atomic.AddInt64(&h.counts[ms], 1)
	atomic.AddInt64(&h.total, 1)
	for {
		cur := atomic.LoadInt64(&h.maxNS)
		if int64(d) <= cur || atomic.CompareAndSwapInt64(&h.maxNS, cur, int64(d)) {
			break
		}
	}
}

func (h *histogram) merge(o *histogram) {
	for i := range h.counts {
		h.counts[i] += atomic.LoadInt64(&o.counts[i])
	}
	h.total += atomic.LoadInt64(&o.total)
	if o.maxNS > h.maxNS {
		h.maxNS = o.maxNS
	}
}

// percentile returns the latency at fraction p (0..1). Sub-ms samples report
// as 0ms (resolution limit); the overflow bucket reports max.
func (h *histogram) percentile(p float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	target := int64(float64(h.total) * p)
	if target < 1 {
		target = 1
	}
	var cum int64
	for i := 0; i <= 1000; i++ {
		cum += h.counts[i]
		if cum >= target {
			if i == 1000 {
				return time.Duration(h.maxNS)
			}
			return time.Duration(i) * time.Millisecond
		}
	}
	return time.Duration(h.maxNS)
}

func (h *histogram) max() time.Duration { return time.Duration(h.maxNS) }
