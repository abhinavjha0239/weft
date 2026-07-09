package loadtest

import (
	"testing"
	"time"
)

func TestHistogramPercentiles(t *testing.T) {
	var h histogram
	// 100 samples: 1ms..100ms, one each.
	for i := 1; i <= 100; i++ {
		h.record(time.Duration(i) * time.Millisecond)
	}
	if got := h.percentile(0.50); got < 49*time.Millisecond || got > 51*time.Millisecond {
		t.Fatalf("p50 = %v, want ~50ms", got)
	}
	if got := h.percentile(0.99); got < 98*time.Millisecond || got > 100*time.Millisecond {
		t.Fatalf("p99 = %v, want ~99ms", got)
	}
	if h.max() != 100*time.Millisecond {
		t.Fatalf("max = %v, want 100ms", h.max())
	}
}

func TestHistogramMerge(t *testing.T) {
	var a, b histogram
	a.record(2 * time.Millisecond)
	b.record(3 * time.Millisecond)
	b.record(5 * time.Second) // overflow
	a.merge(&b)
	if a.total != 3 {
		t.Fatalf("total = %d, want 3", a.total)
	}
	if a.max() != 5*time.Second {
		t.Fatalf("max = %v, want 5s", a.max())
	}
}

func TestHistogramOverflowTail(t *testing.T) {
	var h histogram
	for i := 0; i < 99; i++ {
		h.record(10 * time.Millisecond)
	}
	h.record(5 * time.Second) // the 1% tail lands in overflow
	// p99 sits right at the boundary; p999 (via max) is the overflow value.
	if got := h.max(); got != 5*time.Second {
		t.Fatalf("max = %v, want 5s", got)
	}
	if got := h.percentile(0.50); got != 10*time.Millisecond {
		t.Fatalf("p50 = %v, want 10ms", got)
	}
}
