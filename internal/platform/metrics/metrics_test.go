package metrics

import (
	"expvar"
	"testing"
)

// floatVal reads a published unlabeled series' value; -1 if absent.
func floatVal(t *testing.T, name string) float64 {
	t.Helper()
	v := expvar.Get(name)
	if v == nil {
		return -1
	}
	f, ok := v.(*expvar.Float)
	if !ok {
		t.Fatalf("%s is %T, want *expvar.Float", name, v)
	}
	return f.Value()
}

// mapVal reads one labeled series' value from a published Map; -1 if absent.
func mapVal(t *testing.T, name, key string) float64 {
	t.Helper()
	v := expvar.Get(name)
	if v == nil {
		return -1
	}
	m, ok := v.(*expvar.Map)
	if !ok {
		t.Fatalf("%s is %T, want *expvar.Map", name, v)
	}
	kv := m.Get(key)
	if kv == nil {
		return -1
	}
	f, ok := kv.(*expvar.Float)
	if !ok {
		t.Fatalf("%s[%s] is %T, want *expvar.Float", name, key, kv)
	}
	return f.Value()
}

// TestNop: the default driver discards silently and publishes nothing — the
// zero-cost posture an un-wired hot path relies on.
func TestNop(t *testing.T) {
	r := Nop()
	r.Counter("nop_counter_total").Add(5)
	r.Gauge("nop_gauge", "org").Set(9, "1")
	if v := expvar.Get("nop_counter_total"); v != nil {
		t.Fatalf("Nop published %q; want nothing", "nop_counter_total")
	}
	if v := expvar.Get("nop_gauge"); v != nil {
		t.Fatalf("Nop published %q; want nothing", "nop_gauge")
	}
}

func TestOpen(t *testing.T) {
	for _, driver := range []string{"", "noop"} {
		r, err := Open(driver)
		if err != nil {
			t.Fatalf("Open(%q): %v", driver, err)
		}
		if _, ok := r.(nopRegistry); !ok {
			t.Fatalf("Open(%q) = %T, want nopRegistry", driver, r)
		}
	}
	r, err := Open("expvar")
	if err != nil {
		t.Fatalf("Open(expvar): %v", err)
	}
	if _, ok := r.(expvarRegistry); !ok {
		t.Fatalf("Open(expvar) = %T, want expvarRegistry", r)
	}
	if _, err := Open("prometheus"); err == nil {
		t.Fatal("Open(prometheus) should error on an unknown driver")
	}
}

// TestExpvarRegistry: the shipped driver publishes counters as increasing
// values and gauges as overwriting ones, unlabeled as a Float and labeled as
// Map keys, and constructs repeatedly WITHOUT the duplicate-publish panic.
func TestExpvarRegistry(t *testing.T) {
	r := NewExpvar()

	// Unlabeled counter accumulates.
	c := r.Counter("test_events_total")
	c.Add(2)
	c.Add(3)
	if got := floatVal(t, "test_events_total"); got != 5 {
		t.Fatalf("counter = %v, want 5", got)
	}

	// Labeled counter keys by the label=value tuple, in labelName order.
	lc := r.Counter("test_fanout_total", "consumer")
	lc.Add(4, "gateway")
	lc.Add(1, "gateway")
	lc.Add(7, "search")
	if got := mapVal(t, "test_fanout_total", "consumer=gateway"); got != 5 {
		t.Fatalf("fanout{gateway} = %v, want 5", got)
	}
	if got := mapVal(t, "test_fanout_total", "consumer=search"); got != 7 {
		t.Fatalf("fanout{search} = %v, want 7", got)
	}

	// Gauge SETS (does not accumulate), multi-label key ordering preserved.
	g := r.Gauge("test_lag", "consumer", "org")
	g.Set(10, "notifications", "42")
	g.Set(3, "notifications", "42")
	if got := mapVal(t, "test_lag", "consumer=notifications,org=42"); got != 3 {
		t.Fatalf("gauge = %v, want 3 (Set overwrites)", got)
	}

	// A SECOND registry over the SAME names must not panic and must share the
	// series (this is the repeated-construction guarantee the tests depend on).
	r2 := NewExpvar()
	r2.Counter("test_events_total").Add(1)
	if got := floatVal(t, "test_events_total"); got != 6 {
		t.Fatalf("shared counter = %v, want 6 (second registry adds to the same series)", got)
	}
	r2.Gauge("test_lag", "consumer", "org").Set(99, "notifications", "42")
	if got := mapVal(t, "test_lag", "consumer=notifications,org=42"); got != 99 {
		t.Fatalf("shared gauge = %v, want 99", got)
	}
}

func TestSeriesKey(t *testing.T) {
	cases := []struct {
		names, values []string
		want          string
	}{
		{nil, nil, ""},
		{[]string{"org"}, []string{"7"}, "org=7"},
		{[]string{"consumer", "org"}, []string{"gateway", "7"}, "consumer=gateway,org=7"},
		{[]string{"consumer", "org"}, []string{"gateway"}, "consumer=gateway,org="}, // short tail
	}
	for _, c := range cases {
		if got := seriesKey(c.names, c.values); got != c.want {
			t.Fatalf("seriesKey(%v,%v) = %q, want %q", c.names, c.values, got, c.want)
		}
	}
}
