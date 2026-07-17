// Package metrics is the observability seam (the platform/blob.Store and
// platform/mail.Sender pattern): the core backend speaks ONLY the
// Registry/Counter/Gauge interfaces, and which driver records the numbers is
// an operator choice, not a code change. Two drivers ship: Nop (the zero-cost
// default when metrics are off) and expvar (stdlib, exposed at /debug/vars —
// NO new dependency).
//
// Scale role (docs/ARCHITECTURE.md §6, docs/SCHEMA.md scale contract): consumer
// lag per (consumer, org) is THE health signal of the whole design. This seam
// is how S0 builds the proving ground FIRST, so every later scale slice can
// attach a red/green pin to a real number rather than an unfalsifiable claim.
//
// Metric names carry NO brand prefix (docs/BRANDING.md): they are wire-ish
// identifiers other tools scrape, and a rebrand must never move a series.
package metrics

import (
	"expvar"
	"fmt"
	"strings"
	"sync"
)

// Registry mints metric handles. Implementations must be safe for concurrent
// use. This API is PINNED — other slices assert against it verbatim.
type Registry interface {
	Counter(name string, labelNames ...string) Counter
	Gauge(name string, labelNames ...string) Gauge
}

// Counter is a monotonically increasing series (e.g. events fanned out).
type Counter interface {
	Add(delta float64, labelValues ...string)
}

// Gauge is a point-in-time value that rises and falls (e.g. consumer lag).
type Gauge interface {
	Set(value float64, labelValues ...string)
}

// Open selects the driver by name — the blob/mail seam factory shape, so an
// operator swaps backends by config alone: "" / "noop" → Nop(); "expvar" →
// NewExpvar(). An unknown driver is a config error, never a silent fallback.
func Open(driver string) (Registry, error) {
	switch driver {
	case "", "noop":
		return Nop(), nil
	case "expvar":
		return NewExpvar(), nil
	default:
		return nil, fmt.Errorf("metrics: unknown driver %q (implement Registry and register it here)", driver)
	}
}

// Nop is the zero-cost default: every handle discards. Subsystems default to it
// so an un-wired hot path pays nothing (Add/Set inline to an empty method).
func Nop() Registry { return nopRegistry{} }

type nopRegistry struct{}

func (nopRegistry) Counter(string, ...string) Counter { return nopMetric{} }
func (nopRegistry) Gauge(string, ...string) Gauge     { return nopMetric{} }

type nopMetric struct{}

func (nopMetric) Add(float64, ...string) {}
func (nopMetric) Set(float64, ...string) {}

// NewExpvar is the shipped driver: series publish to expvar and are exposed at
// /debug/vars. It is SAFE to construct repeatedly in one process (tests run
// -p 1 in a single binary): publishing looks up an already-registered var
// first (expvar.Get), because a bare expvar.Publish PANICS on a duplicate name.
func NewExpvar() Registry { return expvarRegistry{} }

type expvarRegistry struct{}

func (expvarRegistry) Counter(name string, labelNames ...string) Counter {
	return newExpvarSeries(name, labelNames)
}

func (expvarRegistry) Gauge(name string, labelNames ...string) Gauge {
	return newExpvarSeries(name, labelNames)
}

// publishMu serializes get-or-publish and per-key Float creation so two
// handles for the same metric never register divergent vars (the duplicate
// registration would otherwise race or panic).
var publishMu sync.Mutex

// expvarSeries backs both Counter and Gauge — Add and Set act on the same
// underlying float. An unlabeled metric publishes a single expvar.Float under
// the name; a labeled one publishes an expvar.Map whose keys are the
// `name1=value1,name2=value2` tuples (in labelName order) and whose values are
// per-series Floats.
type expvarSeries struct {
	labelNames []string
	single     *expvar.Float // used iff len(labelNames) == 0
	m          *expvar.Map   // used iff len(labelNames) > 0

	mu     sync.Mutex // guards the per-handle key→Float memo
	floats map[string]*expvar.Float
}

func newExpvarSeries(name string, labelNames []string) *expvarSeries {
	s := &expvarSeries{labelNames: labelNames}
	if len(labelNames) == 0 {
		s.single = getOrPublishFloat(name)
		return s
	}
	s.m = getOrPublishMap(name)
	s.floats = map[string]*expvar.Float{}
	return s
}

func (s *expvarSeries) Add(delta float64, labelValues ...string) {
	s.float(labelValues).Add(delta)
}

func (s *expvarSeries) Set(value float64, labelValues ...string) {
	s.float(labelValues).Set(value)
}

// float returns the Float for one label tuple, memoized per handle after the
// canonical Float is resolved once from the shared map under publishMu.
func (s *expvarSeries) float(labelValues []string) *expvar.Float {
	if s.single != nil {
		return s.single
	}
	key := seriesKey(s.labelNames, labelValues)
	s.mu.Lock()
	f := s.floats[key]
	s.mu.Unlock()
	if f != nil {
		return f
	}
	publishMu.Lock()
	if v := s.m.Get(key); v != nil {
		if existing, ok := v.(*expvar.Float); ok {
			f = existing
		}
	}
	if f == nil {
		f = new(expvar.Float)
		s.m.Set(key, f)
	}
	publishMu.Unlock()
	s.mu.Lock()
	s.floats[key] = f
	s.mu.Unlock()
	return f
}

// seriesKey renders a label tuple as `name1=value1,name2=value2` in labelName
// order — the expvar.Map key for one series. A short values slice renders the
// missing tail empty (defensive; the pinned API expects matching counts).
func seriesKey(names, values []string) string {
	var b strings.Builder
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteByte('=')
		if i < len(values) {
			b.WriteString(values[i])
		}
	}
	return b.String()
}

func getOrPublishFloat(name string) *expvar.Float {
	publishMu.Lock()
	defer publishMu.Unlock()
	if v := expvar.Get(name); v != nil {
		if f, ok := v.(*expvar.Float); ok {
			return f
		}
		// A non-Float already owns the name (never for our names): hand back a
		// detached Float rather than panic on a duplicate Publish.
		return new(expvar.Float)
	}
	f := new(expvar.Float)
	expvar.Publish(name, f)
	return f
}

func getOrPublishMap(name string) *expvar.Map {
	publishMu.Lock()
	defer publishMu.Unlock()
	if v := expvar.Get(name); v != nil {
		if m, ok := v.(*expvar.Map); ok {
			return m
		}
		return new(expvar.Map).Init()
	}
	m := new(expvar.Map).Init()
	expvar.Publish(name, m)
	return m
}
