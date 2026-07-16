package automation

// Schedule triggers fire on a wall-clock cadence rather than an event. The
// grammar is STRUCTURAL (no cron dependency — go.mod stays clean): one of four
// shapes, each with an optional IANA timezone. nextFire is a pure function of
// (schedule, now, location), so the scheduler lane stays a thin claim around
// it and the whole cadence surface is unit-testable without a clock or a
// database. Cron-string grammar is a recorded gap.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Schedule is the trigger's cadence document. Exactly one "every" shape is
// active; the fields a shape ignores are not read (validation checks only the
// fields its shape needs).
//
//	{"every":"minutes","n":N>=5}
//	{"every":"hour","minute":0..59}
//	{"every":"day","at":"HH:MM"}
//	{"every":"week","on":"mon".."sun","at":"HH:MM"}
//	  + optional "tz": IANA name (default UTC)
type Schedule struct {
	Every  string `json:"every"`
	N      int    `json:"n,omitempty"`
	Minute int    `json:"minute,omitempty"`
	At     string `json:"at,omitempty"`
	On     string `json:"on,omitempty"`
	TZ     string `json:"tz,omitempty"`
}

const minScheduleMinutes = 5

var weekdays = map[string]time.Weekday{
	"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday,
	"wed": time.Wednesday, "thu": time.Thursday, "fri": time.Friday,
	"sat": time.Saturday,
}

// scheduleLocation resolves the schedule's timezone. An empty tz is UTC; a
// non-empty one must be a loadable IANA name (400 otherwise). Shared by
// validation (fail fast at write) and nextFire (the claim's compute).
func scheduleLocation(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, apperr.Invalid(fmt.Sprintf("definition: schedule tz %q is not a valid IANA timezone", tz))
	}
	return loc, nil
}

// parseHHMM parses a "HH:MM" 24-hour clock time, rejecting anything else.
func parseHHMM(s string) (hh, mm int, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[0]) > 2 || len(parts[1]) != 2 {
		return 0, 0, apperr.Invalid(fmt.Sprintf("definition: schedule at %q must be HH:MM", s))
	}
	hh, e1 := strconv.Atoi(parts[0])
	mm, e2 := strconv.Atoi(parts[1])
	if e1 != nil || e2 != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, 0, apperr.Invalid(fmt.Sprintf("definition: schedule at %q must be HH:MM (00:00..23:59)", s))
	}
	return hh, mm, nil
}

// validateSchedule enforces the grammar at write time so a bad tz, out-of-range
// minute, malformed HH:MM, sub-5-minute cadence, or unknown weekday is a 400 —
// never a runtime surprise in the claim.
func validateSchedule(s *Schedule) error {
	if s == nil {
		return apperr.Invalid("definition: a schedule trigger requires a schedule")
	}
	if _, err := scheduleLocation(s.TZ); err != nil {
		return err
	}
	switch s.Every {
	case "minutes":
		if s.N < minScheduleMinutes {
			return apperr.Invalid(fmt.Sprintf("definition: schedule minutes n must be >= %d", minScheduleMinutes))
		}
	case "hour":
		if s.Minute < 0 || s.Minute > 59 {
			return apperr.Invalid("definition: schedule hour minute must be 0..59")
		}
	case "day":
		if _, _, err := parseHHMM(s.At); err != nil {
			return err
		}
	case "week":
		if _, ok := weekdays[s.On]; !ok {
			return apperr.Invalid("definition: schedule week 'on' must be mon..sun")
		}
		if _, _, err := parseHHMM(s.At); err != nil {
			return err
		}
	default:
		return apperr.Invalid("definition: schedule 'every' must be minutes, hour, day, or week")
	}
	return nil
}

// nextFire computes the first fire strictly after now, in the schedule's
// location. It is pure and total: an invalid schedule (rejected at write)
// falls through to now, never panics.
//
// DST: a wall-clock time skipped by a spring-forward gap does not exist.
// time.Date maps such a time back to the pre-transition offset (it reads an
// hour EARLIER than requested); atWallClock detects the non-round-trip and
// normalizes FORWARD, past the gap, so a schedule never silently fires an hour
// before its intended wall-clock time.
func nextFire(s Schedule, now time.Time, loc *time.Location) time.Time {
	switch s.Every {
	case "minutes":
		n := s.N
		if n < 1 {
			n = 1
		}
		// A pure interval: aligned to the minute so the instant is
		// deterministic, then advanced by the cadence. Timezone-independent.
		return now.Truncate(time.Minute).Add(time.Duration(n) * time.Minute)
	case "hour":
		ln := now.In(loc)
		cand := time.Date(ln.Year(), ln.Month(), ln.Day(), ln.Hour(), s.Minute, 0, 0, loc)
		// Advance by absolute hours rather than re-deriving a wall clock: when
		// the next wall hour falls in a spring-forward gap, time.Date can hand
		// back a PAST instant, and the loop keeps "strictly after now" true.
		for !cand.After(now) {
			cand = cand.Add(time.Hour)
		}
		return cand
	case "day":
		hh, mm, _ := parseHHMM(s.At)
		ln := now.In(loc)
		cand := atWallClock(ln.Year(), ln.Month(), ln.Day(), hh, mm, loc)
		if !cand.After(now) {
			cand = atWallClock(ln.Year(), ln.Month(), ln.Day()+1, hh, mm, loc)
		}
		return cand
	case "week":
		hh, mm, _ := parseHHMM(s.At)
		target := weekdays[s.On]
		ln := now.In(loc)
		delta := (int(target) - int(ln.Weekday()) + 7) % 7
		cand := atWallClock(ln.Year(), ln.Month(), ln.Day()+delta, hh, mm, loc)
		if !cand.After(now) {
			cand = atWallClock(ln.Year(), ln.Month(), ln.Day()+delta+7, hh, mm, loc)
		}
		return cand
	}
	return now
}

// atWallClock builds the instant for wall time hh:mm on the given date in loc.
// If that wall time was skipped by a spring-forward DST gap, time.Date returns
// it at the pre-transition offset (reading hh-1:mm), so we advance one hour to
// land at the post-gap wall clock — the forward normalization. Every IANA zone
// Weft supports uses a one-hour DST shift; sub-hour shifts are a non-goal.
func atWallClock(year int, month time.Month, day, hh, mm int, loc *time.Location) time.Time {
	t := time.Date(year, month, day, hh, mm, 0, 0, loc)
	if t.Hour() == hh && t.Minute() == mm {
		return t
	}
	return t.Add(time.Hour)
}

// scheduleNextAt is the schedule_next_at lifecycle value for a rule: the next
// fire when the rule is enabled AND its trigger is a schedule, otherwise nil
// (NULL). An unparseable or non-schedule definition yields nil — the claim
// never invents a fire. Called from Update; Create stores rules disabled, so
// it never sets one.
func scheduleNextAt(enabled bool, rawDef json.RawMessage) *time.Time {
	if !enabled {
		return nil
	}
	var d Definition
	if err := json.Unmarshal(rawDef, &d); err != nil {
		return nil
	}
	if d.Trigger.Kind != kindSchedule || d.Trigger.Schedule == nil {
		return nil
	}
	loc, err := scheduleLocation(d.Trigger.Schedule.TZ)
	if err != nil {
		return nil
	}
	n := nextFire(*d.Trigger.Schedule, time.Now(), loc)
	return &n
}
