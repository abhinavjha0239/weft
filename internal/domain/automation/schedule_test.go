package automation

import (
	"testing"
	"time"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// TestNextFire pins the pure cadence function across all four grammars, a
// spring-forward DST gap (the nonexistent wall time normalizes FORWARD), and
// week rollover. Every case asserts the exact instant, so a regression in the
// wall-clock math is caught without a clock or a database.
func TestNextFire(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load NY: %v", err)
	}
	mk := func(y int, mo time.Month, d, h, mi int, loc *time.Location) time.Time {
		return time.Date(y, mo, d, h, mi, 0, 0, loc)
	}
	cases := []struct {
		name  string
		sched Schedule
		now   time.Time
		want  time.Time
	}{
		{
			"minutes aligns and advances",
			Schedule{Every: "minutes", N: 15},
			time.Date(2024, 1, 1, 10, 7, 30, 0, time.UTC),
			mk(2024, 1, 1, 10, 22, time.UTC),
		},
		{
			"hour minute still ahead today",
			Schedule{Every: "hour", Minute: 15},
			mk(2024, 1, 1, 10, 7, time.UTC),
			mk(2024, 1, 1, 10, 15, time.UTC),
		},
		{
			"hour minute already passed rolls to next hour",
			Schedule{Every: "hour", Minute: 15},
			mk(2024, 1, 1, 10, 20, time.UTC),
			mk(2024, 1, 1, 11, 15, time.UTC),
		},
		{
			"day time still ahead today",
			Schedule{Every: "day", At: "09:00"},
			mk(2024, 1, 1, 8, 0, time.UTC),
			mk(2024, 1, 1, 9, 0, time.UTC),
		},
		{
			"day time passed rolls to tomorrow",
			Schedule{Every: "day", At: "09:00"},
			mk(2024, 1, 1, 10, 0, time.UTC),
			mk(2024, 1, 2, 9, 0, time.UTC),
		},
		{
			"week finds the coming weekday",
			Schedule{Every: "week", On: "wed", At: "12:00"},
			mk(2024, 1, 1, 0, 0, time.UTC), // Monday
			mk(2024, 1, 3, 12, 0, time.UTC),
		},
		{
			"week same day before time fires today",
			Schedule{Every: "week", On: "wed", At: "12:00"},
			mk(2024, 1, 3, 11, 0, time.UTC), // Wednesday
			mk(2024, 1, 3, 12, 0, time.UTC),
		},
		{
			"week rollover: same day past time jumps seven days",
			Schedule{Every: "week", On: "wed", At: "12:00"},
			mk(2024, 1, 3, 13, 0, time.UTC), // Wednesday, past noon
			mk(2024, 1, 10, 12, 0, time.UTC),
		},
		{
			// 2023-03-12: NY springs forward 02:00 EST -> 03:00 EDT, so 02:30
			// does not exist. It normalizes FORWARD to 03:30 EDT, never back to
			// 01:30 EST (an hour before the intended time).
			"DST spring-forward gap normalizes forward",
			Schedule{Every: "day", At: "02:30", TZ: "America/New_York"},
			mk(2023, 3, 12, 1, 0, ny),
			mk(2023, 3, 12, 3, 30, ny),
		},
		{
			// The hour shape's next slot (02:30) falls in the same gap;
			// advancing by absolute hours keeps the fire strictly in the
			// future instead of handing back a pre-gap instant.
			"DST gap in the hour shape stays strictly future",
			Schedule{Every: "hour", Minute: 30, TZ: "America/New_York"},
			mk(2023, 3, 12, 1, 45, ny),
			mk(2023, 3, 12, 3, 30, ny),
		},
	}
	for _, tc := range cases {
		loc, err := scheduleLocation(tc.sched.TZ)
		if err != nil {
			t.Fatalf("%s: loc: %v", tc.name, err)
		}
		got := nextFire(tc.sched, tc.now, loc)
		if !got.Equal(tc.want) {
			t.Errorf("%s: nextFire = %s, want %s",
				tc.name, got.UTC().Format(time.RFC3339), tc.want.UTC().Format(time.RFC3339))
		}
		if !got.After(tc.now) {
			t.Errorf("%s: nextFire %s is not after now %s",
				tc.name, got.UTC().Format(time.RFC3339), tc.now.UTC().Format(time.RFC3339))
		}
	}
}

func TestValidateSchedule(t *testing.T) {
	valid := []Schedule{
		{Every: "minutes", N: 5},
		{Every: "minutes", N: 60},
		{Every: "hour", Minute: 0},
		{Every: "hour", Minute: 59},
		{Every: "day", At: "00:00"},
		{Every: "day", At: "9:05"},
		{Every: "day", At: "23:59", TZ: "Europe/Paris"},
		{Every: "week", On: "sun", At: "06:30"},
		{Every: "week", On: "mon", At: "18:00", TZ: "Asia/Kolkata"},
	}
	for i, s := range valid {
		sc := s
		if err := validateSchedule(&sc); err != nil {
			t.Errorf("valid[%d] %+v = %v, want nil", i, s, err)
		}
	}
	invalid := map[string]Schedule{
		"nil-like unknown every": {Every: "fortnight"},
		"minutes below floor":    {Every: "minutes", N: 4},
		"minutes zero":           {Every: "minutes"},
		"hour minute too high":   {Every: "hour", Minute: 60},
		"hour minute negative":   {Every: "hour", Minute: -1},
		"day bad time":           {Every: "day", At: "24:00"},
		"day malformed time":     {Every: "day", At: "9am"},
		"day missing time":       {Every: "day"},
		"week bad weekday":       {Every: "week", On: "funday", At: "09:00"},
		"week missing weekday":   {Every: "week", At: "09:00"},
		"bad tz":                 {Every: "day", At: "09:00", TZ: "Mars/Phobos"},
	}
	for name, s := range invalid {
		sc := s
		err := validateSchedule(&sc)
		if err == nil {
			t.Errorf("%s: validateSchedule = nil, want error", name)
			continue
		}
		if apperr.KindOf(err) != apperr.KindInvalid {
			t.Errorf("%s: kind = %v, want Invalid (400)", name, apperr.KindOf(err))
		}
	}
	// A nil schedule (a schedule kind with no schedule) is invalid.
	if err := validateSchedule(nil); err == nil {
		t.Error("nil schedule = nil, want error")
	}
}
