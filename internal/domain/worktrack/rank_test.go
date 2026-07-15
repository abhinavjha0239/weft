package worktrack

import "testing"

// TestRankBetween pins the LexoRank midpoint helper: sentinels, the append path
// when the first differing bytes are adjacent, digit-bearing legacy ranks, and
// the !ok cases (equal, out of order, adjacent-with-no-midpoint) that drive a
// rebalance. Every ok result is also checked to sort strictly between lo and hi
// under byte order — the property the whole board ordering rests on.
func TestRankBetween(t *testing.T) {
	cases := []struct {
		name   string
		lo, hi string
		want   string // expected output; ignored when !ok
		wantOK bool
	}{
		{"both sentinels", "", "", "m", true},
		{"start sentinel", "", "b", "a", true},
		{"end sentinel", "y", "", "z", true},
		{"end sentinel appends past z", "z", "", "zm", true},
		{"adjacent letters append", "a", "b", "am", true},
		{"shared prefix then append", "ab", "ac", "abm", true},
		{"legacy digit ranks append", "m00000001", "m00000002", "m00000001m", true},
		{"wide gap picks a midpoint", "a", "z", "m", true},
		{"equal bounds", "a", "a", "", false},
		{"out of order", "b", "a", "", false},
		{"adjacent with no midpoint", "ab", "aba", "", false},
		{"legacy adjacent no midpoint", "m", "ma", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := rankBetween(tc.lo, tc.hi)
			if ok != tc.wantOK {
				t.Fatalf("rankBetween(%q,%q) ok = %v, want %v (got %q)", tc.lo, tc.hi, ok, tc.wantOK, got)
			}
			if !ok {
				return
			}
			if got != tc.want {
				t.Fatalf("rankBetween(%q,%q) = %q, want %q", tc.lo, tc.hi, got, tc.want)
			}
			if !(tc.lo < got) {
				t.Fatalf("rankBetween(%q,%q) = %q not strictly after lo", tc.lo, tc.hi, got)
			}
			if tc.hi != "" && !(got < tc.hi) {
				t.Fatalf("rankBetween(%q,%q) = %q not strictly before hi", tc.lo, tc.hi, got)
			}
		})
	}
}

// TestRankBetweenEvenRanks checks the backfill/rebalance spacing helper: n
// evenly spread 3-char a..z ranks must be strictly ascending and land between
// any adjacent pair via rankBetween (so a rebalance always leaves room).
func TestRankBetweenEvenRanks(t *testing.T) {
	for _, n := range []int{1, 2, 3, 10, 100, 1000} {
		ranks := evenRanks(n)
		if len(ranks) != n {
			t.Fatalf("evenRanks(%d) length = %d", n, len(ranks))
		}
		for i, r := range ranks {
			if len(r) != 3 {
				t.Fatalf("evenRanks(%d)[%d] = %q, want 3 chars", n, i, r)
			}
			for _, c := range []byte(r) {
				if c < 'a' || c > 'z' {
					t.Fatalf("evenRanks(%d)[%d] = %q has a non a-z byte", n, i, r)
				}
			}
			if i > 0 {
				if !(ranks[i-1] < r) {
					t.Fatalf("evenRanks(%d) not ascending at %d: %q !< %q", n, i, ranks[i-1], r)
				}
				if _, ok := rankBetween(ranks[i-1], r); !ok {
					t.Fatalf("evenRanks(%d) leaves no gap between %q and %q", n, ranks[i-1], r)
				}
			}
		}
	}
}
