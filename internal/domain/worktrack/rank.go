package worktrack

// LexoRank-style sparse ordering (F-21; work_item.rank is TEXT COLLATE "C" in
// migration 0005, so items sort by raw byte order). rankBetween mints a rank
// strictly between two neighbours without renumbering the rest, so a
// drag-and-drop reorder rewrites exactly one row in the common case; when a gap
// is exhausted the caller rebalances the whole context and retries once.
//
// Bytes that rankBetween *adds* are drawn from the lowercase alphabet a..z. Any
// shared prefix of the two bounds is preserved verbatim, so the digit-bearing
// ranks CreateItem mints today ("m00000001", ...) participate correctly: only
// their ordering under COLLATE "C" matters, never the byte class.

const (
	rankLow  = byte('a') // smallest byte rankBetween will emit
	rankHigh = byte('z') // largest byte rankBetween will emit
	rankSpan = 26 * 26 * 26
)

// rankBetween returns a rank r with lo < r < hi under byte ("C" collation)
// order, emitting only a..z for any byte it adds. An empty lo is the start
// sentinel (before every rank); an empty hi is the end sentinel (after every
// rank). ok is false only when lo and hi are equal, out of order, or so
// adjacent that no a..z string fits strictly between them (e.g. "ab" and
// "aba") — the caller then rebalances the context and retries.
func rankBetween(lo, hi string) (rank string, ok bool) {
	if hi != "" && lo >= hi {
		return "", false
	}
	out := make([]byte, 0, len(lo)+1)
	// freeHi becomes true once our prefix is strictly below hi, after which hi
	// no longer bounds us from above. The end sentinel is free from the start.
	freeHi := hi == ""
	for i := 0; ; i++ {
		lc := -1 // byte of lo at i, or "below every byte" once lo is exhausted
		if i < len(lo) {
			lc = int(lo[i])
		}
		hc := 256 // byte of hi at i, or "above every byte" when unbounded above
		if !freeHi {
			if i >= len(hi) {
				// Tight against hi with hi exhausted: our prefix equals hi, so
				// anything appended exceeds it. Unreachable while lo < hi (it
				// needs hi to be a prefix of lo), but guard against looping.
				return "", false
			}
			hc = int(hi[i])
		}
		// The a..z bytes strictly between lc and hc available at this position.
		low := lc + 1
		if low < int(rankLow) {
			low = int(rankLow)
		}
		high := hc - 1
		if high > int(rankHigh) {
			high = int(rankHigh)
		}
		if low <= high {
			out = append(out, byte((low+high)/2))
			return string(out), true
		}
		// No room here: stay tight on lo (emit its byte) and descend a place.
		if lc < 0 {
			// lo is exhausted and nothing a..z fits above it: no rank exists.
			return "", false
		}
		out = append(out, byte(lc))
		if !freeHi && lc < hc {
			freeHi = true // our prefix is now strictly below hi's
		}
	}
}

// evenRanks returns n distinct 3-char a..z ranks in strictly ascending byte
// order, spread evenly across the a..z^3 space. It underlies both the
// NULL-rank backfill (applied in id order) and the full-context rebalance
// (applied in rank order) — respacing leaves huge gaps so subsequent
// rankBetween calls succeed without renumbering. n must be <= rankSpan; a
// v1 context never approaches 17,576 items (real clients paginate long
// before), and the caller sizes n from a live row count.
func evenRanks(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = rank3((i + 1) * rankSpan / (n + 1))
	}
	return out
}

// rank3 maps v in [0, rankSpan) to its 3-char base-26 a..z representation.
func rank3(v int) string {
	if v < 0 {
		v = 0
	}
	if v >= rankSpan {
		v = rankSpan - 1
	}
	return string([]byte{
		rankLow + byte(v/(26*26)),
		rankLow + byte((v/26)%26),
		rankLow + byte(v%26),
	})
}
