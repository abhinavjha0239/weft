package files

import (
	"context"
	"io"
)

// Upload malware-scan seam (P-19, ADR-012 F-7). The core speaks only Scanner;
// which engine reads the bytes (clamav, an ICAP proxy, a cloud API) is an
// operator choice added later as one driver + a config case — exactly the
// blob.Store / mail.Sender pattern. No real driver exists yet, so the seam
// ships with a test double only and weftd wires no scanner: uploads then stay
// scan_status 0 (pending), never a fake "clean".

// Verdict is a scanner's ruling on one file. The values equal the file
// table's scan_status codes so a scanned upload records the verdict directly.
type Verdict int16

const (
	// Clean → scan_status 1: the bytes may be referenced and served.
	Clean Verdict = 1
	// Quarantined → scan_status 2: the row and bytes are kept (evidence for
	// compliance/holds) but no reference can form and every read 404s.
	Quarantined Verdict = 2
)

// Scanner inspects an uploaded file's bytes and returns a verdict. r delivers
// the fully-spooled content (already size-capped); implementations must be
// safe for concurrent use. A non-nil error fails the upload CLOSED — no row,
// no blob — so a scanner outage never admits unscanned bytes.
type Scanner interface {
	Scan(ctx context.Context, name, mime string, r io.Reader) (Verdict, error)
}

// SetScanner wires the scan seam at composition (the SetSigningSecret pattern).
// Optional: with no scanner, uploads are recorded scan_status 0 (pending).
func (s *Service) SetScanner(sc Scanner) { s.scanner = sc }
