package automation

// Outbound HTTP steps (P-24). An http_request step names a STATIC destination
// and optional headers; when the rule fires, execute() enqueues a
// webhook_delivery row (it never dials — that is the delivery lane's job, see
// runner.go). The destination is shape-checked here at definition time so an
// operator gets early feedback, but the load-bearing SSRF defence is the
// egress guard's pinned dialer at SEND time: the URL is never templated, so
// attacker-influenced text can never choose the destination.

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// Step kinds (Step.Kind).
const (
	stepPostMessage = "post_message"
	stepHTTPRequest = "http_request"
)

const (
	// maxHTTPSteps caps outbound calls per definition (blast-radius bound).
	maxHTTPSteps = 3
	// maxHTTPHeaders / maxHeaderValueLen bound one step's custom headers.
	maxHTTPHeaders    = 5
	maxHeaderValueLen = 512
)

// headerNameRe bounds a custom header name to token-safe characters.
var headerNameRe = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// senderHeaders are set by the sender (egress or the transport) and must never
// be author-controlled — matched case-insensitively.
var senderHeaders = map[string]bool{
	"host":              true,
	"content-length":    true,
	"content-type":      true,
	"transfer-encoding": true,
	"connection":        true,
	"user-agent":        true,
}

// validateHTTPStep checks an http_request step's destination and headers at
// write time. NO templating in a url, ever (attacker-chosen destinations are
// the SSRF-adjacent shape the guard exists to kill), the url passes the guard's
// static shape check, and every header name/value is token- and
// injection-safe. Authorization IS allowed — that is the auth use-case, and the
// definition is already scope-admin-gated (the Jira model).
func validateHTTPStep(i int, rawURL string, headers map[string]string) error {
	if rawURL == "" {
		return apperr.Invalid(fmt.Sprintf("definition: step %d: http_request requires a url", i))
	}
	if strings.Contains(rawURL, "{{") {
		return apperr.Invalid(fmt.Sprintf("definition: step %d: a url may not be templated", i))
	}
	if err := egress.VetURLShape(rawURL); err != nil {
		return apperr.Invalid(fmt.Sprintf("definition: step %d: %v", i, err))
	}
	if len(headers) > maxHTTPHeaders {
		return apperr.Invalid(fmt.Sprintf("definition: step %d: at most %d headers", i, maxHTTPHeaders))
	}
	for name, value := range headers {
		if !headerNameRe.MatchString(name) {
			return apperr.Invalid(fmt.Sprintf(
				"definition: step %d: header name %q must match ^[A-Za-z0-9-]{1,64}$", i, name))
		}
		if senderHeaders[strings.ToLower(name)] {
			return apperr.Invalid(fmt.Sprintf("definition: step %d: header %q is set by the sender", i, name))
		}
		if len([]rune(value)) > maxHeaderValueLen {
			return apperr.Invalid(fmt.Sprintf(
				"definition: step %d: header %q value exceeds %d chars", i, name, maxHeaderValueLen))
		}
		for _, r := range value {
			if r == '\r' || r == '\n' || r < 0x20 || r == 0x7f {
				return apperr.Invalid(fmt.Sprintf(
					"definition: step %d: header %q value has a control character", i, name))
			}
		}
	}
	return nil
}

// httpStepHeaders returns the custom headers of the first http_request step in
// def whose url matches. The delivery row stores only the url + event snapshot
// (the verbatim schema), so headers ride the rule's CURRENT definition at send
// — a rotated Authorization reaches pending deliveries. Nil when no such step
// remains (the step was edited away), in which case the send carries only the
// fixed sender headers.
func httpStepHeaders(def Definition, rawURL string) map[string]string {
	for _, st := range def.Steps {
		if st.Kind == stepHTTPRequest && st.URL == rawURL {
			return st.Headers
		}
	}
	return nil
}

// deliveryEnvelope is the FIXED, server-marshaled outbound body (no author body
// templating v1): the receiver gets the whole trigger event — strictly more
// than templates could extract — deleting the JSON-injection and template-SSRF
// classes outright. delivery_id is the dedupe key receivers key on
// (deliveries are at-least-once). Event is the snapshot stored on the row.
type deliveryEnvelope struct {
	AutomationID   int64           `json:"automation_id"`
	AutomationName string          `json:"automation_name"`
	RunID          int64           `json:"run_id"`
	DeliveryID     int64           `json:"delivery_id"`
	Attempt        int             `json:"attempt"`
	Event          json.RawMessage `json:"event"`
}
