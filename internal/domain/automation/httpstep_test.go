package automation

import (
	"strings"
	"testing"
)

func TestValidateHTTPStep(t *testing.T) {
	ok := []struct {
		name    string
		url     string
		headers map[string]string
	}{
		{"bare https", "https://example.com/hook", nil},
		{"http standard port", "http://example.com/hook", nil},
		{"explicit 443", "https://example.com:443/hook", nil},
		{"auth header allowed", "https://example.com/h", map[string]string{"Authorization": "Bearer tok"}},
		{"custom header", "https://example.com/h", map[string]string{"X-Custom-Sig": "abc"}},
	}
	for _, tc := range ok {
		if err := validateHTTPStep(0, tc.url, tc.headers); err != nil {
			t.Errorf("%s: validateHTTPStep = %v, want nil", tc.name, err)
		}
	}
	bad := []struct {
		name    string
		url     string
		headers map[string]string
	}{
		{"empty url", "", nil},
		{"templated url", "https://example.com/{{event.x}}", nil},
		{"templated host", "https://{{event.host}}/hook", nil},
		{"bad scheme", "ftp://example.com/", nil},
		{"userinfo", "https://user:pass@example.com/", nil},
		{"odd port", "https://example.com:6379/", nil},
		{"bad header name", "https://example.com/", map[string]string{"X Bad": "v"}},
		{"sender header host", "https://example.com/", map[string]string{"Host": "evil"}},
		{"sender header ua mixed case", "https://example.com/", map[string]string{"User-Agent": "x"}},
		{"sender header content-type", "https://example.com/", map[string]string{"Content-Type": "text/plain"}},
		{"crlf value", "https://example.com/", map[string]string{"X-A": "v\r\nX-B: injected"}},
		{"control value", "https://example.com/", map[string]string{"X-A": "v\x01"}},
		{"long value", "https://example.com/", map[string]string{"X-A": strings.Repeat("a", 513)}},
	}
	for _, tc := range bad {
		if err := validateHTTPStep(0, tc.url, tc.headers); err == nil {
			t.Errorf("%s: validateHTTPStep = nil, want error", tc.name)
		}
	}
	// The header-count cap is its own case (map literals above stay small).
	six := map[string]string{}
	for _, k := range []string{"X-A", "X-B", "X-C", "X-D", "X-E", "X-F"} {
		six[k] = "v"
	}
	if err := validateHTTPStep(0, "https://example.com/", six); err == nil {
		t.Error("six headers: validateHTTPStep = nil, want error")
	}
}
