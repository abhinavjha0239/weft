package mail

import (
	"strings"
	"testing"
)

// TestBuildSMTPTextOnly: with no HTML the driver emits a bare text/plain
// message and none of the multipart or List-Unsubscribe machinery — the
// original behavior, preserved.
func TestBuildSMTPTextOnly(t *testing.T) {
	got, err := buildSMTP("from@x.test", Message{
		To: "to@x.test", Subject: "Hello", Text: "plain body",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s := string(got)
	for _, want := range []string{
		"From: from@x.test\r\n",
		"To: to@x.test\r\n",
		"Subject: Hello\r\n",
		"Content-Type: text/plain; charset=utf-8\r\n\r\n",
		"plain body",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("text-only missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "multipart/alternative") || strings.Contains(s, "text/html") {
		t.Fatalf("text-only must not be multipart:\n%s", s)
	}
	if strings.Contains(s, "List-Unsubscribe") {
		t.Fatalf("no List-Unsubscribe header without ListUnsubscribe set:\n%s", s)
	}
}

// TestBuildSMTPMultipart: an HTML alternative yields multipart/alternative
// with the text/plain part FIRST (RFC 2046), both bodies present, and the RFC
// 8058 one-click headers when requested.
func TestBuildSMTPMultipart(t *testing.T) {
	got, err := buildSMTP("from@x.test", Message{
		To: "to@x.test", Subject: "Hi", Text: "plain part", HTML: "<p>rich part</p>",
		ListUnsubscribe: "https://host/api/v1/unsubscribe?o=1&u=2&sig=ab", ListUnsubscribePost: true,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, "Content-Type: multipart/alternative; boundary=\"") {
		t.Fatalf("expected multipart/alternative:\n%s", s)
	}
	// text/plain must be declared before text/html (least-rich first).
	textIdx := strings.Index(s, "Content-Type: text/plain")
	htmlIdx := strings.Index(s, "Content-Type: text/html")
	if textIdx < 0 || htmlIdx < 0 || textIdx > htmlIdx {
		t.Fatalf("text part must precede html part (text=%d html=%d):\n%s", textIdx, htmlIdx, s)
	}
	for _, want := range []string{
		"plain part", "<p>rich part</p>",
		"List-Unsubscribe: <https://host/api/v1/unsubscribe?o=1&u=2&sig=ab>\r\n",
		"List-Unsubscribe-Post: List-Unsubscribe=One-Click\r\n",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("multipart missing %q:\n%s", want, s)
		}
	}
}

// TestBuildSMTPListUnsubscribeWithoutPost: the RFC 8058 companion rides only
// when ListUnsubscribePost is set; the RFC 2369 header can appear alone.
func TestBuildSMTPListUnsubscribeWithoutPost(t *testing.T) {
	got, err := buildSMTP("from@x.test", Message{
		To: "to@x.test", Subject: "Hi", Text: "t",
		ListUnsubscribe: "https://host/u", ListUnsubscribePost: false,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, "List-Unsubscribe: <https://host/u>\r\n") {
		t.Fatalf("expected List-Unsubscribe header:\n%s", s)
	}
	if strings.Contains(s, "List-Unsubscribe-Post") {
		t.Fatalf("must not emit One-Click companion without ListUnsubscribePost:\n%s", s)
	}
}

// TestBuildSMTPHeaderInjection: CRLF smuggled into any header value is
// flattened to spaces, so an attacker-controlled subject/recipient/unsubscribe
// URL cannot inject additional headers.
func TestBuildSMTPHeaderInjection(t *testing.T) {
	got, err := buildSMTP("from@x.test", Message{
		To:              "to@x.test\r\nBcc: evil@x.test",
		Subject:         "Subject\r\nX-Injected: yes",
		Text:            "body",
		ListUnsubscribe: "https://host/u\r\nX-Evil: 1",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s := string(got)
	// Neutralization means the smuggled text never becomes its OWN header
	// line: a CRLF in a value is flattened to spaces, so it stays on the
	// value's line. Assert no LINE starts a forged header (the naive
	// substring check would false-positive on the flattened value itself).
	for _, line := range strings.Split(s, "\r\n") {
		for _, bad := range []string{"Bcc:", "X-Injected:", "X-Evil:"} {
			if strings.HasPrefix(line, bad) {
				t.Fatalf("header injection survived — %q begins a forged header:\n%s", line, s)
			}
		}
	}
	// The recipient value is preserved on ONE line (each CRLF became spaces).
	if !strings.Contains(s, "To: to@x.test  Bcc: evil@x.test\r\n") {
		t.Fatalf("expected flattened To header:\n%s", s)
	}
}
