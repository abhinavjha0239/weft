// Package mail is the outbound-email seam (the blob.Store pattern): the
// core backend speaks ONLY Sender — which relay carries the bytes is an
// operator choice, not a code change. Adding a provider (SES API, Mailgun,
// …) = one file implementing Sender + one case in Open; the SMTP driver
// already covers every provider with an SMTP endpoint.
package mail

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
)

// Message is one outbound email. Text is the plain-text body and is ALWAYS
// present; when HTML is non-empty the message is multipart/alternative with
// the text part first (widest client compatibility). ListUnsubscribe, when
// set, emits the RFC 2369 header, and ListUnsubscribePost adds the RFC 8058
// one-click companion — together they render the mail client's native
// "Unsubscribe" affordance.
type Message struct {
	To                  string
	Subject             string
	Text                string
	HTML                string
	ListUnsubscribe     string
	ListUnsubscribePost bool
}

type Sender interface {
	// Send delivers one message. Implementations must be safe for concurrent
	// use.
	Send(m Message) error
}

// Open constructs the configured driver.
//
//	driver "log" (default): writes to the log, never sends — the dev and
//	  test posture; email silently "working" against a real relay in dev
//	  is how test mail escapes to real inboxes.
//	driver "smtp": addr "host:port", from, and optionally user/pass for
//	  PLAIN auth (STARTTLS negotiated by net/smtp when offered).
func Open(driver, addr, from, user, pass string, log *slog.Logger) (Sender, error) {
	switch driver {
	case "", "log":
		return &logSender{log: log}, nil
	case "smtp":
		if addr == "" || from == "" {
			return nil, fmt.Errorf("mail: smtp driver needs addr and from")
		}
		host := addr
		if i := strings.IndexByte(addr, ':'); i > 0 {
			host = addr[:i]
		}
		var auth smtp.Auth
		if user != "" {
			auth = smtp.PlainAuth("", user, pass, host)
		}
		return &smtpSender{addr: addr, from: from, auth: auth}, nil
	default:
		return nil, fmt.Errorf("mail: unknown driver %q (implement Sender and register it here)", driver)
	}
}

type logSender struct{ log *slog.Logger }

func (s *logSender) Send(m Message) error {
	s.log.Info("mail (log driver)", "to", m.To, "subject", m.Subject,
		"text_bytes", len(m.Text), "html_bytes", len(m.HTML))
	return nil
}

type smtpSender struct {
	addr, from string
	auth       smtp.Auth
}

func (s *smtpSender) Send(m Message) error {
	body, err := buildSMTP(s.from, m)
	if err != nil {
		return err
	}
	return smtp.SendMail(s.addr, s.auth, s.from, []string{m.To}, body)
}

// buildSMTP assembles the RFC 5322 bytes. Header injection is blocked by
// construction: every header value is flattened to a single line. With no
// HTML it is a bare text/plain message (unchanged from the original driver);
// with HTML it is multipart/alternative, text/plain FIRST per RFC 2046 (least
// rich alternative first). The boundary is crypto-random so it can never
// collide with body content. The List-Unsubscribe headers ride only when
// ListUnsubscribe is set.
func buildSMTP(from string, m Message) ([]byte, error) {
	headers := []string{
		"From: " + sanitizeHeader(from),
		"To: " + sanitizeHeader(m.To),
		"Subject: " + sanitizeHeader(m.Subject),
		"MIME-Version: 1.0",
	}
	if m.ListUnsubscribe != "" {
		headers = append(headers, "List-Unsubscribe: <"+sanitizeHeader(m.ListUnsubscribe)+">")
		if m.ListUnsubscribePost {
			headers = append(headers, "List-Unsubscribe-Post: List-Unsubscribe=One-Click")
		}
	}

	var b strings.Builder
	if m.HTML == "" {
		headers = append(headers, "Content-Type: text/plain; charset=utf-8")
		b.WriteString(strings.Join(headers, "\r\n"))
		b.WriteString("\r\n\r\n")
		b.WriteString(m.Text)
		return []byte(b.String()), nil
	}

	boundary, err := randomBoundary()
	if err != nil {
		return nil, err
	}
	headers = append(headers, `Content-Type: multipart/alternative; boundary="`+boundary+`"`)
	b.WriteString(strings.Join(headers, "\r\n"))
	b.WriteString("\r\n\r\n")
	b.WriteString("--" + boundary + "\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
	b.WriteString(m.Text)
	b.WriteString("\r\n--" + boundary + "\r\n")
	b.WriteString("Content-Type: text/html; charset=utf-8\r\n\r\n")
	b.WriteString(m.HTML)
	b.WriteString("\r\n--" + boundary + "--\r\n")
	return []byte(b.String()), nil
}

// randomBoundary is 16 crypto-random bytes as hex — no MIME special chars, so
// it needs no quoting and never appears in escaped body content.
func randomBoundary() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func sanitizeHeader(v string) string {
	v = strings.ReplaceAll(v, "\r", " ")
	return strings.ReplaceAll(v, "\n", " ")
}
