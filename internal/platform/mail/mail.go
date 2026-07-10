// Package mail is the outbound-email seam (the blob.Store pattern): the
// core backend speaks ONLY Sender — which relay carries the bytes is an
// operator choice, not a code change. Adding a provider (SES API, Mailgun,
// …) = one file implementing Sender + one case in Open; the SMTP driver
// already covers every provider with an SMTP endpoint.
package mail

import (
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
)

type Sender interface {
	// Send delivers one plain-text message. Implementations must be safe
	// for concurrent use.
	Send(to, subject, body string) error
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

func (s *logSender) Send(to, subject, body string) error {
	s.log.Info("mail (log driver)", "to", to, "subject", subject, "bytes", len(body))
	return nil
}

type smtpSender struct {
	addr, from string
	auth       smtp.Auth
}

func (s *smtpSender) Send(to, subject, body string) error {
	// Header injection is blocked by construction: to/subject are
	// sanitized to a single line each.
	msg := strings.Join([]string{
		"From: " + s.from,
		"To: " + sanitizeHeader(to),
		"Subject: " + sanitizeHeader(subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"",
		body,
	}, "\r\n")
	return smtp.SendMail(s.addr, s.auth, s.from, []string{to}, []byte(msg))
}

func sanitizeHeader(v string) string {
	v = strings.ReplaceAll(v, "\r", " ")
	return strings.ReplaceAll(v, "\n", " ")
}
