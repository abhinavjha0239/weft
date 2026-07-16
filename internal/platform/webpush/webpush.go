// Package webpush implements Web Push (RFC 8030/8291/8292) in-house — no new
// dependency (VAPID rides stdlib crypto/ecdsa, the aes128gcm payload rides
// stdlib AES-GCM + crypto/ecdh + x/crypto/hkdf, already in the module graph).
//
// The one thing hand-rolled crypto demands is a known-answer test: the payload
// encryption is pinned to RFC 8291 Appendix A byte-for-byte (encrypt_test.go),
// and the VAPID JWT is parsed and verified in vapid_test.go. A Sender is built
// once from the configured VAPID key pair; the push lane calls Push per live
// subscription. FCM/APNs bridges would be later Sender-like seams (recorded
// gap); today every endpoint is a standard Web Push service reached through the
// SSRF-guarded egress client, because the endpoint URL is user-registered.
package webpush

import (
	"crypto/ecdsa"
	"fmt"
	"strings"
	"time"
)

// ContentType is the RFC 8188 media type for an aes128gcm-encoded body. The
// push lane passes it to egress.PostRaw (whose default application/json is
// wrong for an encrypted blob).
const ContentType = "application/octet-stream"

// KeyLen and AuthLen are the exact byte lengths of a Web Push subscription's
// key material: p256dh is a 65-byte uncompressed P-256 point, auth is 16 bytes
// (RFC 8291 §3.2). The subscription API length-checks against these.
const (
	KeyLen  = 65
	AuthLen = 16
)

// Subscription is one browser push registration's crypto material. Endpoint is
// the (capability) URL the encrypted body is POSTed to.
type Subscription struct {
	Endpoint string
	P256dh   []byte
	Auth     []byte
}

// Sender carries the server's VAPID identity. It is safe for concurrent use
// (it holds only immutable key material).
type Sender struct {
	vapidPriv   *ecdsa.PrivateKey
	vapidPubRaw []byte
	subject     string
}

// NewSender parses the configured base64url (raw) VAPID key pair and the
// contact subject. It fails if either key is malformed or the subject is not a
// mailto:/https: URI (RFC 8292 §2.1). An empty key pair means push is
// unconfigured — the caller (config) decides to run without a Sender rather
// than pass empty strings here.
func NewSender(publicKey, privateKey, subject string) (*Sender, error) {
	pub, err := b64.DecodeString(publicKey)
	if err != nil {
		return nil, fmt.Errorf("webpush: vapid public key: %w", err)
	}
	if len(pub) != KeyLen {
		return nil, fmt.Errorf("webpush: vapid public key must be %d bytes, got %d", KeyLen, len(pub))
	}
	priv, err := b64.DecodeString(privateKey)
	if err != nil {
		return nil, fmt.Errorf("webpush: vapid private key: %w", err)
	}
	ec, err := parseVAPIDPrivate(priv)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(subject, "mailto:") && !strings.HasPrefix(subject, "https:") {
		return nil, fmt.Errorf("webpush: subject must be a mailto: or https: URI")
	}
	return &Sender{vapidPriv: ec, vapidPubRaw: pub, subject: subject}, nil
}

// Push encrypts plaintext for sub and returns the request body plus the RFC
// 8291/8292 headers to POST it: the aes128gcm body, TTL, Urgency, the
// Content-Encoding, and the per-endpoint VAPID Authorization. The caller sends
// it through egress.PostRaw with ContentType.
func (s *Sender) Push(sub Subscription, plaintext []byte) (body []byte, headers map[string]string, err error) {
	body, err = encrypt(sub, plaintext)
	if err != nil {
		return nil, nil, err
	}
	auth, err := s.authorization(sub.Endpoint, time.Now())
	if err != nil {
		return nil, nil, err
	}
	headers = map[string]string{
		"TTL":              "86400",
		"Urgency":          "normal",
		"Content-Encoding": "aes128gcm",
		"Authorization":    auth,
	}
	return body, headers, nil
}

// PublicKey returns the base64url VAPID public key clients need to subscribe
// (served by GET /api/v1/push/vapid-key).
func (s *Sender) PublicKey() string { return b64.EncodeToString(s.vapidPubRaw) }
