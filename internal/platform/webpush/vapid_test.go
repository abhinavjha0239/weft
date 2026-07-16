package webpush

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

// TestVAPIDAuthorization: the RFC 8292 Authorization header parses as a
// well-formed ES256 JWT, its signature verifies under the advertised public
// key, and aud/exp/sub are correct. Verifying with the SAME public key the
// header carries is what a push service does.
func TestVAPIDAuthorization(t *testing.T) {
	pub, priv, err := GenerateVAPIDKeys()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	s, err := NewSender(pub, priv, "mailto:ops@example.test")
	if err != nil {
		t.Fatalf("sender: %v", err)
	}

	now := time.Unix(1_700_000_000, 0)
	header, err := s.authorization("https://push.example.test/send/abc123?x=1", now)
	if err != nil {
		t.Fatalf("authorization: %v", err)
	}

	// Shape: `vapid t=<jwt>,k=<key>`, and k is the server's public key.
	rest, ok := strings.CutPrefix(header, "vapid ")
	if !ok {
		t.Fatalf("missing vapid scheme: %q", header)
	}
	tPart, kPart, ok := strings.Cut(rest, ",")
	if !ok || !strings.HasPrefix(tPart, "t=") || !strings.HasPrefix(kPart, "k=") {
		t.Fatalf("bad param shape: %q", rest)
	}
	jwt := strings.TrimPrefix(tPart, "t=")
	if key := strings.TrimPrefix(kPart, "k="); key != pub {
		t.Fatalf("k= %q, want the VAPID public key %q", key, pub)
	}

	// Decode and check the header + claims.
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("jwt has %d parts, want 3", len(parts))
	}
	var h jwtHeader
	if err := json.Unmarshal(mustB64(t, parts[0]), &h); err != nil {
		t.Fatalf("header: %v", err)
	}
	if h.Typ != "JWT" || h.Alg != "ES256" {
		t.Fatalf("header = %+v, want JWT/ES256", h)
	}
	var c jwtClaims
	if err := json.Unmarshal(mustB64(t, parts[1]), &c); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if c.Aud != "https://push.example.test" {
		t.Fatalf("aud = %q, want the endpoint origin only", c.Aud)
	}
	if c.Sub != "mailto:ops@example.test" {
		t.Fatalf("sub = %q", c.Sub)
	}
	if want := now.Add(12 * time.Hour).Unix(); c.Exp != want {
		t.Fatalf("exp = %d, want %d (now+12h)", c.Exp, want)
	}

	// Verify the ES256 signature with the advertised public key.
	kb := mustB64(t, pub)
	pk := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(kb[1:33]),
		Y:     new(big.Int).SetBytes(kb[33:65]),
	}
	sig := mustB64(t, parts[2])
	if len(sig) != 64 {
		t.Fatalf("signature is %d bytes, want 64 (JOSE r||s)", len(sig))
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	r := new(big.Int).SetBytes(sig[:32])
	sv := new(big.Int).SetBytes(sig[32:])
	if !ecdsa.Verify(pk, digest[:], r, sv) {
		t.Fatal("ES256 signature did not verify under the VAPID public key")
	}
}

// TestNewSenderValidation: malformed keys and a non-mailto/https subject are
// refused (an operator gets a clear config error, not a silent bad Sender).
func TestNewSenderValidation(t *testing.T) {
	pub, priv, err := GenerateVAPIDKeys()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if _, err := NewSender(pub, priv, "ops@example.test"); err == nil {
		t.Fatal("bare email subject accepted, want error")
	}
	if _, err := NewSender("not-base64!!", priv, "mailto:x@y.test"); err == nil {
		t.Fatal("bad public key accepted, want error")
	}
	if _, err := NewSender(pub, "AAAA", "mailto:x@y.test"); err == nil {
		t.Fatal("short private key accepted, want error")
	}
	if _, err := NewSender(pub, priv, "https://example.test/contact"); err != nil {
		t.Fatalf("https subject rejected: %v", err)
	}
}
