package webpush

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"time"
)

// RFC 8292 VAPID: the application server authenticates to the push service
// with a signed JWT (ES256 over P-256) plus its public key. aud is the push
// endpoint's origin, exp is bounded (we use 12h; the RFC caps it at 24h), and
// sub is a contact URI (mailto:/https:) the push operator can reach.

// vapidTTL bounds the JWT lifetime. RFC 8292 requires exp within 24h; 12h
// leaves generous clock skew while keeping a leaked token short-lived.
const vapidTTL = 12 * time.Hour

type jwtHeader struct {
	Typ string `json:"typ"`
	Alg string `json:"alg"`
}

type jwtClaims struct {
	Aud string `json:"aud"`
	Exp int64  `json:"exp"`
	Sub string `json:"sub"`
}

// authorization builds the RFC 8292 "vapid" Authorization header value for a
// push to endpoint: `vapid t=<jwt>,k=<public key>`. now is a parameter so the
// test can pin exp.
func (s *Sender) authorization(endpoint string, now time.Time) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("webpush: endpoint: %w", err)
	}
	aud := u.Scheme + "://" + u.Host
	jwt, err := signES256(s.vapidPriv, jwtClaims{
		Aud: aud,
		Exp: now.Add(vapidTTL).Unix(),
		Sub: s.subject,
	})
	if err != nil {
		return "", err
	}
	return "vapid t=" + jwt + ",k=" + b64.EncodeToString(s.vapidPubRaw), nil
}

// b64 is base64url without padding — the JOSE and Web Push encoding.
var b64 = base64.RawURLEncoding

// signES256 produces a compact ES256 JWT. The signature is the JOSE fixed-width
// r||s form (each 32 bytes), NOT the ASN.1 form crypto/ecdsa emits by default.
func signES256(priv *ecdsa.PrivateKey, claims jwtClaims) (string, error) {
	head, err := json.Marshal(jwtHeader{Typ: "JWT", Alg: "ES256"})
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signingInput := b64.EncodeToString(head) + "." + b64.EncodeToString(body)
	digest := sha256.Sum256([]byte(signingInput))
	r, sig, err := ecdsa.Sign(rand.Reader, priv, digest[:])
	if err != nil {
		return "", fmt.Errorf("webpush: sign: %w", err)
	}
	jose := make([]byte, 64)
	r.FillBytes(jose[:32])
	sig.FillBytes(jose[32:])
	return signingInput + "." + b64.EncodeToString(jose), nil
}

// parseVAPIDPrivate turns a raw 32-byte P-256 scalar into an ecdsa private key
// WITHOUT the deprecated crypto/elliptic scalar-mult path: crypto/ecdh
// validates the scalar and yields the matching public point, which we split
// into the ecdsa key's coordinates.
func parseVAPIDPrivate(raw []byte) (*ecdsa.PrivateKey, error) {
	ek, err := ecdh.P256().NewPrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("webpush: vapid private key: %w", err)
	}
	pub := ek.PublicKey().Bytes() // 0x04 || X(32) || Y(32)
	return &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{
			Curve: elliptic.P256(),
			X:     new(big.Int).SetBytes(pub[1:33]),
			Y:     new(big.Int).SetBytes(pub[33:65]),
		},
		D: new(big.Int).SetBytes(raw),
	}, nil
}

// GenerateVAPIDKeys returns a fresh base64url (raw) P-256 key pair: the public
// key is the 65-byte uncompressed point, the private key the 32-byte scalar.
// weftd's gen-vapid-keys subcommand prints these for an operator to configure.
func GenerateVAPIDKeys() (publicKey, privateKey string, err error) {
	k, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("webpush: generate: %w", err)
	}
	return b64.EncodeToString(k.PublicKey().Bytes()), b64.EncodeToString(k.Bytes()), nil
}
