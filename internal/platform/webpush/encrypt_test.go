package webpush

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

// The RFC 8291 Appendix A "Push Message Encryption Example" — every value is
// copied verbatim from the RFC (base64url, unpadded).
const (
	rfcPlaintext  = "V2hlbiBJIGdyb3cgdXAsIEkgd2FudCB0byBiZSBhIHdhdGVybWVsb24"
	rfcASPrivate  = "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw"
	rfcASPublic   = "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8"
	rfcUAPrivate  = "q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94"
	rfcUAPublic   = "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
	rfcSalt       = "DGv6ra1nlYgDCS1FRnbzlw"
	rfcAuthSecret = "BTBZMqHH6r4Tts7J_aSIgg"
	rfcECDHSecret = "kyrL1jIIOHEzg3sM2ZWRHDRB62YACZhhSlknJ672kSs"
	rfcCEK        = "oIhVW04MRdy2XN9CiKLxTg"
	rfcNonce      = "4h_95klXJ5E_qnoN"
	// The complete aes128gcm message (RFC 8188 header + ciphertext).
	rfcCiphertext = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN"
)

func mustB64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return b
}

// TestRFC8291Vector is the load-bearing known-answer test: our RFC 8291
// aes128gcm encryption must reproduce the RFC's Appendix A example
// byte-for-byte from its exact keys/salt/plaintext. It also pins the
// intermediate ECDH secret, CEK, and nonce so a regression names its layer.
// This vector is what makes hand-rolling the crypto responsible.
func TestRFC8291Vector(t *testing.T) {
	asPriv, err := ecdh.P256().NewPrivateKey(mustB64(t, rfcASPrivate))
	if err != nil {
		t.Fatalf("as private: %v", err)
	}
	uaPublic := mustB64(t, rfcUAPublic)
	asPublic := mustB64(t, rfcASPublic)
	authSecret := mustB64(t, rfcAuthSecret)
	salt := mustB64(t, rfcSalt)
	plaintext := mustB64(t, rfcPlaintext)

	// The ephemeral public key we emit as the RFC 8188 key id must match the
	// RFC's as_public (proves NewPrivateKey → PublicKey is the SEC1 form).
	if got := asPriv.PublicKey().Bytes(); !bytes.Equal(got, asPublic) {
		t.Fatalf("as_public\n got %x\nwant %x", got, asPublic)
	}

	// Intermediate: the ECDH shared secret.
	uaPub, err := ecdh.P256().NewPublicKey(uaPublic)
	if err != nil {
		t.Fatalf("ua public: %v", err)
	}
	ecdhSecret, err := asPriv.ECDH(uaPub)
	if err != nil {
		t.Fatalf("ecdh: %v", err)
	}
	if !bytes.Equal(ecdhSecret, mustB64(t, rfcECDHSecret)) {
		t.Fatalf("ecdh secret\n got %x\nwant %x", ecdhSecret, mustB64(t, rfcECDHSecret))
	}

	// Intermediate: the content-encryption key and nonce.
	cek, nonce, err := deriveKeys(ecdhSecret, authSecret, uaPublic, asPublic, salt)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	if !bytes.Equal(cek, mustB64(t, rfcCEK)) {
		t.Fatalf("CEK\n got %x\nwant %x", cek, mustB64(t, rfcCEK))
	}
	if !bytes.Equal(nonce, mustB64(t, rfcNonce)) {
		t.Fatalf("nonce\n got %x\nwant %x", nonce, mustB64(t, rfcNonce))
	}

	// The whole message, byte-for-byte.
	got, err := seal(uaPublic, authSecret, plaintext, asPriv, salt)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	want := mustB64(t, rfcCiphertext)
	if !bytes.Equal(got, want) {
		t.Fatalf("ciphertext mismatch\n got %s\nwant %s",
			base64.RawURLEncoding.EncodeToString(got), rfcCiphertext)
	}

	// And Decrypt is the true inverse: the receiver's private key recovers the
	// plaintext (this is exactly what the e2e test does with a subscription).
	back, err := Decrypt(mustB64(t, rfcUAPrivate), authSecret, got)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(back, plaintext) {
		t.Fatalf("round-trip\n got %q\nwant %q", back, plaintext)
	}
}

// TestEncryptRoundTrip: a freshly generated ephemeral key + random salt (the
// production path) still round-trips through Decrypt, and two encryptions of
// the same plaintext differ (fresh salt/ephemeral each time).
func TestEncryptRoundTrip(t *testing.T) {
	ua, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ua key: %v", err)
	}
	auth := []byte("0123456789abcdef") // 16 bytes
	sub := Subscription{P256dh: ua.PublicKey().Bytes(), Auth: auth}
	msg := []byte(`{"kind":2,"actor_name":"Alice"}`)

	one, err := encrypt(sub, msg)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	back, err := Decrypt(ua.Bytes(), auth, one)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(back, msg) {
		t.Fatalf("round-trip got %q want %q", back, msg)
	}
	two, err := encrypt(sub, msg)
	if err != nil {
		t.Fatalf("encrypt 2: %v", err)
	}
	if bytes.Equal(one, two) {
		t.Fatal("two encryptions identical — salt/ephemeral not fresh")
	}
}
