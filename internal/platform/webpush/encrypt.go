package webpush

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// RFC 8291 / RFC 8188 aes128gcm message encryption for Web Push.
//
// The receiver (user agent) hands us, at subscription time, its ECDH public
// key (p256dh, a 65-byte uncompressed P-256 point) and a 16-byte auth secret.
// For each message we mint an ephemeral ECDH key pair and a random 16-byte
// salt, run the RFC 8291 key-combining step to fold the shared secret with the
// auth secret, then the RFC 8188 content-encryption step, and emit the single
// aes128gcm record framed by the RFC 8188 header (salt, record size, and our
// ephemeral public key as the key id). The whole construction is pinned to
// RFC 8291 Appendix A byte-for-byte in encrypt_test.go — that vector is what
// makes hand-rolling this responsible.

const (
	saltLen = 16
	// recordSize is the RFC 8188 "rs" header field. Our payloads are a few
	// hundred bytes, so one record always suffices; 4096 matches the RFC 8291
	// Appendix A vector.
	recordSize = 4096
	// keyLabel is the RFC 8291 §3.4 key-combining label: the ASCII string
	// "WebPush: info" (NOT NUL-terminated) followed by a single zero octet.
	keyLabel = "WebPush: info\x00"
	// cekLabel / nonceLabel are the RFC 8188 §2.2 content-encoding info strings.
	cekLabel   = "Content-Encoding: aes128gcm\x00"
	nonceLabel = "Content-Encoding: nonce\x00"
)

// encrypt mints a fresh ephemeral key pair and salt, then seals plaintext for
// the subscription. Production path; the deterministic seal underneath is what
// the RFC vector test drives.
func encrypt(sub Subscription, plaintext []byte) ([]byte, error) {
	asPriv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("webpush: ephemeral key: %w", err)
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("webpush: salt: %w", err)
	}
	return seal(sub.P256dh, sub.Auth, plaintext, asPriv, salt)
}

// seal is the deterministic RFC 8291 core: given the receiver's public key and
// auth secret, an application-server (ephemeral) private key, and the message
// salt, it produces the full aes128gcm-encoded message. Randomness is the
// caller's (encrypt supplies fresh values; the vector test supplies the RFC's).
func seal(uaPublic, authSecret, plaintext []byte, asPriv *ecdh.PrivateKey, salt []byte) ([]byte, error) {
	uaPub, err := ecdh.P256().NewPublicKey(uaPublic)
	if err != nil {
		return nil, fmt.Errorf("webpush: receiver public key: %w", err)
	}
	ecdhSecret, err := asPriv.ECDH(uaPub)
	if err != nil {
		return nil, fmt.Errorf("webpush: ecdh: %w", err)
	}
	asPublic := asPriv.PublicKey().Bytes()

	cek, nonce, err := deriveKeys(ecdhSecret, authSecret, uaPublic, asPublic, salt)
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(cek)
	if err != nil {
		return nil, err
	}
	// RFC 8188 §2.1: the sole (last) record's plaintext is closed with the
	// 0x02 padding delimiter; no further padding is needed for a single record.
	record := make([]byte, 0, len(plaintext)+1)
	record = append(record, plaintext...)
	record = append(record, 0x02)
	ciphertext := gcm.Seal(nil, nonce, record, nil)

	// RFC 8188 §2.1 header: salt(16) || rs(4, big-endian) || idlen(1) ||
	// keyid(idlen). For Web Push the keyid is the ephemeral public key.
	out := make([]byte, 0, saltLen+4+1+len(asPublic)+len(ciphertext))
	out = append(out, salt...)
	var rs [4]byte
	binary.BigEndian.PutUint32(rs[:], recordSize)
	out = append(out, rs[:]...)
	out = append(out, byte(len(asPublic)))
	out = append(out, asPublic...)
	out = append(out, ciphertext...)
	return out, nil
}

// Decrypt reverses seal with the receiver's PRIVATE key + auth secret. The
// server never needs it (it only sends), but it is the exact inverse the RFC
// 8291 vector round-trips against and the way a test — or a future Go client —
// reads a payload back. body is the full aes128gcm message from seal.
func Decrypt(uaPrivate, authSecret, body []byte) ([]byte, error) {
	if len(body) < saltLen+4+1 {
		return nil, fmt.Errorf("webpush: message too short")
	}
	salt := body[:saltLen]
	idlen := int(body[saltLen+4])
	off := saltLen + 4 + 1
	if len(body) < off+idlen {
		return nil, fmt.Errorf("webpush: truncated key id")
	}
	asPublic := body[off : off+idlen]
	ciphertext := body[off+idlen:]

	uaPriv, err := ecdh.P256().NewPrivateKey(uaPrivate)
	if err != nil {
		return nil, fmt.Errorf("webpush: receiver private key: %w", err)
	}
	asPub, err := ecdh.P256().NewPublicKey(asPublic)
	if err != nil {
		return nil, fmt.Errorf("webpush: sender public key: %w", err)
	}
	ecdhSecret, err := uaPriv.ECDH(asPub)
	if err != nil {
		return nil, fmt.Errorf("webpush: ecdh: %w", err)
	}
	cek, nonce, err := deriveKeys(ecdhSecret, authSecret, uaPriv.PublicKey().Bytes(), asPublic, salt)
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(cek)
	if err != nil {
		return nil, err
	}
	record, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("webpush: decrypt: %w", err)
	}
	return unpad(record)
}

// deriveKeys runs the RFC 8291 §3.4 key-combining step (fold the ECDH secret
// with the auth secret, keyed by both public keys) and then the RFC 8188 §2.2
// content-encoding step (the message salt) to yield the AES-128 key and the
// GCM nonce. It is side-symmetric: the key_info order (ua_public then
// as_public) is the same whichever party derives it.
func deriveKeys(ecdhSecret, authSecret, uaPublic, asPublic, salt []byte) (cek, nonce []byte, err error) {
	keyInfo := make([]byte, 0, len(keyLabel)+len(uaPublic)+len(asPublic))
	keyInfo = append(keyInfo, keyLabel...)
	keyInfo = append(keyInfo, uaPublic...)
	keyInfo = append(keyInfo, asPublic...)

	ikm := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ecdhSecret, authSecret, keyInfo), ikm); err != nil {
		return nil, nil, fmt.Errorf("webpush: ikm: %w", err)
	}
	cek = make([]byte, 16)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(cekLabel)), cek); err != nil {
		return nil, nil, fmt.Errorf("webpush: cek: %w", err)
	}
	nonce = make([]byte, 12)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(nonceLabel)), nonce); err != nil {
		return nil, nil, fmt.Errorf("webpush: nonce: %w", err)
	}
	return cek, nonce, nil
}

func newGCM(cek []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(cek)
	if err != nil {
		return nil, fmt.Errorf("webpush: aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("webpush: gcm: %w", err)
	}
	return gcm, nil
}

// unpad strips the RFC 8188 padding: trailing zero octets then the single
// delimiter (0x02 last record, 0x01 otherwise).
func unpad(record []byte) ([]byte, error) {
	i := len(record)
	for i > 0 && record[i-1] == 0x00 {
		i--
	}
	if i == 0 {
		return nil, fmt.Errorf("webpush: missing padding delimiter")
	}
	if d := record[i-1]; d != 0x01 && d != 0x02 {
		return nil, fmt.Errorf("webpush: bad padding delimiter 0x%02x", d)
	}
	return record[:i-1], nil
}
