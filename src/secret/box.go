package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	keySize = 32
	// sealedOverhead is the key ID, the 12-byte nonce and the 16-byte tag.
	sealedOverhead = 1 + 12 + 16
)

var (
	errEmptyRing   = errors.New("no keys")
	errEntryFormat = errors.New("want <id>:<base64 key>")
	errKeyID       = errors.New("the key ID must be a number from 0 to 255")
	errDuplicateID = errors.New("duplicate key ID")
	errKeyEncoding = errors.New("the key isn't valid padded standard base64")
	errKeySize     = errors.New("the key must be 32 bytes")

	errTooShort       = errors.New("too short")
	errUnknownKey     = errors.New("no key in the ring has ID")
	errAuthentication = errors.New("message authentication failed")
)

// Box seals secrets with AES-256-GCM under the first key in its ring, and opens them under any key in it.
// A sealed value is the key ID, a random 12-byte nonce, then the ciphertext and its 16-byte tag.
type Box struct {
	sealID byte
	// A closure rather than a map field: fmt prints fields by reflection,
	// and under %s that reaches the AES key schedule.
	aead func(id byte) cipher.AEAD
}

// NewBox parses a key ring, <id>:<base64 key>[,...], where each ID is a number from 0 to 255
// and each key is 32 bytes in padded standard base64.
func NewBox(keyRing string) (*Box, error) {
	if keyRing == "" {
		return nil, fmt.Errorf("error parsing key ring: %w", errEmptyRing)
	}
	var sealID byte
	aeads := make(map[byte]cipher.AEAD)
	for i, entry := range strings.Split(keyRing, ",") {
		id, aead, err := parseKey(entry)
		if err != nil {
			return nil, fmt.Errorf("error parsing key ring entry %d: %w", i+1, err)
		}
		if _, ok := aeads[id]; ok {
			return nil, fmt.Errorf("error parsing key ring entry %d: %w %d", i+1, errDuplicateID, id)
		}
		if i == 0 {
			sealID = id
		}
		aeads[id] = aead
	}
	return &Box{sealID: sealID, aead: func(id byte) cipher.AEAD { return aeads[id] }}, nil
}

// parseKey's errors never quote the entry, which holds the key.
func parseKey(entry string) (byte, cipher.AEAD, error) {
	idText, keyText, ok := strings.Cut(entry, ":")
	if !ok {
		return 0, nil, errEntryFormat
	}
	id, err := strconv.ParseUint(idText, 10, 8)
	if err != nil {
		return 0, nil, errKeyID
	}
	key, err := base64.StdEncoding.Strict().DecodeString(keyText)
	if err != nil {
		return 0, nil, errKeyEncoding
	}
	if len(key) != keySize {
		return 0, nil, fmt.Errorf("%w, got %d", errKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return 0, nil, err
	}
	return byte(id), aead, nil
}

// Seal encrypts v under the ring's first key, bound to aad.
func (b *Box) Seal(v Value, aad []byte) []byte {
	plaintext := v.Bytes()
	sealed := make([]byte, 1, sealedOverhead+len(plaintext))
	sealed[0] = b.sealID
	return b.aead(b.sealID).Seal(sealed, nil, plaintext, aad)
}

// Open decrypts a sealed value under the key whose ID it carries, if aad is what it was sealed with.
func (b *Box) Open(sealed, aad []byte) (Value, error) {
	if len(sealed) < sealedOverhead {
		return Value{}, fmt.Errorf("error opening sealed secret: %w", errTooShort)
	}
	aead := b.aead(sealed[0])
	if aead == nil {
		return Value{}, fmt.Errorf("error opening sealed secret: %w %d", errUnknownKey, sealed[0])
	}
	plaintext, err := aead.Open(nil, nil, sealed[1:], aad)
	if err != nil {
		return Value{}, fmt.Errorf("error opening sealed secret: %w", errAuthentication)
	}
	return wrap(plaintext), nil
}
