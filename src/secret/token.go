package secret

import (
	"crypto/rand"
	"encoding/base32"
)

const tokenAlphabet = "abcdefghijklmnopqrstuvwxyz234567"

// NewAPIKey returns sm_ and 160 random bits in unpadded lower-case base32: 35 characters.
func NewAPIKey() Value {
	return newToken("sm_", 160/8)
}

// NewWebhookSecret returns whsec_ and 192 random bits in unpadded lower-case base32: 45 characters.
func NewWebhookSecret() Value {
	return newToken("whsec_", 192/8)
}

func newToken(prefix string, size int) Value {
	b := make([]byte, size)
	rand.Read(b)
	enc := base32.NewEncoding(tokenAlphabet).WithPadding(base32.NoPadding)
	return wrap(enc.AppendEncode([]byte(prefix), b))
}
