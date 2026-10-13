package secret

import (
	"encoding/base32"
	"strings"
	"testing"
)

func TestTokens(t *testing.T) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	const n = 10_000
	tests := []struct {
		name       string
		generate   func() Value
		prefix     string
		length     int
		randomBits int
	}{
		{"API key", NewAPIKey, "sm_", 35, 160},
		{"webhook secret", NewWebhookSecret, "whsec_", 45, 192},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(map[string]bool, n)
			used := make(map[rune]bool, len(alphabet))
			for range n {
				token := string(tc.generate().Bytes())
				if len(token) != tc.length {
					t.Fatalf("%q has %d characters, want %d", token, len(token), tc.length)
				}
				body, ok := strings.CutPrefix(token, tc.prefix)
				if !ok {
					t.Fatalf("%q doesn't start with %q", token, tc.prefix)
				}
				for _, r := range body {
					if !strings.ContainsRune(alphabet, r) {
						t.Fatalf("%q has %q, which isn't in the lower-case base32 alphabet", token, r)
					}
					used[r] = true
				}
				raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(body))
				if err != nil {
					t.Fatalf("decoding %q: %v", token, err)
				}
				if len(raw)*8 != tc.randomBits {
					t.Fatalf("%q decodes to %d bits, want %d", token, len(raw)*8, tc.randomBits)
				}
				if seen[token] {
					t.Fatalf("%q was generated twice", token)
				}
				seen[token] = true
			}
			if len(used) != len(alphabet) {
				t.Errorf("%d tokens used %d of the alphabet's %d characters", n, len(used), len(alphabet))
			}
		})
	}
}
