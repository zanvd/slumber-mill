package secret

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	rowAAD   = "tenants/acme/webhook_secret"
	otherAAD = "tenants/globex/webhook_secret"
)

func testKey(seed byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func keyEntry(id int, key []byte) string {
	return strconv.Itoa(id) + ":" + base64.StdEncoding.EncodeToString(key)
}

func mustNewBox(t *testing.T, entries ...string) *Box {
	t.Helper()
	box, err := NewBox(strings.Join(entries, ","))
	if err != nil {
		t.Fatalf("NewBox() error = %v", err)
	}
	return box
}

func TestNewBox(t *testing.T) {
	k1, k2, k3 := testKey(0x10), testKey(0x40), testKey(0x70)
	tests := []struct {
		name       string
		entries    []string
		wantSealID byte
	}{
		{"one key", []string{keyEntry(1, k1)}, 1},
		{"several keys", []string{keyEntry(3, k3), keyEntry(1, k1), keyEntry(2, k2)}, 3},
		{"lowest ID", []string{keyEntry(0, k1)}, 0},
		{"highest ID", []string{keyEntry(255, k1), keyEntry(254, k2)}, 255},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			box := mustNewBox(t, tc.entries...)
			sealed := box.Seal(NewValue([]byte(probeSecret)), []byte(rowAAD))
			if sealed[0] != tc.wantSealID {
				t.Errorf("sealed under key ID %d, want %d", sealed[0], tc.wantSealID)
			}
			opened, err := box.Open(sealed, []byte(rowAAD))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			if got := string(opened.Bytes()); got != probeSecret {
				t.Errorf("Open() = %q, want %q", got, probeSecret)
			}
		})
	}
}

func TestNewBoxRefuses(t *testing.T) {
	key := testKey(0x10)
	b64 := base64.StdEncoding.EncodeToString(key)
	other := base64.StdEncoding.EncodeToString(testKey(0x40))
	sized := func(n int) string {
		return "1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xa5}, n))
	}
	tests := []struct {
		name      string
		ring      string
		wantEntry int // the entry the error names, if any
		want      error
	}{
		{"empty ring", "", 0, errEmptyRing},
		{"empty entry", "1:" + b64 + ",", 2, errEntryFormat},
		{"only a comma", ",", 1, errEntryFormat},
		{"no separator", b64, 1, errEntryFormat},
		{"no ID", ":" + b64, 1, errKeyID},
		{"ID not a number", "a:" + b64, 1, errKeyID},
		{"negative ID", "-1:" + b64, 1, errKeyID},
		{"ID over 255", "256:" + b64, 1, errKeyID},
		{"ID with a sign", "+1:" + b64, 1, errKeyID},
		{"hexadecimal ID", "0x1:" + b64, 1, errKeyID},
		{"space before an ID", "1:" + b64 + ", 2:" + other, 2, errKeyID},
		{"key and ID swapped", b64 + ":1", 1, errKeyID},
		{"duplicate ID", "1:" + b64 + ",1:" + other, 2, errDuplicateID},
		{"duplicate ID with a leading zero", "1:" + b64 + ",01:" + other, 2, errDuplicateID},
		{"invalid base64", "1:*" + b64[1:], 1, errKeyEncoding},
		{"URL-safe base64", "1:" + base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb}, 32)), 1, errKeyEncoding},
		{"unpadded base64", "1:" + base64.RawStdEncoding.EncodeToString(key), 1, errKeyEncoding},
		{"empty key", "1:", 1, errKeySize},
		{"16-byte key", sized(16), 1, errKeySize},
		{"24-byte key", sized(24), 1, errKeySize},
		{"31-byte key", sized(31), 1, errKeySize},
		{"33-byte key", sized(33), 1, errKeySize},
		{"short second key", "1:" + b64 + ",2:" + other[:40], 2, errKeySize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			box, err := NewBox(tc.ring)
			if !errors.Is(err, tc.want) {
				t.Fatalf("NewBox() error = %v, want %v", err, tc.want)
			}
			if box != nil {
				t.Errorf("NewBox() = %v, want nil", box)
			}
			msg := err.Error()
			if tc.wantEntry > 0 && !strings.Contains(msg, fmt.Sprintf("entry %d:", tc.wantEntry)) {
				t.Errorf("error %q doesn't name entry %d", msg, tc.wantEntry)
			}
			for i := 0; i+8 <= len(tc.ring); i++ {
				if w := tc.ring[i : i+8]; strings.Contains(msg, w) {
					t.Fatalf("error %q quotes the ring's %q", msg, w)
				}
			}
		})
	}
}

func TestBoxRoundTrip(t *testing.T) {
	box := mustNewBox(t, keyEntry(1, testKey(0x10)))
	tests := []struct {
		name string
		v    Value
	}{
		{"webhook secret", NewWebhookSecret()},
		{"API key", NewAPIKey()},
		{"empty", NewValue(nil)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sealed := box.Seal(tc.v, []byte(rowAAD))
			opened, err := box.Open(sealed, []byte(rowAAD))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			if !bytes.Equal(opened.Bytes(), tc.v.Bytes()) {
				t.Errorf("Open() = %q, want %q", opened.Bytes(), tc.v.Bytes())
			}
		})
	}
}

func TestBoxSealedFormat(t *testing.T) {
	key := testKey(0x10)
	box := mustNewBox(t, keyEntry(7, key))
	v := NewWebhookSecret()
	aad := []byte(rowAAD)
	sealed := box.Seal(v, aad)

	if len(v.Bytes()) != 45 || len(sealed) != 74 {
		t.Fatalf("sealed a %d-byte secret into %d bytes, want 45 into 74", len(v.Bytes()), len(sealed))
	}
	if sealed[0] != 7 {
		t.Errorf("key ID = %d, want 7", sealed[0])
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher() error = %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("NewGCM() error = %v", err)
	}
	plaintext, err := gcm.Open(nil, sealed[1:13], sealed[13:], aad)
	if err != nil {
		t.Fatalf("opening as key ID, nonce, then ciphertext and tag: %v", err)
	}
	if !bytes.Equal(plaintext, v.Bytes()) {
		t.Errorf("plaintext = %q, want %q", plaintext, v.Bytes())
	}
	if again := box.Seal(v, aad); bytes.Equal(again[1:13], sealed[1:13]) {
		t.Errorf("two seals used the same nonce %x", sealed[1:13])
	}
}

func TestBoxOpenRefuses(t *testing.T) {
	k1, k2 := testKey(0x10), testKey(0x40)
	box := mustNewBox(t, keyEntry(1, k1))
	sealed := box.Seal(NewValue([]byte(probeSecret)), []byte(rowAAD))

	type openCase struct {
		name   string
		box    *Box
		sealed []byte
		aad    string
		want   error
	}
	tests := []openCase{
		{"another tenant's row", box, sealed, otherAAD, errAuthentication},
		{"no additional data", box, sealed, "", errAuthentication},
		{"unknown key ID", mustNewBox(t, keyEntry(2, k2)), sealed, rowAAD, errUnknownKey},
		{"another key under the same ID", mustNewBox(t, keyEntry(1, k2)), sealed, rowAAD, errAuthentication},
		{"extended", box, append(bytes.Clone(sealed), 0), rowAAD, errAuthentication},
		{"nil", box, nil, rowAAD, errTooShort},
	}
	for i := range sealed {
		altered := bytes.Clone(sealed)
		altered[i] ^= 0x01
		want := errAuthentication
		if i == 0 {
			want = errUnknownKey
		}
		tests = append(tests, openCase{fmt.Sprintf("byte %d altered", i), box, altered, rowAAD, want})
	}
	for n := range len(sealed) {
		want := errAuthentication
		if n < sealedOverhead {
			want = errTooShort
		}
		tests = append(tests, openCase{fmt.Sprintf("truncated to %d bytes", n), box, sealed[:n:n], rowAAD, want})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.box.Open(tc.sealed, []byte(tc.aad))
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open() error = %v, want %v", err, tc.want)
			}
			if got.Bytes() != nil {
				t.Errorf("Open() = %q, want nothing", got.Bytes())
			}
			assertNoLeak(t, err.Error(), leakForms([]byte(probeSecret)))
		})
	}
}

func TestBoxKeyRotation(t *testing.T) {
	k1, k2 := testKey(0x10), testKey(0x40)
	v := NewValue([]byte(probeSecret))
	aad := []byte(rowAAD)
	before := mustNewBox(t, keyEntry(1, k1))
	sealed := before.Seal(v, aad)

	after := mustNewBox(t, keyEntry(2, k2), keyEntry(1, k1))
	opened, err := after.Open(sealed, aad)
	if err != nil {
		t.Fatalf("Open() under the rotated ring error = %v", err)
	}
	if got := string(opened.Bytes()); got != probeSecret {
		t.Errorf("Open() = %q, want %q", got, probeSecret)
	}

	resealed := after.Seal(v, aad)
	if resealed[0] != 2 {
		t.Errorf("the rotated ring sealed under key ID %d, want 2", resealed[0])
	}
	if _, err := before.Open(resealed, aad); !errors.Is(err, errUnknownKey) {
		t.Errorf("Open() under the old ring error = %v, want %v", err, errUnknownKey)
	}
}

func TestBoxOpensOnlyInItsOwnRowUnderAnyKey(t *testing.T) {
	entries := []string{keyEntry(1, testKey(0x10)), keyEntry(2, testKey(0x40)), keyEntry(3, testKey(0x70))}
	opener := mustNewBox(t, entries[2], entries[0], entries[1])
	v := NewValue([]byte(probeSecret))
	for i := range entries {
		t.Run(fmt.Sprintf("sealed under key %d", i+1), func(t *testing.T) {
			sealer := mustNewBox(t, slices.Concat(entries[i:], entries[:i])...)
			sealed := sealer.Seal(v, []byte(rowAAD))
			if sealed[0] != byte(i+1) {
				t.Fatalf("sealed under key ID %d, want %d", sealed[0], i+1)
			}

			opened, err := opener.Open(sealed, []byte(rowAAD))
			if err != nil {
				t.Fatalf("Open() in its own row error = %v", err)
			}
			if got := string(opened.Bytes()); got != probeSecret {
				t.Errorf("Open() = %q, want %q", got, probeSecret)
			}
			if _, err := opener.Open(sealed, []byte(otherAAD)); !errors.Is(err, errAuthentication) {
				t.Errorf("Open() in another row error = %v, want %v", err, errAuthentication)
			}
		})
	}
}

func TestBoxConcurrentUse(t *testing.T) {
	box := mustNewBox(t, keyEntry(1, testKey(0x10)))
	v := NewValue([]byte(probeSecret))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				opened, err := box.Open(box.Seal(v, []byte(rowAAD)), []byte(rowAAD))
				if err != nil || string(opened.Bytes()) != probeSecret {
					t.Errorf("Open(Seal()) = %q, %v", opened.Bytes(), err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestBoxRedacted(t *testing.T) {
	key := testKey(0xa0)
	box := mustNewBox(t, keyEntry(1, key))
	// AES-256's key schedule starts with the key itself, as 32-bit words.
	forms := slices.Concat(
		leakForms(key),
		printedForms([]uint32{binary.BigEndian.Uint32(key), binary.BigEndian.Uint32(key[4:])}),
		printedForms([]uint32{binary.LittleEndian.Uint32(key), binary.LittleEndian.Uint32(key[4:])}),
	)

	shapes := []struct {
		name string
		arg  any
	}{
		{"pointer", box},
		{"value", *box},
		{"unexported field", struct{ b Box }{*box}},
		{"unexported pointer field", struct{ b *Box }{box}},
	}
	for _, s := range shapes {
		for _, verb := range fmtVerbs {
			t.Run(s.name+" "+verb, func(t *testing.T) {
				assertNoLeak(t, fmt.Sprintf(verb, s.arg), forms)
			})
		}
		for _, h := range slogHandlers {
			t.Run(s.name+" "+h.name, func(t *testing.T) {
				assertNoLeak(t, logAttr(t, h, slog.Any("box", s.arg)), forms)
			})
		}
	}
}
