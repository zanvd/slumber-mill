package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
)

const SignatureHeader = "SlumberMill-Signature"

var (
	ErrEmptySecret             = errors.New("webhook: empty secret")
	ErrMalformedSignature      = errors.New("webhook: malformed signature header")
	ErrTimestampOutOfTolerance = errors.New("webhook: signature timestamp outside the tolerance")
	ErrSignatureMismatch       = errors.New("webhook: no matching signature")
)

// Sign returns the SignatureHeader value for body, signed at the given time.
func Sign(s secret.Value, at time.Time, body []byte) (string, error) {
	key := s.Bytes()
	defer clear(key)
	if len(key) == 0 {
		return "", ErrEmptySecret
	}
	t := at.Unix()
	return "t=" + strconv.FormatInt(t, 10) + ",v1=" + hex.EncodeToString(mac(key, t, body)), nil
}

// Verify checks a SignatureHeader value for body against the receiver's secret.
// The header is "t=<unix seconds>,v1=<hex>[,v1=<hex>...]" with no whitespace, in any order;
// unknown schemes are ignored, and any one matching v1 is enough.
// The signature is checked before the timestamp, so ErrTimestampOutOfTolerance means a genuine but stale
// or premature signature.
func Verify(key []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	if len(key) == 0 {
		return ErrEmptySecret
	}
	t, sigs, err := parseHeader(header)
	if err != nil {
		return err
	}
	want := mac(key, t, body)
	if !slices.ContainsFunc(sigs, func(sig []byte) bool { return hmac.Equal(sig, want) }) {
		return ErrSignatureMismatch
	}
	at := time.Unix(t, 0)
	d := now.Sub(at)
	if tolerance < 0 || at.Before(time.Unix(0, 0)) || d > tolerance || d < -tolerance {
		return ErrTimestampOutOfTolerance
	}
	return nil
}

func mac(key []byte, t int64, body []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(strconv.FormatInt(t, 10)))
	h.Write([]byte("."))
	h.Write(body)
	return h.Sum(nil)
}

func parseHeader(header string) (t int64, sigs [][]byte, err error) {
	if header == "" {
		return 0, nil, malformed("empty header")
	}
	for i := range len(header) {
		if c := header[i]; c <= ' ' || c >= 0x7f {
			return 0, nil, malformed("whitespace or non-printable character")
		}
	}
	haveT := false
	for part := range strings.SplitSeq(header, ",") {
		scheme, value, ok := strings.Cut(part, "=")
		if !ok || scheme == "" {
			return 0, nil, malformed("element is not scheme=value")
		}
		switch scheme {
		case "t":
			if haveT {
				return 0, nil, malformed("repeated t")
			}
			if t, err = parseTimestamp(value); err != nil {
				return 0, nil, err
			}
			haveT = true
		case "v1":
			sig, err := parseSignature(value)
			if err != nil {
				return 0, nil, err
			}
			sigs = append(sigs, sig)
		}
	}
	if !haveT {
		return 0, nil, malformed("missing t")
	}
	if len(sigs) == 0 {
		return 0, nil, malformed("missing v1")
	}
	return t, sigs, nil
}

// parseTimestamp accepts only the form Sign writes: decimal digits without a sign or leading zeros.
func parseTimestamp(s string) (int64, error) {
	if s == "" || (len(s) > 1 && s[0] == '0') || strings.ContainsFunc(s, func(r rune) bool { return r < '0' || r > '9' }) {
		return 0, malformed("t is not a unix timestamp")
	}
	t, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, malformed("t is out of range")
	}
	return t, nil
}

func parseSignature(s string) ([]byte, error) {
	if len(s) != 2*sha256.Size {
		return nil, malformed("v1 is not 64 hex characters")
	}
	sig, err := hex.DecodeString(s)
	if err != nil {
		return nil, malformed("v1 is not 64 hex characters")
	}
	return sig, nil
}

func malformed(reason string) error {
	return fmt.Errorf("%w: %s", ErrMalformedSignature, reason)
}
