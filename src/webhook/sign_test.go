package webhook

import (
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
)

const (
	unixSigned    = 1700000000
	testTolerance = 5 * time.Minute
)

var (
	signedAt   = time.Unix(unixSigned, 0)
	testSecret = []byte("whsec_known-answer-secret-not-a-real-key")
	testBody   = []byte(`{"id":"0190a2b4-7c3e-7a52-9d41-5b6f8e0c1a23","type":"asset.ready"}`)

	// These digests come from openssl, not from this package:
	//   printf '%s' '<t>.<body>' | openssl dgst -sha256 -hmac '<secret>'
	knownAnswerDigest      = "8f8466f250a8cb5b3cb2f04b364a394898b38af1a398215591f39a72d700de2b"
	knownAnswerEmptyDigest = "cfec8f9369b3835d3d2207438734fc0a0bed0e60f21612f172a35d454247f9ea"
	knownAnswerOtherDigest = "5257eacace791f80f3dcc04807c026fbcd24ff40392624ae70c71d13825ed055"

	sentinels = []error{ErrEmptySecret, ErrMalformedSignature, ErrTimestampOutOfTolerance, ErrSignatureMismatch}
)

func validSig() string {
	return hex.EncodeToString(mac(testSecret, unixSigned, testBody))
}

func sigFor(key string, t int64, body []byte) string {
	return hex.EncodeToString(mac([]byte(key), t, body))
}

func headerWith(t string, v1s ...string) string {
	parts := []string{"t=" + t}
	for _, v := range v1s {
		parts = append(parts, "v1="+v)
	}
	return strings.Join(parts, ",")
}

func TestSignKnownAnswer(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		at     time.Time
		body   string
		want   string
	}{
		{"json body", string(testSecret), signedAt, string(testBody),
			"t=1700000000,v1=" + knownAnswerDigest},
		{"empty body", string(testSecret), signedAt, "",
			"t=1700000000,v1=" + knownAnswerEmptyDigest},
		{"other secret and time", "whsec_another-known-answer-secret-0123456", time.Unix(1893456000, 0), `{"a":1}`,
			"t=1893456000,v1=" + knownAnswerOtherDigest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Sign(secret.NewValue([]byte(tc.secret)), tc.at, []byte(tc.body))
			if err != nil {
				t.Fatalf("Sign() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("Sign() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSignTimestamp(t *testing.T) {
	want := "t=1700000000,v1=" + knownAnswerDigest
	tests := []struct {
		name string
		at   time.Time
	}{
		{"whole second", signedAt},
		{"sub-second part is dropped", signedAt.Add(999 * time.Millisecond)},
		{"other location", signedAt.In(time.FixedZone("UTC+9", 9*60*60))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Sign(secret.NewValue(testSecret), tc.at, testBody)
			if err != nil {
				t.Fatalf("Sign() error = %v", err)
			}
			if got != want {
				t.Errorf("Sign() = %q, want %q", got, want)
			}
		})
	}
}

func TestEmptySecret(t *testing.T) {
	t.Run("sign zero value", func(t *testing.T) {
		if _, err := Sign(secret.Value{}, signedAt, testBody); !errors.Is(err, ErrEmptySecret) {
			t.Errorf("Sign() error = %v, want ErrEmptySecret", err)
		}
	})
	t.Run("sign empty bytes", func(t *testing.T) {
		if _, err := Sign(secret.NewValue(nil), signedAt, testBody); !errors.Is(err, ErrEmptySecret) {
			t.Errorf("Sign() error = %v, want ErrEmptySecret", err)
		}
	})
	for _, key := range [][]byte{nil, {}} {
		t.Run("verify", func(t *testing.T) {
			header := headerWith("1700000000", hex.EncodeToString(mac(key, unixSigned, testBody)))
			if err := Verify(key, header, testBody, signedAt, testTolerance); !errors.Is(err, ErrEmptySecret) {
				t.Errorf("Verify() error = %v, want ErrEmptySecret", err)
			}
		})
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"json", testBody},
		{"empty", nil},
		{"dots and commas", []byte("1700000000.v1=ab,t=1")},
		{"binary", []byte{0, 1, 2, 0xff, 0xfe, '\n', 0}},
		{"large", []byte(strings.Repeat("x", 1<<20))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header, err := Sign(secret.NewValue(testSecret), signedAt, tc.body)
			if err != nil {
				t.Fatalf("Sign() error = %v", err)
			}
			if err := Verify(testSecret, header, tc.body, signedAt.Add(time.Minute), testTolerance); err != nil {
				t.Errorf("Verify(%q) error = %v", header, err)
			}
		})
	}
}

func TestVerifyRotation(t *testing.T) {
	good := validSig()
	oldA := sigFor("whsec_retired-one", unixSigned, testBody)
	oldB := sigFor("whsec_retired-two", unixSigned, testBody)
	const ts = "1700000000"
	tests := []struct {
		name   string
		header string
		want   error
	}{
		{"only", headerWith(ts, good), nil},
		{"first of two", headerWith(ts, good, oldA), nil},
		{"last of two", headerWith(ts, oldA, good), nil},
		{"middle of three", headerWith(ts, oldA, good, oldB), nil},
		{"last of three", headerWith(ts, oldA, oldB, good), nil},
		{"same value twice", headerWith(ts, good, good), nil},
		{"t after the signatures", "v1=" + oldA + ",v1=" + good + ",t=" + ts, nil},
		{"t between the signatures", "v1=" + oldA + ",t=" + ts + ",v1=" + good, nil},
		{"unknown scheme before", "t=" + ts + ",v0=" + oldA + ",v1=" + good, nil},
		{"unknown scheme after", "t=" + ts + ",v1=" + good + ",v2=not-even-hex", nil},
		{"unknown scheme with = in its value", "t=" + ts + ",v1=" + good + ",v9=a=b", nil},
		{"none match", headerWith(ts, oldA, oldB), ErrSignatureMismatch},
		{"match only under an unknown scheme", "t=" + ts + ",v1=" + oldA + ",v0=" + good, ErrSignatureMismatch},
		{"match for another timestamp", headerWith("1700000001", good, oldA), ErrSignatureMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertVerify(t, tc.header, testBody, tc.want)
		})
	}
}

func TestVerifyRejects(t *testing.T) {
	good := validSig()
	tests := []struct {
		name   string
		key    []byte
		header string
		body   []byte
		now    time.Time
		want   error
	}{
		{"tampered body", testSecret, headerWith("1700000000", good), []byte(string(testBody) + " "), signedAt, ErrSignatureMismatch},
		{"truncated body", testSecret, headerWith("1700000000", good), testBody[:len(testBody)-1], signedAt, ErrSignatureMismatch},
		{"empty body", testSecret, headerWith("1700000000", good), nil, signedAt, ErrSignatureMismatch},
		{"wrong secret", []byte("whsec_some-other-secret"), headerWith("1700000000", good), testBody, signedAt, ErrSignatureMismatch},
		{"prefix of the secret", testSecret[:len(testSecret)-1], headerWith("1700000000", good), testBody, signedAt, ErrSignatureMismatch},
		{"timestamp changed under the signature", testSecret, headerWith("1700000001", good), testBody, signedAt, ErrSignatureMismatch},
		{"all-zero signature", testSecret, headerWith("1700000000", strings.Repeat("0", 64)), testBody, signedAt, ErrSignatureMismatch},
		{"older than the tolerance", testSecret, headerWith("1700000000", good), testBody, signedAt.Add(time.Hour), ErrTimestampOutOfTolerance},
		{"further in the future than the tolerance", testSecret, headerWith("1700000000", good), testBody, signedAt.Add(-time.Hour), ErrTimestampOutOfTolerance},
		{"now is the zero time", testSecret, headerWith("1700000000", good), testBody, time.Time{}, ErrTimestampOutOfTolerance},
		{"stale and forged is a mismatch", testSecret, headerWith("1700000000", good), testBody[1:], signedAt.Add(time.Hour), ErrSignatureMismatch},
		{"timestamp at the int64 limit", testSecret,
			headerWith(strconv.FormatInt(math.MaxInt64, 10), sigFor(string(testSecret), math.MaxInt64, testBody)),
			testBody, signedAt, ErrTimestampOutOfTolerance},
		{"timestamp zero", testSecret, headerWith("0", sigFor(string(testSecret), 0, testBody)), testBody, signedAt, ErrTimestampOutOfTolerance},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(tc.key, tc.header, tc.body, tc.now, testTolerance)
			assertError(t, err, tc.want)
		})
	}
}

func TestVerifyToleranceBoundaries(t *testing.T) {
	header := headerWith("1700000000", validSig())
	tests := []struct {
		name      string
		now       time.Time
		tolerance time.Duration
		want      error
	}{
		{"now is the timestamp", signedAt, testTolerance, nil},
		{"exactly the tolerance after", signedAt.Add(testTolerance), testTolerance, nil},
		{"a nanosecond past the tolerance after", signedAt.Add(testTolerance + time.Nanosecond), testTolerance, ErrTimestampOutOfTolerance},
		{"a second past the tolerance after", signedAt.Add(testTolerance + time.Second), testTolerance, ErrTimestampOutOfTolerance},
		{"a nanosecond within the tolerance after", signedAt.Add(testTolerance - time.Nanosecond), testTolerance, nil},
		{"exactly the tolerance before", signedAt.Add(-testTolerance), testTolerance, nil},
		{"a nanosecond past the tolerance before", signedAt.Add(-testTolerance - time.Nanosecond), testTolerance, ErrTimestampOutOfTolerance},
		{"a second past the tolerance before", signedAt.Add(-testTolerance - time.Second), testTolerance, ErrTimestampOutOfTolerance},
		{"a nanosecond within the tolerance before", signedAt.Add(-testTolerance + time.Nanosecond), testTolerance, nil},
		{"zero tolerance, same instant", signedAt, 0, nil},
		{"zero tolerance, a nanosecond after", signedAt.Add(time.Nanosecond), 0, ErrTimestampOutOfTolerance},
		{"zero tolerance, a nanosecond before", signedAt.Add(-time.Nanosecond), 0, ErrTimestampOutOfTolerance},
		{"negative tolerance accepts nothing", signedAt, -time.Second, ErrTimestampOutOfTolerance},
		{"negative tolerance, minimum duration", signedAt, math.MinInt64, ErrTimestampOutOfTolerance},
		{"maximum tolerance", signedAt.Add(24 * time.Hour), math.MaxInt64, nil},
		{"other location", signedAt.Add(testTolerance).In(time.FixedZone("UTC-5", -5*60*60)), testTolerance, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(testSecret, header, testBody, tc.now, tc.tolerance)
			assertError(t, err, tc.want)
		})
	}
}

func TestVerifyMaximumToleranceRejectsTimestampAtInt64Limit(t *testing.T) {
	header := headerWith(strconv.FormatInt(math.MaxInt64, 10), sigFor(string(testSecret), math.MaxInt64, testBody))
	err := Verify(testSecret, header, testBody, signedAt, math.MaxInt64)
	assertError(t, err, ErrTimestampOutOfTolerance)
}

func TestVerifyMalformedHeaders(t *testing.T) {
	for _, tc := range malformedHeaders() {
		t.Run(tc.name, func(t *testing.T) {
			err := Verify(testSecret, tc.header, testBody, signedAt, testTolerance)
			assertError(t, err, ErrMalformedSignature)
		})
	}
}

func TestVerifyAcceptsUppercaseHex(t *testing.T) {
	assertVerify(t, headerWith("1700000000", strings.ToUpper(validSig())), testBody, nil)
}

func TestErrorsDoNotLeak(t *testing.T) {
	good := validSig()
	other := sigFor("whsec_some-other-secret", unixSigned, testBody)
	headers := []string{headerWith("1700000000", other), headerWith("1700000000", good), "garbage", headerWith("-1", good)}
	for _, tc := range malformedHeaders() {
		headers = append(headers, tc.header)
	}
	bodies := [][]byte{testBody, []byte("tampered")}
	nows := []time.Time{signedAt, signedAt.Add(time.Hour)}
	for _, header := range headers {
		for _, body := range bodies {
			for _, now := range nows {
				err := Verify([]byte("whsec_some-other-secret"), header, body, now, testTolerance)
				if err == nil {
					continue
				}
				msg := err.Error()
				for _, leak := range []string{"whsec_some-other-secret", good, other, strings.ToUpper(good), strings.ToUpper(other)} {
					if strings.Contains(msg, leak) {
						t.Errorf("Verify(%q) error %q contains %q", header, msg, leak)
					}
				}
			}
		}
	}
}

func FuzzVerify(f *testing.F) {
	good := validSig()
	other := sigFor("whsec_some-other-secret", unixSigned, testBody)
	seeds := []string{
		headerWith("1700000000", good),
		headerWith("1700000000", other, good),
		headerWith("1700000000", good, other),
		"t=1700000000,v0=abc,v1=" + good,
		headerWith("1700000000", other),
		headerWith("1699999000", good),
		headerWith("1700001000", good),
		headerWith("9223372036854775807", good),
		headerWith("9223372036854775808", good),
	}
	for _, tc := range malformedHeaders() {
		seeds = append(seeds, tc.header)
	}
	for _, header := range seeds {
		f.Add(testSecret, header, testBody, int64(unixSigned), int64(testTolerance))
	}
	f.Add([]byte(nil), headerWith("1700000000", good), []byte(nil), int64(0), int64(0))
	f.Add(testSecret, headerWith("1700000000", good), testBody, int64(math.MinInt64), int64(math.MinInt64))
	f.Add(testSecret, headerWith("1700000000", good), testBody, int64(math.MaxInt64), int64(math.MaxInt64))
	f.Add(testSecret, headerWith("1700000000", good), testBody, int64(unixSigned), int64(-1))

	f.Fuzz(func(t *testing.T, key []byte, header string, body []byte, now, tol int64) {
		err := Verify(key, header, body, time.Unix(now, 0), time.Duration(tol))
		if err == nil {
			return
		}
		for _, s := range sentinels {
			if errors.Is(err, s) {
				return
			}
		}
		t.Errorf("Verify() error = %v, want one of the package's errors", err)
	})
}

type headerCase struct {
	name   string
	header string
}

func malformedHeaders() []headerCase {
	good := validSig()
	other := sigFor("whsec_some-other-secret", unixSigned, testBody)
	const ts = "1700000000"
	return []headerCase{
		{"empty", ""},
		{"not a header", "garbage"},
		{"a separator only", ","},
		{"an equals sign only", "="},
		{"missing t", "v1=" + good},
		{"missing v1", "t=" + ts},
		{"only unknown schemes", "t=" + ts + ",v0=" + good},
		{"v1 in a different case", "t=" + ts + ",V1=" + good},
		{"t in a different case", "T=" + ts + ",v1=" + good},
		{"empty t", headerWith("", good)},
		{"empty v1", headerWith(ts, "")},
		{"t without a value", "t,v1=" + good},
		{"v1 without a value", "t=" + ts + ",v1"},
		{"text t", headerWith("abc", good)},
		{"negative t", headerWith("-1700000000", good)},
		{"negative zero t", headerWith("-0", good)},
		{"plus sign t", headerWith("+1700000000", good)},
		{"decimal t", headerWith("1700000000.5", good)},
		{"exponent t", headerWith("1.7e9", good)},
		{"hex t", headerWith("0x65536c00", good)},
		{"leading zero t", headerWith("01700000000", good)},
		{"double zero t", headerWith("00", good)},
		{"t overflowing int64", headerWith("9223372036854775808", good)},
		{"huge t", headerWith(strings.Repeat("9", 200), good)},
		{"non-ASCII digits in t", headerWith("١٧٠٠٠٠٠٠٠٠", good)},
		{"t twice", "t=" + ts + ",t=" + ts + ",v1=" + good},
		{"t twice with different values", "t=" + ts + ",t=1700000001,v1=" + good},
		{"non-hex v1", headerWith(ts, strings.Repeat("z", 64))},
		{"v1 with one non-hex character", headerWith(ts, good[:63]+"g")},
		{"v1 too short", headerWith(ts, good[:62])},
		{"v1 too long", headerWith(ts, good+"00")},
		{"v1 an odd number of digits", headerWith(ts, good[:63])},
		{"v1 prefixed 0x", headerWith(ts, "0x"+good[:62])},
		{"v1 base64", headerWith(ts, "jyg7yV3yURjRXvLwOx1m2Zp5gqGpIdE7Tv1hJm+kqzI=")},
		{"bad v1 beside a good one", headerWith(ts, good, good[:60])},
		{"bad v1 before a good one", headerWith(ts, "zz", good)},
		{"bad v1 beside a good one, last", headerWith(ts, other, good, strings.Repeat("g", 64))},
		{"trailing separator", headerWith(ts, good) + ","},
		{"leading separator", "," + headerWith(ts, good)},
		{"doubled separator", "t=" + ts + ",,v1=" + good},
		{"semicolon separators", "t=" + ts + ";v1=" + good},
		{"space separators", "t=" + ts + " v1=" + good},
		{"element without an equals sign", headerWith(ts, good) + ",garbage"},
		{"element without a scheme", headerWith(ts, good) + ",=abc"},
		{"leading space", " " + headerWith(ts, good)},
		{"trailing space", headerWith(ts, good) + " "},
		{"space after a separator", "t=" + ts + ", v1=" + good},
		{"space before the equals sign", "t =" + ts + ",v1=" + good},
		{"space after the equals sign", "t= " + ts + ",v1=" + good},
		{"space inside t", headerWith("1700 000000", good)},
		{"space inside v1", headerWith(ts, good[:32]+" "+good[33:])},
		{"trailing newline", headerWith(ts, good) + "\n"},
		{"trailing carriage return", headerWith(ts, good) + "\r"},
		{"embedded newline", "t=" + ts + "\nv1=" + good},
		{"tab", "t=" + ts + ",\tv1=" + good},
		{"NUL byte", headerWith(ts, good) + "\x00"},
		{"DEL byte", headerWith(ts, good) + "\x7f"},
		{"non-ASCII in an unknown scheme", headerWith(ts, good) + ",v9=é"},
		{"invalid UTF-8", headerWith(ts, good) + ",v9=\xff"},
		{"non-breaking space", headerWith(ts, good) + "\u00a0"},
		{"two headers joined", headerWith(ts, good) + "," + headerWith(ts, other)},
		{"quoted values", `t="` + ts + `",v1="` + good + `"`},
	}
}

func assertVerify(t *testing.T, header string, body []byte, want error) {
	t.Helper()
	assertError(t, Verify(testSecret, header, body, signedAt, testTolerance), want)
}

func assertError(t *testing.T, got, want error) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("Verify() error = %v, want nil", got)
		}
		return
	}
	if !errors.Is(got, want) {
		t.Errorf("Verify() error = %v, want %v", got, want)
	}
	for _, other := range sentinels {
		if other != want && errors.Is(got, other) {
			t.Errorf("Verify() error = %v also matches %v", got, other)
		}
	}
}
