package db

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestClip(t *testing.T) {
	tests := []struct {
		name string
		s    string
		n    int
		want string
	}{
		{"short ASCII unchanged", "abc", 10, "abc"},
		{"short multibyte unchanged", "straße €🌙", 100, "straße €🌙"},
		{"exact length unchanged", "abc", 3, "abc"},
		{"ASCII cut", "abcdef", 4, "abcd"},
		{"inside a 2-byte character", "aé", 2, "a"},
		{"after a 2-byte character", "aéb", 3, "aé"},
		{"inside a 3-byte character", "a€", 3, "a"},
		{"inside a 4-byte character", "a🌙", 4, "a"},
		{"first character too long", "🌙", 3, ""},
		{"zero limit", "abc", 0, ""},
		{"negative limit", "abc", -1, ""},
		{"empty", "", 5, ""},
		{"invalid byte replaced", "a\xffb", 10, "a\uFFFDb"},
		{"truncated character replaced", "ab\xe2\x82", 10, "ab\uFFFD"},
		{"replacement cut whole", "a\xffb", 3, "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Clip(tc.s, tc.n); got != tc.want {
				t.Errorf("Clip(%q, %d) = %q, want %q", tc.s, tc.n, got, tc.want)
			}
		})
	}
}

func TestClipEveryLimit(t *testing.T) {
	inputs := []string{
		"plain ASCII text",
		"ffmpeg: straße, café, €100, 🌙 and 日本語",
		"🌙🌙🌙",
		"bad \xff\xfe bytes \xe2\x82 mid-text",
	}
	for _, s := range inputs {
		valid := strings.ToValidUTF8(s, "\uFFFD")
		for n := 0; n <= len(valid)+1; n++ {
			got := Clip(s, n)
			if !utf8.ValidString(got) {
				t.Fatalf("Clip(%q, %d) = %q, not valid UTF-8", s, n, got)
			}
			if len(got) > n {
				t.Fatalf("Clip(%q, %d) = %q, %d bytes", s, n, got, len(got))
			}
			if !strings.HasPrefix(valid, got) {
				t.Fatalf("Clip(%q, %d) = %q, not a prefix of %q", s, n, got, valid)
			}
			if n >= len(valid) && got != valid {
				t.Fatalf("Clip(%q, %d) = %q, want all of %q, which fits", s, n, got, valid)
			}
			if len(got) < n && len(got) < len(valid) {
				if r, size := utf8.DecodeRuneInString(valid[len(got):]); len(got)+size <= n {
					t.Fatalf("Clip(%q, %d) = %q, dropped %q that fits", s, n, got, r)
				}
			}
		}
	}
}
