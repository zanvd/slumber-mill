package db

import (
	"strings"
	"unicode/utf8"
)

// Clip returns s as valid UTF-8, cut to at most n bytes without splitting a character.
// Invalid byte sequences become U+FFFD first.
func Clip(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= n {
		return s
	}
	if n <= 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
