package secret

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestValueBytesIsACopy(t *testing.T) {
	src := []byte(probeSecret)
	v := NewValue(src)
	clear(src)
	if got := string(v.Bytes()); got != probeSecret {
		t.Fatalf("Bytes() = %q after the source changed, want %q", got, probeSecret)
	}

	b := v.Bytes()
	clear(b)
	if got := string(v.Bytes()); got != probeSecret {
		t.Fatalf("Bytes() = %q after its result changed, want %q", got, probeSecret)
	}
}

func TestZeroValue(t *testing.T) {
	var v Value
	if b := v.Bytes(); b != nil {
		t.Errorf("Bytes() = %q, want nil", b)
	}
	if s := fmt.Sprint(v); s != "[redacted]" {
		t.Errorf("Sprint() = %q, want [redacted]", s)
	}
}

func TestValueStringAndLogValue(t *testing.T) {
	v := NewValue([]byte(probeSecret))
	if got := v.String(); got != "[redacted]" {
		t.Errorf("String() = %q, want [redacted]", got)
	}
	if got := v.LogValue(); got.Kind() != slog.KindString || got.String() != "[redacted]" {
		t.Errorf("LogValue() = %v (%v), want the string [redacted]", got, got.Kind())
	}
}

func TestValueRedactedByFmt(t *testing.T) {
	v := NewValue([]byte(probeSecret))
	tests := []struct {
		name string
		arg  any
		want string // the whole output, if it is fixed for every verb but %p
	}{
		{"value", v, "[redacted]"},
		{"pointer", &v, "[redacted]"},
		{"exported field", struct{ V Value }{v}, ""},
		{"unexported field", struct{ v Value }{v}, ""},
		{"pointer to unexported field", &struct{ v Value }{v}, ""},
		{"unexported pointer field", struct{ p *Value }{&v}, ""},
		{"slice", []Value{v}, ""},
		{"map", map[string]Value{"k": v}, ""},
		{"interface slice", []any{v}, ""},
	}
	for _, tc := range tests {
		for _, verb := range fmtVerbs {
			t.Run(tc.name+" "+verb, func(t *testing.T) {
				out := fmt.Sprintf(verb, tc.arg)
				assertNoLeak(t, out, leakForms([]byte(probeSecret)))
				if tc.want != "" && verb != "%p" && out != tc.want {
					t.Errorf("Sprintf(%q) = %q, want %q", verb, out, tc.want)
				}
			})
		}
	}
}

func TestValueRedactedBySlog(t *testing.T) {
	v := NewValue([]byte(probeSecret))
	tests := []struct {
		name         string
		attr         slog.Attr
		wantRedacted bool
	}{
		{"value", slog.Any("secret", v), true},
		{"pointer", slog.Any("secret", &v), true},
		{"exported field", slog.Any("target", struct{ V Value }{v}), true},
		{"unexported field", slog.Any("target", struct{ v Value }{v}), false},
		{"slice", slog.Any("secrets", []Value{v}), true},
		{"group", slog.Group("tenant", "secret", v), true},
	}
	for _, tc := range tests {
		for _, h := range slogHandlers {
			t.Run(tc.name+" "+h.name, func(t *testing.T) {
				out := logAttr(t, h, tc.attr)
				assertNoLeak(t, out, leakForms([]byte(probeSecret)))
				if tc.wantRedacted && !strings.Contains(out, "[redacted]") {
					t.Errorf("output doesn't contain [redacted]:\n%s", out)
				}
			})
		}
	}
}

func TestValueRedactedByJSON(t *testing.T) {
	v := NewValue([]byte(probeSecret))
	tests := []struct {
		name string
		arg  any
		want string
	}{
		{"value", v, `"[redacted]"`},
		{"pointer", &v, `"[redacted]"`},
		{"exported field", struct{ V Value }{v}, `{"V":"[redacted]"}`},
		{"unexported field", struct{ v Value }{v}, `{}`},
		{"slice", []Value{v}, `["[redacted]"]`},
		{"map", map[string]Value{"k": v}, `{"k":"[redacted]"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := json.Marshal(tc.arg)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			if string(out) != tc.want {
				t.Errorf("Marshal() = %s, want %s", out, tc.want)
			}
		})
	}
}
