package secret

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// probeSecret is the secret the redaction tests look for in everything they print.
const probeSecret = "whsec_mv2gk43uozqwy5lfon2gk43uozqwy5lfon2gk43"

var fmtVerbs = []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%o", "%b", "%p"}

// leakForms returns the ways fmt, log/slog and encoding/json can print b.
func leakForms(b []byte) []string {
	return append(printedForms(b), string(b), base64.StdEncoding.EncodeToString(b))
}

// printedForms returns how fmt prints the elements of s under each numeric verb, without the brackets around them.
func printedForms[E byte | uint32](s []E) []string {
	var forms []string
	for _, verb := range []string{"%v", "%x", "%X", "%o", "%b", "%#v"} {
		f := fmt.Sprintf(verb, s)
		f = f[strings.LastIndexAny(f, "[{")+1:]
		forms = append(forms, strings.TrimRight(f, "]}"))
	}
	return forms
}

func assertNoLeak(t *testing.T, out string, forms []string) {
	t.Helper()
	for _, f := range forms {
		if strings.Contains(out, f) {
			t.Fatalf("output contains %q:\n%s", f, out)
		}
	}
}

type slogHandler struct {
	name string
	new  func(io.Writer) slog.Handler
}

var slogHandlers = []slogHandler{
	{"text", func(w io.Writer) slog.Handler { return slog.NewTextHandler(w, nil) }},
	{"json", func(w io.Writer) slog.Handler { return slog.NewJSONHandler(w, nil) }},
}

func logAttr(t *testing.T, h slogHandler, attr slog.Attr) string {
	t.Helper()
	var buf bytes.Buffer
	slog.New(h.new(&buf)).LogAttrs(t.Context(), slog.LevelInfo, "message", attr)
	return buf.String()
}
