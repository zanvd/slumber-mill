package validation

import (
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func collect(fields ...Field) error {
	var c Collector
	for _, f := range fields {
		c.Add(constant(f.Name), constant(f.Message))
	}
	return c.Err()
}

func TestCollector(t *testing.T) {
	tests := []struct {
		name string
		add  []Field
		want []Field
	}{
		{name: "nothing added"},
		{
			name: "one field",
			add:  []Field{{"title", "is required"}},
			want: []Field{{"title", "is required"}},
		},
		{
			name: "fields keep the order they were added",
			add:  []Field{{"title", "is required"}, {"file", "is empty"}, {"idempotency_key", "is too long"}},
			want: []Field{{"title", "is required"}, {"file", "is empty"}, {"idempotency_key", "is too long"}},
		},
		{
			name: "first message for a field wins",
			add:  []Field{{"title", "is required"}, {"file", "is empty"}, {"title", "is too long"}},
			want: []Field{{"title", "is required"}, {"file", "is empty"}},
		},
		{
			name: "field names compared exactly",
			add:  []Field{{"title", "is required"}, {"Title", "is unknown"}},
			want: []Field{{"title", "is required"}, {"Title", "is unknown"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := collect(tc.add...)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Err() = %#v, want nil", err)
				}
				return
			}
			verr, ok := err.(*Error)
			if !ok {
				t.Fatalf("Err() = %#v, want *Error", err)
			}
			if got := verr.Fields(); !slices.Equal(got, tc.want) {
				t.Errorf("Fields() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestErrorString(t *testing.T) {
	tests := []struct {
		name   string
		fields []Field
		want   string
	}{
		{"no fields", nil, "invalid input"},
		{"one field", []Field{{"title", "is required"}}, "invalid input: title: is required"},
		{
			"several fields",
			[]Field{{"title", "is required"}, {"file", "is empty"}, {"idempotency_key", "is too long"}},
			"invalid input: title: is required; file: is empty; idempotency_key: is too long",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&Error{fields: tc.fields}).Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestErrorsAs(t *testing.T) {
	invalid := collect(Field{"title", "is required"})
	other := errors.New("disk full")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unwrapped", invalid, true},
		{"wrapped", fmt.Errorf("error creating asset: %w", invalid), true},
		{"wrapped twice", fmt.Errorf("error handling upload: %w", fmt.Errorf("error creating asset: %w", invalid)), true},
		{"joined", errors.Join(other, invalid), true},
		{"wrapped beside another error", fmt.Errorf("%w: %w", other, invalid), true},
		{"formatted without wrapping", fmt.Errorf("error creating asset: %v", invalid), false},
		{"other error", other, false},
		{"other error wrapped", fmt.Errorf("error creating asset: %w", other), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var want *Error
			if tc.want {
				want = invalid.(*Error)
			}
			var got *Error
			if ok := errors.As(tc.err, &got); ok != tc.want || got != want {
				t.Errorf("errors.As = %v, %p; want %v, %p", ok, got, tc.want, want)
			}
			if got, ok := errors.AsType[*Error](tc.err); ok != tc.want || got != want {
				t.Errorf("errors.AsType = %p, %v; want %p, %v", got, ok, want, tc.want)
			}
		})
	}
}

func TestErrorUnaffectedByLaterChanges(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Collector, *Error)
	}{
		{"field added to the collector", func(c *Collector, _ *Error) { c.Add("file", "is empty") }},
		{"returned fields edited", func(_ *Collector, e *Error) { e.Fields()[0].Message = "is fine" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var c Collector
			c.Add("title", "is required")
			e := c.Err().(*Error)
			tc.change(&c, e)
			if got, want := e.Fields(), []Field{{"title", "is required"}}; !slices.Equal(got, want) {
				t.Errorf("Fields() = %q, want %q", got, want)
			}
		})
	}
}

func TestAddTakesOnlyConstants(t *testing.T) {
	cmd := exec.Command("go", "list", "-export", "-f", "{{.ImportPath}} {{.Export}}", ".")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}
	path, export, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "gc", func(p string) (io.ReadCloser, error) {
		if p != path {
			return nil, fmt.Errorf("unexpected import %q", p)
		}
		return os.Open(export)
	})

	tests := []struct {
		name string
		body string
		ok   bool
	}{
		{"literals", `c.Add("title", "is required")`, true},
		{"untyped constants", `const f, m = "title", "is required"; c.Add(f, m)`, true},
		{"constant expression", `c.Add("title", "must be at most " + "255 characters")`, true},
		{"message from a variable", `m := "is required"; c.Add("title", m)`, false},
		{"message from an error", `c.Add("title", err.Error())`, false},
		{"message built from the input", `c.Add("title", "is not " + input)`, false},
		{"typed constant", `const m string = "is required"; c.Add("title", m)`, false},
		{"field from the input", `c.Add(input, "is unknown")`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := fmt.Sprintf("package p\nimport %q\nfunc f(c *validation.Collector, err error, input string) {\n%s\n}\n", path, tc.body)
			file, err := parser.ParseFile(fset, "p.go", src, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = (&types.Config{Importer: imp}).Check("p", fset, []*ast.File{file}, nil)
			switch {
			case tc.ok && err != nil:
				t.Errorf("type check failed: %v", err)
			case !tc.ok && (err == nil || !strings.Contains(err.Error(), "in argument to c.Add")):
				t.Errorf("type check error = %v, want one about the argument to c.Add", err)
			}
		})
	}
}
