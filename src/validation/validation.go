package validation

import (
	"slices"
	"strings"
)

// constant can only be given as an untyped string constant. Field names and messages reach the tenant,
// so they are fixed phrases in the code, never built from an error or from the input.
type constant string

type Field struct {
	Name    string
	Message string
}

// Error reports one or more invalid fields, in the order they were added.
type Error struct {
	fields []Field
}

func (e *Error) Fields() []Field {
	return slices.Clone(e.fields)
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("invalid input")
	for i, f := range e.fields {
		if i == 0 {
			b.WriteString(": ")
		} else {
			b.WriteString("; ")
		}
		b.WriteString(f.Name)
		b.WriteString(": ")
		b.WriteString(f.Message)
	}
	return b.String()
}

// Collector gathers field errors. The zero value is ready to use.
type Collector struct {
	fields []Field
}

// Add records a field error. Only the first message for a field is kept.
func (c *Collector) Add(field, message constant) {
	if slices.ContainsFunc(c.fields, func(f Field) bool { return f.Name == string(field) }) {
		return
	}
	c.fields = append(c.fields, Field{Name: string(field), Message: string(message)})
}

// Err returns the fields added so far as an *Error, or nil if there are none.
func (c *Collector) Err() error {
	if len(c.fields) == 0 {
		return nil
	}
	return &Error{fields: slices.Clone(c.fields)}
}
