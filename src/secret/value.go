package secret

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
)

const redacted = "[redacted]"

// Value holds a secret. It prints as [redacted] through fmt, log/slog and encoding/json;
// only Bytes returns the secret itself.
type Value struct {
	// A closure rather than a []byte field: fmt prints fields by reflection when it can't call Format,
	// as for a Value in another type's unexported field, and %s would print a pointed-to slice's bytes.
	reveal func() []byte
}

func NewValue(b []byte) Value {
	return wrap(bytes.Clone(b))
}

// wrap takes ownership of b.
func wrap(b []byte) Value {
	return Value{reveal: func() []byte { return bytes.Clone(b) }}
}

// Bytes returns a copy of the secret.
func (v Value) Bytes() []byte {
	if v.reveal == nil {
		return nil
	}
	return v.reveal()
}

func (Value) String() string {
	return redacted
}

// Format prints [redacted] for every verb, so %x, %d and %#v don't fall back to the fields.
func (Value) Format(f fmt.State, _ rune) {
	io.WriteString(f, redacted)
}

func (Value) LogValue() slog.Value {
	return slog.StringValue(redacted)
}

func (Value) MarshalText() ([]byte, error) {
	return []byte(redacted), nil
}
