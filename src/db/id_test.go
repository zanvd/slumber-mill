package db

import (
	"bytes"
	"strings"
	"testing"
)

const canonicalID = "01890a5d-ac96-774b-bcce-b302099a8057"

func TestIDValueScanRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   ID
	}{
		{"new", NewID()},
		{"parsed", mustParseID(t, canonicalID)},
		{"zero", ID{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := tc.id.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			b, ok := v.([]byte)
			if !ok {
				t.Fatalf("Value() = %T, want []byte", v)
			}
			if !bytes.Equal(b, tc.id[:]) {
				t.Fatalf("Value() = %x, want %x", b, tc.id[:])
			}

			var got ID
			if err := got.Scan(b); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if got != tc.id {
				t.Errorf("Scan(Value()) = %s, want %s", got, tc.id)
			}
		})
	}
}

func TestIDScanCopiesSource(t *testing.T) {
	want := mustParseID(t, canonicalID)
	src := bytes.Clone(want[:])
	var id ID
	if err := id.Scan(src); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	clear(src)
	if id.String() != canonicalID {
		t.Errorf("ID changed with the driver's buffer: %s", id)
	}
}

func TestIDScanErrors(t *testing.T) {
	tests := []struct {
		name string
		src  any
	}{
		{"15 bytes", make([]byte, 15)},
		{"17 bytes", make([]byte, 17)},
		{"empty", []byte{}},
		{"NULL", nil},
		{"string", canonicalID},
		{"int64", int64(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := mustParseID(t, canonicalID)
			if err := id.Scan(tc.src); err == nil {
				t.Errorf("Scan(%v) error = nil, want an error", tc.src)
			}
			if id.String() != canonicalID {
				t.Errorf("failed Scan changed the ID to %s", id)
			}
		})
	}
}

func TestParseID(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"lower case", canonicalID, canonicalID, false},
		{"upper case", strings.ToUpper(canonicalID), canonicalID, false},
		{"mixed case", "01890A5D-ac96-774B-BCCE-b302099a8057", canonicalID, false},
		{"empty", "", "", true},
		{"too short", canonicalID[:35], "", true},
		{"not hex", "0189xa5d-ac96-774b-bcce-b302099a8057", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseID(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseID(%q) error = %v, wantErr %t", tc.in, err, tc.wantErr)
			}
			if err == nil && id.String() != tc.want {
				t.Errorf("ParseID(%q).String() = %q, want %q", tc.in, id.String(), tc.want)
			}
		})
	}
}

func TestNewIDIsVersion7(t *testing.T) {
	for range 100 {
		id := NewID()
		if v := id[6] >> 4; v != 7 {
			t.Fatalf("%s has version %d, want 7", id, v)
		}
		if variant := id[8] >> 6; variant != 0b10 {
			t.Fatalf("%s has variant %b, want 10", id, variant)
		}
		if s := id.String(); s != strings.ToLower(s) {
			t.Fatalf("String() = %q, want lower case", s)
		}
	}
}

func mustParseID(t *testing.T, s string) ID {
	t.Helper()
	id, err := ParseID(s)
	if err != nil {
		t.Fatalf("ParseID(%q) error = %v", s, err)
	}
	return id
}
