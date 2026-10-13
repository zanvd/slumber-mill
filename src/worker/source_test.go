package worker

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanent(t *testing.T) {
	cause := errors.New("not audio")

	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"permanent", Permanent(cause), true},
		{"wrapped permanent", fmt.Errorf("error probing: %w", Permanent(cause)), true},
		{"joined permanent", errors.Join(errors.New("other"), Permanent(cause)), true},
		{"plain", cause, false},
		{"wrapped plain", fmt.Errorf("error probing: %w", cause), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanent(tc.err); got != tc.want {
				t.Errorf("IsPermanent(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestPermanentKeepsTheCause(t *testing.T) {
	cause := errors.New("not audio")
	err := Permanent(cause)
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(%v, cause) = false, want true", err)
	}
	if err.Error() != cause.Error() {
		t.Errorf("Permanent(cause).Error() = %q, want %q", err.Error(), cause.Error())
	}
	if !errors.Is(fmt.Errorf("x: %w", Permanent(ErrLeaseLost)), ErrLeaseLost) {
		t.Error("a permanent ErrLeaseLost doesn't match ErrLeaseLost")
	}
}
