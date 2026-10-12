package db

import (
	"database/sql/driver"
	"fmt"
	"uuid"
)

// ID is a UUID stored as BINARY(16).
type ID uuid.UUID

func NewID() ID {
	return ID(uuid.NewV7())
}

func ParseID(s string) (ID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return ID{}, err
	}
	return ID(u), nil
}

func (id ID) String() string {
	return uuid.UUID(id).String()
}

func (id ID) Value() (driver.Value, error) {
	return id[:], nil
}

func (id *ID) Scan(src any) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("error scanning ID: unsupported type %T", src)
	}
	if len(b) != len(id) {
		return fmt.Errorf("error scanning ID: got %d bytes, want %d", len(b), len(id))
	}
	copy(id[:], b)
	return nil
}
