package db

import (
	"errors"
	"slices"
	"testing"
	"testing/fstest"
)

func TestLoadMigrations(t *testing.T) {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }

	tests := []struct {
		name    string
		files   fstest.MapFS
		want    []string
		wantErr bool
	}{
		{
			name: "sorted by version",
			files: fstest.MapFS{
				"migrations/10_later.sql":  file("C"),
				"migrations/0002_add.sql":  file("B"),
				"migrations/0001_init.sql": file("A"),
			},
			want: []string{"0001_init.sql", "0002_add.sql", "10_later.sql"},
		},
		{
			name:    "repeated version",
			files:   fstest.MapFS{"migrations/0001_a.sql": file("A"), "migrations/1_b.sql": file("B")},
			wantErr: true,
		},
		{"no prefix", fstest.MapFS{"migrations/init.sql": file("A")}, nil, true},
		{"version zero", fstest.MapFS{"migrations/0000_init.sql": file("A")}, nil, true},
		{"not a number", fstest.MapFS{"migrations/v1_init.sql": file("A")}, nil, true},
		{"wrong extension", fstest.MapFS{"migrations/0001_init.txt": file("A")}, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadMigrations(tc.files)
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadMigrations() error = %v, wantErr %t", err, tc.wantErr)
			}
			var names []string
			for _, m := range got {
				names = append(names, m.name)
			}
			if !slices.Equal(names, tc.want) {
				t.Errorf("loadMigrations() = %v, want %v", names, tc.want)
			}
		})
	}
}

func TestEmbeddedMigrations(t *testing.T) {
	migrations, err := loadMigrations(migrationFiles)
	if err != nil {
		t.Fatalf("loadMigrations() error = %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no embedded migrations")
	}
	if first := migrations[0]; first.name != "0001_init.sql" || first.version != 1 || first.sql == "" {
		t.Errorf("first migration is %s, version %d, %d bytes; want 0001_init.sql, version 1, not empty",
			first.name, first.version, len(first.sql))
	}
}

func TestUnsafeDSNRefusedBeforeConnecting(t *testing.T) {
	const unreachable = "u:p@tcp(127.0.0.1:1)/x"

	tests := []struct {
		name        string
		dsn         string
		wantOpen    error
		wantMigrate error
	}{
		{"interpolateParams", unreachable + "?interpolateParams=true", errInterpolateParams, errInterpolateParams},
		{"multiStatements", unreachable + "?multiStatements=true", errMultiStatements, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Open(tc.dsn, discardLogger()); !errors.Is(err, tc.wantOpen) {
				t.Errorf("Open() error = %v, want %v", err, tc.wantOpen)
			}
			if tc.wantMigrate == nil {
				return
			}
			if _, err := Migrate(t.Context(), tc.dsn, discardLogger()); !errors.Is(err, tc.wantMigrate) {
				t.Errorf("Migrate() error = %v, want %v", err, tc.wantMigrate)
			}
		})
	}
}
