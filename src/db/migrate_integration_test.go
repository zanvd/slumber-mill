//go:build integration

package db_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
)

var wantTables = []string{"asset_events", "assets", "renditions", "schema_migrations", "tenants", "webhook_deliveries"}

type schemaMigration struct {
	Version   uint64
	Name      string
	AppliedAt time.Time
}

func TestMigrateConcurrently(t *testing.T) {
	dsn := testutil.NewEmptyDatabase(t)

	const runs = 5
	results := make([][]string, runs)
	errs := make([]error, runs)
	log := testutil.Logger(t)
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() { results[i], errs[i] = db.Migrate(t.Context(), dsn, log) })
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Migrate() call %d error = %v", i, err)
		}
	}
	gdb := open(t, dsn)
	recorded := recordedMigrations(t, gdb)
	var names []string
	for _, m := range recorded {
		names = append(names, m.Name)
	}
	if len(names) == 0 || names[0] != "0001_init.sql" {
		t.Fatalf("schema_migrations holds %v, want 0001_init.sql first", names)
	}

	var appliers int
	for i, applied := range results {
		switch {
		case len(applied) == 0:
		case slices.Equal(applied, names):
			appliers++
		default:
			t.Errorf("Migrate() call %d applied %v, want all of %v or nothing", i, applied, names)
		}
	}
	if appliers != 1 {
		t.Errorf("%d of %d calls applied the migrations, want 1; results %v", appliers, runs, results)
	}
	if got := tables(t, gdb); !slices.Equal(got, wantTables) {
		t.Errorf("tables = %v, want %v", got, wantTables)
	}
}

func TestMigrateAgain(t *testing.T) {
	tests := []struct {
		name        string
		forget      bool
		wantApplied []string
	}{
		{"recorded migrations are skipped", false, nil},
		{"an unrecorded migration applies again", true, []string{"0001_init.sql"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dsn := testutil.NewDatabase(t)
			gdb := open(t, dsn)
			if tc.forget {
				if err := gdb.Exec("DELETE FROM schema_migrations WHERE version = 1").Error; err != nil {
					t.Fatalf("error forgetting migration 1: %v", err)
				}
			}
			before := recordedMigrations(t, gdb)

			applied, err := db.Migrate(t.Context(), dsn, testutil.Logger(t))
			if err != nil {
				t.Fatalf("Migrate() error = %v", err)
			}
			if !slices.Equal(applied, tc.wantApplied) {
				t.Errorf("Migrate() applied %v, want %v", applied, tc.wantApplied)
			}
			after := recordedMigrations(t, gdb)
			if !tc.forget && !slices.Equal(after, before) {
				t.Errorf("schema_migrations changed from %v to %v", before, after)
			}
			if len(after) == 0 || after[0].Name != "0001_init.sql" {
				t.Errorf("schema_migrations holds %v, want 0001_init.sql recorded", after)
			}
			if got := tables(t, gdb); !slices.Equal(got, wantTables) {
				t.Errorf("tables = %v, want %v", got, wantTables)
			}
		})
	}
}

func recordedMigrations(t *testing.T, gdb *gorm.DB) []schemaMigration {
	t.Helper()
	var rows []schemaMigration
	if err := gdb.WithContext(t.Context()).Table("schema_migrations").Order("version").Find(&rows).Error; err != nil {
		t.Fatalf("error reading schema_migrations: %v", err)
	}
	return rows
}

func tables(t *testing.T, gdb *gorm.DB) []string {
	t.Helper()
	var names []string
	err := gdb.WithContext(t.Context()).
		Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name").
		Scan(&names).Error
	if err != nil {
		t.Fatalf("error listing tables: %v", err)
	}
	return names
}

func open(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	gdb, err := db.Open(dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("error getting the pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return gdb
}
