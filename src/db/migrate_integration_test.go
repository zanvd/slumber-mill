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

// TestMigrateTargetDefault applies 0002 to a database whose tenants were created under 0001's default, twice.
func TestMigrateTargetDefault(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	gdb := open(t, dsn).WithContext(t.Context())
	if got := targetDefault(t, gdb); got != "-20.0" {
		t.Errorf("target_lufs defaults to %s after migrating, want -20.0", got)
	}
	if err := gdb.Exec("ALTER TABLE tenants ALTER COLUMN target_lufs SET DEFAULT -16.0").Error; err != nil {
		t.Fatalf("error restoring 0001's default: %v", err)
	}
	existing := createTenant(t, gdb, "before-0002")

	for _, run := range []string{"first run", "second run"} {
		if err := gdb.Exec("DELETE FROM schema_migrations WHERE version = 2").Error; err != nil {
			t.Fatalf("%s: error forgetting migration 2: %v", run, err)
		}
		applied, err := db.Migrate(t.Context(), dsn, testutil.Logger(t))
		if err != nil {
			t.Fatalf("%s: Migrate() error = %v", run, err)
		}
		if want := []string{"0002_tenant_target_default.sql"}; !slices.Equal(applied, want) {
			t.Errorf("%s: Migrate() applied %v, want %v", run, applied, want)
		}
		if got := targetDefault(t, gdb); got != "-20.0" {
			t.Errorf("%s: target_lufs defaults to %s, want -20.0", run, got)
		}
	}

	created := createTenant(t, gdb, "after-0002")
	for id, want := range map[uint64]float64{existing: -16, created: -20} {
		var got float64
		if err := gdb.Raw("SELECT target_lufs FROM tenants WHERE id = ?", id).Scan(&got).Error; err != nil {
			t.Fatalf("error reading tenant %d: %v", id, err)
		}
		if got != want {
			t.Errorf("tenant %d has target_lufs %v, want %v", id, got, want)
		}
	}
}

// TestMigrateTraceContext applies 0003 to tables with rows but no trace_context, again as if the first run had
// stopped after its first table, and once more with every column in place. Each run leaves every table with
// a nullable ASCII column of 512 characters, and the rows that were there with NULL in it.
func TestMigrateTraceContext(t *testing.T) {
	tables := []string{"assets", "asset_events", "webhook_deliveries"}
	dsn := testutil.NewDatabase(t)
	gdb := open(t, dsn).WithContext(t.Context())
	for _, table := range tables {
		if err := gdb.Exec("ALTER TABLE " + table + " DROP COLUMN trace_context").Error; err != nil {
			t.Fatalf("error restoring %s as 0002 left it: %v", table, err)
		}
	}
	tenantID := createTenant(t, gdb, "before-0003")
	asset := newAsset(tenantID, db.NewID(), nil)
	if err := gdb.Create(asset).Error; err != nil {
		t.Fatalf("error creating an asset: %v", err)
	}
	for query, args := range map[string][]any{
		"INSERT INTO asset_events (event_id, tenant_id, asset_id, type) VALUES (?, ?, ?, 'asset.ready')": {
			db.NewID(), tenantID, asset.ID},
		"INSERT INTO webhook_deliveries (event_id, tenant_id, body) VALUES (?, ?, '{}')": {db.NewID(), tenantID},
	} {
		if err := gdb.Exec(query, args...).Error; err != nil {
			t.Fatalf("error running %q: %v", query, err)
		}
	}

	runs := []struct {
		name   string
		before func(t *testing.T)
	}{
		{"first run", func(*testing.T) {}},
		{"after a run that stopped partway", func(t *testing.T) {
			for _, table := range tables[1:] {
				if err := gdb.Exec("ALTER TABLE " + table + " DROP COLUMN trace_context").Error; err != nil {
					t.Fatalf("error dropping %s.trace_context: %v", table, err)
				}
			}
		}},
		{"with every column in place", func(*testing.T) {}},
	}
	for _, run := range runs {
		run.before(t)
		if err := gdb.Exec("DELETE FROM schema_migrations WHERE version = 3").Error; err != nil {
			t.Fatalf("%s: error forgetting migration 3: %v", run.name, err)
		}
		applied, err := db.Migrate(t.Context(), dsn, testutil.Logger(t))
		if err != nil {
			t.Fatalf("%s: Migrate() error = %v", run.name, err)
		}
		if want := []string{"0003_trace_context.sql"}; !slices.Equal(applied, want) {
			t.Errorf("%s: Migrate() applied %v, want %v", run.name, applied, want)
		}
		for _, table := range tables {
			var columnType, charset, nullable string
			err := gdb.Raw("SELECT column_type, character_set_name, is_nullable FROM information_schema.columns "+
				"WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'trace_context'", table).
				Row().Scan(&columnType, &charset, &nullable)
			if err != nil {
				t.Fatalf("%s: error reading %s.trace_context: %v", run.name, table, err)
			}
			if columnType != "varchar(512)" || charset != "ascii" || nullable != "YES" {
				t.Errorf("%s: %s.trace_context is %s, %s, nullable %s; want a nullable ASCII varchar(512)",
					run.name, table, columnType, charset, nullable)
			}
			var rows, null int
			err = gdb.Raw("SELECT COUNT(*), COUNT(*) - COUNT(trace_context) FROM "+table).Row().Scan(&rows, &null)
			if err != nil {
				t.Fatalf("%s: error counting %s: %v", run.name, table, err)
			}
			if rows != 1 || null != 1 {
				t.Errorf("%s: %s has %d rows, %d of them with a NULL trace_context; want the one row, NULL",
					run.name, table, rows, null)
			}
		}
	}
}

func targetDefault(t *testing.T, gdb *gorm.DB) string {
	t.Helper()
	var def string
	err := gdb.Raw("SELECT column_default FROM information_schema.columns " +
		"WHERE table_schema = DATABASE() AND table_name = 'tenants' AND column_name = 'target_lufs'").Scan(&def).Error
	if err != nil {
		t.Fatalf("error reading the target_lufs default: %v", err)
	}
	return def
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
