package asset

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"uuid"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var errNoDatabase = errors.New("dry run reached the connection pool")

// noConn fails every query: in a dry run GORM builds statements without sending them.
type noConn struct{}

func (noConn) PrepareContext(context.Context, string) (*sql.Stmt, error) { return nil, errNoDatabase }

func (noConn) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errNoDatabase
}

func (noConn) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errNoDatabase
}

func (noConn) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

type statement struct {
	sql  string
	vars []any
}

// recorder keeps every statement GORM builds, and answers asset reads with rows,
// so the renditions' preload runs too.
type recorder struct {
	statements []statement
}

func (r *recorder) record(gdb *gorm.DB) {
	r.statements = append(r.statements, statement{gdb.Statement.SQL.String(), slices.Clone(gdb.Statement.Vars)})
}

func (r *recorder) query(gdb *gorm.DB) {
	r.record(gdb)
	switch dest := gdb.Statement.Dest.(type) {
	case *assetRow:
		*dest = assetRow{ID: 11, TenantID: 42}
	case *[]assetRow:
		*dest = append(*dest, assetRow{ID: 12, TenantID: 42}, assetRow{ID: 11, TenantID: 42})
	}
}

// dryRun returns a repository whose statements are recorded rather than sent (pitfall 9).
func dryRun(t *testing.T) (*Repository, *recorder) {
	t.Helper()
	gdb, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: noConn{}, SkipInitializeWithVersion: true}),
		&gorm.Config{
			SkipDefaultTransaction: true,
			TranslateError:         true,
			DryRun:                 true,
			DisableAutomaticPing:   true,
			Logger:                 logger.Discard,
		})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	rec := &recorder{}
	cb := gdb.Callback()
	for _, err := range []error{
		cb.Query().After("gorm:query").Before("gorm:preload").Register("test:record", rec.query),
		cb.Create().After("gorm:create").Register("test:record", rec.record),
	} {
		if err != nil {
			t.Fatalf("error registering a recording callback: %v", err)
		}
	}
	return NewRepository(gdb), rec
}

// TestInsertAssetSQL inserts an asset whose fields are zero wherever the type allows,
// so a dropped zero value would show as a missing column.
func TestInsertAssetSQL(t *testing.T) {
	r, rec := dryRun(t)
	if err := r.insertAsset(t.Context(), Asset{Status: StatusQueued}); err != nil {
		t.Fatalf("insertAsset() error = %v", err)
	}
	if len(rec.statements) != 1 {
		t.Fatalf("got %d statements, want 1: %v", len(rec.statements), rec.statements)
	}
	got := rec.statements[0]
	const want = "INSERT INTO `assets` (`public_id`,`tenant_id`,`idempotency_key`,`title`,`status`," +
		"`source_sha256`,`source_bytes`,`target_lufs`) VALUES (?,?,?,?,?,?,?,?)"
	if got.sql != want {
		t.Errorf("SQL = %s\nwant  %s", got.sql, want)
	}
	if len(got.vars) != 8 {
		t.Fatalf("got %d values, want 8: %v", len(got.vars), got.vars)
	}
	if key, ok := got.vars[2].(*string); !ok || key != nil {
		t.Errorf("idempotency_key = %#v, want NULL", got.vars[2])
	}
	for i, want := range map[int]any{3: "", 4: StatusQueued, 6: uint64(0), 7: float64(0)} {
		if got.vars[i] != want {
			t.Errorf("value %d = %#v, want %#v", i, got.vars[i], want)
		}
	}
}

func TestReadsFilterOnTenant(t *testing.T) {
	const tenant = 42
	id := uuid.NewV7()
	tests := []struct {
		name string
		read func(ctx context.Context, r *Repository) error
		// want lists, per statement, the tenant conditions it must have.
		want [][]string
	}{
		{
			name: "assetByID",
			read: func(ctx context.Context, r *Repository) error {
				_, err := r.assetByID(ctx, tenant, id)
				return err
			},
			want: [][]string{{"tenant_id = ?"}, {"tenant_id = ?"}},
		},
		{
			name: "assetByIdempotencyKey",
			read: func(ctx context.Context, r *Repository) error {
				_, err := r.assetByIdempotencyKey(ctx, tenant, "upload-42")
				return err
			},
			want: [][]string{{"tenant_id = ?"}, {"tenant_id = ?"}},
		},
		{
			name: "listAssets",
			read: func(ctx context.Context, r *Repository) error {
				_, _, err := r.listAssets(ctx, tenant, 0, 1)
				return err
			},
			want: [][]string{{"tenant_id = ?"}, {"tenant_id = ?"}},
		},
		{
			name: "listAssets from a cursor",
			read: func(ctx context.Context, r *Repository) error {
				_, _, err := r.listAssets(ctx, tenant, 99, 1)
				return err
			},
			want: [][]string{{"tenant_id = ?", "id < ?"}, {"tenant_id = ?"}},
		},
		{
			name: "rendition",
			read: func(ctx context.Context, r *Repository) error {
				_, err := r.rendition(ctx, tenant, id, ProfileAAC64k)
				return err
			},
			want: [][]string{{"assets.tenant_id = ?", "renditions.tenant_id = ?"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, rec := dryRun(t)
			if err := tc.read(t.Context(), r); err != nil {
				t.Fatalf("read error = %v", err)
			}
			if len(rec.statements) != len(tc.want) {
				t.Fatalf("got %d statements, want %d: %v", len(rec.statements), len(tc.want), rec.statements)
			}
			for i, s := range rec.statements {
				for _, cond := range tc.want[i] {
					if !strings.Contains(s.sql, cond) {
						t.Errorf("statement %d lacks %q: %s", i, cond, s.sql)
					}
				}
				if !slices.Contains(s.vars, any(uint64(tenant))) {
					t.Errorf("statement %d doesn't pass tenant %d: %s %v", i, tenant, s.sql, s.vars)
				}
			}
		})
	}
}
