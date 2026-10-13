package tenant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

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

type recorder struct {
	statements []statement
}

func (r *recorder) record(gdb *gorm.DB) {
	r.statements = append(r.statements, statement{gdb.Statement.SQL.String(), slices.Clone(gdb.Statement.Vars)})
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
		cb.Query().After("gorm:query").Register("test:record", rec.record),
		cb.Create().After("gorm:create").Register("test:record", rec.record),
	} {
		if err != nil {
			t.Fatalf("error registering a recording callback: %v", err)
		}
	}
	return NewRepository(gdb), rec
}

// TestInsertTenantSQL inserts tenants whose fields are zero wherever the type allows,
// so a dropped zero value would show as a missing column, and a timestamp as an extra one.
func TestInsertTenantSQL(t *testing.T) {
	const want = "INSERT INTO `tenants` (`slug`,`api_key_hash`,`webhook_url`,`webhook_secret_enc`,`target_lufs`) " +
		"VALUES (?,?,?,?,?)"
	var zeroHash [sha256.Size]byte
	hash := sha256.Sum256([]byte("sm_test"))
	sealed := []byte{1, 2, 3}
	url := testURL

	tests := []struct {
		name       string
		tenant     Tenant
		hash       [sha256.Size]byte
		sealed     []byte
		wantURL    *string
		wantSealed []byte
	}{
		{"zero values", Tenant{}, zeroHash, nil, nil, nil},
		{"with a webhook", Tenant{Slug: "acme", WebhookURL: url, TargetLUFS: -16}, hash, sealed, &url, sealed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, rec := dryRun(t)
			if _, err := r.insertTenant(t.Context(), tc.tenant, tc.hash, tc.sealed); err != nil {
				t.Fatalf("insertTenant() error = %v", err)
			}
			if len(rec.statements) != 1 {
				t.Fatalf("got %d statements, want 1: %v", len(rec.statements), rec.statements)
			}
			got := rec.statements[0]
			if got.sql != want {
				t.Errorf("SQL = %s\nwant  %s", got.sql, want)
			}
			if len(got.vars) != 5 {
				t.Fatalf("got %d values, want 5: %v", len(got.vars), got.vars)
			}
			if got.vars[0] != tc.tenant.Slug {
				t.Errorf("slug = %#v, want %q", got.vars[0], tc.tenant.Slug)
			}
			if h, ok := got.vars[1].([]byte); !ok || !bytes.Equal(h, tc.hash[:]) {
				t.Errorf("api_key_hash = %#v, want %x", got.vars[1], tc.hash)
			}
			switch u, ok := got.vars[2].(*string); {
			case !ok:
				t.Errorf("webhook_url = %#v, want a *string", got.vars[2])
			case tc.wantURL == nil && u != nil, tc.wantURL != nil && (u == nil || *u != *tc.wantURL):
				t.Errorf("webhook_url = %v, want %v", u, tc.wantURL)
			}
			if s, ok := got.vars[3].([]byte); !ok || (s == nil) != (tc.wantSealed == nil) || !bytes.Equal(s, tc.wantSealed) {
				t.Errorf("webhook_secret_enc = %#v, want %#v", got.vars[3], tc.wantSealed)
			}
			if got.vars[4] != tc.tenant.TargetLUFS {
				t.Errorf("target_lufs = %#v, want %v", got.vars[4], tc.tenant.TargetLUFS)
			}
		})
	}
}

func TestTenantReadsLeaveOutSecrets(t *testing.T) {
	tests := []struct {
		name  string
		read  func(ctx context.Context, r *Repository) error
		where string
	}{
		{"tenantByID", func(ctx context.Context, r *Repository) error {
			_, err := r.tenantByID(ctx, 7)
			return err
		}, "WHERE id = ?"},
		{"tenantByAPIKeyHash", func(ctx context.Context, r *Repository) error {
			_, err := r.tenantByAPIKeyHash(ctx, sha256.Sum256([]byte("sm_test")))
			return err
		}, "WHERE api_key_hash = ?"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, rec := dryRun(t)
			if err := tc.read(t.Context(), r); err != nil {
				t.Fatalf("read error = %v", err)
			}
			if len(rec.statements) != 1 {
				t.Fatalf("got %d statements, want 1: %v", len(rec.statements), rec.statements)
			}
			got := rec.statements[0].sql
			selected, _, _ := strings.Cut(got, " FROM ")
			const wantSelect = "SELECT `id`,`slug`,`webhook_url`,`target_lufs`,`created_at`"
			if selected != wantSelect {
				t.Errorf("SQL = %s\nwant it to start %s", got, wantSelect)
			}
			if !strings.Contains(got, tc.where) || !strings.HasSuffix(got, "LIMIT ?") {
				t.Errorf("SQL = %s, want %q and a LIMIT", got, tc.where)
			}
		})
	}
}
