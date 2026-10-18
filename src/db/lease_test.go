package db

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	errNoDatabase = errors.New("dry run reached the connection pool")
	setPattern    = regexp.MustCompile("`(\\w+)`=")
)

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

// dryRunPool begins transactions without a database and records their isolation levels.
type dryRunPool struct {
	noConn
	isolation []sql.IsolationLevel
}

func (p *dryRunPool) BeginTx(_ context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	level := sql.LevelDefault
	if opts != nil {
		level = opts.Isolation
	}
	p.isolation = append(p.isolation, level)
	return &dryRunTx{}, nil
}

type dryRunTx struct{ noConn }

func (*dryRunTx) Commit() error { return nil }

func (*dryRunTx) Rollback() error { return nil }

type statement struct {
	sql  string
	vars []any
}

// recorder keeps every statement GORM builds. It answers a single-row read with a row of attempts 0,
// a multi-row read with rows of attempts 1 and 3, and counts every update as one matched row.
type recorder struct {
	statements []statement
}

func (r *recorder) record(db *gorm.DB) {
	r.statements = append(r.statements, statement{db.Statement.SQL.String(), slices.Clone(db.Statement.Vars)})
}

func (r *recorder) query(db *gorm.DB) {
	r.record(db)
	switch dest := db.Statement.Dest.(type) {
	case *leaseRow:
		*dest = leaseRow{ID: 7, TenantID: 3}
	case *[]leaseRow:
		*dest = append(*dest, leaseRow{ID: 8, TenantID: 3, Attempts: 1}, leaseRow{ID: 9, TenantID: 3, Attempts: 3})
	}
}

func (r *recorder) update(db *gorm.DB) {
	r.record(db)
	db.RowsAffected = 1
}

// dryRun opens GORM in dry-run mode without a database (pitfall 9).
func dryRun(t *testing.T) (*gorm.DB, *dryRunPool, *recorder) {
	t.Helper()
	pool := &dryRunPool{}
	cfg := gormConfig(discardLogger())
	cfg.DryRun = true
	cfg.DisableAutomaticPing = true
	gdb, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: pool, SkipInitializeWithVersion: true}), cfg)
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	rec := &recorder{}
	cb := gdb.Callback()
	for _, err := range []error{
		cb.Query().After("gorm:query").Register("test:record", rec.query),
		cb.Update().After("gorm:update").Register("test:record", rec.update),
		cb.Create().After("gorm:create").Register("test:record", rec.record),
		cb.Delete().After("gorm:delete").Register("test:record", rec.record),
		cb.Raw().After("gorm:raw").Register("test:record", rec.record),
	} {
		if err != nil {
			t.Fatalf("error registering a recording callback: %v", err)
		}
	}
	return gdb, pool, rec
}

type wantStatement struct {
	contains []string
	// set lists the columns an UPDATE assigns, in GORM's sorted order; nil for a SELECT.
	set []string
}

type leaseCase struct {
	name       string
	run        func(ctx context.Context, l *Leases, writes func(tx *gorm.DB) error) error
	want       []wantStatement
	wantTx     []sql.IsolationLevel
	wantWrites bool
}

// TestLeaseSQL runs every lease operation with zero-valued inputs wherever they're allowed,
// so a write that dropped a zero value would be missing a column.
func TestLeaseSQL(t *testing.T) {
	for _, newLeases := range []func(*gorm.DB) *Leases{NewAssetLeases, NewWebhookDeliveryLeases} {
		lt := newLeases(nil).t
		for _, tc := range leaseCases(lt) {
			t.Run(lt.name+"/"+tc.name, func(t *testing.T) {
				gdb, pool, rec := dryRun(t)
				wrote := false
				writes := func(*gorm.DB) error {
					wrote = true
					return nil
				}

				if err := tc.run(t.Context(), newLeases(gdb), writes); err != nil {
					t.Fatalf("error = %v", err)
				}

				if wrote != tc.wantWrites {
					t.Errorf("writes ran: %t, want %t", wrote, tc.wantWrites)
				}
				if !slices.Equal(pool.isolation, tc.wantTx) {
					t.Errorf("transaction isolation levels = %v, want %v", pool.isolation, tc.wantTx)
				}
				if len(rec.statements) != len(tc.want) {
					t.Fatalf("ran %d statements, want %d: %q", len(rec.statements), len(tc.want), rec.statements)
				}
				for i, want := range tc.want {
					checkStatement(t, rec.statements[i], want)
				}
			})
		}
	}
}

func checkStatement(t *testing.T, got statement, want wantStatement) {
	t.Helper()
	for _, s := range want.contains {
		if !strings.Contains(got.sql, s) {
			t.Errorf("%s\ndoesn't contain %q", got.sql, s)
		}
	}
	for _, column := range []string{"created_at", "updated_at"} {
		if strings.Contains(got.sql, column) {
			t.Errorf("%s\nmentions %s", got.sql, column)
		}
	}
	for _, v := range got.vars {
		if _, ok := v.(time.Time); ok {
			t.Errorf("%s\nbinds the Go time %v; times come from NOW(6)", got.sql, v)
		}
	}
	if want.set == nil {
		return
	}
	_, rest, okSet := strings.Cut(got.sql, " SET ")
	set, _, okWhere := strings.Cut(rest, " WHERE ")
	if !strings.HasPrefix(got.sql, "UPDATE ") || !okSet || !okWhere {
		t.Errorf("%s\nisn't an UPDATE ... SET ... WHERE", got.sql)
		return
	}
	var columns []string
	for _, m := range setPattern.FindAllStringSubmatch(set, -1) {
		columns = append(columns, m[1])
	}
	if !slices.Equal(columns, want.set) {
		t.Errorf("%s\nsets %v, want %v", got.sql, columns, want.set)
	}
}

func leaseCases(lt leasedTable) []leaseCase {
	fence := "WHERE id = ? AND " + lt.column + " = ? AND attempts = ?"
	sorted := func(columns ...string) []string {
		return slices.Sorted(slices.Values(append(columns, lt.column)))
	}
	leaving := sorted("lease_expires_at", "locked_by")
	failing := sorted("last_error", "lease_expires_at", "locked_by")
	retrying := sorted("last_error", "lease_expires_at", "locked_by", "run_after")
	return []leaseCase{
		{
			name: "claim",
			run: func(ctx context.Context, l *Leases, _ func(*gorm.DB) error) error {
				_, _, err := l.Claim(ctx, "", time.Microsecond)
				return err
			},
			want: []wantStatement{
				{contains: []string{
					"SELECT `id`,`tenant_id`,`attempts` FROM `" + lt.name + "` FORCE INDEX (`ix_" + lt.name + "_claim`) " +
						"WHERE " + lt.column + " = ? AND run_after <= NOW(6) ORDER BY run_after, id LIMIT ?",
					" FOR UPDATE SKIP LOCKED",
				}},
				{
					contains: []string{"`attempts`=attempts + 1", "`lease_expires_at`=NOW(6) + INTERVAL ? MICROSECOND"},
					set:      sorted("attempts", "lease_expires_at", "locked_by"),
				},
			},
			wantTx: []sql.IsolationLevel{sql.LevelReadCommitted},
		},
		{
			name: "extend",
			run: func(ctx context.Context, l *Leases, _ func(*gorm.DB) error) error {
				return l.Extend(ctx, Lease{}, time.Microsecond)
			},
			want: []wantStatement{{
				contains: []string{fence, "`lease_expires_at`=NOW(6) + INTERVAL ? MICROSECOND"},
				set:      []string{"lease_expires_at"},
			}},
		},
		{
			name: "complete",
			run: func(ctx context.Context, l *Leases, _ func(*gorm.DB) error) error {
				return l.Complete(ctx, Lease{}, nil)
			},
			want: []wantStatement{{contains: []string{fence}, set: leaving}},
		},
		{
			name: "complete with writes",
			run: func(ctx context.Context, l *Leases, writes func(*gorm.DB) error) error {
				return l.Complete(ctx, Lease{}, writes)
			},
			want:       []wantStatement{{contains: []string{fence}, set: leaving}},
			wantTx:     []sql.IsolationLevel{sql.LevelReadCommitted},
			wantWrites: true,
		},
		{
			name: "fail and retry",
			run: func(ctx context.Context, l *Leases, writes func(*gorm.DB) error) error {
				_, err := l.Fail(ctx, Lease{Attempt: 1}, Failure{MaxAttempts: 2, Writes: writes})
				return err
			},
			want: []wantStatement{{
				contains: []string{fence, "`run_after`=NOW(6) + INTERVAL ? MICROSECOND"},
				set:      retrying,
			}},
		},
		{
			name: "fail permanently",
			run: func(ctx context.Context, l *Leases, _ func(*gorm.DB) error) error {
				_, err := l.Fail(ctx, Lease{Attempt: 1}, Failure{Permanent: true, MaxAttempts: 2})
				return err
			},
			want: []wantStatement{{contains: []string{fence}, set: failing}},
		},
		{
			name: "fail the last attempt with writes",
			run: func(ctx context.Context, l *Leases, writes func(*gorm.DB) error) error {
				_, err := l.Fail(ctx, Lease{Attempt: 2}, Failure{MaxAttempts: 2, Writes: writes})
				return err
			},
			want:       []wantStatement{{contains: []string{fence}, set: failing}},
			wantTx:     []sql.IsolationLevel{sql.LevelReadCommitted},
			wantWrites: true,
		},
		{
			name: "release",
			run: func(ctx context.Context, l *Leases, _ func(*gorm.DB) error) error {
				return l.Release(ctx, Lease{})
			},
			want: []wantStatement{{
				contains: []string{fence, "`attempts`=attempts - 1"},
				set:      sorted("attempts", "lease_expires_at", "locked_by"),
			}},
		},
		{
			name: "reap",
			run: func(ctx context.Context, l *Leases, writes func(*gorm.DB) error) error {
				_, _, err := l.Reap(ctx, 3, func(tx *gorm.DB, failed []Lease) error {
					if want := []Lease{{ID: 9, TenantID: 3, Attempt: 3}}; !slices.Equal(failed, want) {
						return errors.New("reap passed the wrong failed leases")
					}
					return writes(tx)
				})
				return err
			},
			want: []wantStatement{
				{contains: []string{
					"SELECT `id`,`tenant_id`,`attempts` FROM `" + lt.name + "`",
					"WHERE " + lt.column + " = ? AND lease_expires_at < NOW(6) ORDER BY lease_expires_at, id LIMIT ?",
					" FOR UPDATE SKIP LOCKED",
				}},
				{
					contains: []string{"`run_after`=NOW(6) + INTERVAL ? MICROSECOND", "WHERE id IN (?)"},
					set:      retrying,
				},
				{contains: []string{"WHERE id IN (?)"}, set: failing},
			},
			wantTx:     []sql.IsolationLevel{sql.LevelReadCommitted},
			wantWrites: true,
		},
	}
}
