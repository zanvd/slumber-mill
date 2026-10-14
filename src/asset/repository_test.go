package asset

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/worker"
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
	inTx bool
}

// recorder keeps every statement GORM builds, and answers asset reads with rows,
// so the renditions' preload runs too. It answers db's scan for expired leases with leaseRows,
// and counts every update as one matched row.
type recorder struct {
	pool       *dryRunPool
	statements []statement
	leaseRows  []lease
}

func (r *recorder) record(gdb *gorm.DB) {
	_, inTx := gdb.Statement.ConnPool.(*dryRunTx)
	r.statements = append(r.statements,
		statement{gdb.Statement.SQL.String(), slices.Clone(gdb.Statement.Vars), inTx})
}

func (r *recorder) query(gdb *gorm.DB) {
	r.record(gdb)
	switch dest := gdb.Statement.Dest.(type) {
	case *assetRow:
		*dest = assetRow{ID: 11, TenantID: 42}
	case *[]assetRow:
		*dest = append(*dest, assetRow{ID: 12, TenantID: 42}, assetRow{ID: 11, TenantID: 42})
	default:
		if gdb.Statement.Table == "assets" {
			r.answerLeaseScan(gdb)
		}
	}
}

// answerLeaseScan fills a scan into db's own row type by column name, which is all the lease SQL shares with us.
func (r *recorder) answerLeaseScan(gdb *gorm.DB) {
	rows, sch := gdb.Statement.ReflectValue, gdb.Statement.Schema
	if len(r.leaseRows) == 0 || rows.Kind() != reflect.Slice || sch == nil {
		return
	}
	for _, l := range r.leaseRows {
		row := reflect.New(rows.Type().Elem()).Elem()
		for column, v := range map[string]any{"id": l.ID, "tenant_id": l.TenantID, "attempts": l.Attempt} {
			f := sch.LookUpField(column)
			if f == nil {
				gdb.AddError(fmt.Errorf("db's lease scan has no %s column", column))
				return
			}
			if err := f.Set(gdb.Statement.Context, row, v); err != nil {
				gdb.AddError(err)
				return
			}
		}
		rows.Set(reflect.Append(rows, row))
	}
}

func (r *recorder) update(gdb *gorm.DB) {
	r.record(gdb)
	gdb.RowsAffected = 1
}

// dryRun returns a repository whose statements are recorded rather than sent (pitfall 9).
func dryRun(t *testing.T) (*Repository, *recorder) {
	t.Helper()
	pool := &dryRunPool{}
	gdb, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: pool, SkipInitializeWithVersion: true}),
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
	rec := &recorder{pool: pool}
	cb := gdb.Callback()
	for _, err := range []error{
		cb.Query().After("gorm:query").Before("gorm:preload").Register("test:record", rec.query),
		cb.Create().After("gorm:create").Register("test:record", rec.record),
		cb.Update().After("gorm:update").Register("test:record", rec.update),
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

// The processor converts between worker.Lease and the repository's lease.
var (
	_ = worker.Lease(lease{})
	_ = lease(worker.Lease{})
)

type wantStatement struct {
	contains []string
	// columns lists, sorted, the columns an UPDATE sets or an INSERT names; nil for a SELECT.
	columns []string
	// vars holds values the statement binds, by position: a func(any) bool matches, anything else must be equal.
	vars map[int]any
}

// TestLeaseWritesSQL runs every lease write with zero values wherever they're allowed,
// so a write that dropped a zero value would be missing a column.
func TestLeaseWritesSQL(t *testing.T) {
	const fence = "WHERE id = ? AND status = ? AND attempts = ?"
	sorted := func(columns ...string) []string { return slices.Sorted(slices.Values(columns)) }
	leaving := sorted("status", "locked_by", "lease_expires_at")
	retrying := sorted("status", "locked_by", "lease_expires_at", "last_error", "run_after")
	failing := sorted("status", "locked_by", "lease_expires_at", "last_error")
	event := func(id, tenantID uint64, typ EventType) wantStatement {
		return wantStatement{
			contains: []string{"INSERT INTO `asset_events`"},
			columns:  sorted("event_id", "tenant_id", "asset_id", "type"),
			vars:     map[int]any{0: isUUIDv7, 1: tenantID, 2: id, 3: typ},
		}
	}
	failed := func(message string, id, tenantID uint64) []wantStatement {
		return []wantStatement{
			{
				contains: []string{"UPDATE `assets` SET", "WHERE (id, tenant_id) IN ((?,?))"},
				columns:  []string{"error"},
				vars:     map[int]any{0: message, 1: id, 2: tenantID},
			},
			event(id, tenantID, EventFailed),
		}
	}

	tests := []struct {
		name string
		// leaseRows answers the reaper's scan.
		leaseRows []lease
		run       func(ctx context.Context, r *Repository) error
		want      []wantStatement
		// wantTx means every statement runs in one READ COMMITTED transaction, and false that none does.
		wantTx bool
	}{
		{
			name: "claim",
			run: func(ctx context.Context, r *Repository) error {
				_, _, err := r.claim(ctx, "", time.Microsecond)
				return err
			},
			want: []wantStatement{
				{contains: []string{"FROM `assets` WHERE status = ? AND run_after <= NOW(6)", "FOR UPDATE SKIP LOCKED"}},
				{
					contains: []string{"`attempts`=attempts + 1"},
					columns:  sorted("status", "attempts", "locked_by", "lease_expires_at"),
				},
			},
			wantTx: true,
		},
		{
			name: "extend",
			run: func(ctx context.Context, r *Repository) error {
				return r.extend(ctx, lease{}, time.Microsecond)
			},
			want: []wantStatement{{contains: []string{fence}, columns: []string{"lease_expires_at"}}},
		},
		{
			name: "finish",
			run: func(ctx context.Context, r *Repository) error {
				return r.finish(ctx, lease{}, processed{Renditions: []Rendition{{}}})
			},
			want: []wantStatement{
				{contains: []string{fence}, columns: leaving},
				{
					contains: []string{"INSERT INTO `renditions`", "ON DUPLICATE KEY UPDATE " +
						"`tenant_id`=VALUES(`tenant_id`),`codec`=VALUES(`codec`),`bitrate_kbps`=VALUES(`bitrate_kbps`)," +
						"`channels`=VALUES(`channels`),`bytes`=VALUES(`bytes`),`sha256`=VALUES(`sha256`)," +
						"`lufs`=VALUES(`lufs`),`true_peak_dbtp`=VALUES(`true_peak_dbtp`),`lra_lu`=VALUES(`lra_lu`)," +
						"`normalization`=VALUES(`normalization`)"},
					columns: sorted("tenant_id", "asset_id", "profile", "codec", "bitrate_kbps", "channels", "bytes",
						"sha256", "lufs", "true_peak_dbtp", "lra_lu", "normalization"),
				},
				{
					contains: []string{"UPDATE `assets` SET", "WHERE (id, tenant_id) IN ((?,?))"},
					columns: sorted("source_codec", "duration_ms", "input_lufs", "input_true_peak_dbtp", "input_lra_lu",
						"input_max_momentary", "warnings"),
					vars: map[int]any{
						0: uint64(0), 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: (*string)(nil), 6: isEmptySet,
						7: uint64(0), 8: uint64(0),
					},
				},
				event(0, 0, EventReady),
			},
			wantTx: true,
		},
		{
			name: "fail and retry",
			run: func(ctx context.Context, r *Repository) error {
				_, err := r.fail(ctx, lease{Attempt: 1}, failure{})
				return err
			},
			want: []wantStatement{{contains: []string{fence, "`run_after`=NOW(6) + INTERVAL ? MICROSECOND"},
				columns: retrying}},
		},
		{
			name: "fail permanently",
			run: func(ctx context.Context, r *Repository) error {
				_, err := r.fail(ctx, lease{Attempt: 1}, failure{Permanent: true, Reason: "not audio"})
				return err
			},
			want:   append([]wantStatement{{contains: []string{fence}, columns: failing}}, failed("not audio", 0, 0)...),
			wantTx: true,
		},
		{
			name: "fail permanently without a reason",
			run: func(ctx context.Context, r *Repository) error {
				_, err := r.fail(ctx, lease{Attempt: 1}, failure{Permanent: true})
				return err
			},
			want:   append([]wantStatement{{contains: []string{fence}, columns: failing}}, failed(failedMessage, 0, 0)...),
			wantTx: true,
		},
		{
			name: "fail the last attempt",
			run: func(ctx context.Context, r *Repository) error {
				_, err := r.fail(ctx, lease{Attempt: maxAttempts}, failure{Reason: "only for rejections"})
				return err
			},
			want:   append([]wantStatement{{contains: []string{fence}, columns: failing}}, failed(failedMessage, 0, 0)...),
			wantTx: true,
		},
		{
			name: "release",
			run: func(ctx context.Context, r *Repository) error {
				return r.release(ctx, lease{})
			},
			want: []wantStatement{{
				contains: []string{fence, "`attempts`=attempts - 1"},
				columns:  sorted("status", "attempts", "locked_by", "lease_expires_at"),
			}},
		},
		{
			name:      "reap",
			leaseRows: []lease{{ID: 8, TenantID: 42, Attempt: 1}, {ID: 9, TenantID: 43, Attempt: maxAttempts}},
			run: func(ctx context.Context, r *Repository) error {
				requeued, failed, err := r.reap(ctx)
				if err == nil && (requeued != 1 || failed != 1) {
					err = fmt.Errorf("reap() = %d requeued, %d failed; want 1, 1", requeued, failed)
				}
				return err
			},
			want: append([]wantStatement{
				{contains: []string{"FROM `assets` WHERE status = ? AND lease_expires_at < NOW(6)", "FOR UPDATE SKIP LOCKED"}},
				{contains: []string{"WHERE id IN (?)"}, columns: retrying, vars: map[int]any{5: uint64(8)}},
				{contains: []string{"WHERE id IN (?)"}, columns: failing, vars: map[int]any{4: uint64(9)}},
			}, failed(failedMessage, 9, 43)...),
			wantTx: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, rec := dryRun(t)
			rec.leaseRows = tc.leaseRows

			if err := tc.run(t.Context(), r); err != nil {
				t.Fatalf("error = %v", err)
			}

			var wantIsolation []sql.IsolationLevel
			if tc.wantTx {
				wantIsolation = []sql.IsolationLevel{sql.LevelReadCommitted}
			}
			if !slices.Equal(rec.pool.isolation, wantIsolation) {
				t.Errorf("transaction isolation levels = %v, want %v", rec.pool.isolation, wantIsolation)
			}
			if len(rec.statements) != len(tc.want) {
				t.Fatalf("ran %d statements, want %d: %v", len(rec.statements), len(tc.want), rec.statements)
			}
			for i, want := range tc.want {
				got := rec.statements[i]
				if got.inTx != tc.wantTx {
					t.Errorf("%s\nran in a transaction: %t, want %t", got.sql, got.inTx, tc.wantTx)
				}
				checkStatement(t, got, want)
			}
		})
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
	if n := strings.Count(got.sql, "?"); n != len(got.vars) {
		t.Errorf("%s\nhas %d placeholders for %d values: %v", got.sql, n, len(got.vars), got.vars)
	}
	for _, v := range got.vars {
		if _, ok := v.(time.Time); ok {
			t.Errorf("%s\nbinds the Go time %v; times come from NOW(6)", got.sql, v)
		}
	}
	for i, wantVar := range want.vars {
		if i >= len(got.vars) {
			t.Errorf("%s\nhas no value %d", got.sql, i)
			continue
		}
		match, ok := wantVar.(func(any) bool)
		if ok && !match(got.vars[i]) || !ok && !reflect.DeepEqual(got.vars[i], wantVar) {
			t.Errorf("%s\nvalue %d = %#v, want %#v", got.sql, i, got.vars[i], wantVar)
		}
	}
	if want.columns != nil {
		if columns := columnsOf(got.sql); !slices.Equal(columns, want.columns) {
			t.Errorf("%s\nwrites %v, want %v", got.sql, columns, want.columns)
		}
	}
}

var setColumn = regexp.MustCompile("`(\\w+)`=")

// columnsOf returns, sorted, the columns an UPDATE sets or an INSERT names.
func columnsOf(query string) []string {
	var columns []string
	if _, rest, ok := strings.Cut(query, " SET "); ok && strings.HasPrefix(query, "UPDATE ") {
		set, _, _ := strings.Cut(rest, " WHERE ")
		for _, m := range setColumn.FindAllStringSubmatch(set, -1) {
			columns = append(columns, m[1])
		}
	} else if _, rest, ok := strings.Cut(query, " ("); ok && strings.HasPrefix(query, "INSERT ") {
		names, _, _ := strings.Cut(rest, ") VALUES ")
		for name := range strings.SplitSeq(names, ",") {
			columns = append(columns, strings.Trim(name, "`"))
		}
	}
	slices.Sort(columns)
	return columns
}

func isUUIDv7(v any) bool {
	id, ok := v.(db.ID)
	return ok && id[6]>>4 == 7
}

func isEmptySet(v any) bool {
	valuer, ok := v.(driver.Valuer)
	if !ok {
		return false
	}
	got, err := valuer.Value()
	return err == nil && got == ""
}
