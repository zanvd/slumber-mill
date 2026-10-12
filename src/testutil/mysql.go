package testutil

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/zanvd/slumber-mill/src/db"
)

const (
	dsnEnv         = "SLUMBER_MILL_DSN"
	databasePrefix = "slumber_mill_test_"
	adminTimeout   = 30 * time.Second
)

// NewDatabase creates a migrated throwaway database that is dropped when tb ends, and returns its DSN.
func NewDatabase(tb testing.TB) string {
	tb.Helper()
	dsn := NewEmptyDatabase(tb)
	if _, err := db.Migrate(tb.Context(), dsn, Logger(tb)); err != nil {
		tb.Fatalf("error migrating the test database: %v", err)
	}
	return dsn
}

// NewEmptyDatabase creates a throwaway database without migrations that is dropped when tb ends,
// and returns its DSN. The server and account come from SLUMBER_MILL_DSN; any database it names is ignored.
func NewEmptyDatabase(tb testing.TB) string {
	tb.Helper()
	dsn := strings.TrimSpace(os.Getenv(dsnEnv))
	if dsn == "" {
		tb.Fatalf("%s is not set; integration tests need MySQL", dsnEnv)
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		tb.Fatalf("error parsing %s: %v", dsnEnv, err)
	}

	server := cfg.Clone()
	server.DBName = ""
	connector, err := mysql.NewConnector(server)
	if err != nil {
		tb.Fatalf("error connecting to MySQL from %s: %v", dsnEnv, err)
	}
	admin := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(tb.Context(), adminTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		admin.Close()
		tb.Fatalf("error connecting to MySQL from %s: %v", dsnEnv, err)
	}

	name := databasePrefix + strings.ToLower(rand.Text())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		tb.Fatalf("error creating test database %s: %v", name, err)
	}
	tb.Cleanup(func() {
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), adminTimeout)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
			tb.Errorf("error dropping test database %s: %v", name, err)
		}
	})

	cfg.DBName = name
	return cfg.FormatDSN()
}

// parseDSN is mysql.ParseDSN with the panic it raises on removed parameters, such as strict, returned as an error.
func parseDSN(dsn string) (cfg *mysql.Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			cfg, err = nil, fmt.Errorf("%v", r)
		}
	}()
	return mysql.ParseDSN(dsn)
}
