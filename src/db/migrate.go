package db

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	createSchemaMigrations = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT UNSIGNED NOT NULL,
    name       VARCHAR(255)    NOT NULL,
    applied_at DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci`

	releaseTimeout = 15 * time.Second
)

type migration struct {
	version uint64
	name    string
	sql     string
}

// Migrate applies the embedded migrations that the DSN's database hasn't recorded, in order,
// and returns the names of those it applied. Concurrent callers wait for each other.
// The driver logs through log.
func Migrate(ctx context.Context, dsn string, log *slog.Logger) (applied []string, err error) {
	migrations, err := loadMigrations(migrationFiles)
	if err != nil {
		return nil, err
	}
	cfg, err := migrationConfig(dsn, log)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating migration connector: %w", err)
	}
	pool := sql.OpenDB(connector)
	defer pool.Close()

	// GET_LOCK belongs to the session, so the lock, the migrations and its release share one connection.
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("error connecting for migrations: %w", err)
	}
	defer conn.Close()

	var locked sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK('slumber-mill.migrate', 60)").Scan(&locked); err != nil {
		return nil, fmt.Errorf("error taking the migration lock: %w", err)
	}
	if !locked.Valid || locked.Int64 != 1 {
		return nil, errors.New("error taking the migration lock: timed out")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if _, releaseErr := conn.ExecContext(ctx, "DO RELEASE_LOCK('slumber-mill.migrate')"); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("error releasing the migration lock: %w", releaseErr))
		}
	}()

	if _, err := conn.ExecContext(ctx, createSchemaMigrations); err != nil {
		return nil, fmt.Errorf("error creating schema_migrations: %w", err)
	}
	done, err := appliedVersions(ctx, conn)
	if err != nil {
		return nil, err
	}
	for _, m := range migrations {
		if done[m.version] {
			continue
		}
		if _, err := conn.ExecContext(ctx, m.sql); err != nil {
			return applied, fmt.Errorf("error applying migration %s: %w", m.name, err)
		}
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO schema_migrations (version, name) VALUES (?, ?)", m.version, m.name); err != nil {
			return applied, fmt.Errorf("error recording migration %s: %w", m.name, err)
		}
		applied = append(applied, m.name)
	}
	return applied, nil
}

func appliedVersions(ctx context.Context, conn *sql.Conn) (map[uint64]bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("error reading schema_migrations: %w", err)
	}
	defer rows.Close()
	done := make(map[uint64]bool)
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("error reading schema_migrations: %w", err)
		}
		done[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading schema_migrations: %w", err)
	}
	return done, nil
}

// loadMigrations reads the files named NNNN_description.sql under migrations/, sorted by version.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, fmt.Errorf("error listing migrations: %w", err)
	}
	var migrations []migration
	for _, e := range entries {
		name := e.Name()
		prefix, _, ok := strings.Cut(name, "_")
		version, err := strconv.ParseUint(prefix, 10, 64)
		if !ok || err != nil || version == 0 || path.Ext(name) != ".sql" {
			return nil, fmt.Errorf("migration %s isn't named NNNN_description.sql", name)
		}
		body, err := fs.ReadFile(fsys, path.Join("migrations", name))
		if err != nil {
			return nil, fmt.Errorf("error reading migration %s: %w", name, err)
		}
		migrations = append(migrations, migration{version: version, name: name, sql: string(body)})
	}
	slices.SortFunc(migrations, func(a, b migration) int { return cmp.Compare(a.version, b.version) })
	for i := 1; i < len(migrations); i++ {
		if a, b := migrations[i-1], migrations[i]; a.version == b.version {
			return nil, fmt.Errorf("migrations %s and %s share version %d", a.name, b.name, a.version)
		}
	}
	return migrations, nil
}
