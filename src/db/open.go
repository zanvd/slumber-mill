package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	maxOpenConns    = 25
	maxIdleConns    = 25
	connMaxLifetime = 5 * time.Minute
)

// Open connects to MySQL with the application's pool and GORM settings.
// The DSN is normalised first, and refused if it asks for interpolateParams or multiStatements.
// The driver logs through log, and so do GORM's logs whose context carries no logger.
// A statement whose context carries a sampled span gets a child span.
func Open(dsn string, log *slog.Logger) (*gorm.DB, error) {
	cfg, err := appConfig(dsn, log)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating connector: %w", err)
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(maxOpenConns)
	pool.SetMaxIdleConns(maxIdleConns)
	pool.SetConnMaxLifetime(connMaxLifetime)

	db, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: pool}), gormConfig(log))
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("error opening database: %w", err)
	}
	if err := traceStatements(db); err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

func gormConfig(log *slog.Logger) *gorm.Config {
	return &gorm.Config{
		SkipDefaultTransaction: true,
		TranslateError:         true,
		Logger:                 newGormLogger(log),
	}
}
