package db

import (
	"database/sql"
	"fmt"
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
func Open(dsn string) (*gorm.DB, error) {
	cfg, err := appConfig(dsn)
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

	db, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: pool}), gormConfig())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("error opening database: %w", err)
	}
	return db, nil
}

func gormConfig() *gorm.Config {
	return &gorm.Config{
		SkipDefaultTransaction: true,
		TranslateError:         true,
		Logger:                 newGormLogger(),
	}
}
