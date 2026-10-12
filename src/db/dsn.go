package db

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	errInterpolateParams = errors.New("interpolateParams=true is not allowed in the DSN")
	errMultiStatements   = errors.New("multiStatements=true is allowed only on the migration pool")
	errNoLogger          = errors.New("a logger is required")
)

func appConfig(dsn string, log *slog.Logger) (*mysql.Config, error) {
	cfg, err := parseDSN(dsn, log)
	if err != nil {
		return nil, err
	}
	if cfg.MultiStatements {
		return nil, errMultiStatements
	}
	return cfg, nil
}

func migrationConfig(dsn string, log *slog.Logger) (*mysql.Config, error) {
	cfg, err := parseDSN(dsn, log)
	if err != nil {
		return nil, err
	}
	cfg.MultiStatements = true
	return cfg, nil
}

// parseDSN applies the DSN rules, and has the driver log through log rather than its own plain-text logger.
func parseDSN(dsn string, log *slog.Logger) (cfg *mysql.Config, err error) {
	if log == nil {
		return nil, errNoLogger
	}
	// The driver panics on parameters it has removed, such as strict.
	defer func() {
		if r := recover(); r != nil {
			cfg, err = nil, fmt.Errorf("error parsing DSN: %v", r)
		}
	}()
	cfg, err = mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing DSN: %w", err)
	}
	if cfg.InterpolateParams {
		return nil, errInterpolateParams
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.ClientFoundRows = true
	for name := range cfg.Params {
		if isTimeZoneVar(name) {
			delete(cfg.Params, name)
		}
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string, 1)
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Logger = driverLogger{log: log}
	return cfg, nil
}

// isTimeZoneVar reports whether a DSN parameter, which the driver sends as SET name = value,
// sets the session time zone under any of its spellings.
func isTimeZoneVar(name string) bool {
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "@@")
	for _, scope := range []string{"session.", "local."} {
		name = strings.TrimPrefix(name, scope)
	}
	return name == "time_zone"
}
