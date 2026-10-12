package db

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	errInterpolateParams = errors.New("interpolateParams=true is not allowed in the DSN")
	errMultiStatements   = errors.New("multiStatements=true is allowed only on the migration pool")
)

func appConfig(dsn string) (*mysql.Config, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.MultiStatements {
		return nil, errMultiStatements
	}
	return cfg, nil
}

func migrationConfig(dsn string) (*mysql.Config, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MultiStatements = true
	return cfg, nil
}

func parseDSN(dsn string) (cfg *mysql.Config, err error) {
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
