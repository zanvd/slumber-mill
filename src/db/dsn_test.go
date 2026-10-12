package db

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const baseDSN = "user:pass@tcp(mysql:3306)/slumber_mill"

var pools = map[string]func(string, *slog.Logger) (*mysql.Config, error){
	"app":       appConfig,
	"migration": migrationConfig,
}

func TestParseDSNForcesSessionSettings(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"no parameters", baseDSN},
		{"no database", "user:pass@tcp(mysql:3306)/"},
		{"parseTime=false", baseDSN + "?parseTime=false"},
		{"loc=Local", baseDSN + "?loc=Local"},
		{"loc=Europe/Ljubljana", baseDSN + "?loc=Europe%2FLjubljana"},
		{"clientFoundRows=false", baseDSN + "?clientFoundRows=false"},
		{"time_zone", baseDSN + "?time_zone=%27-05%3A00%27"},
		{"TIME_ZONE", baseDSN + "?TIME_ZONE=%27-05%3A00%27"},
		{"@@session.time_zone", baseDSN + "?@@session.time_zone=%27-05%3A00%27"},
		{"@@local.Time_Zone", baseDSN + "?@@local.Time_Zone=%27-05%3A00%27"},
		{"all conflicting", baseDSN + "?parseTime=false&loc=Local&clientFoundRows=false&time_zone=%27SYSTEM%27"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for pool, parse := range pools {
				cfg, err := parse(tc.dsn, discardLogger())
				if err != nil {
					t.Fatalf("%s pool: unexpected error: %v", pool, err)
				}
				if !cfg.ParseTime {
					t.Errorf("%s pool: ParseTime = false, want true", pool)
				}
				if cfg.Loc != time.UTC {
					t.Errorf("%s pool: Loc = %v, want UTC", pool, cfg.Loc)
				}
				if !cfg.ClientFoundRows {
					t.Errorf("%s pool: ClientFoundRows = false, want true", pool)
				}
				want := map[string]string{"time_zone": "'+00:00'"}
				if len(cfg.Params) != len(want) || cfg.Params["time_zone"] != want["time_zone"] {
					t.Errorf("%s pool: Params = %v, want %v", pool, cfg.Params, want)
				}
			}
		})
	}
}

func TestParseDSNKeepsOtherSettings(t *testing.T) {
	cfg, err := appConfig("u:p@tcp(db.example:3307)/app?timeout=5s&tls=skip-verify&sql_mode=%27TRADITIONAL%27",
		discardLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.User != "u" || cfg.Passwd != "p" || cfg.Addr != "db.example:3307" || cfg.DBName != "app" {
		t.Errorf("connection settings changed: user %q, addr %q, db %q", cfg.User, cfg.Addr, cfg.DBName)
	}
	if cfg.Timeout != 5*time.Second || cfg.TLSConfig != "skip-verify" {
		t.Errorf("driver settings changed: timeout %v, tls %q", cfg.Timeout, cfg.TLSConfig)
	}
	if cfg.Params["sql_mode"] != "'TRADITIONAL'" {
		t.Errorf("sql_mode = %q, want 'TRADITIONAL'", cfg.Params["sql_mode"])
	}
}

func TestParseDSNPoolRules(t *testing.T) {
	tests := []struct {
		name          string
		dsn           string
		wantAppErr    error
		wantMigErr    error
		wantMultiStmt bool
	}{
		{"defaults", baseDSN, nil, nil, true},
		{"interpolateParams=true", baseDSN + "?interpolateParams=true", errInterpolateParams, errInterpolateParams, false},
		{"interpolateParams=false", baseDSN + "?interpolateParams=false", nil, nil, true},
		{"multiStatements=true", baseDSN + "?multiStatements=true", errMultiStatements, nil, true},
		{"multiStatements=false", baseDSN + "?multiStatements=false", nil, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app, err := appConfig(tc.dsn, discardLogger())
			if !errors.Is(err, tc.wantAppErr) {
				t.Errorf("appConfig() error = %v, want %v", err, tc.wantAppErr)
			}
			if err == nil && (app.MultiStatements || app.InterpolateParams) {
				t.Errorf("app pool: MultiStatements = %t, InterpolateParams = %t, want both false",
					app.MultiStatements, app.InterpolateParams)
			}

			mig, err := migrationConfig(tc.dsn, discardLogger())
			if !errors.Is(err, tc.wantMigErr) {
				t.Errorf("migrationConfig() error = %v, want %v", err, tc.wantMigErr)
			}
			if err == nil && mig.MultiStatements != tc.wantMultiStmt {
				t.Errorf("migration pool: MultiStatements = %t, want %t", mig.MultiStatements, tc.wantMultiStmt)
			}
		})
	}
}

func TestParseDSNInvalid(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"no slash", "no slash here"},
		{"strict=true", baseDSN + "?strict=true"},
		{"strict=false", baseDSN + "?strict=false"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for pool, parse := range pools {
				if _, err := parse(tc.dsn, discardLogger()); err == nil {
					t.Errorf("%s pool: error = nil, want a parse error", pool)
				}
			}
			if _, err := Open(tc.dsn, discardLogger()); err == nil {
				t.Error("Open() error = nil, want a parse error")
			}
			if _, err := Migrate(t.Context(), tc.dsn, discardLogger()); err == nil {
				t.Error("Migrate() error = nil, want a parse error")
			}
		})
	}
}

func TestLoggerRequired(t *testing.T) {
	for pool, parse := range pools {
		if _, err := parse(baseDSN, nil); !errors.Is(err, errNoLogger) {
			t.Errorf("%s pool: error = %v, want %v", pool, err, errNoLogger)
		}
	}
	if _, err := Open(baseDSN, nil); !errors.Is(err, errNoLogger) {
		t.Errorf("Open() error = %v, want %v", err, errNoLogger)
	}
	if _, err := Migrate(t.Context(), baseDSN, nil); !errors.Is(err, errNoLogger) {
		t.Errorf("Migrate() error = %v, want %v", err, errNoLogger)
	}
}

func TestDriverLogsThroughSlog(t *testing.T) {
	for pool, parse := range pools {
		t.Run(pool, func(t *testing.T) {
			var buf bytes.Buffer
			cfg, err := parse(baseDSN, slog.New(slog.NewJSONHandler(&buf, nil)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// The driver's own call, from connection.go, with the file and line it prepends.
			cfg.Logger.Print("connection.go:803 ", "closing bad idle connection: ", io.EOF)

			rec := decodeRecord(t, &buf)
			want := "connection.go:803 closing bad idle connection: EOF"
			if rec["level"] != "ERROR" || rec["msg"] != want || rec["component"] != "mysql" {
				t.Errorf("logged %v, want an ERROR %q from component mysql", rec, want)
			}
		})
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
