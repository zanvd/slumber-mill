package db

import (
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const baseDSN = "user:pass@tcp(mysql:3306)/slumber_mill"

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
			for pool, parse := range map[string]func(string) (*mysql.Config, error){
				"app":       appConfig,
				"migration": migrationConfig,
			} {
				cfg, err := parse(tc.dsn)
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
	cfg, err := appConfig("u:p@tcp(db.example:3307)/app?timeout=5s&tls=skip-verify&sql_mode=%27TRADITIONAL%27")
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
			app, err := appConfig(tc.dsn)
			if !errors.Is(err, tc.wantAppErr) {
				t.Errorf("appConfig() error = %v, want %v", err, tc.wantAppErr)
			}
			if err == nil && (app.MultiStatements || app.InterpolateParams) {
				t.Errorf("app pool: MultiStatements = %t, InterpolateParams = %t, want both false",
					app.MultiStatements, app.InterpolateParams)
			}

			mig, err := migrationConfig(tc.dsn)
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
	if _, err := appConfig("no slash here"); err == nil {
		t.Error("appConfig() error = nil, want a parse error")
	}
}
