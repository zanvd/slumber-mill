package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/zanvd/slumber-mill/src/logging"
)

const tracedSQL = "UPDATE assets SET status = ? WHERE id = ?"

func TestGormLoggerTrace(t *testing.T) {
	fast := time.Now()
	slow := time.Now().Add(-time.Hour)
	failure := errors.New("boom")

	tests := []struct {
		name      string
		level     logger.LogLevel
		begin     time.Time
		err       error
		wantLevel string
		wantMsg   string
	}{
		{"error", logger.Warn, fast, failure, "ERROR", "query failed"},
		{"wrapped record not found", logger.Info, fast, fmt.Errorf("x: %w", gorm.ErrRecordNotFound), "INFO", "query"},
		{"record not found", logger.Warn, fast, gorm.ErrRecordNotFound, "", ""},
		{"slow", logger.Warn, slow, nil, "WARN", "slow query"},
		{"slow record not found", logger.Warn, slow, gorm.ErrRecordNotFound, "WARN", "slow query"},
		{"fast", logger.Warn, fast, nil, "", ""},
		{"fast at info", logger.Info, fast, nil, "INFO", "query"},
		{"slow at error", logger.Error, slow, nil, "", ""},
		{"error when silent", logger.Silent, fast, failure, "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&buf, nil)))
			l := newGormLogger()
			l.slow = time.Minute
			gl := l.LogMode(tc.level)

			gl.Trace(ctx, tc.begin, func() (string, int64) { return tracedSQL, 1 }, tc.err)

			if tc.wantMsg == "" {
				if buf.Len() > 0 {
					t.Fatalf("logged %s, want nothing", buf.String())
				}
				return
			}
			rec := decodeRecord(t, &buf)
			if rec["level"] != tc.wantLevel || rec["msg"] != tc.wantMsg {
				t.Errorf("logged %s %q, want %s %q", rec["level"], rec["msg"], tc.wantLevel, tc.wantMsg)
			}
			if rec["sql"] != tracedSQL || rec["rows"] != 1.0 || rec["took"] == nil {
				t.Errorf("record %v lacks sql, rows or took", rec)
			}
			if tc.wantLevel == "ERROR" && rec["error"] != failure.Error() {
				t.Errorf("error = %v, want %q", rec["error"], failure.Error())
			}
		})
	}
}

func TestGormLoggerMessages(t *testing.T) {
	var buf bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&buf, nil)))
	l := newGormLogger()

	l.Info(ctx, "hidden %d", 1)
	l.Warn(ctx, "parsed %s with %d%%", "assets", 100)
	rec := decodeRecord(t, &buf)
	if rec["level"] != "WARN" || rec["msg"] != "parsed assets with 100%" {
		t.Errorf("logged %v, want only the warning", rec)
	}
}

func TestGormLoggerParamsFilter(t *testing.T) {
	sql, vars := newGormLogger().ParamsFilter(context.Background(), tracedSQL, "secret", 7)
	if sql != tracedSQL || vars != nil {
		t.Errorf("ParamsFilter() = %q, %v; want the SQL unchanged and no values", sql, vars)
	}
	if _, ok := any(newGormLogger()).(gorm.ParamsFilter); !ok {
		t.Error("gormLogger doesn't implement gorm.ParamsFilter")
	}
}

func TestGormConfig(t *testing.T) {
	cfg := gormConfig()
	if !cfg.SkipDefaultTransaction || !cfg.TranslateError {
		t.Errorf("SkipDefaultTransaction = %t, TranslateError = %t, want both true",
			cfg.SkipDefaultTransaction, cfg.TranslateError)
	}
	if _, ok := cfg.Logger.(gormLogger); !ok {
		t.Errorf("Logger = %T, want gormLogger", cfg.Logger)
	}
}

func decodeRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("want one log record, got %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("error decoding %q: %v", lines[0], err)
	}
	return rec
}
