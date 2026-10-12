package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/zanvd/slumber-mill/src/logging"
)

const slowQueryThreshold = 200 * time.Millisecond

// gormLogger writes GORM's logs through the context's logger.
// Queries are logged with their placeholders, never their values.
type gormLogger struct {
	level logger.LogLevel
	slow  time.Duration
}

func newGormLogger() gormLogger {
	return gormLogger{level: logger.Warn, slow: slowQueryThreshold}
}

func (l gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	l.level = level
	return l
}

func (l gormLogger) Info(ctx context.Context, msg string, args ...any) {
	l.log(ctx, logger.Info, slog.LevelInfo, msg, args)
}

func (l gormLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.log(ctx, logger.Warn, slog.LevelWarn, msg, args)
}

func (l gormLogger) Error(ctx context.Context, msg string, args ...any) {
	l.log(ctx, logger.Error, slog.LevelError, msg, args)
}

func (l gormLogger) log(ctx context.Context, minLevel logger.LogLevel, level slog.Level, msg string, args []any) {
	if l.level < minLevel {
		return
	}
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	logging.LoggerFromContext(ctx).Log(ctx, level, msg)
}

func (l gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.level <= logger.Silent {
		return
	}
	took := time.Since(begin)
	switch {
	case err != nil && l.level >= logger.Error && !errors.Is(err, gorm.ErrRecordNotFound):
		l.trace(ctx, slog.LevelError, "query failed", took, fc, slog.String("error", err.Error()))
	case l.slow > 0 && took > l.slow && l.level >= logger.Warn:
		l.trace(ctx, slog.LevelWarn, "slow query", took, fc)
	case l.level >= logger.Info:
		l.trace(ctx, slog.LevelInfo, "query", took, fc)
	}
}

func (l gormLogger) trace(ctx context.Context, level slog.Level, msg string, took time.Duration,
	fc func() (string, int64), attrs ...slog.Attr) {
	sql, rows := fc()
	attrs = append(attrs, slog.String("sql", sql), slog.Duration("took", took))
	if rows >= 0 {
		attrs = append(attrs, slog.Int64("rows", rows))
	}
	logging.LoggerFromContext(ctx).LogAttrs(ctx, level, msg, attrs...)
}

// ParamsFilter drops the query's values, so the SQL that Trace logs keeps its placeholders.
func (gormLogger) ParamsFilter(_ context.Context, sql string, _ ...any) (string, []any) {
	return sql, nil
}
