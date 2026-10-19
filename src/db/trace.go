package db

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

const (
	tracerName = "github.com/zanvd/slumber-mill/src/db"
	spanKey    = "slumber_mill:span"
)

// traceStatements makes a client span for every statement whose context carries a sampled, recording span,
// from that span's provider. The SQL keeps its placeholders, since interpolateParams is refused,
// so no bound value reaches a span, and neither does an error's text, which can quote one.
func traceStatements(g *gorm.DB) error {
	cb := g.Callback()
	for _, err := range []error{
		cb.Create().Before("gorm:create").Register("tracing:before_create", startSpan),
		cb.Create().After("gorm:create").Register("tracing:after_create", endSpan("INSERT")),
		cb.Query().Before("gorm:query").Register("tracing:before_query", startSpan),
		cb.Query().After("gorm:query").Register("tracing:after_query", endSpan("SELECT")),
		cb.Update().Before("gorm:update").Register("tracing:before_update", startSpan),
		cb.Update().After("gorm:update").Register("tracing:after_update", endSpan("UPDATE")),
		cb.Delete().Before("gorm:delete").Register("tracing:before_delete", startSpan),
		cb.Delete().After("gorm:delete").Register("tracing:after_delete", endSpan("DELETE")),
		cb.Row().Before("gorm:row").Register("tracing:before_row", startSpan),
		cb.Row().After("gorm:row").Register("tracing:after_row", endSpan("")),
		cb.Raw().Before("gorm:raw").Register("tracing:before_raw", startSpan),
		cb.Raw().After("gorm:raw").Register("tracing:after_raw", endSpan("")),
	} {
		if err != nil {
			return fmt.Errorf("error registering the tracing callbacks: %w", err)
		}
	}
	return nil
}

func startSpan(g *gorm.DB) {
	ctx := g.Statement.Context
	parent := trace.SpanFromContext(ctx)
	if !parent.IsRecording() || !parent.SpanContext().IsSampled() {
		return
	}
	_, span := parent.TracerProvider().Tracer(tracerName).Start(ctx, "db",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "mysql")),
	)
	g.InstanceSet(spanKey, statementSpan{span: span, raw: g.Statement.SQL.Len() > 0})
}

// statementSpan is a statement's span. A raw statement's SQL was written before the callbacks ran,
// so its table is only the destination's, and its operation is the SQL's first keyword.
type statementSpan struct {
	span trace.Span
	raw  bool
}

func endSpan(operation string) func(*gorm.DB) {
	return func(g *gorm.DB) {
		v, ok := g.InstanceGet(spanKey)
		if !ok {
			return
		}
		s, ok := v.(statementSpan)
		if !ok {
			return
		}
		span := s.span
		query := g.Statement.SQL.String()
		op, table := operation, g.Statement.Table
		if s.raw || op == "" {
			op, table = firstKeyword(query), ""
		}
		name := op
		if table != "" {
			name += " " + table
		}
		if name != "" {
			span.SetName(name)
		}
		attrs := []attribute.KeyValue{attribute.String("db.query.text", query)}
		if op != "" {
			attrs = append(attrs, attribute.String("db.operation.name", op))
		}
		if table != "" {
			attrs = append(attrs, attribute.String("db.collection.name", table))
		}
		span.SetAttributes(attrs...)
		if g.Error != nil && !errors.Is(g.Error, gorm.ErrRecordNotFound) {
			span.SetStatus(codes.Error, "")
		}
		span.End()
	}
}

func firstKeyword(query string) string {
	word := strings.TrimLeftFunc(query, unicode.IsSpace)
	if i := strings.IndexFunc(word, unicode.IsSpace); i >= 0 {
		word = word[:i]
	}
	return strings.ToUpper(word)
}
