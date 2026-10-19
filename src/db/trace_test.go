package db

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

const boundSecret = "bound_secret_value"

func TestTraceStatements(t *testing.T) {
	type row struct{ ID uint64 }
	tests := []struct {
		name      string
		run       func(g *gorm.DB) error
		wantName  string
		wantOp    string
		wantTable string
		wantSQL   string
		wantError bool
	}{
		{name: "select", run: func(g *gorm.DB) error {
			var r row
			return g.Table("assets").Where("title = ?", boundSecret).Take(&r).Error
		}, wantName: "SELECT assets", wantOp: "SELECT", wantTable: "assets",
			wantSQL: "SELECT * FROM `assets` WHERE title = ? LIMIT ?"},
		{name: "insert", run: func(g *gorm.DB) error {
			return g.Table("assets").Create(map[string]any{"title": boundSecret}).Error
		}, wantName: "INSERT assets", wantOp: "INSERT", wantTable: "assets",
			wantSQL: "INSERT INTO `assets` (`title`) VALUES (?)"},
		{name: "update", run: func(g *gorm.DB) error {
			return g.Table("assets").Where("id = ?", 7).Updates(map[string]any{"title": boundSecret}).Error
		}, wantName: "UPDATE assets", wantOp: "UPDATE", wantTable: "assets",
			wantSQL: "UPDATE `assets` SET `title`=? WHERE id = ?"},
		{name: "update without a where clause", run: func(g *gorm.DB) error {
			_ = g.Table("assets").Updates(map[string]any{"title": boundSecret}).Error
			return nil
		}, wantName: "UPDATE assets", wantOp: "UPDATE", wantTable: "assets", wantError: true},
		{name: "raw statement", run: func(g *gorm.DB) error {
			return g.Exec("\n  delete FROM assets WHERE title = ?", boundSecret).Error
		}, wantName: "DELETE", wantOp: "DELETE", wantSQL: "\n  delete FROM assets WHERE title = ?"},
		{name: "raw query", run: func(g *gorm.DB) error {
			var r row
			return g.Raw("select id FROM assets WHERE title = ?", boundSecret).Find(&r).Error
		}, wantName: "SELECT", wantOp: "SELECT", wantSQL: "select id FROM assets WHERE title = ?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, _, _ := dryRun(t)
			if err := traceStatements(g); err != nil {
				t.Fatalf("traceStatements() error = %v", err)
			}
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			ctx, parent := tp.Tracer("test").Start(context.Background(), "request")
			if err := tt.run(g.WithContext(ctx)); err != nil {
				t.Fatalf("statement error = %v", err)
			}
			parent.End()

			ended := rec.Ended()
			if len(ended) != 2 {
				t.Fatalf("got %d spans, want the statement's and the parent", len(ended))
			}
			span := ended[0]
			if span.Parent().SpanID() != parent.SpanContext().SpanID() {
				t.Errorf("parent = %s, want %s", span.Parent().SpanID(), parent.SpanContext().SpanID())
			}
			if span.SpanKind() != trace.SpanKindClient || span.Name() != tt.wantName {
				t.Errorf("span = %v %q, want client %q", span.SpanKind(), span.Name(), tt.wantName)
			}
			a := map[attribute.Key]string{}
			for _, kv := range span.Attributes() {
				a[kv.Key] = kv.Value.Emit()
				if strings.Contains(kv.Value.Emit(), boundSecret) {
					t.Errorf("%s carries a bound value: %s", kv.Key, kv.Value.Emit())
				}
			}
			want := map[attribute.Key]string{
				"db.system": "mysql", "db.operation.name": tt.wantOp, "db.collection.name": tt.wantTable,
			}
			if tt.wantSQL != "" {
				want["db.query.text"] = tt.wantSQL
			}
			for k, v := range want {
				if a[k] != v {
					t.Errorf("%s = %q, want %q", k, a[k], v)
				}
			}
			if got := span.Status().Code == codes.Error; got != tt.wantError {
				t.Errorf("status = %v, want error %v", span.Status(), tt.wantError)
			}
			if span.Status().Description != "" {
				t.Errorf("status description = %q, want none", span.Status().Description)
			}
		})
	}
}

func TestTraceStatementsOnlyUnderSampledSpans(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()), sdktrace.WithSpanProcessor(rec))
	unsampled, span := tp.Tracer("test").Start(context.Background(), "request")
	defer span.End()
	for name, ctx := range map[string]context.Context{"no span": context.Background(), "unsampled": unsampled} {
		t.Run(name, func(t *testing.T) {
			g, _, _ := dryRun(t)
			if err := traceStatements(g); err != nil {
				t.Fatalf("traceStatements() error = %v", err)
			}
			var r struct{ ID uint64 }
			_ = g.WithContext(ctx).Table("assets").Take(&r).Error
			if n := len(rec.Started()); n != 0 {
				t.Errorf("started %d spans, want none", n)
			}
		})
	}
}
