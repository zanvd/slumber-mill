package audio

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/zanvd/slumber-mill/src/audio"

	attrProfile  = "slumber_mill.profile"
	attrProfiles = "slumber_mill.profiles"
	attrLUFS     = "slumber_mill.lufs"
	attrTruePeak = "slumber_mill.true_peak_dbtp"
	attrInSpec   = "slumber_mill.in_spec"
	attrGain     = "slumber_mill.gain_db"
)

// startSpan starts the span of a step under the span in ctx, from that span's provider,
// so steps are traced only within a traced encode.
func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, name,
		trace.WithAttributes(attrs...))
}

// endSpan ends a step's span. A rejection's status carries its tenant-safe message, and any other error's
// carries nothing, since ffmpeg's output names internal paths.
func endSpan(span trace.Span, err error) {
	if err != nil {
		message := ""
		if rejection, ok := errors.AsType[*Rejection](err); ok {
			message = rejection.Message
		}
		span.SetStatus(codes.Error, message)
	}
	span.End()
}
