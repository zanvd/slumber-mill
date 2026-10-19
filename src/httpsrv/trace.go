package httpsrv

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/zanvd/slumber-mill/src/httpsrv"

	attrMethod = "http.request.method"
	attrRoute  = "http.route"
	attrStatus = "http.response.status_code"
	attrTenant = "slumber_mill.tenant"

	otherMethod = "_OTHER"
)

// traceRequests starts each request's server span, continuing the caller's trace from its traceparent.
// It wraps the engine because Gin answers a trailing-slash redirect without running any middleware.
// Spans carry the method, route, status and tenant, and never a header, the query string or the body.
func (s *Server) traceRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		method := spanMethod(r.Method)
		name := method
		if method == otherMethod {
			name = "HTTP"
		}
		ctx, span := s.tracer.Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String(attrMethod, method)),
		)
		r = r.WithContext(ctx)
		if !span.IsRecording() {
			next.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			// Only http.ErrAbortHandler gets past logRequests.
			v := recover()
			status := sw.status
			if status == 0 && v == nil {
				status = http.StatusOK
			}
			if status != 0 {
				span.SetAttributes(attribute.Int(attrStatus, status))
			}
			switch {
			case v != nil:
				span.SetStatus(codes.Error, "response aborted")
			case status >= http.StatusInternalServerError:
				span.SetStatus(codes.Error, "")
			}
			span.End()
			if v != nil {
				panic(v)
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

// spanMethod returns the method, or _OTHER for one HTTP doesn't define, which a client could use to name spans.
func spanMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	}
	return otherMethod
}

// nameSpan names the request's span after its route, once the engine has matched one.
func nameSpan(span trace.Span, method, route string) {
	if route == "" || !span.IsRecording() {
		return
	}
	span.SetName(spanMethod(method) + " " + route)
	span.SetAttributes(attribute.String(attrRoute, route))
}
