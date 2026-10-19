package httpsrv

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
)

const tenantKey = "httpsrv.tenant"

// releaseUnreadBody stops a client that declares a request body and never sends it from holding the connection.
// To reuse a connection, net/http reads up to 256 KiB of a body the handler left unread, with no deadline:
// before it writes the response, which may be while the handler runs, and again once the handler returns.
// A response that goes out before the body has reached io.EOF says Connection: close, which skips the first read;
// the header is set up front and removed when the body reaches io.EOF. Once the handler is done,
// an expired read deadline makes the second read fail at once.
// It wraps the engine because Gin answers a trailing-slash redirect without running any middleware.
func releaseUnreadBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Without a body, net/http already watches the connection for the client going away,
		// and a deadline expiring there would cancel the request's context.
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		body := &eofReader{ReadCloser: r.Body, header: w.Header()}
		body.header.Set("Connection", "close")
		r.Body = body
		// Deferred, so a handler aborted with http.ErrAbortHandler releases the connection too.
		defer func() {
			if !body.eof {
				_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// eofReader records whether its reader reached io.EOF, which is when net/http starts
// watching the connection for the client going away. It then lets the response keep the connection.
type eofReader struct {
	io.ReadCloser
	header http.Header
	eof    bool
}

func (r *eofReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF && !r.eof {
		r.eof = true
		r.header.Del("Connection")
	}
	return n, err
}

type requestLogKey struct{}

// requestLog carries the request's logger out of the engine to logRequests, which writes the request line.
// It stays empty when Gin answers without running any middleware.
type requestLog struct {
	log *slog.Logger
}

// logRequests writes one line per request once it's done, and recovers a panic from anywhere in the engine.
// It wraps the engine because Gin answers a trailing-slash redirect without running any middleware.
// It logs no headers, so the Authorization value never reaches the log.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rl := &requestLog{}
		r = r.WithContext(context.WithValue(r.Context(), requestLogKey{}, rl))
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			v := recover()
			log := rl.log
			if log == nil {
				log = s.requestLogger(r.Context(), r.Method, "", s.clientIP(r))
			}
			// net/http's sentinel for aborting a response must reach the server to cut the connection.
			aborted := v == http.ErrAbortHandler
			if v != nil && !aborted {
				log.Error("panic serving request", "panic", v, "stack", string(debug.Stack()))
				if sw.status == 0 {
					writeInternalError(sw)
				}
			}
			status := sw.status
			if status == 0 && !aborted {
				status = http.StatusOK
			}
			log.LogAttrs(r.Context(), slog.LevelInfo, "request",
				slog.Int("status", status),
				slog.Duration("duration", time.Since(start)),
			)
			if aborted {
				panic(v)
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

// describeRequest names the request's span after its route, and gives the request a logger with its method, route
// and client, for its handlers and its request line.
func (s *Server) describeRequest(c *gin.Context) {
	ctx := c.Request.Context()
	nameSpan(trace.SpanFromContext(ctx), c.Request.Method, c.FullPath())
	s.setLogger(c, s.requestLogger(ctx, c.Request.Method, c.FullPath(), c.ClientIP()))
}

// requestLogger adds the trace and span IDs when the request's trace is exported, so its lines can be found from it.
func (s *Server) requestLogger(ctx context.Context, method, route, clientIP string) *slog.Logger {
	attrs := []any{
		slog.String("method", method),
		slog.String("route", route),
		slog.String("client_ip", clientIP),
	}
	// A span that doesn't record may still hold the caller's sampled IDs, as when tracing is off.
	if span := trace.SpanFromContext(ctx); span.IsRecording() && span.SpanContext().IsSampled() {
		sc := span.SpanContext()
		attrs = append(attrs, slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return s.log.With(attrs...)
}

// clientIP resolves the client's address through the engine's trusted proxies for a request
// the engine answered without a context of its own.
func (s *Server) clientIP(r *http.Request) string {
	c := gin.CreateTestContextOnly(nil, s.engine)
	c.Request = r
	return c.ClientIP()
}

func (s *Server) setLogger(c *gin.Context, log *slog.Logger) {
	ctx := c.Request.Context()
	if rl, ok := ctx.Value(requestLogKey{}).(*requestLog); ok {
		rl.log = log
	}
	c.Request = c.Request.WithContext(logging.CtxWithLogger(ctx, log))
}

func writeInternalError(w http.ResponseWriter) {
	body, err := json.Marshal(internalError())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)
	if err == nil {
		_, _ = w.Write(body)
	}
}

// statusWriter records the status given to the writer, which a client may never see if the connection is cut.
// Gin's writer asserts Hijacker and CloseNotifier on it without checking, so it forwards both.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.ResponseWriter.WriteHeader(code)
	if w.status == 0 {
		w.status = code
	}
}

func (w *statusWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return n, err
}

func (w *statusWriter) Flush() {
	_ = http.NewResponseController(w.ResponseWriter).Flush()
	if w.status == 0 {
		w.status = http.StatusOK
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *statusWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (s *Server) authenticate(c *gin.Context) {
	key, ok := bearerToken(c.GetHeader("Authorization"))
	if !ok {
		s.writeError(c, tenant.ErrUnauthenticated)
		return
	}
	t, err := s.tenants.Authenticate(c.Request.Context(), key)
	if err != nil {
		s.writeError(c, err)
		return
	}
	trace.SpanFromContext(c.Request.Context()).SetAttributes(attribute.String(attrTenant, t.Slug))
	s.setLogger(c, s.logger(c).With(slog.String("tenant", t.Slug)))
	c.Set(tenantKey, t)
	c.Next()
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// tenantFrom returns the tenant the auth middleware stored; only routes behind it may call it.
func tenantFrom(c *gin.Context) tenant.Tenant {
	return c.MustGet(tenantKey).(tenant.Tenant)
}
