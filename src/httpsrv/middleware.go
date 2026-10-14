package httpsrv

import (
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
)

const tenantKey = "httpsrv.tenant"

// releaseUnreadBody stops a client that declares a request body and never sends it from holding the connection.
// To reuse a connection, net/http reads up to 256 KiB of a body the handler left unread, with no deadline:
// before it writes the response, which may be while the handler runs, and again once the handler returns.
// Full duplex skips the first read. After the handler, an expired deadline limits the second read
// to what has already arrived, and the connection is closed unless that was the whole body.
// It wraps the engine because Gin answers a trailing-slash redirect without running any middleware.
func releaseUnreadBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Without a body, net/http already watches the connection for the client going away,
		// and a deadline expiring there would cancel the request's context.
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex()
		body := &eofReader{ReadCloser: r.Body}
		r.Body = body
		next.ServeHTTP(w, r)
		if !body.eof {
			_ = rc.SetReadDeadline(time.Now())
		}
	})
}

// eofReader records whether its reader reached io.EOF, which is when net/http starts
// watching the connection for the client going away.
type eofReader struct {
	io.ReadCloser
	eof bool
}

func (r *eofReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF {
		r.eof = true
	}
	return n, err
}

// logRequest puts the request's logger into its context and writes one line once the request is done.
// It logs no headers, so the Authorization value never reaches the log.
func (s *Server) logRequest(c *gin.Context) {
	start := time.Now()
	log := s.log.With(
		slog.String("method", c.Request.Method),
		slog.String("route", c.FullPath()),
		slog.String("client_ip", c.ClientIP()),
	)
	c.Request = c.Request.WithContext(logging.CtxWithLogger(c.Request.Context(), log))

	c.Next()

	// Authentication replaces the logger with one carrying the tenant.
	s.logger(c).LogAttrs(c.Request.Context(), slog.LevelInfo, "request",
		slog.Int("status", c.Writer.Status()),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) recover(c *gin.Context) {
	defer func() {
		v := recover()
		if v == nil {
			return
		}
		// net/http's sentinel for aborting a response must reach the server to cut the connection.
		if v == http.ErrAbortHandler {
			panic(v)
		}
		s.logger(c).Error("panic serving request", "panic", v, "stack", string(debug.Stack()))
		if c.Writer.Written() {
			c.Abort()
			return
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, internalError())
	}()
	c.Next()
}

func (s *Server) authenticate(c *gin.Context) {
	key, ok := bearerToken(c.GetHeader("Authorization"))
	if !ok {
		s.writeError(c, tenant.ErrUnauthenticated)
		return
	}
	ctx := c.Request.Context()
	t, err := s.tenants.Authenticate(ctx, key)
	if err != nil {
		s.writeError(c, err)
		return
	}
	log := s.logger(c).With(slog.String("tenant", t.Slug))
	c.Request = c.Request.WithContext(logging.CtxWithLogger(ctx, log))
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
