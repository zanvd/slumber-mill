package httpsrv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/validation"
)

const validKey = "sm_test_only_not_a_real_key"

var (
	acme    = tenant.Tenant{ID: 42, Slug: "acme", TargetLUFS: -18}
	errBoom = errors.New("boom")
)

type fixture struct {
	srv     *Server
	tenants *fakeTenants
	assets  *fakeAssets
	ping    *fakePinger
	logs    *bytes.Buffer
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := &fixture{
		tenants: &fakeTenants{keys: map[string]tenant.Tenant{validKey: acme}},
		assets:  &fakeAssets{},
		ping:    &fakePinger{},
		logs:    &bytes.Buffer{},
	}
	cfg.Readiness = f.ping
	log := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, err := NewServer(log, f.tenants, f.assets, cfg)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	f.srv = srv
	return f
}

func (f *fixture) serve(r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(w, r)
	return w
}

// logLines returns every line logged so far, decoded.
func (f *fixture) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var lines []map[string]any
	dec := json.NewDecoder(bytes.NewReader(f.logs.Bytes()))
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("decoding log line: %v\n%s", err, f.logs)
		}
		lines = append(lines, m)
	}
	return lines
}

// logLine returns the only line logged with msg.
func (f *fixture) logLine(t *testing.T, msg string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, l := range f.logLines(t) {
		if l["msg"] == msg {
			found = append(found, l)
		}
	}
	if len(found) != 1 {
		t.Fatalf("got %d log lines with msg %q, want 1:\n%s", len(found), msg, f.logs)
	}
	return found[0]
}

func authed(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("Authorization", "Bearer "+validKey)
	return r
}

func unauthed(target string) *http.Request {
	return httptest.NewRequest(http.MethodGet, target, nil)
}

func wantErrorBody(t *testing.T, w *httptest.ResponseRecorder, status int, want apiv1.ErrorResponse) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d; body %s", w.Code, status, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got apiv1.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body %q: %v", w.Body, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %+v, want %+v", got, want)
	}
}

func TestNewServer(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	ok := Config{Readiness: &fakePinger{}}
	tests := []struct {
		name    string
		tenants tenantService
		assets  assetService
		cfg     Config
		wantErr bool
	}{
		{name: "valid", tenants: &fakeTenants{}, assets: &fakeAssets{}, cfg: ok},
		{name: "proxy CIDRs", tenants: &fakeTenants{}, assets: &fakeAssets{},
			cfg: Config{Readiness: &fakePinger{}, TrustedProxies: []string{"10.0.0.0/8", "fd00::/8", "192.0.2.1"}}},
		{name: "bad proxy", tenants: &fakeTenants{}, assets: &fakeAssets{},
			cfg: Config{Readiness: &fakePinger{}, TrustedProxies: []string{"not-a-cidr"}}, wantErr: true},
		{name: "upload limits", tenants: &fakeTenants{}, assets: &fakeAssets{},
			cfg: Config{Readiness: &fakePinger{}, MaxUploadBytes: 1 << 20, MaxUploadsPerTenant: 1}},
		{name: "negative upload bytes", tenants: &fakeTenants{}, assets: &fakeAssets{},
			cfg: Config{Readiness: &fakePinger{}, MaxUploadBytes: -1}, wantErr: true},
		{name: "negative uploads per tenant", tenants: &fakeTenants{}, assets: &fakeAssets{},
			cfg: Config{Readiness: &fakePinger{}, MaxUploadsPerTenant: -1}, wantErr: true},
		{name: "no readiness", tenants: &fakeTenants{}, assets: &fakeAssets{}, wantErr: true},
		{name: "no tenants", assets: &fakeAssets{}, cfg: ok, wantErr: true},
		{name: "no assets", tenants: &fakeTenants{}, cfg: ok, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := NewServer(log, tt.tenants, tt.assets, tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewServer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && srv.Handler() == nil {
				t.Error("Handler() = nil")
			}
		})
	}
}

func TestAuth(t *testing.T) {
	id := uuid.NewV7().String()
	unauthorized := apiv1.NewError("unauthorized", "missing or invalid API key")
	tests := []struct {
		name        string
		header      string
		authErr     error
		wantStatus  int
		wantKey     string
		wantService bool
	}{
		{name: "missing header", wantStatus: http.StatusUnauthorized},
		{name: "basic scheme", header: "Basic " + validKey, wantStatus: http.StatusUnauthorized},
		{name: "no scheme", header: validKey, wantStatus: http.StatusUnauthorized},
		{name: "empty token", header: "Bearer ", wantStatus: http.StatusUnauthorized},
		{name: "unknown key", header: "Bearer sm_unknown", wantStatus: http.StatusUnauthorized,
			wantKey: "sm_unknown"},
		{name: "authenticate fails", header: "Bearer " + validKey, authErr: errBoom,
			wantStatus: http.StatusInternalServerError, wantKey: validKey},
		{name: "valid", header: "Bearer " + validKey, wantStatus: http.StatusOK, wantKey: validKey,
			wantService: true},
		{name: "scheme case-insensitive", header: "bearer " + validKey, wantStatus: http.StatusOK,
			wantKey: validKey, wantService: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.tenants.err = tt.authErr
			r := unauthed("/v1/assets/" + id)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}
			w := f.serve(r)

			switch tt.wantStatus {
			case http.StatusUnauthorized:
				wantErrorBody(t, w, tt.wantStatus, unauthorized)
				if got := w.Header().Get("WWW-Authenticate"); got != "Bearer" {
					t.Errorf("WWW-Authenticate = %q, want Bearer", got)
				}
			case http.StatusInternalServerError:
				wantErrorBody(t, w, tt.wantStatus, internalError())
			default:
				if w.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
				}
			}

			var wantKeys []string
			if tt.wantKey != "" {
				wantKeys = []string{tt.wantKey}
			}
			if !reflect.DeepEqual(f.tenants.got, wantKeys) {
				t.Errorf("Authenticate() keys = %q, want %q", f.tenants.got, wantKeys)
			}
			if !tt.wantService {
				if len(f.assets.calls) != 0 {
					t.Errorf("asset service called: %+v", f.assets.calls)
				}
				return
			}
			if len(f.assets.calls) != 1 || f.assets.calls[0].tenantID != acme.ID {
				t.Errorf("asset calls = %+v, want one with tenant %d", f.assets.calls, acme.ID)
			}
		})
	}
}

func TestErrorMapping(t *testing.T) {
	var v validation.Collector
	v.Add("title", "is required")
	v.Add("limit", "must be between 1 and 100")
	verr := v.Err()

	tests := []struct {
		name       string
		err        error
		wantStatus int
		want       apiv1.ErrorResponse
		wantHeader string
	}{
		{name: "not found", err: asset.ErrNotFound, wantStatus: http.StatusNotFound,
			want: apiv1.NewError("not_found", "no such asset")},
		{name: "wrapped not found", err: fmt.Errorf("error getting asset: %w", asset.ErrNotFound),
			wantStatus: http.StatusNotFound, want: apiv1.NewError("not_found", "no such asset")},
		{name: "validation", err: fmt.Errorf("wrapped: %w", verr), wantStatus: http.StatusBadRequest,
			want: apiv1.ErrorResponse{Error: apiv1.ErrorBody{Code: "invalid_request", Message: "invalid request",
				Fields: []apiv1.FieldError{
					{Name: "title", Message: "is required"},
					{Name: "limit", Message: "must be between 1 and 100"},
				}}}},
		{name: "unauthenticated", err: tenant.ErrUnauthenticated, wantStatus: http.StatusUnauthorized,
			want: apiv1.NewError("unauthorized", "missing or invalid API key"), wantHeader: "Bearer"},
		{name: "idempotency key reused", err: asset.ErrIdempotencyKeyReused,
			wantStatus: http.StatusUnprocessableEntity,
			want:       apiv1.NewError("idempotency_key_reused", "idempotency key already used for a different upload")},
		{name: "upload too large", err: fmt.Errorf("%w: http: request body too large", errUploadTooLarge),
			wantStatus: http.StatusRequestEntityTooLarge,
			want:       apiv1.NewError("file_too_large", "upload exceeds the size limit")},
		{name: "upload stalled", err: errUploadStalled, wantStatus: http.StatusRequestTimeout,
			want: apiv1.NewError("request_timeout", "timed out waiting for the upload")},
		{name: "too many uploads", err: errTooManyUploads, wantStatus: http.StatusTooManyRequests,
			want: apiv1.NewError("too_many_uploads", "too many uploads in progress")},
		{name: "malformed upload", err: fmt.Errorf("%w: unexpected EOF", errMalformedUpload),
			wantStatus: http.StatusBadRequest, want: apiv1.NewError("invalid_request", "malformed multipart body")},
		{name: "anything else", err: fmt.Errorf("error getting asset: %w: dial tcp 10.0.0.3:3306: secret detail", errBoom),
			wantStatus: http.StatusInternalServerError, want: internalError()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.assets.err = tt.err
			w := f.serve(authed("/v1/assets/" + uuid.NewV7().String()))

			wantErrorBody(t, w, tt.wantStatus, tt.want)
			if got := w.Header().Get("WWW-Authenticate"); got != tt.wantHeader {
				t.Errorf("WWW-Authenticate = %q, want %q", got, tt.wantHeader)
			}
			if tt.wantStatus != http.StatusInternalServerError {
				return
			}
			for _, s := range []string{"secret detail", "10.0.0.3", "boom"} {
				if strings.Contains(w.Body.String(), s) {
					t.Errorf("500 body %s contains %q", w.Body, s)
				}
			}
			line := f.logLine(t, "request failed")
			if line["level"] != "ERROR" || !strings.Contains(fmt.Sprint(line["error"]), "secret detail") {
				t.Errorf("log line = %v, want the error at ERROR", line)
			}
		})
	}
}

func TestUnknownRoute(t *testing.T) {
	for _, r := range []*http.Request{
		unauthed("/nope"),
		authed("/v1/nope"),
		httptest.NewRequest(http.MethodDelete, "/v1/assets", nil),
	} {
		t.Run(r.Method+" "+r.URL.Path, func(t *testing.T) {
			f := newFixture(t, Config{})
			wantErrorBody(t, f.serve(r), http.StatusNotFound, apiv1.NewError("not_found", "no such route"))
		})
	}
}

func TestClientGone(t *testing.T) {
	tests := []struct {
		name      string
		cancel    bool
		wantLevel string
	}{
		{name: "client gone", cancel: true, wantLevel: "INFO"},
		{name: "client still there", wantLevel: "ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.assets.err = fmt.Errorf("error listing assets: %w", context.Canceled)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancel {
				// net/http cancels the request's context when the client hangs up mid-request.
				f.assets.onCall = func(assetCall) { cancel() }
			}

			w := f.serve(authed("/v1/assets").WithContext(ctx))
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500", w.Code)
			}
			if got := f.logLine(t, "request failed")["level"]; got != tt.wantLevel {
				t.Errorf("level = %v, want %s", got, tt.wantLevel)
			}
			if tt.cancel {
				for _, l := range f.logLines(t) {
					if l["level"] == "ERROR" {
						t.Errorf("logged at ERROR after the client went away: %v", l)
					}
				}
			}
		})
	}
}

func TestHealth(t *testing.T) {
	notReady := apiv1.NewError("unavailable", "not ready")
	tests := []struct {
		name       string
		pingErr    error
		shutdown   bool
		target     *http.Request
		wantStatus int
		wantPings  int
	}{
		{name: "healthz", target: unauthed("/healthz"), wantStatus: http.StatusOK},
		{name: "healthz ignores the database", pingErr: errBoom, target: unauthed("/healthz"),
			wantStatus: http.StatusOK},
		{name: "readyz", target: unauthed("/readyz"), wantStatus: http.StatusOK, wantPings: 1},
		{name: "readyz ping fails", pingErr: errBoom, target: unauthed("/readyz"),
			wantStatus: http.StatusServiceUnavailable, wantPings: 1},
		{name: "readyz after shutdown begins", shutdown: true, target: unauthed("/readyz"),
			wantStatus: http.StatusServiceUnavailable},
		{name: "healthz after shutdown begins", shutdown: true, target: unauthed("/healthz"),
			wantStatus: http.StatusOK},
		{name: "v1 after shutdown begins", shutdown: true, target: authed("/v1/assets"),
			wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.ping.err = tt.pingErr
			if tt.shutdown {
				f.srv.BeginShutdown()
			}
			w := f.serve(tt.target)
			if tt.wantStatus == http.StatusServiceUnavailable {
				wantErrorBody(t, w, tt.wantStatus, notReady)
			} else if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
			}
			if f.ping.calls != tt.wantPings {
				t.Errorf("pings = %d, want %d", f.ping.calls, tt.wantPings)
			}
			if tt.wantPings == 0 {
				return
			}
			deadline, ok := f.ping.ctx.Deadline()
			if left := time.Until(deadline); !ok || left > readyTimeout {
				t.Errorf("ping deadline = %v (set %v), want at most %v away", deadline, ok, readyTimeout)
			}
			if _, isGin := f.ping.ctx.(*gin.Context); isGin {
				t.Error("ping got a *gin.Context")
			}
		})
	}
}

func TestBeginShutdownConcurrent(t *testing.T) {
	f := newFixture(t, Config{})
	done := make(chan int)
	go func() {
		w := httptest.NewRecorder()
		f.srv.Handler().ServeHTTP(w, unauthed("/readyz"))
		done <- w.Code
	}()
	f.srv.BeginShutdown()
	if code := <-done; code != http.StatusOK && code != http.StatusServiceUnavailable {
		t.Errorf("readyz during BeginShutdown = %d, want 200 or 503", code)
	}
	if w := f.serve(unauthed("/readyz")); w.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503", w.Code)
	}
}

func TestTrustedProxies(t *testing.T) {
	const forwarded = "198.51.100.66"
	tests := []struct {
		name    string
		proxies []string
		remote  string
		want    string
	}{
		{name: "none trusted, direct", remote: "203.0.113.9:4321", want: "203.0.113.9"},
		{name: "none trusted, private peer", remote: "10.0.0.5:4321", want: "10.0.0.5"},
		{name: "direct client of a configured proxy", proxies: []string{"10.0.0.0/8"},
			remote: "203.0.113.9:4321", want: "203.0.113.9"},
		{name: "from a configured proxy", proxies: []string{"10.0.0.0/8"},
			remote: "10.0.0.5:4321", want: forwarded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{TrustedProxies: tt.proxies})
			r := unauthed("/healthz")
			r.RemoteAddr = tt.remote
			r.Header.Set("X-Forwarded-For", forwarded)
			r.Header.Set("X-Real-IP", forwarded)
			f.serve(r)
			if got := f.logLine(t, "request")["client_ip"]; got != tt.want {
				t.Errorf("client_ip = %v, want %s", got, tt.want)
			}
		})
	}
}

func TestRequestLog(t *testing.T) {
	id := uuid.NewV7().String()
	tests := []struct {
		name       string
		req        *http.Request
		wantRoute  string
		wantStatus float64
		wantTenant any
	}{
		{name: "authenticated", req: authed("/v1/assets/" + id), wantRoute: "/v1/assets/:id",
			wantStatus: 200, wantTenant: "acme"},
		{name: "unauthenticated", req: unauthed("/v1/assets/" + id), wantRoute: "/v1/assets/:id",
			wantStatus: 401},
		{name: "unknown key", req: func() *http.Request {
			r := unauthed("/v1/assets")
			r.Header.Set("Authorization", "Bearer sm_wrong_key_value")
			return r
		}(), wantRoute: "/v1/assets", wantStatus: 401},
		{name: "unknown route", req: unauthed("/nope"), wantRoute: "", wantStatus: 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			tt.req.Header.Set("Cookie", "session=cookie_value")
			f.serve(tt.req)

			lines := f.logLines(t)
			if len(lines) != 1 {
				t.Fatalf("got %d log lines, want 1:\n%s", len(lines), f.logs)
			}
			line := lines[0]
			want := map[string]any{
				"msg": "request", "level": "INFO", "method": "GET", "route": tt.wantRoute,
				"status": tt.wantStatus, "client_ip": "192.0.2.1",
			}
			for k, v := range want {
				if line[k] != v {
					t.Errorf("%s = %v, want %v", k, line[k], v)
				}
			}
			if line["tenant"] != tt.wantTenant {
				t.Errorf("tenant = %v, want %v", line["tenant"], tt.wantTenant)
			}
			if d, ok := line["duration"].(float64); !ok || d < 0 {
				t.Errorf("duration = %v, want a non-negative number", line["duration"])
			}
			for _, s := range []string{validKey, "sm_wrong_key_value", "Bearer", "Authorization", "cookie_value"} {
				if strings.Contains(f.logs.String(), s) {
					t.Errorf("log contains %q:\n%s", s, f.logs)
				}
			}
		})
	}
}

func TestContexts(t *testing.T) {
	id := uuid.NewV7().String()
	for _, target := range []string{
		"/v1/assets",
		"/v1/assets/" + id,
		"/v1/assets/" + id + "/renditions/aac_128k",
	} {
		t.Run(target, func(t *testing.T) {
			f := newFixture(t, Config{})
			if w := f.serve(authed(target)); w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body)
			}
			if len(f.tenants.ctxs) != 1 || len(f.assets.calls) != 1 {
				t.Fatalf("got %d Authenticate and %d asset calls, want 1 each",
					len(f.tenants.ctxs), len(f.assets.calls))
			}
			for name, ctx := range map[string]context.Context{
				"Authenticate":           f.tenants.ctxs[0],
				f.assets.calls[0].method: f.assets.calls[0].ctx,
			} {
				if _, isGin := ctx.(*gin.Context); isGin {
					t.Errorf("%s got a *gin.Context", name)
				}
				log := logging.LoggerFromContextOr(ctx, nil)
				if log == nil {
					t.Fatalf("%s got a context with no logger", name)
				}
				log.Info("probe " + name)
			}

			auth := f.logLine(t, "probe Authenticate")
			svc := f.logLine(t, "probe "+f.assets.calls[0].method)
			for _, l := range []map[string]any{auth, svc} {
				if l["route"] == nil || l["method"] != "GET" || l["client_ip"] != "192.0.2.1" {
					t.Errorf("probe line %v lacks the request's fields", l)
				}
			}
			if auth["tenant"] != nil {
				t.Errorf("Authenticate's logger has tenant %v before authenticating", auth["tenant"])
			}
			if svc["tenant"] != "acme" {
				t.Errorf("service logger tenant = %v, want acme", svc["tenant"])
			}
		})
	}
}

func TestRecovery(t *testing.T) {
	f := newFixture(t, Config{})
	f.assets.panicOnce = "kaboom"
	target := "/v1/assets/" + uuid.NewV7().String()

	wantErrorBody(t, f.serve(authed(target)), http.StatusInternalServerError, internalError())
	line := f.logLine(t, "panic serving request")
	if line["level"] != "ERROR" || line["panic"] != "kaboom" || line["stack"] == "" {
		t.Errorf("panic log line = %v", line)
	}
	if got := f.logLine(t, "request")["status"]; got != float64(500) {
		t.Errorf("request line status = %v, want 500", got)
	}

	if w := f.serve(authed(target)); w.Code != http.StatusOK {
		t.Errorf("request after the panic: status = %d, want 200", w.Code)
	}
}

// rawConn dials f's in-memory server for requests written by hand. Call it inside a synctest bubble.
// Reads on the connection fail after clientPatience, so a server that never answers fails the test.
func (f *fixture) rawConn(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	client := f.uploadClient(t)
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport is %T, want *http.Transport", client.Transport)
	}
	conn, err := tr.DialContext(t.Context(), "tcp", "api.test:80")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(clientPatience)); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

func send(t *testing.T, conn net.Conn, s string) {
	t.Helper()
	if _, err := io.WriteString(conn, s); err != nil {
		t.Fatal(err)
	}
}

// readResponse reads a whole response from br.
func readResponse(t *testing.T, br *bufio.Reader) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

// A client that declares a request body and never sends it gets its response at once,
// and loses the connection when the grace for the body runs out, whichever route answers.
func TestUnreadBodyReleasesConnection(t *testing.T) {
	id := uuid.NewV7().String()
	rendition := bytes.Repeat([]byte("a"), 64<<10)
	const auth = "Authorization: Bearer " + validKey + "\r\n"
	tests := []struct {
		name string
		// target is the request line's method and path.
		target string
		// header holds extra header lines, each ending in CRLF.
		header     string
		assetErr   error
		wantStatus int
		wantBody   []byte
	}{
		{name: "upload without a key", target: "POST /v1/assets",
			header: "Content-Type: " + formContentType + "\r\n", wantStatus: http.StatusUnauthorized},
		{name: "upload without a key expecting 100-continue", target: "POST /v1/assets",
			header:     "Content-Type: " + formContentType + "\r\nExpect: 100-continue\r\n",
			wantStatus: http.StatusUnauthorized},
		{name: "healthz", target: "GET /healthz", wantStatus: http.StatusOK},
		{name: "unknown route", target: "POST /nope", wantStatus: http.StatusNotFound},
		{name: "missing asset", target: "GET /v1/assets/" + id, header: auth, assetErr: asset.ErrNotFound,
			wantStatus: http.StatusNotFound},
		{name: "trailing-slash redirect", target: "POST /v1/assets/", wantStatus: http.StatusTemporaryRedirect},
		// The rendition outgrows net/http's response buffer, so the response starts while the handler runs.
		{name: "rendition", target: "GET /v1/assets/" + id + "/renditions/aac_128k", header: auth,
			wantStatus: http.StatusOK, wantBody: rendition},
	}
	framings := []struct{ name, header string }{
		{name: "declared length", header: "Content-Length: 1000\r\n"},
		{name: "chunked", header: "Transfer-Encoding: chunked\r\n"},
	}
	for _, tt := range tests {
		for _, fr := range framings {
			t.Run(tt.name+", "+fr.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t, Config{})
					f.assets.err = tt.assetErr
					f.assets.content = rendition
					var ctx context.Context
					f.assets.onCall = func(c assetCall) { ctx = c.ctx }
					ctxErr := make(chan error, 1)
					f.assets.fileClosed = func() { ctxErr <- ctx.Err() }
					conn, br := f.rawConn(t)

					start := time.Now()
					send(t, conn, tt.target+" HTTP/1.1\r\nHost: api.test\r\n"+tt.header+fr.header+"\r\n")
					resp, body := readResponse(t, br)
					if resp.StatusCode != tt.wantStatus || time.Since(start) != 0 {
						t.Fatalf("status %d after %v, want %d at once; body %s",
							resp.StatusCode, time.Since(start), tt.wantStatus, body)
					}
					if tt.wantBody != nil && !bytes.Equal(body, tt.wantBody) {
						t.Errorf("got a %d-byte body, want %d bytes", len(body), len(tt.wantBody))
					}
					if _, err := br.ReadByte(); err != io.EOF || time.Since(start) != unreadBodyGrace {
						t.Errorf("reading after the %d returned %v after %v, want io.EOF after %v",
							tt.wantStatus, err, time.Since(start), unreadBodyGrace)
					}
					select {
					case err := <-ctxErr:
						if err != nil {
							t.Errorf("the handler's context ended while it wrote the rendition: %v", err)
						}
					default:
						if tt.wantBody != nil {
							t.Error("the handler never closed the rendition")
						}
					}
				})
			})
		}
	}
}

// Requests without a body, or whose body was read or sent in full, keep their connection for the next request.
func TestConnectionReuse(t *testing.T) {
	id := uuid.NewV7().String()
	upload := string(multipartBody(t, titlePart("Rain"), filePart("RIFF-audio")))
	const uploadHeader = "POST /v1/assets HTTP/1.1\r\nHost: api.test\r\nAuthorization: Bearer " + validKey +
		"\r\nContent-Type: " + formContentType + "\r\n"
	const healthz = "GET /healthz HTTP/1.1\r\nHost: api.test\r\n\r\n"
	tests := []struct {
		name string
		req  string
		// late is sent after req while the handler is held in the asset service, so it arrives after the header.
		late       string
		wantStatus int
	}{
		{name: "no body", req: healthz, wantStatus: http.StatusOK},
		{name: "upload with a declared length",
			req:        fmt.Sprintf("%sContent-Length: %d\r\n\r\n%s", uploadHeader, len(upload), upload),
			wantStatus: http.StatusAccepted},
		{name: "chunked upload",
			req: fmt.Sprintf("%sTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n0\r\n\r\n",
				uploadHeader, len(upload), upload),
			wantStatus: http.StatusAccepted},
		{name: "unread body sent with the header",
			req: "GET /healthz HTTP/1.1\r\nHost: api.test\r\nContent-Length: 5\r\n\r\nhello", wantStatus: http.StatusOK},
		// net/http reads 4 KiB with the header, so most of this body is still on the connection.
		{name: "unread body larger than the read buffer",
			req:        "GET /healthz HTTP/1.1\r\nHost: api.test\r\nContent-Length: 10000\r\n\r\n" + strings.Repeat("x", 10000),
			wantStatus: http.StatusOK},
		{name: "unread body sent after the header",
			req: "GET /v1/assets/" + id + " HTTP/1.1\r\nHost: api.test\r\nAuthorization: Bearer " + validKey +
				"\r\nContent-Length: 5\r\n\r\n",
			late: "hello", wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, Config{})
				f.assets.asset = queuedAsset()
				held := make(chan struct{})
				if tt.late != "" {
					f.assets.onCall = func(assetCall) { <-held }
				}
				conn, br := f.rawConn(t)

				send(t, conn, tt.req)
				if tt.late != "" {
					synctest.Wait()
					send(t, conn, tt.late)
					synctest.Wait()
				}
				close(held)
				for i, want := range []int{tt.wantStatus, http.StatusOK} {
					if i > 0 {
						send(t, conn, healthz)
					}
					resp, body := readResponse(t, br)
					if resp.StatusCode != want || resp.Close {
						t.Fatalf("request %d: status %d with connection closed = %v, want %d and kept open; body %s",
							i, resp.StatusCode, resp.Close, want, body)
					}
				}
			})
		})
	}
}

// A handler aborted with http.ErrAbortHandler before the declared body arrives must not keep the connection.
func TestAbortedHandlerReleasesConnection(t *testing.T) {
	for _, framing := range []string{"Content-Length: 1000\r\n", "Transfer-Encoding: chunked\r\n"} {
		t.Run(framing[:strings.Index(framing, ":")], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, Config{})
				f.srv.engine.POST("/abort", func(*gin.Context) { panic(http.ErrAbortHandler) })
				conn, br := f.rawConn(t)

				start := time.Now()
				send(t, conn, "POST /abort HTTP/1.1\r\nHost: api.test\r\n"+framing+"\r\n")
				if _, err := br.ReadByte(); err != io.EOF || time.Since(start) != unreadBodyGrace {
					t.Errorf("reading after the abort returned %v after %v, want io.EOF after %v",
						err, time.Since(start), unreadBodyGrace)
				}
			})
		})
	}
}
