package httpsrv

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/validation"
)

const (
	boundary        = "sm-test-boundary"
	formContentType = "multipart/form-data; boundary=" + boundary
	betaKey         = "sm_test_only_beta_key"
)

var beta = tenant.Tenant{ID: 7, Slug: "beta", TargetLUFS: -23}

type formPart struct {
	name string
	// filename makes the part a file part.
	filename string
	content  string
}

func titlePart(s string) formPart { return formPart{name: "title", content: s} }
func filePart(s string) formPart  { return formPart{name: "file", filename: "rain.wav", content: s} }

func multipartBody(t *testing.T, parts ...formPart) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.SetBoundary(boundary); err != nil {
		t.Fatal(err)
	}
	for _, p := range parts {
		var pw io.Writer
		var err error
		if p.filename != "" {
			pw, err = w.CreateFormFile(p.name, p.filename)
		} else {
			pw, err = w.CreateFormField(p.name)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(pw, p.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// cutAt returns the offset just past the first marker in body.
func cutAt(t *testing.T, body []byte, marker string) int {
	t.Helper()
	i := bytes.Index(body, []byte(marker))
	if i < 0 {
		t.Fatalf("%q not in body", marker)
	}
	return i + len(marker)
}

// uploadClient serves f over the in-memory network, so read deadlines run on the bubble's clock.
// Call it inside a synctest bubble.
func (f *fixture) uploadClient(t *testing.T) *http.Client {
	return httptest.NewTestServer(t, f.srv.Handler()).Client()
}

func (f *fixture) uploadsInProgress() int {
	f.srv.uploads.mu.Lock()
	defer f.srv.uploads.mu.Unlock()
	return len(f.srv.uploads.inUse)
}

type upload struct {
	// apiKey is sent as the bearer token unless it is empty.
	apiKey string
	// contentType defaults to multipart/form-data with the test boundary.
	contentType string
	// idempotencyKey is sent unless it is empty.
	idempotencyKey string
	body           io.Reader
	chunked        bool
}

type result struct {
	status  int
	header  http.Header
	close   bool
	body    []byte
	elapsed time.Duration
}

// clientPatience is how long a test client waits, long after any server deadline,
// so that a missing deadline fails the test instead of hanging it.
const clientPatience = 10 * time.Minute

func post(ctx context.Context, client *http.Client, u upload) (result, error) {
	ctx, cancel := context.WithTimeout(ctx, clientPatience)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://api.test/v1/assets", u.body)
	if err != nil {
		return result{}, err
	}
	if u.chunked {
		r.ContentLength = -1
	}
	if u.apiKey != "" {
		r.Header.Set("Authorization", "Bearer "+u.apiKey)
	}
	r.Header.Set("Content-Type", cmp.Or(u.contentType, formContentType))
	if u.idempotencyKey != "" {
		r.Header.Set("Idempotency-Key", u.idempotencyKey)
	}
	start := time.Now()
	resp, err := client.Do(r)
	if err != nil {
		return result{}, fmt.Errorf("POST /v1/assets failed after %v: %w", time.Since(start), err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return result{}, err
	}
	return result{status: resp.StatusCode, header: resp.Header, close: resp.Close, body: b,
		elapsed: time.Since(start)}, nil
}

func mustPost(t *testing.T, client *http.Client, u upload) result {
	t.Helper()
	res, err := post(t.Context(), client, u)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func wantResultError(t *testing.T, res result, status int, want apiv1.ErrorResponse) {
	t.Helper()
	if res.status != status {
		t.Fatalf("status = %d, want %d; body %s", res.status, status, res.body)
	}
	if ct := res.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got apiv1.ErrorResponse
	if err := json.Unmarshal(res.body, &got); err != nil {
		t.Fatalf("decoding body %q: %v", res.body, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %+v, want %+v", got, want)
	}
}

func methods(calls []assetCall) []string {
	var out []string
	for _, c := range calls {
		out = append(out, c.method)
	}
	return out
}

func invalidField(name, message string) *apiv1.ErrorResponse {
	return &apiv1.ErrorResponse{Error: apiv1.ErrorBody{Code: "invalid_request", Message: "invalid request",
		Fields: []apiv1.FieldError{{Name: name, Message: message}}}}
}

func queuedAsset() asset.Asset {
	a := readyAsset()
	a.Status = asset.StatusQueued
	return a
}

func TestCreateAsset(t *testing.T) {
	var v validation.Collector
	v.Add("title", "is required")
	titleRequired := v.Err()

	full := multipartBody(t, titlePart("Rain on a tin roof"), filePart("RIFF-audio"))
	fileThenTitle := multipartBody(t, filePart("RIFF-audio"), titlePart("Rain on a tin roof"))
	longTitle := strings.Repeat("a", maxTitleBytes)
	malformed := new(apiv1.NewError("invalid_request", "malformed multipart body"))

	tests := []struct {
		name           string
		parts          []formPart
		raw            []byte
		contentType    string
		noAuth         bool
		idempotencyKey string
		stageErr       error
		createErr      error
		replayed       bool
		wantStatus     int
		wantError      *apiv1.ErrorResponse
		wantCalls      []string
		wantStaged     string
		wantTitle      string
		wantClose      bool
	}{
		{name: "created", parts: []formPart{titlePart("Rain on a tin roof"), filePart("RIFF-audio")},
			idempotencyKey: "Upload-42", wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"},
			wantStaged: "RIFF-audio", wantTitle: "Rain on a tin roof"},
		{name: "replayed", raw: full, idempotencyKey: "Upload-42", replayed: true,
			wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}, wantStaged: "RIFF-audio",
			wantTitle: "Rain on a tin roof"},
		{name: "no idempotency key", raw: full, wantStatus: http.StatusAccepted,
			wantCalls: []string{"Stage", "Create"}, wantStaged: "RIFF-audio", wantTitle: "Rain on a tin roof"},
		{name: "file before title", raw: fileThenTitle, wantStatus: http.StatusAccepted,
			wantCalls: []string{"Stage", "Create"}, wantStaged: "RIFF-audio", wantTitle: "Rain on a tin roof"},
		{name: "title passed untrimmed", parts: []formPart{titlePart("  Rain \t"), filePart("RIFF-audio")},
			wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}, wantTitle: "  Rain \t"},
		{name: "title at the read bound", parts: []formPart{titlePart(longTitle), filePart("RIFF-audio")},
			wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}, wantTitle: longTitle},
		{name: "other parts skipped", parts: []formPart{
			{name: "note", content: "hello"}, titlePart("Rain"),
			{name: "cover", filename: "cover.png", content: "PNG"}, filePart("RIFF-audio"),
			{name: "extra", content: "more"},
		}, wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}, wantStaged: "RIFF-audio",
			wantTitle: "Rain"},
		{name: "epilogue after the closing boundary", raw: append(slices.Clone(full), "trailing epilogue"...),
			wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}, wantStaged: "RIFF-audio",
			wantTitle: "Rain on a tin roof"},
		{name: "missing title left to the service", parts: []formPart{filePart("RIFF-audio")},
			createErr: titleRequired, wantStatus: http.StatusBadRequest,
			wantError: invalidField("title", "is required"), wantCalls: []string{"Stage", "Create"}},
		{name: "idempotency key reused", raw: full, idempotencyKey: "Upload-42",
			createErr:  fmt.Errorf("error creating asset: %w", asset.ErrIdempotencyKeyReused),
			wantStatus: http.StatusUnprocessableEntity,
			wantError: new(apiv1.NewError("idempotency_key_reused",
				"idempotency key already used for a different upload")),
			wantCalls: []string{"Stage", "Create"}, wantTitle: "Rain on a tin roof"},
		{name: "create fails", raw: full, createErr: errBoom, wantStatus: http.StatusInternalServerError,
			wantError: new(internalError()), wantCalls: []string{"Stage", "Create"}, wantTitle: "Rain on a tin roof"},
		{name: "stage fails", raw: full, stageErr: errBoom, wantStatus: http.StatusInternalServerError,
			wantError: new(internalError()), wantCalls: []string{"Stage"}, wantClose: true},
		{name: "missing file", parts: []formPart{titlePart("Rain")}, wantStatus: http.StatusBadRequest,
			wantError: invalidField("file", "is required"), wantClose: true},
		{name: "two file parts", parts: []formPart{titlePart("Rain"), filePart("one"), filePart("two")},
			wantStatus: http.StatusBadRequest, wantError: invalidField("file", "must be sent once"),
			wantCalls: []string{"Stage", "Discard"}, wantClose: true},
		{name: "two title parts", parts: []formPart{filePart("one"), titlePart("Rain"), titlePart("Snow")},
			wantStatus: http.StatusBadRequest, wantError: invalidField("title", "must be sent once"),
			wantCalls: []string{"Stage", "Discard"}, wantClose: true},
		{name: "title over the read bound", parts: []formPart{filePart("one"), titlePart(longTitle + "a")},
			wantStatus: http.StatusBadRequest, wantError: invalidField("title", "must be at most 255 characters"),
			wantCalls: []string{"Stage", "Discard"}, wantClose: true},
		{name: "not multipart", raw: []byte(`{"title":"Rain"}`), contentType: "application/json",
			wantStatus: http.StatusBadRequest, wantError: invalidField("Content-Type", "must be multipart/form-data"),
			wantClose: true},
		{name: "no boundary", raw: full, contentType: "multipart/form-data", wantStatus: http.StatusBadRequest,
			wantError: invalidField("Content-Type", "must be multipart/form-data"), wantClose: true},
		{name: "empty body", raw: []byte{}, wantStatus: http.StatusBadRequest, wantError: malformed,
			wantClose: true},
		{name: "cut inside the file part", raw: full[:cutAt(t, full, "RIFF")], wantStatus: http.StatusBadRequest,
			wantError: malformed, wantCalls: []string{"Stage"}, wantClose: true},
		{name: "cut after the file part", raw: fileThenTitle[:cutAt(t, fileThenTitle, "Rain on")],
			wantStatus: http.StatusBadRequest, wantError: malformed, wantCalls: []string{"Stage", "Discard"},
			wantClose: true},
		{name: "unauthenticated", raw: full, noAuth: true, wantStatus: http.StatusUnauthorized,
			wantError: new(apiv1.NewError("unauthorized", "missing or invalid API key"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := queuedAsset()
				f := newFixture(t, Config{})
				f.assets.asset = a
				f.assets.stageErr = tt.stageErr
				f.assets.createErr = tt.createErr
				f.assets.replayed = tt.replayed
				body := tt.raw
				if tt.parts != nil {
					body = multipartBody(t, tt.parts...)
				}
				u := upload{apiKey: validKey, contentType: tt.contentType, idempotencyKey: tt.idempotencyKey,
					body: bytes.NewReader(body)}
				if tt.noAuth {
					u.apiKey = ""
				}
				res := mustPost(t, f.uploadClient(t), u)
				synctest.Wait()

				if tt.wantError != nil {
					wantResultError(t, res, tt.wantStatus, *tt.wantError)
					for _, h := range []string{"Location", "Idempotent-Replayed"} {
						if got := res.header.Get(h); got != "" {
							t.Errorf("%s = %q on an error", h, got)
						}
					}
				} else {
					if res.status != tt.wantStatus {
						t.Fatalf("status = %d, want %d; body %s", res.status, tt.wantStatus, res.body)
					}
					wantJSON(t, res.body, apiv1.NewAsset(a))
					if got, want := res.header.Get("Location"), "/v1/assets/"+a.ID.String(); got != want {
						t.Errorf("Location = %q, want %q", got, want)
					}
					wantReplayed := ""
					if tt.replayed {
						wantReplayed = "true"
					}
					if got := res.header.Get("Idempotent-Replayed"); got != wantReplayed {
						t.Errorf("Idempotent-Replayed = %q, want %q", got, wantReplayed)
					}
				}
				if res.close != tt.wantClose {
					t.Errorf("connection closed = %v, want %v", res.close, tt.wantClose)
				}

				calls := f.assets.recorded()
				if got := methods(calls); !slices.Equal(got, tt.wantCalls) {
					t.Fatalf("service calls = %q, want %q", got, tt.wantCalls)
				}
				for _, c := range calls {
					if _, isGin := c.ctx.(*gin.Context); isGin {
						t.Errorf("%s got a *gin.Context", c.method)
					}
					if logging.LoggerFromContextOr(c.ctx, nil) == nil {
						t.Errorf("%s got a context without the request's logger", c.method)
					}
					switch c.method {
					case "Stage":
						if tt.wantStaged != "" && c.content != tt.wantStaged {
							t.Errorf("Stage read %q, want %q", c.content, tt.wantStaged)
						}
					case "Create":
						want := asset.CreateParams{TenantID: acme.ID, TargetLUFS: acme.TargetLUFS,
							Title: tt.wantTitle, IdempotencyKey: tt.idempotencyKey}
						if !reflect.DeepEqual(c.params, want) {
							t.Errorf("Create params = %+v, want %+v", c.params, want)
						}
					}
				}
				if n := f.uploadsInProgress(); n != 0 {
					t.Errorf("%d tenants still hold upload slots", n)
				}
			})
		})
	}
}

func TestCreateAssetSizeLimit(t *testing.T) {
	const limit = 1 << 10
	sized := func(t *testing.T, size int) []byte {
		t.Helper()
		base := len(multipartBody(t, titlePart("Rain"), filePart("")))
		body := multipartBody(t, titlePart("Rain"), filePart(strings.Repeat("a", size-base)))
		if len(body) != size {
			t.Fatalf("built a %d-byte body, want %d", len(body), size)
		}
		return body
	}
	tooLarge := apiv1.NewError("file_too_large", "upload exceeds the size limit")

	tests := []struct {
		name       string
		body       func(t *testing.T) []byte
		chunked    bool
		wantStatus int
		wantCalls  []string
	}{
		{name: "declared length at the limit", body: func(t *testing.T) []byte { return sized(t, limit) },
			wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}},
		{name: "chunked body at the limit", body: func(t *testing.T) []byte { return sized(t, limit) },
			chunked: true, wantStatus: http.StatusAccepted, wantCalls: []string{"Stage", "Create"}},
		{name: "declared length over the limit", body: func(t *testing.T) []byte { return sized(t, limit+1) },
			wantStatus: http.StatusRequestEntityTooLarge},
		{name: "chunked file crosses the limit", body: func(t *testing.T) []byte { return sized(t, limit+100) },
			chunked: true, wantStatus: http.StatusRequestEntityTooLarge, wantCalls: []string{"Stage"}},
		{name: "closing boundary crosses the limit", body: func(t *testing.T) []byte { return sized(t, limit+1) },
			chunked: true, wantStatus: http.StatusRequestEntityTooLarge, wantCalls: []string{"Stage", "Discard"}},
		{name: "a later part crosses the limit", body: func(t *testing.T) []byte {
			return multipartBody(t, filePart("RIFF-audio"), titlePart("Rain"),
				formPart{name: "extra", content: strings.Repeat("x", limit)})
		}, chunked: true, wantStatus: http.StatusRequestEntityTooLarge, wantCalls: []string{"Stage", "Discard"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, Config{MaxUploadBytes: limit})
				f.assets.asset = queuedAsset()
				res := mustPost(t, f.uploadClient(t),
					upload{apiKey: validKey, body: bytes.NewReader(tt.body(t)), chunked: tt.chunked})
				synctest.Wait()

				if tt.wantStatus == http.StatusRequestEntityTooLarge {
					wantResultError(t, res, tt.wantStatus, tooLarge)
					if !res.close {
						t.Error("connection kept open after a 413")
					}
				} else if res.status != tt.wantStatus {
					t.Fatalf("status = %d, want %d; body %s", res.status, tt.wantStatus, res.body)
				}
				if got := methods(f.assets.recorded()); !slices.Equal(got, tt.wantCalls) {
					t.Errorf("service calls = %q, want %q", got, tt.wantCalls)
				}
				if n := f.uploadsInProgress(); n != 0 {
					t.Errorf("%d tenants still hold upload slots", n)
				}
			})
		})
	}
}

func TestCreateAssetIdleDeadline(t *testing.T) {
	type step struct {
		// wait is how long the client pauses before sending the body up to offset to.
		wait time.Duration
		to   int
	}
	titleFirst := multipartBody(t, titlePart("Rain"), filePart(strings.Repeat("a", 1000)))
	fileFirst := multipartBody(t, filePart(strings.Repeat("a", 1000)), titlePart("Rain"))
	file := cutAt(t, titleFirst, "aaa")
	stalled := apiv1.NewError("request_timeout", "timed out waiting for the upload")

	tests := []struct {
		name        string
		body        []byte
		steps       []step
		stall       bool
		wantStatus  int
		wantElapsed time.Duration
		wantCalls   []string
	}{
		{name: "stall before the body", body: titleFirst, stall: true,
			wantStatus: http.StatusRequestTimeout, wantElapsed: uploadIdleTimeout},
		{name: "stall in the file part", body: titleFirst, steps: []step{{0, file + 100}}, stall: true,
			wantStatus: http.StatusRequestTimeout, wantElapsed: uploadIdleTimeout, wantCalls: []string{"Stage"}},
		{name: "stall after steady progress", body: titleFirst,
			steps: []step{{0, file + 100}, {20 * time.Second, file + 200}, {20 * time.Second, file + 300}},
			stall: true, wantStatus: http.StatusRequestTimeout, wantElapsed: 40*time.Second + uploadIdleTimeout,
			wantCalls: []string{"Stage"}},
		{name: "stall after the file part", body: fileFirst,
			steps: []step{{0, cutAt(t, fileFirst, `name="title"`)}}, stall: true,
			wantStatus: http.StatusRequestTimeout, wantElapsed: uploadIdleTimeout,
			wantCalls: []string{"Stage", "Discard"}},
		{name: "slow but steady", body: titleFirst, steps: []step{
			{0, file + 100}, {20 * time.Second, file + 200}, {20 * time.Second, file + 300},
			{20 * time.Second, file + 400}, {20 * time.Second, len(titleFirst)},
		}, wantStatus: http.StatusAccepted, wantElapsed: 80 * time.Second, wantCalls: []string{"Stage", "Create"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, Config{})
				f.assets.asset = queuedAsset()
				pr, pw := io.Pipe()
				hangUp := make(chan struct{})
				go func() {
					defer pw.Close()
					sent := 0
					for _, s := range tt.steps {
						select {
						case <-time.After(s.wait):
						case <-hangUp:
							return
						}
						// A write fails only once the server has answered, which the checks below report.
						if _, err := pw.Write(tt.body[sent:s.to]); err != nil {
							return
						}
						sent = s.to
					}
					if tt.stall {
						select {
						case <-hangUp:
						case <-time.After(clientPatience):
						}
					}
				}()

				res := mustPost(t, f.uploadClient(t), upload{apiKey: validKey, body: pr, chunked: true})
				close(hangUp)
				synctest.Wait()

				if tt.wantStatus == http.StatusRequestTimeout {
					wantResultError(t, res, tt.wantStatus, stalled)
					if !res.close {
						t.Error("connection kept open after a 408")
					}
				} else if res.status != tt.wantStatus {
					t.Fatalf("status = %d, want %d; body %s", res.status, tt.wantStatus, res.body)
				}
				if res.elapsed != tt.wantElapsed {
					t.Errorf("answered after %v, want %v", res.elapsed, tt.wantElapsed)
				}
				if got := methods(f.assets.recorded()); !slices.Equal(got, tt.wantCalls) {
					t.Errorf("service calls = %q, want %q", got, tt.wantCalls)
				}
				if n := f.uploadsInProgress(); n != 0 {
					t.Errorf("%d tenants still hold upload slots", n)
				}
			})
		})
	}
}

// Once the body is in, net/http watches the connection for the client hanging up; an idle deadline
// still set there would cancel the request's context in the middle of a slow Create.
func TestCreateAssetOutlastsIdleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, Config{})
		f.assets.asset = queuedAsset()
		var ctxErr error
		f.assets.onCall = func(c assetCall) {
			if c.method == "Create" {
				time.Sleep(2 * uploadIdleTimeout)
				ctxErr = c.ctx.Err()
			}
		}
		body := multipartBody(t, titlePart("Rain"), filePart("RIFF-audio"))
		res := mustPost(t, f.uploadClient(t), upload{apiKey: validKey, body: bytes.NewReader(body)})
		if res.status != http.StatusAccepted || res.elapsed != 2*uploadIdleTimeout {
			t.Fatalf("status %d after %v, want 202 after %v; body %s",
				res.status, res.elapsed, 2*uploadIdleTimeout, res.body)
		}
		if ctxErr != nil {
			t.Errorf("Create's context ended while it ran: %v", ctxErr)
		}
	})
}

func TestCreateAssetTooManyUploads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, Config{MaxUploadsPerTenant: 2})
		f.tenants.keys[betaKey] = beta
		f.assets.asset = queuedAsset()
		release := make(chan struct{})
		f.assets.staged = func(content string) {
			if content == "held" {
				<-release
			}
		}
		client := f.uploadClient(t)
		held := multipartBody(t, titlePart("Rain"), filePart("held"))
		free := multipartBody(t, titlePart("Rain"), filePart("free"))

		results := make(chan result, 2)
		for range 2 {
			go func() {
				res, err := post(t.Context(), client, upload{apiKey: validKey, body: bytes.NewReader(held)})
				if err != nil {
					t.Error(err)
				}
				results <- res
			}()
		}
		synctest.Wait()
		if got := methods(f.assets.recorded()); !slices.Equal(got, []string{"Stage", "Stage"}) {
			t.Fatalf("service calls with both slots taken = %q", got)
		}

		// This body never arrives, so an answer that takes no time shows the handler didn't wait for it.
		pr, pw := io.Pipe()
		res := mustPost(t, client, upload{apiKey: validKey, body: pr, chunked: true})
		pw.Close()
		wantResultError(t, res, http.StatusTooManyRequests,
			apiv1.NewError("too_many_uploads", "too many uploads in progress"))
		if res.elapsed != 0 || !res.close {
			t.Errorf("429 after %v with connection closed = %v, want at once and closed", res.elapsed, res.close)
		}

		res = mustPost(t, client, upload{apiKey: betaKey, body: bytes.NewReader(free)})
		if res.status != http.StatusAccepted {
			t.Fatalf("other tenant's upload: status = %d, want 202; body %s", res.status, res.body)
		}
		calls := f.assets.recorded()
		if got := methods(calls); !slices.Equal(got, []string{"Stage", "Stage", "Stage", "Create"}) {
			t.Fatalf("service calls = %q", got)
		}
		if p := calls[3].params; p.TenantID != beta.ID || p.TargetLUFS != beta.TargetLUFS {
			t.Errorf("other tenant's Create params = %+v, want tenant %d at %v LUFS", p, beta.ID, beta.TargetLUFS)
		}

		close(release)
		for range 2 {
			if res := <-results; res.status != http.StatusAccepted {
				t.Errorf("held upload: status = %d, want 202; body %s", res.status, res.body)
			}
		}
		synctest.Wait()
		if n := f.uploadsInProgress(); n != 0 {
			t.Errorf("%d tenants still hold upload slots", n)
		}
		res = mustPost(t, client, upload{apiKey: validKey, body: bytes.NewReader(free)})
		if res.status != http.StatusAccepted {
			t.Errorf("upload after the slots freed: status = %d, want 202; body %s", res.status, res.body)
		}
	})
}

// A client that sends the headers and then nothing must not keep the connection once its upload is refused.
func TestRejectedUploadReleasesConnection(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		// fillSlots holds one upload in Stage first, which fills a cap of 1.
		fillSlots bool
		// header is the request's framing and type, sent without a body.
		header     string
		wantStatus int
	}{
		{name: "429 with a declared length", cfg: Config{MaxUploadsPerTenant: 1}, fillSlots: true,
			header:     "Content-Type: " + formContentType + "\r\nContent-Length: 1000",
			wantStatus: http.StatusTooManyRequests},
		{name: "429 chunked", cfg: Config{MaxUploadsPerTenant: 1}, fillSlots: true,
			header:     "Content-Type: " + formContentType + "\r\nTransfer-Encoding: chunked",
			wantStatus: http.StatusTooManyRequests},
		{name: "413 on a declared length", cfg: Config{MaxUploadBytes: 100},
			header:     "Content-Type: " + formContentType + "\r\nContent-Length: 1000",
			wantStatus: http.StatusRequestEntityTooLarge},
		{name: "400 before the body", header: "Content-Type: text/plain\r\nContent-Length: 1000",
			wantStatus: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, tt.cfg)
				f.assets.asset = queuedAsset()
				client := f.uploadClient(t)
				if tt.fillSlots {
					release := make(chan struct{})
					f.assets.staged = func(string) { <-release }
					body := bytes.NewReader(multipartBody(t, titlePart("Rain"), filePart("held")))
					held := make(chan error, 1)
					go func() {
						_, err := post(t.Context(), client, upload{apiKey: validKey, body: body})
						held <- err
					}()
					defer func() {
						close(release)
						if err := <-held; err != nil {
							t.Errorf("held upload: %v", err)
						}
					}()
					synctest.Wait()
				}

				tr, ok := client.Transport.(*http.Transport)
				if !ok {
					t.Fatalf("client transport is %T, want *http.Transport", client.Transport)
				}
				conn, err := tr.DialContext(t.Context(), "tcp", "api.test:80")
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				start := time.Now()
				req := "POST /v1/assets HTTP/1.1\r\nHost: api.test\r\nAuthorization: Bearer " + validKey + "\r\n" +
					tt.header + "\r\n\r\n"
				if _, err := io.WriteString(conn, req); err != nil {
					t.Fatal(err)
				}
				br := bufio.NewReader(conn)
				resp, err := http.ReadResponse(br, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != tt.wantStatus {
					t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
				}

				if err := conn.SetReadDeadline(time.Now().Add(clientPatience)); err != nil {
					t.Fatal(err)
				}
				if _, err := br.ReadByte(); err != io.EOF || time.Since(start) != 0 {
					t.Errorf("reading after the %d returned %v after %v, want io.EOF at once",
						tt.wantStatus, err, time.Since(start))
				}
			})
		})
	}
}

func TestUploadSlots(t *testing.T) {
	u := newUploadSlots(2)
	steps := []struct {
		op     string
		tenant uint64
		want   bool
	}{
		{op: "acquire", tenant: 1, want: true},
		{op: "acquire", tenant: 1, want: true},
		{op: "acquire", tenant: 1, want: false},
		{op: "acquire", tenant: 2, want: true},
		{op: "release", tenant: 1},
		{op: "acquire", tenant: 1, want: true},
		{op: "acquire", tenant: 1, want: false},
		{op: "release", tenant: 1},
		{op: "release", tenant: 1},
		{op: "release", tenant: 2},
	}
	for i, s := range steps {
		if s.op == "release" {
			u.release(s.tenant)
			continue
		}
		if got := u.acquire(s.tenant); got != s.want {
			t.Errorf("step %d: acquire(%d) = %v, want %v", i, s.tenant, got, s.want)
		}
	}
	if len(u.inUse) != 0 {
		t.Errorf("inUse = %v after releasing every slot, want empty", u.inUse)
	}
}
