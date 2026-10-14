package httpsrv

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
)

var created = time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)

func readyAsset() asset.Asset {
	return asset.Asset{
		ID: uuid.NewV7(), TenantID: acme.ID, Title: "Rain on a tin roof", Status: asset.StatusReady,
		SourceSHA256: sha256.Sum256([]byte("source")), SourceBytes: 6, TargetLUFS: -18,
		CreatedAt: created, UpdatedAt: created,
	}
}

func TestGetAsset(t *testing.T) {
	a := readyAsset()
	upper := strings.ToUpper(a.ID.String())
	tests := []struct {
		name       string
		id         string
		err        error
		wantStatus int
	}{
		{name: "found", id: a.ID.String(), wantStatus: http.StatusOK},
		{name: "upper-case ID passed unchanged", id: upper, wantStatus: http.StatusOK},
		{name: "malformed ID passed unchanged", id: "not-a-uuid", err: asset.ErrNotFound,
			wantStatus: http.StatusNotFound},
		{name: "not found", id: a.ID.String(), err: asset.ErrNotFound, wantStatus: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.assets.asset = a
			f.assets.err = tt.err
			w := f.serve(authed("/v1/assets/" + tt.id))

			want := []assetCall{{method: "Get", tenantID: acme.ID, id: tt.id}}
			if got := withoutCtx(f.assets.calls); !reflect.DeepEqual(got, want) {
				t.Errorf("calls = %+v, want %+v", got, want)
			}
			if tt.wantStatus == http.StatusNotFound {
				if body := strings.TrimSpace(w.Body.String()); body != `{"error":{"code":"not_found","message":"no such asset"}}` {
					t.Errorf("body = %s", body)
				}
				wantErrorBody(t, w, http.StatusNotFound, apiv1.NewError("not_found", "no such asset"))
				return
			}
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
			}
			wantJSON(t, w.Body.Bytes(), apiv1.NewAsset(a))
		})
	}
}

func TestListAssets(t *testing.T) {
	a := readyAsset()
	tests := []struct {
		name       string
		query      url.Values
		page       asset.Page
		wantStatus int
		wantCall   *assetCall
		wantNext   string
	}{
		{name: "defaults", page: asset.Page{Assets: []asset.Asset{a}}, wantStatus: http.StatusOK,
			wantCall: &assetCall{limit: asset.DefaultListLimit}, wantNext: "null"},
		{name: "limit and cursor", query: url.Values{"limit": {"5"}, "cursor": {"AAAAAAAAAAc"}},
			page: asset.Page{Assets: []asset.Asset{a}, NextCursor: "AAAAAAAAAAM"}, wantStatus: http.StatusOK,
			wantCall: &assetCall{limit: 5, cursor: "AAAAAAAAAAc"}, wantNext: `"AAAAAAAAAAM"`},
		{name: "cursor passed as is", query: url.Values{"cursor": {"not base64 !"}},
			wantStatus: http.StatusOK, wantCall: &assetCall{limit: asset.DefaultListLimit, cursor: "not base64 !"},
			wantNext: "null"},
		{name: "range left to the service", query: url.Values{"limit": {"500"}},
			wantStatus: http.StatusOK, wantCall: &assetCall{limit: 500}, wantNext: "null"},
		{name: "non-integer limit", query: url.Values{"limit": {"ten"}}, wantStatus: http.StatusBadRequest},
		{name: "empty limit", query: url.Values{"limit": {""}}, wantStatus: http.StatusBadRequest},
		{name: "fractional limit", query: url.Values{"limit": {"2.5"}}, wantStatus: http.StatusBadRequest},
		{name: "overflowing limit", query: url.Values{"limit": {"99999999999999999999"}},
			wantStatus: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.assets.page = tt.page
			w := f.serve(authed("/v1/assets?" + tt.query.Encode()))

			if tt.wantStatus == http.StatusBadRequest {
				wantErrorBody(t, w, http.StatusBadRequest, apiv1.ErrorResponse{Error: apiv1.ErrorBody{
					Code: "invalid_request", Message: "invalid request",
					Fields: []apiv1.FieldError{{Name: "limit", Message: "must be an integer between 1 and 100"}},
				}})
				if len(f.assets.calls) != 0 {
					t.Errorf("service called: %+v", f.assets.calls)
				}
				return
			}
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
			}
			want := *tt.wantCall
			want.method, want.tenantID = "List", acme.ID
			if got := withoutCtx(f.assets.calls); !reflect.DeepEqual(got, []assetCall{want}) {
				t.Errorf("calls = %+v, want %+v", got, want)
			}
			wantJSON(t, w.Body.Bytes(), apiv1.NewAssetList(tt.page))
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
				t.Fatal(err)
			}
			if got := string(raw["next_cursor"]); got != tt.wantNext {
				t.Errorf("next_cursor = %s, want %s", got, tt.wantNext)
			}
			if got := string(raw["data"]); !strings.HasPrefix(got, "[") {
				t.Errorf("data = %s, want an array", got)
			}
		})
	}
}

func TestGetRendition(t *testing.T) {
	content := []byte("0123456789abcdef")
	sum := sha256.Sum256(content)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	id := uuid.NewV7().String()
	rendition := asset.Rendition{Profile: asset.ProfileAAC128k, SHA256: sum, Bytes: uint64(len(content)),
		CreatedAt: created}

	tests := []struct {
		name        string
		header      http.Header
		err         error
		wantStatus  int
		wantBody    string
		wantHeaders map[string]string
		wantError   *apiv1.ErrorResponse
	}{
		{name: "whole file", wantStatus: http.StatusOK, wantBody: string(content),
			wantHeaders: map[string]string{"ETag": etag, "Cache-Control": "private, max-age=31536000, immutable",
				"Content-Type": "audio/mp4", "Accept-Ranges": "bytes", "Content-Length": "16"}},
		{name: "range", header: http.Header{"Range": {"bytes=2-5"}}, wantStatus: http.StatusPartialContent,
			wantBody: "2345", wantHeaders: map[string]string{"ETag": etag, "Content-Type": "audio/mp4",
				"Content-Range": "bytes 2-5/16", "Content-Length": "4"}},
		{name: "if-none-match", header: http.Header{"If-None-Match": {etag}}, wantStatus: http.StatusNotModified,
			wantHeaders: map[string]string{"ETag": etag}},
		{name: "if-range matches", header: http.Header{"Range": {"bytes=10-"}, "If-Range": {etag}},
			wantStatus: http.StatusPartialContent, wantBody: "abcdef"},
		{name: "range not satisfiable", header: http.Header{"Range": {"bytes=100-"}},
			wantStatus:  http.StatusRequestedRangeNotSatisfiable,
			wantHeaders: map[string]string{"Content-Range": "bytes */16", "ETag": "", "Cache-Control": ""},
			wantError:   new(apiv1.NewError("range_not_satisfiable", "requested range not satisfiable"))},
		{name: "if-match fails", header: http.Header{"If-Match": {`"other"`}},
			wantStatus:  http.StatusPreconditionFailed,
			wantHeaders: map[string]string{"ETag": "", "Cache-Control": ""},
			wantError:   new(apiv1.NewError("precondition_failed", "precondition failed"))},
		{name: "not found", err: asset.ErrNotFound, wantStatus: http.StatusNotFound,
			wantHeaders: map[string]string{"ETag": "", "Cache-Control": ""},
			wantError:   new(apiv1.NewError("not_found", "no such asset"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, Config{})
			f.assets.rendition = rendition
			f.assets.content = content
			f.assets.err = tt.err
			r := authed("/v1/assets/" + id + "/renditions/aac_128k")
			for k, v := range tt.header {
				r.Header[k] = v
			}
			w := f.serve(r)

			want := []assetCall{{method: "OpenRendition", tenantID: acme.ID, id: id, profile: "aac_128k"}}
			if got := withoutCtx(f.assets.calls); !reflect.DeepEqual(got, want) {
				t.Errorf("calls = %+v, want %+v", got, want)
			}
			if tt.wantError != nil {
				wantErrorBody(t, w, tt.wantStatus, *tt.wantError)
			} else {
				if w.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
				}
				if got := w.Body.String(); got != tt.wantBody {
					t.Errorf("body = %q, want %q", got, tt.wantBody)
				}
			}
			for k, v := range tt.wantHeaders {
				if got := w.Header().Get(k); got != v {
					t.Errorf("%s = %q, want %q", k, got, v)
				}
			}
			for i, file := range f.assets.files {
				if !file.closed {
					t.Errorf("file %d not closed", i)
				}
			}
			if tt.err == nil && len(f.assets.files) != 1 {
				t.Errorf("opened %d files, want 1", len(f.assets.files))
			}
		})
	}
}

func withoutCtx(calls []assetCall) []assetCall {
	out := make([]assetCall, len(calls))
	for i, c := range calls {
		c.ctx = nil
		out[i] = c
	}
	return out
}

// wantJSON checks that body is exactly what encoding want produces.
func wantJSON(t *testing.T, body []byte, want any) {
	t.Helper()
	wantBody, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(body)); got != string(wantBody) {
		t.Errorf("body = %s\nwant %s", got, wantBody)
	}
}
