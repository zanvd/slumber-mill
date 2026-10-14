package apiv1

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/validation"
)

func mustParseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("uuid.Parse(%q): %v", s, err)
	}
	return id
}

func sequence(first byte) [32]byte {
	var b [32]byte
	for i := range b {
		b[i] = first + byte(i)
	}
	return b
}

func ptr[T any](v T) *T { return &v }

func marshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(b)
}

func keysOf(t *testing.T, doc string) []string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

const (
	idReady  = "0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f03"
	idQueued = "0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f04"
	idFailed = "0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f05"

	sourceHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
)

var (
	created = time.Date(2026, 3, 1, 12, 0, 0, 123456000, time.UTC)
	updated = time.Date(2026, 3, 1, 12, 5, 30, 0, time.UTC)
)

func readyAsset(t *testing.T) asset.Asset {
	return asset.Asset{
		ID:                mustParseUUID(t, idReady),
		TenantID:          42,
		IdempotencyKey:    "upload-42",
		Title:             "Rain on a tin roof",
		Status:            asset.StatusReady,
		SourceSHA256:      sequence(0x00),
		SourceBytes:       12345678,
		SourceCodec:       "mp3",
		DurationMS:        ptr(uint64(1800500)),
		TargetLUFS:        -16,
		InputLUFS:         ptr(-23.4),
		InputTruePeakDBTP: ptr(-1.2),
		InputLRALU:        ptr(7.5),
		InputMaxMomentary: ptr(-14.8),
		Warnings:          []asset.Warning{asset.WarningSuddenLoudEvent, asset.WarningSourceMayClip},
		Renditions: []asset.Rendition{
			{
				Profile:       asset.ProfileAAC128k,
				Codec:         asset.CodecAAC,
				BitrateKbps:   128,
				Channels:      2,
				Bytes:         28800000,
				SHA256:        sequence(0xa0),
				LUFS:          -16.1,
				TruePeakDBTP:  -1.5,
				LRALU:         6.25,
				Normalization: asset.NormalizationLinear,
				CreatedAt:     updated,
			},
			{
				Profile:       asset.ProfileAAC64k,
				Codec:         asset.CodecAAC,
				BitrateKbps:   64,
				Channels:      1,
				Bytes:         14400000,
				SHA256:        sequence(0xc0),
				LUFS:          -15.8,
				TruePeakDBTP:  -2,
				LRALU:         5,
				Normalization: asset.NormalizationDynamic,
				CreatedAt:     updated,
			},
		},
		CreatedAt: created,
		UpdatedAt: updated,
	}
}

const readyJSON = `{"id":"0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f03","title":"Rain on a tin roof","status":"ready",` +
	`"idempotency_key":"upload-42","source_sha256":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",` +
	`"source_bytes":12345678,"source_codec":"mp3","duration_ms":1800500,"target_lufs":-16,` +
	`"input_lufs":-23.4,"input_true_peak_dbtp":-1.2,"input_lra_lu":7.5,"input_max_momentary":-14.8,` +
	`"warnings":["sudden_loud_event","source_may_clip"],"error":null,"renditions":[` +
	`{"profile":"aac_128k","codec":"aac","bitrate_kbps":128,"channels":2,"bytes":28800000,` +
	`"sha256":"a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf",` +
	`"lufs":-16.1,"true_peak_dbtp":-1.5,"lra_lu":6.25,"normalization":"linear","created_at":"2026-03-01T12:05:30Z"},` +
	`{"profile":"aac_64k","codec":"aac","bitrate_kbps":64,"channels":1,"bytes":14400000,` +
	`"sha256":"c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf",` +
	`"lufs":-15.8,"true_peak_dbtp":-2,"lra_lu":5,"normalization":"dynamic","created_at":"2026-03-01T12:05:30Z"}],` +
	`"created_at":"2026-03-01T12:00:00.123456Z","updated_at":"2026-03-01T12:05:30Z"}`

func queuedAsset(t *testing.T) asset.Asset {
	zone := time.FixedZone("CEST", 2*60*60)
	return asset.Asset{
		ID:           mustParseUUID(t, idQueued),
		TenantID:     42,
		Title:        "Ocean waves",
		Status:       asset.StatusQueued,
		SourceSHA256: sequence(0x00),
		SourceBytes:  900,
		TargetLUFS:   -18.5,
		CreatedAt:    created.In(zone),
		UpdatedAt:    updated.In(zone),
	}
}

const queuedJSON = `{"id":"0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f04","title":"Ocean waves","status":"queued",` +
	`"idempotency_key":null,"source_sha256":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",` +
	`"source_bytes":900,"source_codec":null,"duration_ms":null,"target_lufs":-18.5,` +
	`"input_lufs":null,"input_true_peak_dbtp":null,"input_lra_lu":null,"input_max_momentary":null,` +
	`"warnings":[],"error":null,"renditions":[],` +
	`"created_at":"2026-03-01T12:00:00.123456Z","updated_at":"2026-03-01T12:05:30Z"}`

func failedAsset(t *testing.T) asset.Asset {
	a := queuedAsset(t)
	a.ID = mustParseUUID(t, idFailed)
	a.Title = "Broken upload"
	a.Status = asset.StatusFailed
	a.SourceCodec = "mp3"
	a.Error = "the file has no audio stream"
	a.CreatedAt = created
	a.UpdatedAt = updated
	return a
}

const failedJSON = `{"id":"0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f05","title":"Broken upload","status":"failed",` +
	`"idempotency_key":null,"source_sha256":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",` +
	`"source_bytes":900,"source_codec":"mp3","duration_ms":null,"target_lufs":-18.5,` +
	`"input_lufs":null,"input_true_peak_dbtp":null,"input_lra_lu":null,"input_max_momentary":null,` +
	`"warnings":[],"error":"the file has no audio stream","renditions":[],` +
	`"created_at":"2026-03-01T12:00:00.123456Z","updated_at":"2026-03-01T12:05:30Z"}`

func TestNewAsset(t *testing.T) {
	tests := []struct {
		name  string
		asset asset.Asset
		want  string
	}{
		{"ready with measurements, warnings and renditions", readyAsset(t), readyJSON},
		{"queued with nothing measured, times in another zone", queuedAsset(t), queuedJSON},
		{"failed with its tenant-safe error", failedAsset(t), failedJSON},
		{"empty non-nil slices still marshal as arrays", func() asset.Asset {
			a := queuedAsset(t)
			a.Warnings = []asset.Warning{}
			a.Renditions = []asset.Rendition{}
			return a
		}(), queuedJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := marshal(t, NewAsset(tt.asset)); got != tt.want {
				t.Errorf("JSON mismatch\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

func TestNewAssetID(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"lower case", idReady},
		{"upper case input", "0192B7A0-4C3E-7D5A-9B1F-2E8C6D4A1F03"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := readyAsset(t)
			a.ID = mustParseUUID(t, tt.in)
			if got := NewAsset(a).ID; got != idReady {
				t.Errorf("ID = %q, want %q", got, idReady)
			}
		})
	}
}

func TestNewAssetKeys(t *testing.T) {
	wantAsset := []string{
		"created_at", "duration_ms", "error", "id", "idempotency_key", "input_lra_lu", "input_lufs",
		"input_max_momentary", "input_true_peak_dbtp", "renditions", "source_bytes", "source_codec",
		"source_sha256", "status", "target_lufs", "title", "updated_at", "warnings",
	}
	wantRendition := []string{
		"bitrate_kbps", "bytes", "channels", "codec", "created_at", "lra_lu", "lufs",
		"normalization", "profile", "sha256", "true_peak_dbtp",
	}
	tests := []struct {
		name  string
		asset asset.Asset
	}{
		{"ready", readyAsset(t)},
		{"queued", queuedAsset(t)},
		{"failed", failedAsset(t)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := marshal(t, NewAsset(tt.asset))
			if got := keysOf(t, doc); !slices.Equal(got, wantAsset) {
				t.Errorf("asset keys = %v, want %v", got, wantAsset)
			}
			var parsed struct {
				Renditions []json.RawMessage `json:"renditions"`
			}
			if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
				t.Fatal(err)
			}
			for i, r := range parsed.Renditions {
				if got := keysOf(t, string(r)); !slices.Equal(got, wantRendition) {
					t.Errorf("rendition %d keys = %v, want %v", i, got, wantRendition)
				}
			}
		})
	}
}

func TestNewAssetDoesNotAliasTheDomainValue(t *testing.T) {
	a := readyAsset(t)
	got := NewAsset(a)
	*a.DurationMS = 1
	*a.InputLUFS = 0
	*a.InputTruePeakDBTP = 0
	*a.InputLRALU = 0
	*a.InputMaxMomentary = 0
	a.Warnings[0] = "changed"
	a.Renditions[0].Profile = "changed"
	if marshal(t, got) != readyJSON {
		t.Errorf("the API value changed with the domain value: %s", marshal(t, got))
	}
}

func TestNewAssetList(t *testing.T) {
	const cursor = "AAAAAAAAAAo"
	tests := []struct {
		name string
		page asset.Page
		want string
	}{
		{"a page with more after it", asset.Page{
			Assets:     []asset.Asset{queuedAsset(t), failedAsset(t)},
			NextCursor: cursor,
		}, `{"data":[` + queuedJSON + `,` + failedJSON + `],"next_cursor":"` + cursor + `"}`},
		{"the last page", asset.Page{
			Assets: []asset.Asset{queuedAsset(t)},
		}, `{"data":[` + queuedJSON + `],"next_cursor":null}`},
		{"an empty page, nil slice", asset.Page{}, `{"data":[],"next_cursor":null}`},
		{"an empty page, empty slice", asset.Page{Assets: []asset.Asset{}}, `{"data":[],"next_cursor":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := marshal(t, NewAssetList(tt.page)); got != tt.want {
				t.Errorf("JSON mismatch\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

func TestNewEvent(t *testing.T) {
	const eventID = "0192b7a0-4c3e-7d5a-9b1f-2e8c6d4a1f06"
	tests := []struct {
		name  string
		event asset.Event
		want  string
	}{
		{"ready", asset.Event{
			ID:        mustParseUUID(t, eventID),
			TenantID:  42,
			Type:      asset.EventReady,
			CreatedAt: time.Date(2026, 3, 1, 12, 5, 30, 500000000, time.UTC),
			Asset:     readyAsset(t),
		}, `{"id":"` + eventID + `","type":"asset.ready","created_at":"2026-03-01T12:05:30.5Z","data":` + readyJSON + `}`},
		{"failed, created in another zone", asset.Event{
			ID:        mustParseUUID(t, "0192B7A0-4C3E-7D5A-9B1F-2E8C6D4A1F06"),
			TenantID:  42,
			Type:      asset.EventFailed,
			CreatedAt: updated.In(time.FixedZone("CEST", 2*60*60)),
			Asset:     failedAsset(t),
		}, `{"id":"` + eventID + `","type":"asset.failed","created_at":"2026-03-01T12:05:30Z","data":` + failedJSON + `}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := marshal(t, NewEvent(tt.event))
			if doc != tt.want {
				t.Errorf("JSON mismatch\n got: %s\nwant: %s", doc, tt.want)
			}
			if got, want := keysOf(t, doc), []string{"created_at", "data", "id", "type"}; !slices.Equal(got, want) {
				t.Errorf("keys = %v, want %v", got, want)
			}
		})
	}
}

func TestErrorEnvelope(t *testing.T) {
	var v validation.Collector
	v.Add("title", "is required")
	v.Add("file", "is empty")
	verr, ok := v.Err().(*validation.Error)
	if !ok {
		t.Fatalf("Err() = %T, want *validation.Error", v.Err())
	}
	tests := []struct {
		name string
		resp ErrorResponse
		want string
	}{
		{"no fields", NewError("not_found", "no such asset"),
			`{"error":{"code":"not_found","message":"no such asset"}}`},
		{"one field", func() ErrorResponse {
			var one validation.Collector
			one.Add("title", "is required")
			return NewValidationError("invalid_request", "invalid request", one.Err().(*validation.Error))
		}(), `{"error":{"code":"invalid_request","message":"invalid request",` +
			`"fields":[{"name":"title","message":"is required"}]}}`},
		{"fields keep their order", NewValidationError("invalid_request", "invalid request", verr),
			`{"error":{"code":"invalid_request","message":"invalid request",` +
				`"fields":[{"name":"title","message":"is required"},{"name":"file","message":"is empty"}]}}`},
		{"a nil validation error has no fields", NewValidationError("invalid_request", "invalid request", nil),
			`{"error":{"code":"invalid_request","message":"invalid request"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := marshal(t, tt.resp); got != tt.want {
				t.Errorf("JSON mismatch\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}
