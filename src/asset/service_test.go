package asset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/validation"
)

const (
	tenantID    = 7
	otherTenant = 8
)

var errBoom = errors.New("boom")

type fixture struct {
	svc   *Service
	repo  *fakeRepo
	files *fakeFiles
	calls *calls
	root  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	c := &calls{}
	root := t.TempDir()
	repo := &fakeRepo{calls: c, errs: map[string]error{}}
	files := &fakeFiles{calls: c, root: root, errs: map[string]error{}}
	return &fixture{svc: NewService(repo, files), repo: repo, files: files, calls: c, root: root}
}

// stage stages content and forgets the calls that took, so a test sees only what it calls next.
func (f *fixture) stage(t *testing.T, content string) Upload {
	t.Helper()
	u, err := f.svc.Stage(t.Context(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	f.calls.list = nil
	return u
}

func (f *fixture) params(t *testing.T, title, content, key string) CreateParams {
	t.Helper()
	return CreateParams{TenantID: tenantID, TargetLUFS: -18, Title: title, IdempotencyKey: key,
		Upload: f.stage(t, content)}
}

// stored returns an asset as an earlier upload of content would have left it.
func stored(tenant uint64, title, content, key string) Asset {
	return Asset{
		ID:             uuid.NewV7(),
		TenantID:       tenant,
		IdempotencyKey: key,
		Title:          title,
		Status:         StatusReady,
		SourceSHA256:   sha256.Sum256([]byte(content)),
		SourceBytes:    uint64(len(content)),
		CreatedAt:      fakeNow,
		UpdatedAt:      fakeNow,
	}
}

func TestStage(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
	}{
		{"empty", nil},
		{"short", []byte("rain on a tin roof")},
		{"many copy buffers", bytes.Repeat([]byte{0, 1, 2, 0x7f, 0xff}, 100_000)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			u, err := f.svc.Stage(t.Context(), iotest.HalfReader(bytes.NewReader(tc.content)))
			if err != nil {
				t.Fatalf("Stage() error = %v", err)
			}
			if want := sha256.Sum256(tc.content); u.sha256 != want {
				t.Errorf("sha256 = %x, want %x", u.sha256, want)
			}
			if u.size != uint64(len(tc.content)) {
				t.Errorf("size = %d, want %d", u.size, len(tc.content))
			}
			got, err := os.ReadFile(u.path)
			if err != nil {
				t.Fatalf("error reading staged file: %v", err)
			}
			if !bytes.Equal(got, tc.content) {
				t.Errorf("staged file has %d bytes that differ from the %d read", len(got), len(tc.content))
			}
			if dir := filepath.Join(f.root, "tmp"); filepath.Dir(u.path) != dir {
				t.Errorf("staged at %s, want a file in %s", u.path, dir)
			}
		})
	}
}

func TestStageFailures(t *testing.T) {
	tests := []struct {
		name    string
		reader  io.Reader
		tempErr error
	}{
		{"reader fails at once", iotest.ErrReader(errBoom), nil},
		{"reader fails part way", io.MultiReader(strings.NewReader(strings.Repeat("x", 100_000)),
			iotest.ErrReader(errBoom)), nil},
		{"no temp file", strings.NewReader("audio"), errBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.files.errs["TempFile"] = tc.tempErr
			u, err := f.svc.Stage(t.Context(), tc.reader)
			if !errors.Is(err, errBoom) {
				t.Errorf("Stage() error = %v, want %v", err, errBoom)
			}
			if u != (Upload{}) {
				t.Errorf("Stage() = %+v, want the zero Upload", u)
			}
			if files := filesUnder(t, f.root); len(files) > 0 {
				t.Errorf("files left behind: %v", files)
			}
		})
	}
}

func TestDiscard(t *testing.T) {
	f := newFixture(t)
	u := f.stage(t, "audio")
	f.svc.Discard(t.Context(), u)
	if files := filesUnder(t, f.root); len(files) > 0 {
		t.Errorf("files left after Discard: %v", files)
	}
	f.svc.Discard(t.Context(), u)
	f.svc.Discard(t.Context(), Upload{})
	if want := []string{"Remove tmp", "Remove tmp"}; !slices.Equal(f.calls.list, want) {
		t.Errorf("calls = %v, want %v", f.calls.list, want)
	}
}

func TestCreate(t *testing.T) {
	for _, key := range []string{"", "upload-42"} {
		t.Run("key "+key, func(t *testing.T) {
			f := newFixture(t)
			p := f.params(t, "Rain on a tin roof", "audio", key)

			a, replayed, err := f.svc.Create(t.Context(), p)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if replayed {
				t.Error("Create() replayed a new upload")
			}
			if len(f.repo.assets) != 1 {
				t.Fatalf("inserted %d assets, want 1", len(f.repo.assets))
			}
			want := Asset{
				ID:             f.repo.assets[0].ID,
				TenantID:       tenantID,
				IdempotencyKey: key,
				Title:          "Rain on a tin roof",
				Status:         StatusQueued,
				SourceSHA256:   sha256.Sum256([]byte("audio")),
				SourceBytes:    5,
				TargetLUFS:     -18,
				CreatedAt:      fakeNow,
				UpdatedAt:      fakeNow,
			}
			if !reflect.DeepEqual(a, want) {
				t.Errorf("Create() = %+v, want %+v", a, want)
			}
			if v := a.ID[6] >> 4; v != 7 {
				t.Errorf("ID %s has version %d, want 7", a.ID, v)
			}

			wantCalls := []string{"Promote", "insertAsset", "assetByID"}
			if key != "" {
				wantCalls = slices.Insert(wantCalls, 0, "assetByIdempotencyKey")
			}
			if !slices.Equal(f.calls.list, wantCalls) {
				t.Errorf("calls = %v, want %v", f.calls.list, wantCalls)
			}
			source := "sources/7/" + a.ID.String()
			if files := filesUnder(t, f.root); !slices.Equal(files, []string{source}) {
				t.Errorf("files = %v, want only %s", files, source)
			}
			if got, _ := os.ReadFile(filepath.Join(f.root, source)); string(got) != "audio" {
				t.Errorf("source holds %q, want %q", got, "audio")
			}
		})
	}
}

func TestCreateValidation(t *testing.T) {
	const ok = "Rain"
	tests := []struct {
		name    string
		title   string
		content string
		key     string
		want    []validation.Field
	}{
		{"title missing", "", "audio", "", fields("title", "is required")},
		{"title not UTF-8", "Rain \xff", "audio", "", fields("title", "must be valid UTF-8")},
		{"title of 256 characters", strings.Repeat("é", 256), "audio", "", fields("title",
			"must be at most 255 characters")},
		{"file empty", ok, "", "", fields("file", "is empty")},
		{"key with NUL", ok, "audio", "upload\x00", fields("Idempotency-Key", "must be printable ASCII")},
		{"key with a tab", ok, "audio", "up\tload", fields("Idempotency-Key", "must be printable ASCII")},
		{"key with DEL", ok, "audio", "upload\x7f", fields("Idempotency-Key", "must be printable ASCII")},
		{"key with a newline", ok, "audio", "upload\n", fields("Idempotency-Key", "must be printable ASCII")},
		{"key not ASCII", ok, "audio", "résumé-1", fields("Idempotency-Key", "must be printable ASCII")},
		{"key of 256 bytes", ok, "audio", strings.Repeat("k", 256), fields("Idempotency-Key",
			"must be at most 255 bytes")},
		{"key with a leading space", ok, "audio", " upload-42", fields("Idempotency-Key",
			"must not start or end with a space")},
		{"key with a trailing space", ok, "audio", "upload-42 ", fields("Idempotency-Key",
			"must not start or end with a space")},
		{"key of one space", ok, "audio", " ", fields("Idempotency-Key", "must not start or end with a space")},
		{
			name:    "every field",
			content: "",
			key:     "\x00",
			want: []validation.Field{
				{Name: "title", Message: "is required"},
				{Name: "file", Message: "is empty"},
				{Name: "Idempotency-Key", Message: "must be printable ASCII"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.params(t, tc.title, tc.content, tc.key)

			_, _, err := f.svc.Create(t.Context(), p)
			verr, ok := errors.AsType[*validation.Error](err)
			if !ok {
				t.Fatalf("Create() error = %v, want a *validation.Error", err)
			}
			if got := verr.Fields(); !slices.Equal(got, tc.want) {
				t.Errorf("fields = %v, want %v", got, tc.want)
			}
			if want := []string{"Remove tmp"}; !slices.Equal(f.calls.list, want) {
				t.Errorf("calls = %v, want %v", f.calls.list, want)
			}
			if files := filesUnder(t, f.root); len(files) > 0 {
				t.Errorf("files left behind: %v", files)
			}
		})
	}
}

func TestCreateAcceptsLimits(t *testing.T) {
	tests := []struct {
		name, title, key string
	}{
		{"title of 255 two-byte characters", strings.Repeat("é", 255), ""},
		{"title of 255 four-byte characters", strings.Repeat("🌧", 255), ""},
		{"key of 255 bytes", "Rain", strings.Repeat("k", 255)},
		{"key with inner spaces", "Rain", "upload 42 b"},
		{"key with every printable byte", "Rain", printableASCII()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			a, _, err := f.svc.Create(t.Context(), f.params(t, tc.title, "audio", tc.key))
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if a.Title != tc.title || a.IdempotencyKey != tc.key {
				t.Errorf("Create() stored title %q and key %q", a.Title, a.IdempotencyKey)
			}
		})
	}
}

func TestCreateIdempotency(t *testing.T) {
	const (
		title   = "Rain"
		content = "audio"
		key     = "upload-42"
	)
	tests := []struct {
		name     string
		existing []Asset
		winner   *Asset
		// wantAsset is the index into existing, or -1 for the winner, of the asset Create returns.
		wantAsset int
		wantErr   error
		wantCalls []string
	}{
		{
			name:      "replay",
			existing:  []Asset{stored(tenantID, title, content, key)},
			wantAsset: 0,
			wantCalls: []string{"assetByIdempotencyKey", "Remove tmp"},
		},
		{
			name:      "same key with a different file",
			existing:  []Asset{stored(tenantID, title, "other audio", key)},
			wantErr:   ErrIdempotencyKeyReused,
			wantCalls: []string{"assetByIdempotencyKey", "Remove tmp"},
		},
		{
			name:      "same key with a different title",
			existing:  []Asset{stored(tenantID, "Thunder", content, key)},
			wantErr:   ErrIdempotencyKeyReused,
			wantCalls: []string{"assetByIdempotencyKey", "Remove tmp"},
		},
		{
			name:      "lost the race to the same upload",
			winner:    new(stored(tenantID, title, content, key)),
			wantAsset: -1,
			wantCalls: []string{"assetByIdempotencyKey", "Promote", "insertAsset", "Remove sources",
				"assetByIdempotencyKey"},
		},
		{
			name:    "lost the race to a different upload",
			winner:  new(stored(tenantID, title, "other audio", key)),
			wantErr: ErrIdempotencyKeyReused,
			wantCalls: []string{"assetByIdempotencyKey", "Promote", "insertAsset", "Remove sources",
				"assetByIdempotencyKey"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.repo.assets = slices.Clone(tc.existing)
			if tc.winner != nil {
				f.repo.beforeInsert = func(r *fakeRepo) { r.assets = append(r.assets, *tc.winner) }
			}

			a, replayed, err := f.svc.Create(t.Context(), f.params(t, title, content, key))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Create() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				var want Asset
				if tc.wantAsset >= 0 {
					want = tc.existing[tc.wantAsset]
				} else {
					want = *tc.winner
				}
				if !replayed || !reflect.DeepEqual(a, want) {
					t.Errorf("Create() = %+v, replayed %t; want a replay of %+v", a, replayed, want)
				}
			}
			if !slices.Equal(f.calls.list, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", f.calls.list, tc.wantCalls)
			}
			if files := filesUnder(t, f.root); len(files) > 0 {
				t.Errorf("files left behind: %v", files)
			}
		})
	}
}

func TestCreateKeysBelongToTheirTenant(t *testing.T) {
	f := newFixture(t)
	other := stored(otherTenant, "Rain", "audio", "upload-42")
	f.repo.assets = []Asset{other}

	a, replayed, err := f.svc.Create(t.Context(), f.params(t, "Rain", "audio", "upload-42"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if replayed || a.ID == other.ID || a.TenantID != tenantID {
		t.Errorf("Create() = %+v, replayed %t; want a new asset of tenant %d", a, replayed, tenantID)
	}
}

func TestCreateFailures(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		repoErrs     map[string]error
		fileErrs     map[string]error
		beforeInsert func(*fakeRepo)
		wantErr      error
		wantCalls    []string
		// wantSource means the file stays as the source of an asset that was inserted.
		wantSource bool
	}{
		{
			name:      "key lookup fails",
			key:       "upload-42",
			repoErrs:  map[string]error{"assetByIdempotencyKey": errBoom},
			wantErr:   errBoom,
			wantCalls: []string{"assetByIdempotencyKey", "Remove tmp"},
		},
		{
			name:      "promote fails",
			fileErrs:  map[string]error{"Promote": errBoom},
			wantErr:   errBoom,
			wantCalls: []string{"Promote", "Remove tmp"},
		},
		{
			name:      "insert fails",
			repoErrs:  map[string]error{"insertAsset": errBoom},
			wantErr:   errBoom,
			wantCalls: []string{"Promote", "insertAsset", "Remove sources"},
		},
		{
			name:      "duplicate without a key",
			repoErrs:  map[string]error{"insertAsset": errDuplicate},
			wantErr:   errDuplicate,
			wantCalls: []string{"Promote", "insertAsset", "Remove sources"},
		},
		{
			name:     "duplicate with no asset holding the key",
			key:      "upload-42",
			repoErrs: map[string]error{"insertAsset": errDuplicate},
			wantCalls: []string{"assetByIdempotencyKey", "Promote", "insertAsset", "Remove sources",
				"assetByIdempotencyKey"},
		},
		{
			name: "duplicate and the winner can't be read",
			key:  "upload-42",
			beforeInsert: func(r *fakeRepo) {
				r.assets = append(r.assets, stored(tenantID, "Rain", "audio", "upload-42"))
				r.errs["assetByIdempotencyKey"] = errBoom
			},
			wantCalls: []string{"assetByIdempotencyKey", "Promote", "insertAsset", "Remove sources",
				"assetByIdempotencyKey"},
		},
		{
			name:       "created asset can't be read",
			repoErrs:   map[string]error{"assetByID": ErrNotFound},
			wantCalls:  []string{"Promote", "insertAsset", "assetByID"},
			wantSource: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.params(t, "Rain", "audio", tc.key)
			maps.Copy(f.repo.errs, tc.repoErrs)
			maps.Copy(f.files.errs, tc.fileErrs)
			f.repo.beforeInsert = tc.beforeInsert

			a, replayed, err := f.svc.Create(t.Context(), p)
			if err == nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Create() error = %v, want %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrIdempotencyKeyReused) {
				t.Errorf("Create() error = %v, want an internal error", err)
			}
			if replayed || !reflect.DeepEqual(a, Asset{}) {
				t.Errorf("Create() = %+v, replayed %t; want nothing", a, replayed)
			}
			if !slices.Equal(f.calls.list, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", f.calls.list, tc.wantCalls)
			}
			files := filesUnder(t, f.root)
			if tc.wantSource {
				if want := "sources/7/" + f.repo.assets[0].ID.String(); !slices.Equal(files, []string{want}) {
					t.Errorf("files = %v, want only %s", files, want)
				}
			} else if len(files) > 0 {
				t.Errorf("files left behind: %v", files)
			}
		})
	}
}

// TestCreateInsertOutlivesTheRequest shows that a request whose context ends before or during the insert
// still commits its row, and that the row keeps its source file.
func TestCreateInsertOutlivesTheRequest(t *testing.T) {
	tests := []struct {
		name string
		key  string
		// duringInsert cancels the request's context inside the insert rather than before Create.
		duringInsert bool
		wantCalls    []string
	}{
		{
			name:      "cancelled before Create",
			wantCalls: []string{"Promote", "insertAsset", "assetByID"},
		},
		{
			name:         "cancelled during the insert",
			key:          "upload-42",
			duringInsert: true,
			wantCalls:    []string{"assetByIdempotencyKey", "Promote", "insertAsset", "assetByID"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			p := f.params(t, "Rain", "audio", tc.key)
			log := slog.New(slog.DiscardHandler)
			ctx, cancel := context.WithCancel(logging.CtxWithLogger(t.Context(), log))
			defer cancel()
			if tc.duringInsert {
				f.repo.beforeInsert = func(*fakeRepo) { cancel() }
			} else {
				cancel()
			}

			start := time.Now()
			_, _, err := f.svc.Create(ctx, p)
			end := time.Now()
			if err == nil || errors.Is(err, ErrNotFound) {
				t.Errorf("Create() error = %v, want the re-read's internal error", err)
			}
			if len(f.repo.assets) != 1 {
				t.Fatalf("inserted %d assets, want 1", len(f.repo.assets))
			}
			if !slices.Equal(f.calls.list, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", f.calls.list, tc.wantCalls)
			}
			source := "sources/7/" + f.repo.assets[0].ID.String()
			if files := filesUnder(t, f.root); !slices.Equal(files, []string{source}) {
				t.Errorf("files = %v, want only %s", files, source)
			}

			deadline, ok := f.repo.insertCtx.Deadline()
			if !ok || deadline.Before(start.Add(insertTimeout)) || deadline.After(end.Add(insertTimeout)) {
				t.Errorf("insert deadline = %v (set %t), want %v after the call", deadline, ok, insertTimeout)
			}
			if got := logging.LoggerFromContext(f.repo.insertCtx); got != log {
				t.Error("the insert's context lost the request's logger")
			}
		})
	}
}

func TestCreateLogsRemoveFailure(t *testing.T) {
	f := newFixture(t)
	existing := stored(tenantID, "Rain", "audio", "upload-42")
	f.repo.assets = []Asset{existing}
	f.files.errs["Remove"] = errBoom
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

	a, replayed, err := f.svc.Create(ctx, f.params(t, "Rain", "audio", "upload-42"))
	if err != nil || !replayed || a.ID != existing.ID {
		t.Errorf("Create() = %s, replayed %t, error %v; want a replay of %s", a.ID, replayed, err, existing.ID)
	}
	if !strings.Contains(logs.String(), `"msg":"error removing file"`) || !strings.Contains(logs.String(), "boom") {
		t.Errorf("logs = %s, want the failed removal", logs.String())
	}
}

func TestGet(t *testing.T) {
	f := newFixture(t)
	mine := stored(tenantID, "Rain", "audio", "")
	theirs := stored(otherTenant, "Thunder", "audio", "")
	f.repo.assets = []Asset{mine, theirs}

	tests := []struct {
		name      string
		id        string
		repoErr   error
		wantErr   error
		wantCalls int
	}{
		{"lower case", mine.ID.String(), nil, nil, 1},
		{"upper case", strings.ToUpper(mine.ID.String()), nil, nil, 1},
		{"unknown", uuid.NewV7().String(), nil, ErrNotFound, 1},
		{"another tenant's", theirs.ID.String(), nil, ErrNotFound, 1},
		{"malformed", "not-a-uuid", nil, ErrNotFound, 0},
		{"empty", "", nil, ErrNotFound, 0},
		{"one character short", mine.ID.String()[1:], nil, ErrNotFound, 0},
		{"repository fails", mine.ID.String(), errBoom, errBoom, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f.calls.list = nil
			f.repo.errs["assetByID"] = tc.repoErr

			a, err := f.svc.Get(t.Context(), tenantID, tc.id)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Get() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && !reflect.DeepEqual(a, mine) {
				t.Errorf("Get() = %+v, want %+v", a, mine)
			}
			if len(f.calls.list) != tc.wantCalls {
				t.Errorf("calls = %v, want %d", f.calls.list, tc.wantCalls)
			}
		})
	}
}

func TestListRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		cursor string
		want   []validation.Field
	}{
		{"limit zero", 0, "", fields("limit", "must be between 1 and 100")},
		{"limit negative", -1, "", fields("limit", "must be between 1 and 100")},
		{"limit 101", 101, "", fields("limit", "must be between 1 and 100")},
		{"limit huge", math.MaxInt, "", fields("limit", "must be between 1 and 100")},
		{"cursor garbage", 20, "!!!", fields("cursor", "is invalid")},
		{"cursor of 7 bytes", 20, rawURL(make([]byte, 7)), fields("cursor", "is invalid")},
		{"cursor of 9 bytes", 20, rawURL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1}), fields("cursor", "is invalid")},
		{"cursor of ID zero", 20, rawURL(make([]byte, 8)), fields("cursor", "is invalid")},
		{"cursor padded", 20, encodeCursor(1) + "=", fields("cursor", "is invalid")},
		{"cursor in standard base64", 20, base64.RawStdEncoding.EncodeToString(
			[]byte{0xfb, 0xff, 0, 0, 0, 0, 0, 1}), fields("cursor", "is invalid")},
		{"cursor with unused bits set", 20, "AAAAAAAAAAF", fields("cursor", "is invalid")},
		{"cursor with a newline", 20, encodeCursor(1) + "\n", fields("cursor", "is invalid")},
		{"cursor with a space", 20, " " + encodeCursor(1), fields("cursor", "is invalid")},
		{"both", 0, "!!!", []validation.Field{
			{Name: "limit", Message: "must be between 1 and 100"},
			{Name: "cursor", Message: "is invalid"},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.svc.List(t.Context(), tenantID, tc.limit, tc.cursor)
			verr, ok := errors.AsType[*validation.Error](err)
			if !ok {
				t.Fatalf("List() error = %v, want a *validation.Error", err)
			}
			if got := verr.Fields(); !slices.Equal(got, tc.want) {
				t.Errorf("fields = %v, want %v", got, tc.want)
			}
			if len(f.calls.list) > 0 {
				t.Errorf("calls = %v, want none", f.calls.list)
			}
		})
	}
}

func TestCursorRoundTrip(t *testing.T) {
	for _, id := range []uint64{1, 255, 256, 1 << 32, math.MaxInt64, math.MaxUint64} {
		cursor := encodeCursor(id)
		got, ok := decodeCursor(cursor)
		if !ok || got != id {
			t.Errorf("decodeCursor(%q) = %d, %t; want %d, true", cursor, got, ok, id)
		}
	}
}

func TestListPages(t *testing.T) {
	f := newFixture(t)
	var want []uuid.UUID
	for range 5 {
		a := stored(tenantID, "Rain", "audio", "")
		f.repo.assets = append(f.repo.assets, a, stored(otherTenant, "Thunder", "audio", ""))
		want = slices.Insert(want, 0, a.ID)
	}

	for _, limit := range []int{1, 2, 4, 5, 6, MaxListLimit} {
		t.Run("limit "+strconv.Itoa(limit), func(t *testing.T) {
			f.repo.listArgs = nil
			var got []uuid.UUID
			cursor := ""
			for range 10 {
				page, err := f.svc.List(t.Context(), tenantID, limit, cursor)
				if err != nil {
					t.Fatalf("List() error = %v", err)
				}
				for _, a := range page.Assets {
					got = append(got, a.ID)
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("listed %v, want %v", got, want)
			}
			wantPages := (len(want) + limit - 1) / limit
			if len(f.repo.listArgs) != wantPages {
				t.Errorf("listed %d pages, want %d", len(f.repo.listArgs), wantPages)
			}
			for i, args := range f.repo.listArgs {
				if args.tenantID != tenantID || args.limit != limit || (i == 0) != (args.before == 0) {
					t.Errorf("page %d listed with %+v", i, args)
				}
			}
		})
	}
}

func TestListFailure(t *testing.T) {
	f := newFixture(t)
	f.repo.errs["listAssets"] = errBoom
	if _, err := f.svc.List(t.Context(), tenantID, DefaultListLimit, ""); !errors.Is(err, errBoom) {
		t.Errorf("List() error = %v, want %v", err, errBoom)
	}
}

func TestOpenRendition(t *testing.T) {
	f := newFixture(t)
	rendition := Rendition{
		Profile: ProfileAAC64k, Codec: CodecAAC, BitrateKbps: 64, Channels: 1, Bytes: 9,
		SHA256: sha256.Sum256([]byte("rendition")), LUFS: -16.2, TruePeakDBTP: -1.9, LRALU: 3.1,
		Normalization: NormalizationLinear, CreatedAt: fakeNow,
	}
	mine := stored(tenantID, "Rain", "audio", "")
	mine.Renditions = []Rendition{rendition}
	theirs := stored(otherTenant, "Thunder", "audio", "")
	theirs.Renditions = []Rendition{rendition}
	missing := stored(tenantID, "Wind", "audio", "")
	missing.Renditions = []Rendition{rendition}
	f.repo.assets = []Asset{mine, theirs, missing}
	for _, a := range []Asset{mine, theirs} {
		path, _ := f.files.RenditionPath(a.TenantID, a.ID, string(ProfileAAC64k))
		writeFile(t, path, "rendition")
	}

	tests := []struct {
		name      string
		id        string
		profile   string
		wantErr   error
		wantCalls []string
	}{
		{"found", mine.ID.String(), "aac_64k", nil, []string{"rendition", "Open"}},
		{"upper-case ID", strings.ToUpper(mine.ID.String()), "aac_64k", nil, []string{"rendition", "Open"}},
		{"profile the asset lacks", mine.ID.String(), "aac_128k", ErrNotFound, []string{"rendition"}},
		{"unknown profile", mine.ID.String(), "aac_320k", ErrNotFound, nil},
		{"profile in upper case", mine.ID.String(), "AAC_64K", ErrNotFound, nil},
		{"profile with a path", mine.ID.String(), "../aac_64k", ErrNotFound, nil},
		{"empty profile", mine.ID.String(), "", ErrNotFound, nil},
		{"malformed ID", "not-a-uuid", "aac_64k", ErrNotFound, nil},
		{"unknown ID", uuid.NewV7().String(), "aac_64k", ErrNotFound, []string{"rendition"}},
		{"another tenant's asset", theirs.ID.String(), "aac_64k", ErrNotFound, []string{"rendition"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f.calls.list = nil
			r, rc, err := f.svc.OpenRendition(t.Context(), tenantID, tc.id, tc.profile)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("OpenRendition() error = %v, want %v", err, tc.wantErr)
			}
			if !slices.Equal(f.calls.list, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", f.calls.list, tc.wantCalls)
			}
			if tc.wantErr != nil {
				if rc != nil {
					t.Error("OpenRendition() returned a file with its error")
				}
				return
			}
			defer rc.Close()
			if r != rendition {
				t.Errorf("OpenRendition() = %+v, want %+v", r, rendition)
			}
			if got, err := io.ReadAll(rc); err != nil || string(got) != "rendition" {
				t.Errorf("file holds %q (error %v), want %q", got, err, "rendition")
			}
		})
	}

	t.Run("file missing", func(t *testing.T) {
		_, rc, err := f.svc.OpenRendition(t.Context(), tenantID, missing.ID.String(), "aac_64k")
		if err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("OpenRendition() error = %v, want an internal error", err)
		}
		if rc != nil {
			t.Error("OpenRendition() returned a file with its error")
		}
	})
	t.Run("repository fails", func(t *testing.T) {
		f.repo.errs["rendition"] = errBoom
		defer delete(f.repo.errs, "rendition")
		if _, _, err := f.svc.OpenRendition(t.Context(), tenantID, mine.ID.String(), "aac_64k"); !errors.Is(err, errBoom) {
			t.Errorf("OpenRendition() error = %v, want %v", err, errBoom)
		}
	})
}

func fields(name, message string) []validation.Field {
	return []validation.Field{{Name: name, Message: message}}
}

func rawURL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// printableASCII returns every printable ASCII byte, with the space inside.
func printableASCII() string {
	var b strings.Builder
	for c := byte('!'); c <= '~'; c++ {
		b.WriteByte(c)
		if c == 'A' {
			b.WriteByte(' ')
		}
	}
	return b.String()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
}
