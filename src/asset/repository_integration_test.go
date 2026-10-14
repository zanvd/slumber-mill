//go:build integration

package asset

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/testutil"
)

// stack is the real service, repository and storage on a throwaway database and directory.
type stack struct {
	repo  *Repository
	svc   *Service
	store *storage.Local
	root  string
	pool  *sql.DB
}

func newStack(t *testing.T) *stack {
	t.Helper()
	return newStackOn(t, testutil.NewDatabase(t))
}

func newStackOn(t *testing.T, dsn string) *stack {
	t.Helper()
	gdb, err := db.Open(dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("error opening the database: %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("error getting the pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	root := t.TempDir()
	store := storage.NewLocal(root)
	repo := NewRepository(gdb)
	return &stack{repo: repo, svc: NewService(repo, store), store: store, root: root, pool: pool}
}

func (s *stack) tenant(t *testing.T, slug string) uint64 {
	t.Helper()
	hash := make([]byte, sha256.Size)
	rand.Read(hash)
	res, err := s.pool.ExecContext(t.Context(), "INSERT INTO tenants (slug, api_key_hash) VALUES (?, ?)",
		slug, hash)
	if err != nil {
		t.Fatalf("error creating tenant %q: %v", slug, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("error reading tenant ID: %v", err)
	}
	return uint64(id)
}

func (s *stack) create(t *testing.T, tenantID uint64, title, content, key string) (Asset, bool) {
	t.Helper()
	a, replayed, err := s.tryCreate(t, tenantID, title, content, key)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return a, replayed
}

func (s *stack) tryCreate(t *testing.T, tenantID uint64, title, content, key string) (Asset, bool, error) {
	t.Helper()
	u, err := s.svc.Stage(t.Context(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	return s.svc.Create(t.Context(), CreateParams{
		TenantID: tenantID, TargetLUFS: -16, Title: title, IdempotencyKey: key, Upload: u,
	})
}

// addRendition inserts a rendition row for the asset, owned by tenantID, and returns it as read back.
func (s *stack) addRendition(t *testing.T, tenantID uint64, assetID uuid.UUID, r Rendition) Rendition {
	t.Helper()
	res, err := s.pool.ExecContext(t.Context(),
		"INSERT INTO renditions (tenant_id, asset_id, profile, codec, bitrate_kbps, channels, bytes, sha256, "+
			"lufs, true_peak_dbtp, lra_lu, normalization) "+
			"SELECT ?, id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM assets WHERE public_id = ?",
		tenantID, string(r.Profile), string(r.Codec), r.BitrateKbps, r.Channels, r.Bytes, r.SHA256[:],
		r.LUFS, r.TruePeakDBTP, r.LRALU, string(r.Normalization), db.ID(assetID))
	if err != nil {
		t.Fatalf("error inserting rendition: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("inserted %d renditions, want 1", n)
	}
	err = s.pool.QueryRowContext(t.Context(),
		"SELECT r.created_at FROM renditions r JOIN assets a ON a.id = r.asset_id "+
			"WHERE a.public_id = ? AND r.profile = ?", db.ID(assetID), string(r.Profile)).Scan(&r.CreatedAt)
	if err != nil {
		t.Fatalf("error reading rendition: %v", err)
	}
	return r
}

func (s *stack) writeRendition(t *testing.T, tenantID uint64, assetID uuid.UUID, profile Profile, content string) {
	t.Helper()
	path, err := s.store.RenditionPath(tenantID, assetID, string(profile))
	if err != nil {
		t.Fatalf("RenditionPath() error = %v", err)
	}
	writeFile(t, path, content)
}

func testRendition(profile Profile) Rendition {
	r := Rendition{
		Profile: profile, Codec: CodecAAC, BitrateKbps: 128, Channels: 2, Bytes: 9,
		SHA256: sha256.Sum256([]byte(profile)), LUFS: -16.03, TruePeakDBTP: -1.52, LRALU: 4.1,
		Normalization: NormalizationLinear,
	}
	if profile == ProfileAAC64k {
		r.BitrateKbps, r.Channels, r.LUFS, r.Normalization = 64, 1, -15.76, NormalizationDynamic
	}
	return r
}

func TestTenantIsolation(t *testing.T) {
	s := newStack(t)
	owner := s.tenant(t, "owner")
	other := s.tenant(t, "other")
	a, _ := s.create(t, owner, "Rain", "audio", "upload-42")
	r := s.addRendition(t, owner, a.ID, testRendition(ProfileAAC128k))
	s.writeRendition(t, owner, a.ID, ProfileAAC128k, "rendition")

	t.Run("owner", func(t *testing.T) {
		got, err := s.svc.Get(t.Context(), owner, a.ID.String())
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if want := []Rendition{r}; !reflect.DeepEqual(got.Renditions, want) {
			t.Errorf("Renditions = %+v, want %+v", got.Renditions, want)
		}
		gotR, rc, err := s.svc.OpenRendition(t.Context(), owner, a.ID.String(), "aac_128k")
		if err != nil {
			t.Fatalf("OpenRendition() error = %v", err)
		}
		defer rc.Close()
		if gotR != r {
			t.Errorf("OpenRendition() = %+v, want %+v", gotR, r)
		}
		if b, err := io.ReadAll(rc); err != nil || string(b) != "rendition" {
			t.Errorf("rendition file holds %q (error %v), want %q", b, err, "rendition")
		}
		page, err := s.svc.List(t.Context(), owner, DefaultListLimit, "")
		if err != nil || len(page.Assets) != 1 || page.Assets[0].ID != a.ID {
			t.Errorf("List() = %+v, %v; want only %s", page, err, a.ID)
		}
	})
	t.Run("other tenant", func(t *testing.T) {
		if _, err := s.svc.Get(t.Context(), other, a.ID.String()); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get() error = %v, want %v", err, ErrNotFound)
		}
		if _, _, err := s.svc.OpenRendition(t.Context(), other, a.ID.String(), "aac_128k"); !errors.Is(err, ErrNotFound) {
			t.Errorf("OpenRendition() error = %v, want %v", err, ErrNotFound)
		}
		page, err := s.svc.List(t.Context(), other, MaxListLimit, "")
		if err != nil || len(page.Assets) != 0 || page.NextCursor != "" {
			t.Errorf("List() = %+v, %v; want an empty last page", page, err)
		}
		mine, replayed := s.create(t, other, "Rain", "audio", "upload-42")
		if replayed || mine.ID == a.ID {
			t.Errorf("Create() with another tenant's key = %s, replayed %t; want a new asset", mine.ID, replayed)
		}
	})
}

func TestUpperCaseIDs(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	a, _ := s.create(t, tenant, "Rain", "audio", "")
	s.addRendition(t, tenant, a.ID, testRendition(ProfileAAC64k))
	s.writeRendition(t, tenant, a.ID, ProfileAAC64k, "rendition")
	upper := strings.ToUpper(a.ID.String())

	got, err := s.svc.Get(t.Context(), tenant, upper)
	if err != nil || got.ID != a.ID {
		t.Errorf("Get(%q) = %s, %v; want %s", upper, got.ID, err, a.ID)
	}
	_, rc, err := s.svc.OpenRendition(t.Context(), tenant, upper, "aac_64k")
	if err != nil {
		t.Fatalf("OpenRendition(%q) error = %v", upper, err)
	}
	rc.Close()
}

func TestIdempotencyKeys(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")

	first, replayed := s.create(t, tenant, "Rain", "audio", "Upload-42")
	if replayed {
		t.Error("first upload was replayed")
	}
	lower, replayed := s.create(t, tenant, "Rain", "audio", "upload-42")
	if replayed || lower.ID == first.ID {
		t.Errorf("upload-42 gave %s, replayed %t; want an asset apart from Upload-42's %s", lower.ID, replayed, first.ID)
	}
	again, replayed := s.create(t, tenant, "Rain", "audio", "Upload-42")
	if !replayed || !reflect.DeepEqual(again, first) {
		t.Errorf("retry of Upload-42 = %+v, replayed %t; want a replay of %+v", again, replayed, first)
	}
	if _, _, err := s.tryCreate(t, tenant, "Rain", "other audio", "Upload-42"); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Errorf("Upload-42 with another file: error = %v, want %v", err, ErrIdempotencyKeyReused)
	}
	for range 2 {
		s.create(t, tenant, "Rain", "audio", "")
	}

	var keys []sql.NullString
	rows, err := s.pool.QueryContext(t.Context(), "SELECT idempotency_key FROM assets ORDER BY id")
	if err != nil {
		t.Fatalf("error reading keys: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k sql.NullString
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("error reading keys: %v", err)
		}
		keys = append(keys, k)
	}
	want := []sql.NullString{{String: "Upload-42", Valid: true}, {String: "upload-42", Valid: true}, {}, {}}
	if !slices.Equal(keys, want) {
		t.Errorf("stored keys = %v, want %v", keys, want)
	}
	assertFiles(t, s.root, tenant, 4)
}

// TestConcurrentCreates retries one upload from several callers at once: exactly one asset results,
// and every caller gets it.
func TestConcurrentCreates(t *testing.T) {
	const callers = 8
	s := newStack(t)
	tenant := s.tenant(t, "owner")

	uploads := make([]Upload, callers)
	for i := range uploads {
		u, err := s.svc.Stage(t.Context(), strings.NewReader("audio"))
		if err != nil {
			t.Fatalf("Stage() error = %v", err)
		}
		uploads[i] = u
	}
	type result struct {
		asset    Asset
		replayed bool
		err      error
	}
	results := make([]result, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start
			a, replayed, err := s.svc.Create(t.Context(), CreateParams{
				TenantID: tenant, TargetLUFS: -16, Title: "Rain", IdempotencyKey: "upload-42", Upload: uploads[i],
			})
			results[i] = result{a, replayed, err}
		})
	}
	close(start)
	wg.Wait()

	created := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("caller %d: Create() error = %v", i, r.err)
		}
		if r.asset.ID != results[0].asset.ID {
			t.Errorf("caller %d got %s, caller 0 got %s", i, r.asset.ID, results[0].asset.ID)
		}
		if !r.replayed {
			created++
		}
	}
	if created != 1 {
		t.Errorf("%d callers created the asset, want 1", created)
	}
	var count int
	if err := s.pool.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM assets").Scan(&count); err != nil {
		t.Fatalf("error counting assets: %v", err)
	}
	if count != 1 {
		t.Errorf("%d assets, want 1", count)
	}
	sources := assertFiles(t, s.root, tenant, 1)
	if len(sources) == 1 && sources[0] != results[0].asset.ID.String() {
		t.Errorf("source file %s, want %s", sources[0], results[0].asset.ID)
	}
}

// TestCreateCommitsAfterTheRequestEnds cancels the request before Create: the row still commits
// and keeps its source file, and only the re-read fails.
func TestCreateCommitsAfterTheRequestEnds(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	u, err := s.svc.Stage(t.Context(), strings.NewReader("audio"))
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err = s.svc.Create(ctx, CreateParams{TenantID: tenant, TargetLUFS: -16, Title: "Rain", Upload: u})
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("Create() error = %v, want the re-read's internal error", err)
	}
	ids := s.assetIDs(t)
	if len(ids) != 1 {
		t.Fatalf("%d assets, want 1", len(ids))
	}
	if sources := assertFiles(t, s.root, tenant, 1); len(sources) == 1 && sources[0] != ids[0].String() {
		t.Errorf("source file %s, want %s", sources[0], ids[0])
	}
}

// TestCreateKeepsSourceWhenInsertTimesOut holds the tenant's row, so the insert's foreign-key check waits
// past the insert's deadline. MySQL commits the row once the lock is released, and its source is still there.
func TestCreateKeepsSourceWhenInsertTimesOut(t *testing.T) {
	s := newStack(t)
	s.svc.insertTimeout = 500 * time.Millisecond
	tenant := s.tenant(t, "owner")
	lock := s.lockTenant(t, tenant)

	_, _, err := s.tryCreate(t, tenant, "Rain", "audio", "upload-42")
	if !errors.Is(err, errOutcomeUnknown) {
		t.Fatalf("Create() error = %v, want %v", err, errOutcomeUnknown)
	}
	sources := assertFiles(t, s.root, tenant, 1)
	if ids := s.assetIDs(t); len(ids) != 0 {
		t.Fatalf("%d assets while the insert waits, want 0", len(ids))
	}

	if err := lock.Commit(); err != nil {
		t.Fatalf("error releasing the tenant's row: %v", err)
	}
	// Releasing the row granted the waiting insert its shared lock, which it holds until it commits,
	// so locking the row again waits for the insert to finish.
	if err := s.lockTenant(t, tenant).Rollback(); err != nil {
		t.Fatalf("error releasing the tenant's row: %v", err)
	}
	ids := s.assetIDs(t)
	if len(ids) != 1 || len(sources) != 1 || ids[0].String() != sources[0] {
		t.Fatalf("assets %v with source files %v, want one asset and its source", ids, sources)
	}

	a, replayed := s.create(t, tenant, "Rain", "audio", "upload-42")
	if !replayed || a.ID.String() != sources[0] {
		t.Errorf("retry = %s, replayed %t; want a replay of %s", a.ID, replayed, sources[0])
	}
	assertFiles(t, s.root, tenant, 1)
}

// lockTenant holds the tenant's row FOR UPDATE until the returned transaction ends.
func (s *stack) lockTenant(t *testing.T, tenantID uint64) *sql.Tx {
	t.Helper()
	tx, err := s.pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("error beginning a transaction: %v", err)
	}
	t.Cleanup(func() { tx.Rollback() })
	var id uint64
	err = tx.QueryRowContext(t.Context(), "SELECT id FROM tenants WHERE id = ? FOR UPDATE", tenantID).Scan(&id)
	if err != nil {
		t.Fatalf("error locking tenant %d: %v", tenantID, err)
	}
	return tx
}

func (s *stack) assetIDs(t *testing.T) []db.ID {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(), "SELECT public_id FROM assets ORDER BY id")
	if err != nil {
		t.Fatalf("error reading assets: %v", err)
	}
	defer rows.Close()
	var ids []db.ID
	for rows.Next() {
		var id db.ID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("error reading assets: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading assets: %v", err)
	}
	return ids
}

func TestPagination(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	other := s.tenant(t, "other")
	var want []uuid.UUID
	for i := range 6 {
		a, _ := s.create(t, tenant, "Rain "+strconv.Itoa(i), "audio", "")
		want = slices.Insert(want, 0, a.ID)
		if i%2 == 0 {
			s.create(t, other, "Thunder", "audio", "")
		}
	}

	for _, limit := range []int{1, 2, 3, 4, 5, 6, 7, MaxListLimit} {
		t.Run("limit "+strconv.Itoa(limit), func(t *testing.T) {
			var got []uuid.UUID
			cursor := ""
			pages := 0
			for {
				page, err := s.svc.List(t.Context(), tenant, limit, cursor)
				if err != nil {
					t.Fatalf("List() error = %v", err)
				}
				pages++
				if len(page.Assets) > limit || page.NextCursor != "" && len(page.Assets) != limit {
					t.Errorf("page %d has %d assets and cursor %q at limit %d", pages, len(page.Assets),
						page.NextCursor, limit)
				}
				for _, a := range page.Assets {
					got = append(got, a.ID)
				}
				if cursor = page.NextCursor; cursor == "" || pages > len(want) {
					break
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("listed %v, want %v", got, want)
			}
			if wantPages := (len(want) + limit - 1) / limit; pages != wantPages {
				t.Errorf("listed %d pages, want %d", pages, wantPages)
			}
		})
	}
	t.Run("cursor past every row", func(t *testing.T) {
		page, err := s.svc.List(t.Context(), tenant, MaxListLimit, encodeCursor(math.MaxUint64))
		if err != nil || len(page.Assets) != len(want) {
			t.Errorf("List() = %d assets, %v; want %d", len(page.Assets), err, len(want))
		}
	})
}

func TestPreloadIsTenantFiltered(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	other := s.tenant(t, "other")
	a, _ := s.create(t, tenant, "Rain", "audio", "upload-42")
	mine := s.addRendition(t, tenant, a.ID, testRendition(ProfileAAC128k))
	s.addRendition(t, other, a.ID, testRendition(ProfileAAC64k))
	s.writeRendition(t, tenant, a.ID, ProfileAAC64k, "rendition")
	want := []Rendition{mine}

	got, err := s.svc.Get(t.Context(), tenant, a.ID.String())
	if err != nil || !reflect.DeepEqual(got.Renditions, want) {
		t.Errorf("Get() renditions = %+v, %v; want %+v", got.Renditions, err, want)
	}
	page, err := s.svc.List(t.Context(), tenant, DefaultListLimit, "")
	if err != nil || len(page.Assets) != 1 || !reflect.DeepEqual(page.Assets[0].Renditions, want) {
		t.Errorf("List() = %+v, %v; want one asset with renditions %+v", page, err, want)
	}
	byKey, err := s.repo.assetByIdempotencyKey(t.Context(), tenant, "upload-42")
	if err != nil || !reflect.DeepEqual(byKey.Renditions, want) {
		t.Errorf("assetByIdempotencyKey() renditions = %+v, %v; want %+v", byKey.Renditions, err, want)
	}
	if _, _, err := s.svc.OpenRendition(t.Context(), tenant, a.ID.String(), "aac_64k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("OpenRendition() of a row owned by another tenant: error = %v, want %v", err, ErrNotFound)
	}
}

func TestOpenRenditionWithoutFile(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	a, _ := s.create(t, tenant, "Rain", "audio", "")
	s.addRendition(t, tenant, a.ID, testRendition(ProfileAAC128k))

	_, rc, err := s.svc.OpenRendition(t.Context(), tenant, a.ID.String(), "aac_128k")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("OpenRendition() error = %v, want an internal error", err)
	}
	if rc != nil {
		rc.Close()
		t.Error("OpenRendition() returned a file with its error")
	}
}

func TestWarningsRoundTrip(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	a, _ := s.create(t, tenant, "Rain", "audio", "")
	if a.Warnings != nil {
		t.Errorf("new asset has warnings %v", a.Warnings)
	}

	tests := []struct {
		name  string
		write warningSet
		want  []Warning
	}{
		{"one", warningSet{WarningSourceMayClip}, []Warning{WarningSourceMayClip}},
		{"out of order", warningSet{WarningSourceMayClip, WarningSuddenLoudEvent},
			[]Warning{WarningSuddenLoudEvent, WarningSourceMayClip}},
		{"repeated", warningSet{WarningDynamicNormalization, WarningDynamicNormalization},
			[]Warning{WarningDynamicNormalization}},
		{"all", warningSet{WarningSourceMayClip, WarningDynamicNormalization, WarningSuddenLoudEvent},
			[]Warning{WarningSuddenLoudEvent, WarningDynamicNormalization, WarningSourceMayClip}},
		{"none", nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.pool.ExecContext(t.Context(), "UPDATE assets SET warnings = ? WHERE public_id = ?",
				tc.write, db.ID(a.ID))
			if err != nil {
				t.Fatalf("error writing warnings: %v", err)
			}
			got, err := s.repo.assetByID(t.Context(), tenant, a.ID)
			if err != nil {
				t.Fatalf("assetByID() error = %v", err)
			}
			if !slices.Equal(got.Warnings, tc.want) {
				t.Errorf("Warnings = %v, want %v", got.Warnings, tc.want)
			}
		})
	}
}

// TestTimestampsFromMySQL pins every session's clock to 2000-01-01, which the Go clock can't produce.
func TestTimestampsFromMySQL(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutil.NewDatabase(t))
	if err != nil {
		t.Fatalf("error parsing DSN: %v", err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["timestamp"] = "946684800.123456"
	s := newStackOn(t, cfg.FormatDSN())
	tenant := s.tenant(t, "owner")
	want := time.Date(2000, 1, 1, 0, 0, 0, 123456000, time.UTC)

	a, _ := s.create(t, tenant, "Rain", "audio", "")
	r := s.addRendition(t, tenant, a.ID, testRendition(ProfileAAC128k))
	if !a.CreatedAt.Equal(want) || !a.UpdatedAt.Equal(want) || !r.CreatedAt.Equal(want) {
		t.Errorf("created_at %v, updated_at %v, rendition created_at %v; want MySQL's %v",
			a.CreatedAt, a.UpdatedAt, r.CreatedAt, want)
	}
	got, err := s.svc.Get(t.Context(), tenant, a.ID.String())
	if err != nil || len(got.Renditions) != 1 || !got.Renditions[0].CreatedAt.Equal(want) {
		t.Errorf("Get() = %+v, %v; want a rendition created at %v", got, err, want)
	}
}

// TestReadsEveryColumn reads an asset in each state the encode pool can leave it in, as a row written directly.
func TestReadsEveryColumn(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	tests := []struct {
		name string
		want Asset
	}{
		{"processing", Asset{Status: StatusProcessing, SourceCodec: "flac", DurationMS: new(uint64(600_000))}},
		{"ready", Asset{
			Status: StatusReady, SourceCodec: "pcm_s16le", DurationMS: new(uint64(3_000)),
			InputLUFS: new(-23.45), InputTruePeakDBTP: new(-0.25), InputLRALU: new(7.5), InputMaxMomentary: new(-1.0),
			Warnings: []Warning{WarningSuddenLoudEvent, WarningSourceMayClip},
		}},
		{"failed", Asset{Status: StatusFailed, Error: "not audio"}},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.want
			want.ID = uuid.NewV7()
			want.TenantID = tenant
			want.IdempotencyKey = "key-" + strconv.Itoa(i)
			want.Title = "Rain " + tc.name
			want.SourceSHA256 = sha256.Sum256([]byte(tc.name))
			want.SourceBytes = 1 << 40
			want.TargetLUFS = -18.5
			_, err := s.pool.ExecContext(t.Context(),
				"INSERT INTO assets (public_id, tenant_id, idempotency_key, title, status, attempts, "+
					"source_sha256, source_bytes, source_codec, duration_ms, target_lufs, input_lufs, "+
					"input_true_peak_dbtp, input_lra_lu, input_max_momentary, warnings, error, last_error) "+
					"VALUES (?, ?, ?, ?, ?, 2, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)",
				db.ID(want.ID), tenant, want.IdempotencyKey, want.Title, string(want.Status), want.SourceSHA256[:],
				want.SourceBytes, want.SourceCodec, want.DurationMS, want.TargetLUFS, want.InputLUFS,
				want.InputTruePeakDBTP, want.InputLRALU, want.InputMaxMomentary, warningSet(want.Warnings),
				want.Error, "ffmpeg exited 1: /data/tmp/part-123")
			if err != nil {
				t.Fatalf("error inserting asset: %v", err)
			}
			if tc.want.Status == StatusReady {
				want.Renditions = []Rendition{
					s.addRendition(t, tenant, want.ID, testRendition(ProfileAAC64k)),
					s.addRendition(t, tenant, want.ID, testRendition(ProfileAAC128k)),
				}
				slices.Reverse(want.Renditions)
			}

			got, err := s.svc.Get(t.Context(), tenant, want.ID.String())
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
				t.Errorf("timestamps not read: created %v, updated %v", got.CreatedAt, got.UpdatedAt)
			}
			want.CreatedAt, want.UpdatedAt = got.CreatedAt, got.UpdatedAt
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Get() = %+v\nwant  %+v", got, want)
			}
		})
	}
}

func TestRepositoryErrors(t *testing.T) {
	s := newStack(t)
	tenant := s.tenant(t, "owner")
	a, _ := s.create(t, tenant, "Rain", "audio", "")

	newAsset := func(edit func(*Asset)) Asset {
		n := Asset{ID: uuid.NewV7(), TenantID: tenant, Title: "Rain", Status: StatusQueued, SourceBytes: 1}
		edit(&n)
		return n
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	inserts := []struct {
		name        string
		ctx         context.Context
		asset       Asset
		want        error
		wantUnknown bool
	}{
		{"duplicate public ID", t.Context(), newAsset(func(n *Asset) { n.ID = a.ID }), errDuplicate, false},
		{"unknown tenant, an error GORM translates", t.Context(),
			newAsset(func(n *Asset) { n.TenantID = tenant + 1000 }), errDatabase, false},
		{"title too long, an error GORM passes on", t.Context(),
			newAsset(func(n *Asset) { n.Title = strings.Repeat("x", 256) }), errDatabase, false},
		{"context cancelled", cancelled, newAsset(func(*Asset) {}), errOutcomeUnknown, true},
	}
	for _, tc := range inserts {
		t.Run("insert/"+tc.name, func(t *testing.T) {
			err := s.repo.insertAsset(tc.ctx, tc.asset)
			if !errors.Is(err, tc.want) {
				t.Fatalf("insertAsset() error = %v, want %v", err, tc.want)
			}
			if errors.Is(err, errOutcomeUnknown) != tc.wantUnknown {
				t.Errorf("insertAsset() error = %v; outcome unknown should be %t", err, tc.wantUnknown)
			}
			if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
				t.Errorf("the driver's error is reachable through %v", err)
			}
			for _, inner := range []error{gorm.ErrDuplicatedKey, gorm.ErrForeignKeyViolated, context.Canceled} {
				if errors.Is(err, inner) {
					t.Errorf("%v is reachable through %v", inner, err)
				}
			}
		})
	}
	t.Run("not found", func(t *testing.T) {
		if _, err := s.repo.assetByID(t.Context(), tenant, uuid.NewV7()); err != ErrNotFound {
			t.Errorf("assetByID() error = %v, want %v", err, ErrNotFound)
		}
		if _, err := s.repo.rendition(t.Context(), tenant, a.ID, ProfileAAC64k); err != ErrNotFound {
			t.Errorf("rendition() error = %v, want %v", err, ErrNotFound)
		}
	})
}

// assertFiles checks that nothing is left in tmp and that the tenant has n source files, and returns their names.
func assertFiles(t *testing.T, root string, tenantID uint64, n int) []string {
	t.Helper()
	if tmp, _ := os.ReadDir(filepath.Join(root, "tmp")); len(tmp) > 0 {
		t.Errorf("%d files left in tmp", len(tmp))
	}
	entries, err := os.ReadDir(filepath.Join(root, "sources", strconv.FormatUint(tenantID, 10)))
	if err != nil {
		t.Fatalf("error reading sources: %v", err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	if len(names) != n {
		t.Errorf("source files = %v, want %d", names, n)
	}
	return names
}

const (
	testLease = time.Minute
	slack     = 5 * time.Second
)

// newLeaseStack returns a stack whose test ends by checking the outbox: every finished asset has exactly one event,
// of the type its status calls for, and no other asset has any.
func newLeaseStack(t *testing.T) *stack {
	t.Helper()
	s := newStack(t)
	t.Cleanup(func() { s.checkOutbox(t) })
	return s
}

func (s *stack) checkOutbox(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.pool.QueryContext(ctx,
		"SELECT a.id, a.status, COALESCE(GROUP_CONCAT(e.type ORDER BY e.id), '') "+
			"FROM assets a LEFT JOIN asset_events e ON e.asset_id = a.id GROUP BY a.id, a.status")
	if err != nil {
		t.Fatalf("error reading the outbox: %v", err)
	}
	defer rows.Close()
	want := map[string]string{"ready": "asset.ready", "failed": "asset.failed"}
	for rows.Next() {
		var id uint64
		var status, types string
		if err := rows.Scan(&id, &status, &types); err != nil {
			t.Fatalf("error reading the outbox: %v", err)
		}
		if types != want[status] {
			t.Errorf("asset %d is %s with events [%s], want [%s]", id, status, types, want[status])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading the outbox: %v", err)
	}
}

// queue uploads an asset, and returns its public and internal IDs.
func (s *stack) queue(t *testing.T, tenantID uint64) (uuid.UUID, uint64) {
	t.Helper()
	a, _ := s.create(t, tenantID, "Rain", "audio", "")
	var id uint64
	err := s.pool.QueryRowContext(t.Context(), "SELECT id FROM assets WHERE public_id = ?", db.ID(a.ID)).Scan(&id)
	if err != nil {
		t.Fatalf("error reading the asset's ID: %v", err)
	}
	return a.ID, id
}

func (s *stack) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := s.pool.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
}

func (s *stack) claimNext(t *testing.T) lease {
	t.Helper()
	l, ok, err := s.repo.claim(t.Context(), "worker", testLease)
	if err != nil || !ok {
		t.Fatalf("claim() = %+v, %t, %v; want a lease", l, ok, err)
	}
	return l
}

func (s *stack) wantNothingDue(t *testing.T) {
	t.Helper()
	if l, ok, err := s.repo.claim(t.Context(), "idle", testLease); ok || err != nil || l != (lease{}) {
		t.Errorf("claim() = %+v, %t, %v; want nothing due", l, ok, err)
	}
}

func (s *stack) expire(t *testing.T, id uint64) {
	t.Helper()
	s.exec(t, "UPDATE assets SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND WHERE id = ?", id)
}

// leaseState is an asset's lease and outcome columns.
type leaseState struct {
	Status         string
	Attempts       int
	LockedBy       sql.NullString
	LeaseExpiresAt sql.NullTime
	Error          sql.NullString
	LastError      sql.NullString
}

func (s *stack) state(t *testing.T, id uint64) leaseState {
	t.Helper()
	var st leaseState
	err := s.pool.QueryRowContext(t.Context(),
		"SELECT status, attempts, locked_by, lease_expires_at, error, last_error FROM assets WHERE id = ?", id).
		Scan(&st.Status, &st.Attempts, &st.LockedBy, &st.LeaseExpiresAt, &st.Error, &st.LastError)
	if err != nil {
		t.Fatalf("error reading asset %d: %v", id, err)
	}
	return st
}

// wantUnleased checks that the asset is in status on attempt attempts, with its lease columns cleared.
func (s *stack) wantUnleased(t *testing.T, id uint64, status Status, attempts int) leaseState {
	t.Helper()
	st := s.state(t, id)
	if st.Status != string(status) || st.Attempts != attempts || st.LockedBy.Valid || st.LeaseExpiresAt.Valid {
		t.Errorf("asset %d = %+v, want %s on attempt %d without locked_by or lease_expires_at",
			id, st, status, attempts)
	}
	return st
}

// until returns how far column, a DATETIME(6) of asset id, lies after NOW(6).
func (s *stack) until(t *testing.T, id uint64, column string) time.Duration {
	t.Helper()
	var us sql.NullInt64
	err := s.pool.QueryRowContext(t.Context(),
		"SELECT TIMESTAMPDIFF(MICROSECOND, NOW(6), "+column+") FROM assets WHERE id = ?", id).Scan(&us)
	if err != nil || !us.Valid {
		t.Fatalf("error reading %s of asset %d: %v (valid %t)", column, id, err, us.Valid)
	}
	return time.Duration(us.Int64) * time.Microsecond
}

// snapshot returns every column of every row the asset owns, to tell whether a write changed any of them.
func (s *stack) snapshot(t *testing.T, id uint64) map[string][]map[string]any {
	t.Helper()
	return map[string][]map[string]any{
		"assets":          s.rows(t, "SELECT * FROM assets WHERE id = ?", id),
		"renditions":      s.rows(t, "SELECT * FROM renditions WHERE asset_id = ? ORDER BY id", id),
		"asset_events":    s.rows(t, "SELECT * FROM asset_events WHERE asset_id = ? ORDER BY id", id),
		"every event":     s.rows(t, "SELECT id FROM asset_events ORDER BY id"),
		"every rendition": s.rows(t, "SELECT id FROM renditions ORDER BY id"),
	}
}

func (s *stack) rows(t *testing.T, query string, args ...any) []map[string]any {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("error reading the columns of %q: %v", query, err)
	}
	var all []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("error reading %q: %v", query, err)
		}
		row := make(map[string]any, len(columns))
		for i, c := range columns {
			row[c] = values[i]
		}
		all = append(all, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading %q: %v", query, err)
	}
	return all
}

type storedEvent struct {
	EventID   db.ID
	TenantID  uint64
	AssetID   uint64
	Type      EventType
	CreatedAt time.Time
	RelayedAt sql.NullTime
}

func (s *stack) events(t *testing.T) []storedEvent {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(),
		"SELECT event_id, tenant_id, asset_id, type, created_at, relayed_at FROM asset_events ORDER BY id")
	if err != nil {
		t.Fatalf("error reading events: %v", err)
	}
	defer rows.Close()
	var events []storedEvent
	for rows.Next() {
		var e storedEvent
		if err := rows.Scan(&e.EventID, &e.TenantID, &e.AssetID, &e.Type, &e.CreatedAt, &e.RelayedAt); err != nil {
			t.Fatalf("error reading events: %v", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading events: %v", err)
	}
	return events
}

// wantEvents checks that the outbox holds one unrelayed event of type typ for each asset in assetIDs, in order,
// each with the tenant tenantIDs gives at the same index.
func (s *stack) wantEvents(t *testing.T, typ EventType, assetIDs, tenantIDs []uint64) []storedEvent {
	t.Helper()
	events := s.events(t)
	if len(events) != len(assetIDs) {
		t.Fatalf("events = %+v, want %d", events, len(assetIDs))
	}
	for i, e := range events {
		if e.Type != typ || e.AssetID != assetIDs[i] || e.TenantID != tenantIDs[i] {
			t.Errorf("event %d = %+v, want %s of asset %d and tenant %d", i, e, typ, assetIDs[i], tenantIDs[i])
		}
		if e.EventID[6]>>4 != 7 || e.CreatedAt.IsZero() || e.RelayedAt.Valid {
			t.Errorf("event %d = %+v, want a UUIDv7, a creation time and no relay time", i, e)
		}
	}
	return events
}

func (s *stack) wantNoEvents(t *testing.T) {
	t.Helper()
	if events := s.events(t); len(events) > 0 {
		t.Errorf("events = %+v, want none", events)
	}
}

func testProcessed() processed {
	return processed{
		SourceCodec:       "flac",
		DurationMS:        600_000,
		InputLUFS:         -23.45,
		InputTruePeakDBTP: 0,
		InputLRALU:        7.5,
		InputMaxMomentary: -1,
		Warnings:          []Warning{WarningSourceMayClip, WarningSuddenLoudEvent},
		Renditions:        []Rendition{testRendition(ProfileAAC128k), testRendition(ProfileAAC64k)},
	}
}

// withoutCreatedAt returns renditions with their creation times zeroed, after checking that each has one.
func withoutCreatedAt(t *testing.T, renditions []Rendition) []Rendition {
	t.Helper()
	out := slices.Clone(renditions)
	for i := range out {
		if out[i].CreatedAt.IsZero() {
			t.Errorf("rendition %s has no creation time", out[i].Profile)
		}
		out[i].CreatedAt = time.Time{}
	}
	return out
}

func TestClaim(t *testing.T) {
	t.Run("oldest due first", func(t *testing.T) {
		s := newLeaseStack(t)
		owner := s.tenant(t, "owner")
		other := s.tenant(t, "other")
		_, later := s.queue(t, owner)
		_, oldest := s.queue(t, other)
		_, future := s.queue(t, owner)
		s.exec(t, "UPDATE assets SET run_after = '2020-01-01 00:00:02' WHERE id = ?", later)
		s.exec(t, "UPDATE assets SET run_after = '2020-01-01 00:00:01', attempts = 1 WHERE id = ?", oldest)
		s.exec(t, "UPDATE assets SET run_after = NOW(6) + INTERVAL 1 HOUR WHERE id = ?", future)

		l, ok, err := s.repo.claim(t.Context(), "worker-1", testLease)

		if err != nil || !ok {
			t.Fatalf("claim() = %+v, %t, %v; want a lease", l, ok, err)
		}
		if want := (lease{ID: oldest, TenantID: other, Attempt: 2}); l != want {
			t.Errorf("claim() = %+v, want %+v", l, want)
		}
		st := s.state(t, oldest)
		if st.Status != "processing" || st.Attempts != 2 || st.LockedBy.String != "worker-1" {
			t.Errorf("claimed asset = %+v, want processing on attempt 2, locked by worker-1", st)
		}
		if d := s.until(t, oldest, "lease_expires_at"); d <= testLease-slack || d > testLease {
			t.Errorf("lease expires in %v, want about %v", d, testLease)
		}
		if next := s.claimNext(t); next != (lease{ID: later, TenantID: owner, Attempt: 1}) {
			t.Errorf("second claim() = %+v, want asset %d of tenant %d on attempt 1", next, later, owner)
		}
		s.wantNothingDue(t)
	})
	t.Run("nothing due", func(t *testing.T) {
		s := newLeaseStack(t)
		tenant := s.tenant(t, "owner")
		s.wantNothingDue(t)
		_, future := s.queue(t, tenant)
		s.exec(t, "UPDATE assets SET run_after = NOW(6) + INTERVAL 1 HOUR WHERE id = ?", future)
		s.wantNothingDue(t)
	})
}

func TestExtend(t *testing.T) {
	s := newLeaseStack(t)
	tenant := s.tenant(t, "owner")
	_, id := s.queue(t, tenant)
	l := s.claimNext(t)

	if err := s.repo.extend(t.Context(), l, 2*time.Hour); err != nil {
		t.Fatalf("extend() error = %v", err)
	}

	if st := s.state(t, id); st.Status != "processing" || st.Attempts != 1 {
		t.Errorf("extended asset = %+v, want processing on attempt 1", st)
	}
	if d := s.until(t, id, "lease_expires_at"); d <= 2*time.Hour-slack || d > 2*time.Hour {
		t.Errorf("lease expires in %v, want about 2h", d)
	}
}

func TestFinish(t *testing.T) {
	s := newLeaseStack(t)
	tenant := s.tenant(t, "owner")
	publicID, id := s.queue(t, tenant)
	l := s.claimNext(t)

	if err := s.repo.finish(t.Context(), l, testProcessed()); err != nil {
		t.Fatalf("finish() error = %v", err)
	}

	if st := s.wantUnleased(t, id, StatusReady, 1); st.Error.Valid {
		t.Errorf("error = %q, want NULL", st.Error.String)
	}
	got, err := s.repo.assetByID(t.Context(), tenant, publicID)
	if err != nil {
		t.Fatalf("assetByID() error = %v", err)
	}
	want := Asset{
		ID: publicID, TenantID: tenant, Title: "Rain", Status: StatusReady,
		SourceSHA256: sha256.Sum256([]byte("audio")), SourceBytes: 5, SourceCodec: "flac",
		DurationMS: new(uint64(600_000)), TargetLUFS: -16, InputLUFS: new(-23.45), InputTruePeakDBTP: new(0.0),
		InputLRALU: new(7.5), InputMaxMomentary: new(-1.0),
		Warnings:   []Warning{WarningSuddenLoudEvent, WarningSourceMayClip},
		Renditions: testProcessed().Renditions,
		CreatedAt:  got.CreatedAt, UpdatedAt: got.UpdatedAt,
	}
	got.Renditions = withoutCreatedAt(t, got.Renditions)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("finished asset = %+v\nwant  %+v", got, want)
	}
	s.wantEvents(t, EventReady, []uint64{id}, []uint64{tenant})
}

func TestFinishUpsertsRenditions(t *testing.T) {
	s := newLeaseStack(t)
	tenant := s.tenant(t, "owner")
	publicID, id := s.queue(t, tenant)
	stale := testRendition(ProfileAAC128k)
	stale.Bytes, stale.LUFS, stale.SHA256 = 1, -30, sha256.Sum256([]byte("stale"))
	s.addRendition(t, tenant, publicID, stale)
	before := s.rows(t, "SELECT id, created_at FROM renditions WHERE asset_id = ?", id)
	l := s.claimNext(t)

	if err := s.repo.finish(t.Context(), l, testProcessed()); err != nil {
		t.Fatalf("finish() error = %v", err)
	}

	after := s.rows(t, "SELECT id, created_at FROM renditions WHERE asset_id = ? AND profile = 'aac_128k'", id)
	if !reflect.DeepEqual(after, before) {
		t.Errorf("aac_128k row = %v, want %v updated in place", after, before)
	}
	if n := len(s.rows(t, "SELECT id FROM renditions WHERE asset_id = ?", id)); n != 2 {
		t.Errorf("%d renditions, want 2", n)
	}
	got, err := s.repo.assetByID(t.Context(), tenant, publicID)
	if err != nil {
		t.Fatalf("assetByID() error = %v", err)
	}
	if got := withoutCreatedAt(t, got.Renditions); !reflect.DeepEqual(got, testProcessed().Renditions) {
		t.Errorf("renditions = %+v\nwant  %+v", got, testProcessed().Renditions)
	}
}

// TestFinishRollsBack has a write inside the transaction fail: the asset keeps its lease and nothing is written.
func TestFinishRollsBack(t *testing.T) {
	tests := []struct {
		name string
		edit func(*processed)
		// wantDatabase means MySQL rejected a write, rather than finish refusing it.
		wantDatabase bool
	}{
		{"rendition rejected", func(p *processed) { p.Renditions[1].Profile = "aac_320k" }, true},
		{"measurement rejected", func(p *processed) { p.InputLUFS = 1000 }, true},
		{"no renditions", func(p *processed) { p.Renditions = nil }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newLeaseStack(t)
			tenant := s.tenant(t, "owner")
			_, id := s.queue(t, tenant)
			l := s.claimNext(t)
			before := s.snapshot(t, id)
			p := testProcessed()
			tc.edit(&p)

			err := s.repo.finish(t.Context(), l, p)

			if err == nil || errors.Is(err, errLeaseLost) || errors.Is(err, errDatabase) != tc.wantDatabase {
				t.Fatalf("finish() error = %v; want an error, a database error: %t", err, tc.wantDatabase)
			}
			if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
				t.Errorf("the driver's error is reachable through %v", err)
			}
			if after := s.snapshot(t, id); !reflect.DeepEqual(after, before) {
				t.Errorf("rows changed from %v\nto %v", before, after)
			}
			if st := s.state(t, id); st.Status != "processing" || st.Attempts != 1 {
				t.Errorf("asset = %+v, want it still processing on attempt 1", st)
			}
		})
	}
}

func TestFail(t *testing.T) {
	// Both are longer than their columns, and the byte at each column's size is inside a character.
	long := "x" + strings.Repeat("é", 600)
	longDetail := "\xff" + strings.Repeat("ö", 1100)
	tests := []struct {
		name string
		// attempts is the asset's count before the claim.
		attempts      int
		failure       failure
		wantFinal     bool
		wantError     string
		wantLastError string
	}{
		{
			name:          "retryable with attempts left",
			failure:       failure{Detail: "ffmpeg exited 1: /data/tmp/part-1", Delay: 30 * time.Second},
			wantLastError: "ffmpeg exited 1: /data/tmp/part-1",
		},
		{
			name:          "retryable on the attempt before the last",
			attempts:      maxAttempts - 2,
			failure:       failure{Reason: "only for rejections", Detail: "timeout", Delay: time.Minute},
			wantLastError: "timeout",
		},
		{
			name:          "permanent",
			failure:       failure{Permanent: true, Reason: "not audio", Detail: "ffprobe: /data/sources/7/x: invalid data"},
			wantFinal:     true,
			wantError:     "not audio",
			wantLastError: "ffprobe: /data/sources/7/x: invalid data",
		},
		{
			name:          "permanent without a reason",
			failure:       failure{Permanent: true, Detail: "panic: boom"},
			wantFinal:     true,
			wantError:     failedMessage,
			wantLastError: "panic: boom",
		},
		{
			name:          "last attempt",
			attempts:      maxAttempts - 1,
			failure:       failure{Reason: "only for rejections", Detail: "ffmpeg exited 255: /data/tmp/part-2", Delay: time.Hour},
			wantFinal:     true,
			wantError:     failedMessage,
			wantLastError: "ffmpeg exited 255: /data/tmp/part-2",
		},
		{
			name:          "long and multi-byte messages are clipped",
			failure:       failure{Permanent: true, Reason: long, Detail: longDetail},
			wantFinal:     true,
			wantError:     db.Clip(long, 1024),
			wantLastError: db.Clip(longDetail, 2048),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newLeaseStack(t)
			tenant := s.tenant(t, "owner")
			_, id := s.queue(t, tenant)
			s.exec(t, "UPDATE assets SET attempts = ? WHERE id = ?", tc.attempts, id)
			l := s.claimNext(t)

			final, err := s.repo.fail(t.Context(), l, tc.failure)

			if err != nil || final != tc.wantFinal {
				t.Fatalf("fail() = %t, %v; want %t", final, err, tc.wantFinal)
			}
			status := StatusQueued
			if tc.wantFinal {
				status = StatusFailed
			}
			st := s.wantUnleased(t, id, status, tc.attempts+1)
			if st.Error.Valid != tc.wantFinal || st.Error.String != tc.wantError {
				t.Errorf("error = %+v, want %q (set: %t)", st.Error, tc.wantError, tc.wantFinal)
			}
			if st.LastError.String != tc.wantLastError {
				t.Errorf("last_error = %q, want %q", st.LastError.String, tc.wantLastError)
			}
			if !tc.wantFinal {
				if d := s.until(t, id, "run_after"); d <= tc.failure.Delay-slack || d > tc.failure.Delay {
					t.Errorf("due in %v, want about %v", d, tc.failure.Delay)
				}
				s.wantNothingDue(t)
				s.wantNoEvents(t)
				return
			}
			s.wantEvents(t, EventFailed, []uint64{id}, []uint64{tenant})
		})
	}
}

func TestRelease(t *testing.T) {
	s := newLeaseStack(t)
	tenant := s.tenant(t, "owner")
	_, id := s.queue(t, tenant)
	l := s.claimNext(t)

	if err := s.repo.release(t.Context(), l); err != nil {
		t.Fatalf("release() error = %v", err)
	}

	if st := s.wantUnleased(t, id, StatusQueued, 0); st.Error.Valid || st.LastError.Valid {
		t.Errorf("released asset = %+v, want no error or last_error", st)
	}
	s.wantNoEvents(t)
	if again := s.claimNext(t); again != l {
		t.Errorf("claim after the release = %+v, want %+v", again, l)
	}
}

// TestLostLeaseWritesNothing has another worker take over, or the asset finish, before the holder writes.
func TestLostLeaseWritesNothing(t *testing.T) {
	stale := processed{SourceCodec: "mp3", DurationMS: 1, Renditions: []Rendition{testRendition(ProfileAAC64k)}}
	losses := []struct {
		name string
		lose func(t *testing.T, s *stack, id uint64, l lease)
	}{
		{"reaped", func(t *testing.T, s *stack, id uint64, _ lease) {
			s.expire(t, id)
			if requeued, failed, err := s.repo.reap(t.Context()); requeued != 1 || failed != 0 || err != nil {
				t.Fatalf("reap() = %d, %d, %v; want 1 requeued", requeued, failed, err)
			}
		}},
		{"claimed again", func(t *testing.T, s *stack, id uint64, _ lease) {
			s.exec(t, "UPDATE assets SET attempts = attempts + 1, locked_by = 'successor' WHERE id = ?", id)
		}},
		{"finished", func(t *testing.T, s *stack, _ uint64, l lease) {
			if err := s.repo.finish(t.Context(), l, testProcessed()); err != nil {
				t.Fatalf("finish() error = %v", err)
			}
		}},
	}
	ops := []struct {
		name string
		run  func(ctx context.Context, r *Repository, l lease) error
	}{
		{"extend", func(ctx context.Context, r *Repository, l lease) error { return r.extend(ctx, l, testLease) }},
		{"finish", func(ctx context.Context, r *Repository, l lease) error { return r.finish(ctx, l, stale) }},
		{"retry", func(ctx context.Context, r *Repository, l lease) error {
			_, err := r.fail(ctx, l, failure{Detail: "timeout", Delay: time.Second})
			return err
		}},
		{"fail", func(ctx context.Context, r *Repository, l lease) error {
			_, err := r.fail(ctx, l, failure{Permanent: true, Reason: "not audio", Detail: "stale"})
			return err
		}},
		{"release", func(ctx context.Context, r *Repository, l lease) error { return r.release(ctx, l) }},
	}
	for _, loss := range losses {
		for _, op := range ops {
			t.Run(op.name+" after it was "+loss.name, func(t *testing.T) {
				s := newLeaseStack(t)
				tenant := s.tenant(t, "owner")
				_, id := s.queue(t, tenant)
				l := s.claimNext(t)
				loss.lose(t, s, id, l)
				before := s.snapshot(t, id)

				err := op.run(t.Context(), s.repo, l)

				if !errors.Is(err, errLeaseLost) {
					t.Errorf("error = %v, want %v", err, errLeaseLost)
				}
				if errors.Is(err, db.ErrLeaseLost) || errors.Is(err, errDatabase) {
					t.Errorf("db's lost lease or a database error is reachable through %v", err)
				}
				if after := s.snapshot(t, id); !reflect.DeepEqual(after, before) {
					t.Errorf("rows changed from %v\nto %v", before, after)
				}
			})
		}
	}
}

func TestReap(t *testing.T) {
	t.Run("requeues an expired lease with attempts left", func(t *testing.T) {
		s := newLeaseStack(t)
		tenant := s.tenant(t, "owner")
		_, id := s.queue(t, tenant)
		s.claimNext(t)
		s.expire(t, id)

		requeued, failed, err := s.repo.reap(t.Context())

		if err != nil || requeued != 1 || failed != 0 {
			t.Fatalf("reap() = %d requeued, %d failed, %v; want 1, 0", requeued, failed, err)
		}
		st := s.wantUnleased(t, id, StatusQueued, 1)
		if st.Error.Valid || st.LastError.String != "lease expired" {
			t.Errorf("reaped asset = %+v, want no error and last_error %q", st, "lease expired")
		}
		if d := s.until(t, id, "run_after"); d <= 5*time.Second-slack || d > 5*time.Second {
			t.Errorf("due in %v, want about 5s", d)
		}
		s.wantNoEvents(t)
	})
	t.Run("fails an expired lease at the maximum attempts", func(t *testing.T) {
		s := newLeaseStack(t)
		tenant := s.tenant(t, "owner")
		_, id := s.queue(t, tenant)
		s.exec(t, "UPDATE assets SET attempts = ? WHERE id = ?", maxAttempts-1, id)
		s.claimNext(t)
		s.expire(t, id)

		requeued, failed, err := s.repo.reap(t.Context())

		if err != nil || requeued != 0 || failed != 1 {
			t.Fatalf("reap() = %d requeued, %d failed, %v; want 0, 1", requeued, failed, err)
		}
		st := s.wantUnleased(t, id, StatusFailed, maxAttempts)
		if st.Error.String != failedMessage || st.LastError.String != "lease expired" {
			t.Errorf("reaped asset = %+v, want error %q and last_error %q", st, failedMessage, "lease expired")
		}
		s.wantEvents(t, EventFailed, []uint64{id}, []uint64{tenant})
	})
	t.Run("both kinds in one batch", func(t *testing.T) {
		s := newLeaseStack(t)
		owner := s.tenant(t, "owner")
		other := s.tenant(t, "other")
		_, spare := s.queue(t, owner)
		_, dead := s.queue(t, other)
		_, live := s.queue(t, owner)
		s.exec(t, "UPDATE assets SET attempts = ? WHERE id = ?", maxAttempts-1, dead)
		for _, want := range []uint64{spare, dead, live} {
			if l := s.claimNext(t); l.ID != want {
				t.Fatalf("claimed asset %d, want %d", l.ID, want)
			}
		}
		s.expire(t, spare)
		s.expire(t, dead)

		requeued, failed, err := s.repo.reap(t.Context())

		if err != nil || requeued != 1 || failed != 1 {
			t.Fatalf("reap() = %d requeued, %d failed, %v; want 1, 1", requeued, failed, err)
		}
		if st := s.wantUnleased(t, spare, StatusQueued, 1); st.Error.Valid {
			t.Errorf("requeued asset has error %q", st.Error.String)
		}
		if st := s.wantUnleased(t, dead, StatusFailed, maxAttempts); st.Error.String != failedMessage {
			t.Errorf("failed asset has error %q, want %q", st.Error.String, failedMessage)
		}
		if st := s.state(t, live); st.Status != "processing" || !st.LeaseExpiresAt.Valid {
			t.Errorf("live asset = %+v, want it still leased", st)
		}
		s.wantEvents(t, EventFailed, []uint64{dead}, []uint64{other})
	})
}
