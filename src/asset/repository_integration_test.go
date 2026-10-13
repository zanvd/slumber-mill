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
