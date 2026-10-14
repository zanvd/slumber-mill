package asset

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"
)

// fakeNow stands in for MySQL's clock, so a test can tell a re-read asset from the one the service built.
var fakeNow = time.Date(2000, 1, 2, 3, 4, 5, 600000, time.UTC)

// calls records the calls the fakes receive, in order, so tests can check what ran before what.
type calls struct {
	list []string
}

func (c *calls) add(name string) {
	c.list = append(c.list, name)
}

// fakeRepo keeps assets in memory. An asset's internal ID is its index in assets plus one,
// and a key that is already in use in the tenant makes insertAsset fail as the unique index would.
// Like a database driver, every method fails once its context is done.
type fakeRepo struct {
	calls  *calls
	assets []Asset
	errs   map[string]error
	// beforeInsert runs inside insertAsset before it looks at its context,
	// as if another request got there first or the client hung up.
	beforeInsert func(f *fakeRepo)
	// errAfterInsert is returned by an insert that stored its row, as when the commit's reply is lost.
	errAfterInsert error
	insertCtx      context.Context
	listArgs       []listArgs
	// events is the outbox, oldest first; relayed holds the IDs markRelayed has marked.
	events      []Event
	relayed     map[uuid.UUID]bool
	eventLimits []int
	markedIDs   [][]uuid.UUID
}

func (f *fakeRepo) unrelayedEvents(ctx context.Context, limit int) ([]Event, error) {
	f.calls.add("unrelayedEvents")
	f.eventLimits = append(f.eventLimits, limit)
	if err := f.err(ctx, "unrelayedEvents"); err != nil {
		return nil, err
	}
	var events []Event
	for _, e := range f.events {
		if !f.relayed[e.ID] && len(events) < limit {
			events = append(events, e)
		}
	}
	return events, nil
}

func (f *fakeRepo) markRelayed(ctx context.Context, ids []uuid.UUID) error {
	f.calls.add("markRelayed")
	f.markedIDs = append(f.markedIDs, slices.Clone(ids))
	if err := f.err(ctx, "markRelayed"); err != nil {
		return err
	}
	if f.relayed == nil {
		f.relayed = make(map[uuid.UUID]bool)
	}
	for _, id := range ids {
		f.relayed[id] = true
	}
	return nil
}

func (f *fakeRepo) err(ctx context.Context, method string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.errs[method]
}

type listArgs struct {
	tenantID, before uint64
	limit            int
}

func (f *fakeRepo) insertAsset(ctx context.Context, a Asset) error {
	f.calls.add("insertAsset")
	f.insertCtx = ctx
	if f.beforeInsert != nil {
		f.beforeInsert(f)
	}
	if err := f.err(ctx, "insertAsset"); err != nil {
		return err
	}
	for _, existing := range f.assets {
		if existing.ID == a.ID || a.IdempotencyKey != "" &&
			existing.TenantID == a.TenantID && existing.IdempotencyKey == a.IdempotencyKey {
			return errDuplicate
		}
	}
	a.CreatedAt, a.UpdatedAt = fakeNow, fakeNow
	f.assets = append(f.assets, a)
	return f.errAfterInsert
}

func (f *fakeRepo) assetByID(ctx context.Context, tenantID uint64, id uuid.UUID) (Asset, error) {
	f.calls.add("assetByID")
	if err := f.err(ctx, "assetByID"); err != nil {
		return Asset{}, err
	}
	return f.find(func(a Asset) bool { return a.TenantID == tenantID && a.ID == id })
}

func (f *fakeRepo) assetByIdempotencyKey(ctx context.Context, tenantID uint64, key string) (Asset, error) {
	f.calls.add("assetByIdempotencyKey")
	if err := f.err(ctx, "assetByIdempotencyKey"); err != nil {
		return Asset{}, err
	}
	return f.find(func(a Asset) bool { return a.TenantID == tenantID && a.IdempotencyKey == key })
}

func (f *fakeRepo) find(match func(Asset) bool) (Asset, error) {
	i := slices.IndexFunc(f.assets, match)
	if i < 0 {
		return Asset{}, ErrNotFound
	}
	return f.assets[i], nil
}

func (f *fakeRepo) listAssets(ctx context.Context, tenantID, before uint64, limit int) ([]Asset, uint64, error) {
	f.calls.add("listAssets")
	f.listArgs = append(f.listArgs, listArgs{tenantID, before, limit})
	if err := f.err(ctx, "listAssets"); err != nil {
		return nil, 0, err
	}
	var page []Asset
	var last uint64
	for i := len(f.assets) - 1; i >= 0; i-- {
		id := uint64(i + 1)
		if f.assets[i].TenantID != tenantID || before > 0 && id >= before {
			continue
		}
		if len(page) == limit {
			return page, last, nil
		}
		page = append(page, f.assets[i])
		last = id
	}
	return page, 0, nil
}

func (f *fakeRepo) rendition(ctx context.Context, tenantID uint64, assetID uuid.UUID, profile Profile) (
	Rendition, error) {
	f.calls.add("rendition")
	if err := f.err(ctx, "rendition"); err != nil {
		return Rendition{}, err
	}
	a, err := f.find(func(a Asset) bool { return a.TenantID == tenantID && a.ID == assetID })
	if err != nil {
		return Rendition{}, err
	}
	i := slices.IndexFunc(a.Renditions, func(r Rendition) bool { return r.Profile == profile })
	if i < 0 {
		return Rendition{}, ErrNotFound
	}
	return a.Renditions[i], nil
}

// fakeFiles keeps files under a test's temp directory, in the same layout as storage.Local.
type fakeFiles struct {
	calls *calls
	root  string
	errs  map[string]error
}

func (f *fakeFiles) TempFile() (*os.File, error) {
	f.calls.add("TempFile")
	if err := f.errs["TempFile"]; err != nil {
		return nil, err
	}
	dir := filepath.Join(f.root, "tmp")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, "part-*")
}

func (f *fakeFiles) SourcePath(tenantID uint64, assetID uuid.UUID) string {
	return filepath.Join(f.root, "sources", strconv.FormatUint(tenantID, 10), assetID.String())
}

func (f *fakeFiles) RenditionPath(tenantID uint64, assetID uuid.UUID, profile string) (string, error) {
	return filepath.Join(f.root, "renditions", strconv.FormatUint(tenantID, 10), assetID.String(),
		profile+".m4a"), nil
}

func (f *fakeFiles) Promote(tmp, final string) error {
	f.calls.add("Promote")
	if err := f.errs["Promote"]; err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

func (f *fakeFiles) Open(path string) (io.ReadSeekCloser, error) {
	f.calls.add("Open")
	return os.Open(path)
}

// Remove records which directory under the root the file was in: Remove tmp, Remove sources.
func (f *fakeFiles) Remove(path string) error {
	rel, err := filepath.Rel(f.root, path)
	if err != nil {
		return err
	}
	dir, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
	f.calls.add("Remove " + dir)
	if err := f.errs["Remove"]; err != nil {
		return err
	}
	return os.Remove(path)
}

// filesUnder lists the regular files under root as slash-separated relative paths.
func filesUnder(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		files = append(files, filepath.ToSlash(rel))
		return err
	})
	if err != nil {
		t.Fatalf("error listing files: %v", err)
	}
	return files
}
