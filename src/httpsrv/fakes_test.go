package httpsrv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/tenant"
)

// fakeTenants knows one API key per tenant, and returns err for every key when it is set.
type fakeTenants struct {
	mu   sync.Mutex
	keys map[string]tenant.Tenant
	err  error
	ctxs []context.Context
	got  []string
}

func (f *fakeTenants) Authenticate(ctx context.Context, apiKey string) (tenant.Tenant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ctxs = append(f.ctxs, ctx)
	f.got = append(f.got, apiKey)
	if f.err != nil {
		return tenant.Tenant{}, f.err
	}
	t, ok := f.keys[apiKey]
	if !ok {
		return tenant.Tenant{}, tenant.ErrUnauthenticated
	}
	return t, nil
}

type assetCall struct {
	method   string
	ctx      context.Context
	tenantID uint64
	id       string
	limit    int
	cursor   string
	profile  string
	// content is what Stage read.
	content string
	params  asset.CreateParams
}

// fakeAssets returns its canned values, or err when it is set; Stage fails with stageErr and Create with createErr.
// panicOnce, when set, is raised by the next call, and onCall runs at the start of every call, given the call.
type fakeAssets struct {
	asset     asset.Asset
	page      asset.Page
	rendition asset.Rendition
	content   []byte
	seekErr   error
	err       error
	stageErr  error
	createErr error
	replayed  bool
	// staged runs once Stage has read its part, with what it read.
	staged func(content string)
	// fileClosed runs when the handler closes a rendition it opened.
	fileClosed func()
	panicOnce  any
	onCall     func(c assetCall)

	mu    sync.Mutex
	calls []assetCall
	files []*fakeFile
}

func (f *fakeAssets) record(c assetCall) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	p := f.panicOnce
	f.panicOnce = nil
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall(c)
	}
	if p != nil {
		panic(p)
	}
}

func (f *fakeAssets) recorded() []assetCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// Stage reads r to its end, as the real one does, and fails with what reading returned or with stageErr.
func (f *fakeAssets) Stage(ctx context.Context, r io.Reader) (asset.Upload, error) {
	b, err := io.ReadAll(r)
	f.record(assetCall{method: "Stage", ctx: ctx, content: string(b)})
	if f.staged != nil {
		f.staged(string(b))
	}
	if err != nil {
		return asset.Upload{}, fmt.Errorf("error staging upload: %w", err)
	}
	return asset.Upload{}, f.stageErr
}

func (f *fakeAssets) Discard(ctx context.Context, _ asset.Upload) {
	f.record(assetCall{method: "Discard", ctx: ctx})
}

func (f *fakeAssets) Create(ctx context.Context, p asset.CreateParams) (asset.Asset, bool, error) {
	f.record(assetCall{method: "Create", ctx: ctx, params: p})
	if f.createErr != nil {
		return asset.Asset{}, false, f.createErr
	}
	return f.asset, f.replayed, nil
}

func (f *fakeAssets) Get(ctx context.Context, tenantID uint64, id string) (asset.Asset, error) {
	f.record(assetCall{method: "Get", ctx: ctx, tenantID: tenantID, id: id})
	if f.err != nil {
		return asset.Asset{}, f.err
	}
	return f.asset, nil
}

func (f *fakeAssets) List(ctx context.Context, tenantID uint64, limit int, cursor string) (asset.Page, error) {
	f.record(assetCall{method: "List", ctx: ctx, tenantID: tenantID, limit: limit, cursor: cursor})
	if f.err != nil {
		return asset.Page{}, f.err
	}
	return f.page, nil
}

func (f *fakeAssets) OpenRendition(ctx context.Context, tenantID uint64, id, profile string) (
	asset.Rendition, io.ReadSeekCloser, error) {
	f.record(assetCall{method: "OpenRendition", ctx: ctx, tenantID: tenantID, id: id, profile: profile})
	if f.err != nil {
		return asset.Rendition{}, nil, f.err
	}
	file := &fakeFile{Reader: bytes.NewReader(f.content), seekErr: f.seekErr, onClose: f.fileClosed}
	f.mu.Lock()
	f.files = append(f.files, file)
	f.mu.Unlock()
	return f.rendition, file, nil
}

type fakeFile struct {
	*bytes.Reader
	seekErr error
	onClose func()
	closed  bool
}

func (f *fakeFile) Seek(offset int64, whence int) (int64, error) {
	if f.seekErr != nil {
		return 0, f.seekErr
	}
	return f.Reader.Seek(offset, whence)
}

func (f *fakeFile) Close() error {
	f.closed = true
	if f.onClose != nil {
		f.onClose()
	}
	return nil
}

type fakePinger struct {
	err   error
	calls int
	ctx   context.Context
}

func (f *fakePinger) PingContext(ctx context.Context) error {
	f.calls++
	f.ctx = ctx
	return f.err
}
