package httpsrv

import (
	"bytes"
	"context"
	"io"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/tenant"
)

// fakeTenants knows one API key per tenant, and returns err for every key when it is set.
type fakeTenants struct {
	keys map[string]tenant.Tenant
	err  error
	ctxs []context.Context
	got  []string
}

func (f *fakeTenants) Authenticate(ctx context.Context, apiKey string) (tenant.Tenant, error) {
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
}

// fakeAssets returns its canned values, or err when it is set. panicOnce, when set, is raised by the next call,
// and onCall runs at the start of every call.
type fakeAssets struct {
	asset     asset.Asset
	page      asset.Page
	rendition asset.Rendition
	content   []byte
	err       error
	panicOnce any
	onCall    func()
	calls     []assetCall
	files     []*fakeFile
}

func (f *fakeAssets) record(c assetCall) error {
	f.calls = append(f.calls, c)
	if f.onCall != nil {
		f.onCall()
	}
	if p := f.panicOnce; p != nil {
		f.panicOnce = nil
		panic(p)
	}
	return f.err
}

func (f *fakeAssets) Get(ctx context.Context, tenantID uint64, id string) (asset.Asset, error) {
	if err := f.record(assetCall{method: "Get", ctx: ctx, tenantID: tenantID, id: id}); err != nil {
		return asset.Asset{}, err
	}
	return f.asset, nil
}

func (f *fakeAssets) List(ctx context.Context, tenantID uint64, limit int, cursor string) (asset.Page, error) {
	err := f.record(assetCall{method: "List", ctx: ctx, tenantID: tenantID, limit: limit, cursor: cursor})
	if err != nil {
		return asset.Page{}, err
	}
	return f.page, nil
}

func (f *fakeAssets) OpenRendition(ctx context.Context, tenantID uint64, id, profile string) (
	asset.Rendition, io.ReadSeekCloser, error) {
	err := f.record(assetCall{method: "OpenRendition", ctx: ctx, tenantID: tenantID, id: id, profile: profile})
	if err != nil {
		return asset.Rendition{}, nil, err
	}
	file := &fakeFile{Reader: bytes.NewReader(f.content)}
	f.files = append(f.files, file)
	return f.rendition, file, nil
}

type fakeFile struct {
	*bytes.Reader
	closed bool
}

func (f *fakeFile) Close() error {
	f.closed = true
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
