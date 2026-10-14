package tenant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
)

// fakeNow stands in for MySQL's clock, so a test can tell a re-read tenant from the one the service built.
var fakeNow = time.Date(2000, 1, 2, 3, 4, 5, 600000, time.UTC)

type fakeRow struct {
	tenant Tenant
	hash   [sha256.Size]byte
	sealed []byte
}

// fakeRepo keeps tenants in memory. A tenant's ID is its index in rows plus one,
// and a slug already in use makes insertTenant fail as the unique index would.
type fakeRepo struct {
	rows  []fakeRow
	errs  map[string]error
	calls []string
	// idQueries are the ID lists webhookTenantIDs got.
	idQueries [][]uint64
}

func (f *fakeRepo) insertTenant(_ context.Context, t Tenant, hash [sha256.Size]byte, sealed []byte) (uint64, error) {
	f.calls = append(f.calls, "insertTenant")
	if err := f.errs["insertTenant"]; err != nil {
		return 0, err
	}
	for _, r := range f.rows {
		if r.tenant.Slug == t.Slug {
			return 0, errDuplicate
		}
	}
	t.ID = uint64(len(f.rows) + 1)
	t.CreatedAt = fakeNow
	f.rows = append(f.rows, fakeRow{tenant: t, hash: hash, sealed: bytes.Clone(sealed)})
	return t.ID, nil
}

func (f *fakeRepo) tenantByID(_ context.Context, id uint64) (Tenant, error) {
	f.calls = append(f.calls, "tenantByID")
	if err := f.errs["tenantByID"]; err != nil {
		return Tenant{}, err
	}
	r, err := f.row(id)
	return r.tenant, err
}

func (f *fakeRepo) tenantByAPIKeyHash(_ context.Context, hash [sha256.Size]byte) (Tenant, error) {
	f.calls = append(f.calls, "tenantByAPIKeyHash")
	if err := f.errs["tenantByAPIKeyHash"]; err != nil {
		return Tenant{}, err
	}
	for _, r := range f.rows {
		if r.hash == hash {
			return r.tenant, nil
		}
	}
	return Tenant{}, ErrNotFound
}

func (f *fakeRepo) webhook(_ context.Context, id uint64) (sealedWebhook, error) {
	f.calls = append(f.calls, "webhook")
	if err := f.errs["webhook"]; err != nil {
		return sealedWebhook{}, err
	}
	r, err := f.row(id)
	if err != nil {
		return sealedWebhook{}, err
	}
	return sealedWebhook{slug: r.tenant.Slug, url: r.tenant.WebhookURL, sealed: bytes.Clone(r.sealed)}, nil
}

func (f *fakeRepo) webhookTenantIDs(_ context.Context, ids []uint64) ([]uint64, error) {
	f.calls = append(f.calls, "webhookTenantIDs")
	f.idQueries = append(f.idQueries, slices.Clone(ids))
	if err := f.errs["webhookTenantIDs"]; err != nil {
		return nil, err
	}
	var found []uint64
	for _, r := range f.rows {
		if r.tenant.WebhookURL != "" && slices.Contains(ids, r.tenant.ID) {
			found = append(found, r.tenant.ID)
		}
	}
	return found, nil
}

func (f *fakeRepo) row(id uint64) (fakeRow, error) {
	if id == 0 || id > uint64(len(f.rows)) {
		return fakeRow{}, ErrNotFound
	}
	return f.rows[id-1], nil
}

var errFakeOpen = errors.New("fake box: message authentication failed")

// fakeBox "seals" by writing the AAD in front of the inverted plaintext, and opens only under the same AAD,
// so a sealed value moved to another slug fails to open as it would with AES-GCM.
type fakeBox struct {
	sealAADs []string
	sealed   [][]byte
	openAADs []string
}

func (b *fakeBox) Seal(v secret.Value, aad []byte) []byte {
	b.sealAADs = append(b.sealAADs, string(aad))
	b.sealed = append(b.sealed, v.Bytes())
	out := append(append([]byte(nil), aad...), 0)
	for _, c := range v.Bytes() {
		out = append(out, ^c)
	}
	return out
}

func (b *fakeBox) Open(sealed, aad []byte) (secret.Value, error) {
	b.openAADs = append(b.openAADs, string(aad))
	prefix := append(append([]byte(nil), aad...), 0)
	rest, ok := bytes.CutPrefix(sealed, prefix)
	if !ok {
		return secret.Value{}, errFakeOpen
	}
	plain := make([]byte, len(rest))
	for i, c := range rest {
		plain[i] = ^c
	}
	return secret.NewValue(plain), nil
}

// fakeHosts records the hosts it is asked about and rejects them with err, or with the context's error.
// during runs inside CheckHost, as if something happened while the host was being resolved.
type fakeHosts struct {
	hosts  []string
	err    error
	during func()
}

func (h *fakeHosts) CheckHost(ctx context.Context, host string) error {
	h.hosts = append(h.hosts, host)
	if h.during != nil {
		h.during()
	}
	if h.err != nil {
		return h.err
	}
	return ctx.Err()
}
