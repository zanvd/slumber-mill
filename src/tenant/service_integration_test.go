//go:build integration

package tenant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/validation"
)

// Test-only keys of fixed bytes. Key 1 seals in oldRing; newRing puts key 2 first and keeps key 1 for opening.
var (
	oldKey  = bytes.Repeat([]byte{0x00}, 32)
	newKey  = bytes.Repeat([]byte{0x02}, 32)
	oldRing = "1:" + base64.StdEncoding.EncodeToString(oldKey)
	newRing = "2:" + base64.StdEncoding.EncodeToString(newKey) + "," + oldRing
)

// allowHosts accepts every webhook host.
type allowHosts struct{}

func (allowHosts) CheckHost(context.Context, string) error { return nil }

type stack struct {
	repo *Repository
	svc  *Service
	pool *sql.DB
}

func newStack(t *testing.T, dsn, ring string) *stack {
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
	b, err := secret.NewBox(ring)
	if err != nil {
		t.Fatalf("NewBox() error = %v", err)
	}
	repo := NewRepository(gdb)
	return &stack{repo: repo, svc: NewService(repo, b, allowHosts{}), pool: pool}
}

func (s *stack) create(t *testing.T, p CreateParams) (Tenant, Credentials) {
	t.Helper()
	tn, creds, err := s.svc.Create(testCtx(t), p)
	if err != nil {
		t.Fatalf("Create(%+v) error = %v", p, err)
	}
	return tn, creds
}

type storedRow struct {
	hash       []byte
	webhookURL sql.NullString
	sealed     []byte
	targetLUFS string
	createdAt  time.Time
}

func (s *stack) row(t *testing.T, id uint64) storedRow {
	t.Helper()
	var r storedRow
	err := s.pool.QueryRowContext(t.Context(),
		"SELECT api_key_hash, webhook_url, webhook_secret_enc, CAST(target_lufs AS CHAR), created_at "+
			"FROM tenants WHERE id = ?", id).
		Scan(&r.hash, &r.webhookURL, &r.sealed, &r.targetLUFS, &r.createdAt)
	if err != nil {
		t.Fatalf("error reading tenant %d: %v", id, err)
	}
	return r
}

func (s *stack) now(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := s.pool.QueryRowContext(t.Context(), "SELECT NOW(6)").Scan(&now); err != nil {
		t.Fatalf("error reading MySQL's clock: %v", err)
	}
	return now
}

func TestCreateThenAuthenticate(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	created, creds := s.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})

	got, err := s.svc.Authenticate(testCtx(t), string(creds.APIKey.Bytes()))
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if !got.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, created.CreatedAt)
	}
	got.CreatedAt = created.CreatedAt
	if got != created {
		t.Errorf("Authenticate() = %+v, want %+v", got, created)
	}

	for name, key := range map[string]string{
		"wrong key": string(secret.NewAPIKey().Bytes()),
		"empty":     "",
		"malformed": "sm_" + strings.Repeat("!", 32),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.svc.Authenticate(testCtx(t), key); !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("Authenticate() error = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestCreateStoresHashAndCiphertext(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)

	t.Run("with a webhook", func(t *testing.T) {
		tn, creds := s.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
		r := s.row(t, tn.ID)
		if want := sha256.Sum256(creds.APIKey.Bytes()); !bytes.Equal(r.hash, want[:]) {
			t.Errorf("api_key_hash = %x, want sha256 of the API key %x", r.hash, want)
		}
		if !r.webhookURL.Valid || r.webhookURL.String != testURL {
			t.Errorf("webhook_url = %v, want %q", r.webhookURL, testURL)
		}
		if len(r.sealed) != 74 {
			t.Errorf("webhook_secret_enc is %d bytes, want 74", len(r.sealed))
		}
		if len(r.sealed) > 0 && r.sealed[0] != 1 {
			t.Errorf("sealed under key %d, want 1", r.sealed[0])
		}
		if bytes.Contains(r.sealed, creds.WebhookSecret.Bytes()) {
			t.Error("webhook_secret_enc contains the plaintext secret")
		}
	})

	t.Run("without a webhook", func(t *testing.T) {
		tn, creds := s.create(t, CreateParams{Slug: "plain", TargetLUFS: -16})
		if creds.WebhookSecret.Bytes() != nil {
			t.Error("Create returned a webhook secret without a webhook URL")
		}
		var nulls bool
		err := s.pool.QueryRowContext(t.Context(),
			"SELECT webhook_url IS NULL AND webhook_secret_enc IS NULL FROM tenants WHERE id = ?", tn.ID).Scan(&nulls)
		if err != nil {
			t.Fatalf("error reading tenant: %v", err)
		}
		if !nulls {
			t.Error("webhook_url and webhook_secret_enc aren't both NULL")
		}
	})
}

func TestCreateSlugTakenIntegration(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	s.create(t, CreateParams{Slug: "acme", TargetLUFS: -16})

	if _, _, err := s.svc.Create(testCtx(t), CreateParams{Slug: "acme", TargetLUFS: -20}); !errors.Is(err, ErrSlugTaken) {
		t.Errorf("Create(acme) again: error = %v, want ErrSlugTaken", err)
	}
	// ascii_bin would treat "acme " as "acme" (PAD SPACE); validation must stop both before the insert.
	for _, slug := range []string{"Acme", "ACME", "acme ", " acme"} {
		_, _, err := s.svc.Create(testCtx(t), CreateParams{Slug: slug, TargetLUFS: -16})
		if _, ok := errors.AsType[*validation.Error](err); !ok {
			t.Errorf("Create(%q) error = %v, want a *validation.Error", slug, err)
		}
	}
	var n int
	if err := s.pool.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM tenants").Scan(&n); err != nil {
		t.Fatalf("error counting tenants: %v", err)
	}
	if n != 1 {
		t.Errorf("got %d tenants, want 1", n)
	}
}

func TestCreateConcurrentSameSlug(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, _, errs[i] = s.svc.Create(testCtx(t), CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
		})
	}
	wg.Wait()
	var ok, taken int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrSlugTaken):
			taken++
		default:
			t.Errorf("Create() error = %v", err)
		}
	}
	if ok != 1 || taken != n-1 {
		t.Errorf("%d created and %d ErrSlugTaken, want 1 and %d", ok, taken, n-1)
	}
}

func TestInsertTenantDuplicateAPIKeyHash(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	hash := sha256.Sum256([]byte("sm_collision"))
	if _, err := s.repo.insertTenant(t.Context(), Tenant{Slug: "acme", TargetLUFS: -16}, hash, nil); err != nil {
		t.Fatalf("insertTenant() error = %v", err)
	}
	_, err := s.repo.insertTenant(t.Context(), Tenant{Slug: "globex", TargetLUFS: -16}, hash, nil)
	if !errors.Is(err, errDatabase) || errors.Is(err, errDuplicate) {
		t.Errorf("insertTenant() error = %v, want a database error that isn't errDuplicate", err)
	}
}

func TestWebhookTargetIntegration(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	acme, creds := s.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	plain, _ := s.create(t, CreateParams{Slug: "plain", TargetLUFS: -16})

	w, err := s.svc.WebhookTarget(testCtx(t), acme.ID)
	if err != nil {
		t.Fatalf("WebhookTarget() error = %v", err)
	}
	if w.URL != testURL {
		t.Errorf("URL = %q, want %q", w.URL, testURL)
	}
	if !bytes.Equal(w.Secret.Bytes(), creds.WebhookSecret.Bytes()) {
		t.Error("the opened secret differs from the one Create returned")
	}
	if _, err := s.svc.WebhookTarget(testCtx(t), plain.ID); !errors.Is(err, ErrNoWebhook) {
		t.Errorf("WebhookTarget(no URL) error = %v, want ErrNoWebhook", err)
	}
	if _, err := s.svc.WebhookTarget(testCtx(t), plain.ID+100); !errors.Is(err, ErrNotFound) {
		t.Errorf("WebhookTarget(missing) error = %v, want ErrNotFound", err)
	}
}

func TestWebhookTenantsIntegration(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	acme, _ := s.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	plain, _ := s.create(t, CreateParams{Slug: "plain", TargetLUFS: -16})
	globex, _ := s.create(t, CreateParams{Slug: "globex", WebhookURL: testURL, TargetLUFS: -16})
	missing := globex.ID + 100
	noKeyRing := NewService(s.repo, nil, nil)

	tests := []struct {
		name string
		ids  []uint64
		want map[uint64]bool
	}{
		{"with and without a URL", []uint64{acme.ID, plain.ID, globex.ID}, map[uint64]bool{acme.ID: true, globex.ID: true}},
		{"missing tenant", []uint64{missing, acme.ID}, map[uint64]bool{acme.ID: true}},
		{"only a tenant without a URL", []uint64{plain.ID}, map[uint64]bool{}},
		{"only a missing tenant", []uint64{missing}, map[uint64]bool{}},
		{"duplicates", []uint64{acme.ID, acme.ID, plain.ID, plain.ID}, map[uint64]bool{acme.ID: true}},
		{"empty list", []uint64{}, map[uint64]bool{}},
		{"nil list", nil, map[uint64]bool{}},
	}
	for _, tc := range tests {
		for name, svc := range map[string]*Service{"key ring": s.svc, "no key ring": noKeyRing} {
			t.Run(tc.name+", "+name, func(t *testing.T) {
				got, err := svc.WebhookTenants(testCtx(t), tc.ids)
				if err != nil {
					t.Fatalf("WebhookTenants() error = %v", err)
				}
				if got == nil || !maps.Equal(got, tc.want) {
					t.Errorf("WebhookTenants() = %v, want %v", got, tc.want)
				}
			})
		}
	}

	t.Run("database error", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(testCtx(t))
		cancel()
		got, err := noKeyRing.WebhookTenants(cancelled, []uint64{acme.ID})
		if !errors.Is(err, errDatabase) || got != nil {
			t.Fatalf("WebhookTenants() = %v, %v; want nil and %v", got, err, errDatabase)
		}
		if _, ok := errors.AsType[*mysql.MySQLError](err); ok || errors.Is(err, context.Canceled) {
			t.Errorf("the driver's or the context's error is reachable through %v", err)
		}
	})
}

// A sealed secret opens only in its own row, under any key in the ring (§13).
func TestSealedSecretOpensOnlyInItsOwnRow(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	a, aCreds := s.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	b, _ := s.create(t, CreateParams{Slug: "globex", WebhookURL: "https://globex.example.com/hooks", TargetLUFS: -16})

	aSealed := s.row(t, a.ID).sealed
	if _, err := s.pool.ExecContext(t.Context(),
		"UPDATE tenants SET webhook_secret_enc = ? WHERE id = ?", aSealed, b.ID); err != nil {
		t.Fatalf("error copying the sealed secret: %v", err)
	}

	_, err := s.svc.WebhookTarget(testCtx(t), b.ID)
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrNoWebhook) || errors.Is(err, errDatabase) {
		t.Fatalf("WebhookTarget() of another row's secret: error = %v, want a failed open", err)
	}
	for _, form := range append(leakForms(aSealed), leakForms(aSealed[1:])...) {
		if strings.Contains(err.Error(), form) {
			t.Errorf("error %q quotes the ciphertext as %q", err, form)
		}
	}
	for _, form := range append(leakForms(oldKey), leakForms(aCreds.WebhookSecret.Bytes())...) {
		if strings.Contains(err.Error(), form) {
			t.Errorf("error %q quotes the key or secret as %q", err, form)
		}
	}

	w, err := s.svc.WebhookTarget(testCtx(t), a.ID)
	if err != nil || !bytes.Equal(w.Secret.Bytes(), aCreds.WebhookSecret.Bytes()) {
		t.Errorf("WebhookTarget(own row) error = %v, or a different secret", err)
	}
}

func TestSealedSecretOpensUnderAnyKeyInTheRing(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	old := newStack(t, dsn, oldRing)
	rotated := newStack(t, dsn, newRing)

	a, aCreds := old.create(t, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	w, err := rotated.svc.WebhookTarget(testCtx(t), a.ID)
	if err != nil {
		t.Fatalf("WebhookTarget() under the new ring error = %v", err)
	}
	if !bytes.Equal(w.Secret.Bytes(), aCreds.WebhookSecret.Bytes()) {
		t.Error("the new ring opened a different secret")
	}

	b, bCreds := rotated.create(t, CreateParams{Slug: "globex", WebhookURL: testURL, TargetLUFS: -16})
	if id := rotated.row(t, b.ID).sealed[0]; id != 2 {
		t.Errorf("new create sealed under key %d, want 2", id)
	}
	w, err = rotated.svc.WebhookTarget(testCtx(t), b.ID)
	if err != nil || !bytes.Equal(w.Secret.Bytes(), bCreds.WebhookSecret.Bytes()) {
		t.Errorf("WebhookTarget() of the new create error = %v, or a different secret", err)
	}
	_, err = old.svc.WebhookTarget(testCtx(t), b.ID)
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrNoWebhook) || errors.Is(err, errDatabase) {
		t.Errorf("WebhookTarget() under a ring without key 2: error = %v, want a failed open", err)
	}
}

func TestCreatedAtComesFromMySQL(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	before := s.now(t)
	tn, _ := s.create(t, CreateParams{Slug: "acme", TargetLUFS: -16})
	after := s.now(t)

	stored := s.row(t, tn.ID).createdAt
	if !tn.CreatedAt.Equal(stored) {
		t.Errorf("CreatedAt = %v, want the stored %v", tn.CreatedAt, stored)
	}
	if tn.CreatedAt.Before(before) || tn.CreatedAt.After(after) {
		t.Errorf("CreatedAt = %v, want between MySQL's %v and %v", tn.CreatedAt, before, after)
	}
}

func TestTargetLUFSRoundTrip(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	tests := []struct {
		in, want float64
		stored   string
	}{
		{-16, -16, "-16.0"},
		{-23.5, -23.5, "-23.5"},
		{-70, -70, "-70.0"},
		{-5, -5, "-5.0"},
		{-14.25, -14.3, "-14.3"},
		{-14.24, -14.2, "-14.2"},
		{-69.96, -70, "-70.0"},
		{-5.04, -5, "-5.0"},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(tc.in), func(t *testing.T) {
			tn, creds := s.create(t, CreateParams{Slug: fmt.Sprintf("tenant-%d", i), TargetLUFS: tc.in})
			if tn.TargetLUFS != tc.want {
				t.Errorf("returned TargetLUFS = %v, want %v", tn.TargetLUFS, tc.want)
			}
			if got := s.row(t, tn.ID).targetLUFS; got != tc.stored {
				t.Errorf("stored target_lufs = %s, want %s", got, tc.stored)
			}
			authed, err := s.svc.Authenticate(testCtx(t), string(creds.APIKey.Bytes()))
			if err != nil || authed.TargetLUFS != tc.want {
				t.Errorf("Authenticate() TargetLUFS = %v, error = %v, want %v", authed.TargetLUFS, err, tc.want)
			}
		})
	}
}

// Secrets stay out of the service's logs and GORM's, including the failed insert of a duplicate.
func TestSecretsStayOutOfLogsIntegration(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t), oldRing)
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(),
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))

	a, creds, err := s.svc.Create(ctx, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	var errs []error
	_, _, err = s.svc.Create(ctx, CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16})
	errs = append(errs, err)
	b, _, err := s.svc.Create(ctx, CreateParams{Slug: "globex", WebhookURL: testURL, TargetLUFS: -16})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := s.pool.ExecContext(t.Context(), "UPDATE tenants SET webhook_secret_enc = ? WHERE id = ?",
		s.row(t, a.ID).sealed, b.ID); err != nil {
		t.Fatalf("error copying the sealed secret: %v", err)
	}
	_, err = s.svc.WebhookTarget(ctx, b.ID)
	errs = append(errs, err)
	if _, err := s.svc.Authenticate(ctx, string(creds.APIKey.Bytes())); err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if _, err := s.svc.WebhookTarget(ctx, a.ID); err != nil {
		t.Fatalf("WebhookTarget() error = %v", err)
	}

	for _, line := range []string{`"msg":"query failed"`, `"msg":"tenant created"`} {
		if !strings.Contains(logs.String(), line) {
			t.Fatalf("no %s line, so the check below proves less:\n%s", line, logs.String())
		}
	}
	out := logs.String()
	for _, e := range errs {
		if e == nil {
			t.Fatal("an expected error didn't happen")
		}
		out += "\n" + e.Error()
	}
	for _, v := range []secret.Value{creds.APIKey, creds.WebhookSecret} {
		for _, form := range leakForms(v.Bytes()) {
			if strings.Contains(out, form) {
				t.Fatalf("output contains %q:\n%s", form, out)
			}
		}
	}
}
