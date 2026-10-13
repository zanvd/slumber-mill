package tenant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/validation"
)

var (
	_ repo = (*Repository)(nil)
	_ box  = (*secret.Box)(nil)
)

const (
	testURL       = "https://hooks.example.com/slumber"
	acmeAAD       = "tenants/acme/webhook_secret"
	hostRejection = "host must resolve, and only to public addresses"
)

// testCtx carries a logger that writes to the test's output.
func testCtx(t *testing.T) context.Context {
	return logging.CtxWithLogger(t.Context(), testutil.Logger(t))
}

type fixture struct {
	repo  *fakeRepo
	box   *fakeBox
	hosts *fakeHosts
	svc   *Service
}

func newFixture() *fixture {
	f := &fixture{repo: &fakeRepo{}, box: &fakeBox{}, hosts: &fakeHosts{}}
	f.svc = NewService(f.repo, f.box, f.hosts)
	return f
}

func (f *fixture) create(t *testing.T, p CreateParams) (Tenant, Credentials) {
	t.Helper()
	tn, creds, err := f.svc.Create(testCtx(t), p)
	if err != nil {
		t.Fatalf("Create(%+v) error = %v", p, err)
	}
	return tn, creds
}

func validParams() CreateParams {
	return CreateParams{Slug: "acme", WebhookURL: testURL, TargetLUFS: -16}
}

func fieldsOf(t *testing.T, err error) map[string]string {
	t.Helper()
	verr, ok := errors.AsType[*validation.Error](err)
	if !ok {
		t.Fatalf("error = %v, want a *validation.Error", err)
	}
	got := make(map[string]string)
	for _, f := range verr.Fields() {
		got[f.Name] = f.Message
	}
	return got
}

func TestCreateValidation(t *testing.T) {
	const (
		slugChars  = "must be lower-case letters, digits and hyphens, and start and end with a letter or digit"
		urlSyntax  = "must be an absolute http or https URL with a host"
		urlLength  = "must be at most 2048 bytes"
		urlASCII   = "must be printable ASCII without spaces"
		userinfo   = "must not contain a user name or password"
		fragment   = "must not contain a fragment"
		lufsRange  = "must be between -70 and -5"
		urlPrefix  = "https://example.com/"
		maxURLPath = 2048 - len(urlPrefix)
	)
	slug := func(s string) func(*CreateParams) { return func(p *CreateParams) { p.Slug = s } }
	webhookURL := func(s string) func(*CreateParams) { return func(p *CreateParams) { p.WebhookURL = s } }
	lufs := func(f float64) func(*CreateParams) { return func(p *CreateParams) { p.TargetLUFS = f } }

	tests := []struct {
		name   string
		modify func(*CreateParams)
		field  string
		want   string // empty when the params are valid
	}{
		{"slug of 1 byte", slug("a"), "", ""},
		{"slug of 64 bytes", slug(strings.Repeat("a", 64)), "", ""},
		{"slug of 65 bytes", slug(strings.Repeat("a", 65)), "slug", "must be at most 64 characters"},
		{"slug of digits", slug("0123"), "", ""},
		{"slug with inner hyphens", slug("a-b--c"), "", ""},
		{"empty slug", slug(""), "slug", "is required"},
		{"slug of a hyphen", slug("-"), "slug", slugChars},
		{"leading hyphen", slug("-acme"), "slug", slugChars},
		{"trailing hyphen", slug("acme-"), "slug", slugChars},
		{"leading space", slug(" acme"), "slug", slugChars},
		{"trailing space", slug("acme "), "slug", slugChars},
		{"trailing newline", slug("acme\n"), "slug", slugChars},
		{"leading upper case", slug("Acme"), "slug", slugChars},
		{"inner upper case", slug("acMe"), "slug", slugChars},
		{"inner underscore", slug("ac_me"), "slug", slugChars},
		{"inner space", slug("ac me"), "slug", slugChars},
		{"inner slash", slug("ac/me"), "slug", slugChars},
		{"inner dot", slug("ac.me"), "slug", slugChars},
		{"inner NUL", slug("ac\x00me"), "slug", slugChars},
		{"non-ASCII", slug("acmé"), "slug", slugChars},
		{"too long with a bad character", slug(strings.Repeat("A", 65)), "slug", slugChars},

		{"no webhook URL", webhookURL(""), "", ""},
		{"http URL", webhookURL("http://example.com"), "", ""},
		{"URL with port, path and query", webhookURL("https://example.com:8443/hooks?v=1"), "", ""},
		{"IPv6 literal", webhookURL("https://[2001:db8::1]/hooks"), "", ""},
		{"upper-case scheme", webhookURL("HTTPS://example.com/hooks"), "", ""},
		{"URL of 2048 bytes", webhookURL(urlPrefix + strings.Repeat("a", maxURLPath)), "", ""},
		{"URL of 2049 bytes", webhookURL(urlPrefix + strings.Repeat("a", maxURLPath+1)), "webhook_url", urlLength},
		{"ftp scheme", webhookURL("ftp://example.com/hooks"), "webhook_url", urlSyntax},
		{"file scheme", webhookURL("file:///etc/passwd"), "webhook_url", urlSyntax},
		{"javascript scheme", webhookURL("javascript:alert(1)"), "webhook_url", urlSyntax},
		{"mailto scheme", webhookURL("mailto:ops@example.com"), "webhook_url", urlSyntax},
		{"scheme-relative", webhookURL("//example.com/hooks"), "webhook_url", urlSyntax},
		{"relative path", webhookURL("/hooks"), "webhook_url", urlSyntax},
		{"no scheme", webhookURL("example.com/hooks"), "webhook_url", urlSyntax},
		{"opaque", webhookURL("http:example.com"), "webhook_url", urlSyntax},
		{"scheme only", webhookURL("https://"), "webhook_url", urlSyntax},
		{"empty host", webhookURL("https:///hooks"), "webhook_url", urlSyntax},
		{"port without host", webhookURL("https://:443/hooks"), "webhook_url", urlSyntax},
		{"percent-encoded space", webhookURL("https://example.com/a%20b?q=%09"), "", ""},
		{"percent-encoded non-ASCII", webhookURL("https://example.com/caf%C3%A9"), "", ""},
		{"first and last printable bytes", webhookURL("https://example.com/!~"), "", ""},
		{"space in host", webhookURL("https://exa mple.com/"), "webhook_url", urlASCII},
		{"space in path", webhookURL("https://example.com/a b"), "webhook_url", urlASCII},
		{"space in query", webhookURL("https://example.com/hooks?q=a b"), "webhook_url", urlASCII},
		{"leading space", webhookURL(" https://example.com/"), "webhook_url", urlASCII},
		{"trailing space", webhookURL("https://example.com/hooks "), "webhook_url", urlASCII},
		{"tab", webhookURL("https://example.com/a\tb"), "webhook_url", urlASCII},
		{"trailing newline", webhookURL("https://example.com/hooks\n"), "webhook_url", urlASCII},
		{"NUL", webhookURL("https://example.com/\x00"), "webhook_url", urlASCII},
		{"DEL", webhookURL("https://example.com/\x7f"), "webhook_url", urlASCII},
		{"non-ASCII host", webhookURL("https://exämple.com/hooks"), "webhook_url", urlASCII},
		{"non-ASCII path", webhookURL("https://example.com/café"), "webhook_url", urlASCII},
		{"invalid UTF-8 byte", webhookURL("https://example.com/\x80"), "webhook_url", urlASCII},
		{"too long and not ASCII", webhookURL(urlPrefix + strings.Repeat("é", maxURLPath)), "webhook_url", urlLength},
		{"user and password", webhookURL("https://user:pass@example.com/"), "webhook_url", userinfo},
		{"user", webhookURL("https://user@example.com/"), "webhook_url", userinfo},
		{"empty userinfo", webhookURL("https://@example.com/"), "webhook_url", userinfo},
		{"fragment", webhookURL("https://example.com/hooks#top"), "webhook_url", fragment},
		{"empty fragment", webhookURL("https://example.com/hooks#"), "webhook_url", fragment},

		{"lowest target", lufs(-70), "", ""},
		{"highest target", lufs(-5), "", ""},
		{"rounds onto the lowest target", lufs(-69.96), "", ""},
		{"rounds onto the highest target", lufs(-5.04), "", ""},
		{"below the lowest target", lufs(-70.1), "target_lufs", lufsRange},
		{"rounds onto the lowest target from below", lufs(-70.04), "target_lufs", lufsRange},
		{"above the highest target", lufs(-4.9), "target_lufs", lufsRange},
		{"zero target", lufs(0), "target_lufs", lufsRange},
		{"NaN target", lufs(math.NaN()), "target_lufs", lufsRange},
		{"infinite target", lufs(math.Inf(1)), "target_lufs", lufsRange},
		{"negative infinite target", lufs(math.Inf(-1)), "target_lufs", lufsRange},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			p := validParams()
			tc.modify(&p)
			_, creds, err := f.svc.Create(testCtx(t), p)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Create() error = %v", err)
				}
				return
			}
			got := fieldsOf(t, err)
			if len(got) != 1 || got[tc.field] != tc.want {
				t.Errorf("fields = %v, want %s: %s", got, tc.field, tc.want)
			}
			if creds.APIKey.Bytes() != nil || creds.WebhookSecret.Bytes() != nil {
				t.Error("Create returned credentials with a validation error")
			}
			if len(f.repo.calls) != 0 || len(f.box.sealAADs) != 0 {
				t.Errorf("repo calls = %v, seals = %d, want none", f.repo.calls, len(f.box.sealAADs))
			}
		})
	}
}

func TestCreateValidationReportsEveryField(t *testing.T) {
	f := newFixture()
	_, _, err := f.svc.Create(testCtx(t), CreateParams{Slug: "Acme", WebhookURL: "ftp://x", TargetLUFS: 0})
	verr, ok := errors.AsType[*validation.Error](err)
	if !ok {
		t.Fatalf("error = %v, want a *validation.Error", err)
	}
	var names []string
	for _, field := range verr.Fields() {
		names = append(names, field.Name)
	}
	if want := []string{"slug", "webhook_url", "target_lufs"}; !slices.Equal(names, want) {
		t.Errorf("fields = %v, want %v", names, want)
	}
}

func TestCreate(t *testing.T) {
	f := newFixture()
	tn, creds := f.create(t, validParams())

	want := Tenant{ID: 1, Slug: "acme", WebhookURL: testURL, TargetLUFS: -16, CreatedAt: fakeNow}
	if tn != want {
		t.Errorf("tenant = %+v, want %+v", tn, want)
	}
	if key := string(creds.APIKey.Bytes()); !wellFormedAPIKey(key) {
		t.Errorf("API key %q isn't well-formed", key)
	}
	ws := creds.WebhookSecret.Bytes()
	if !bytes.HasPrefix(ws, []byte("whsec_")) || len(ws) != 45 {
		t.Errorf("webhook secret %q isn't whsec_ and 39 characters", ws)
	}
	if !slices.Equal(f.repo.calls, []string{"insertTenant", "tenantByID"}) {
		t.Errorf("repo calls = %v, want insertTenant then tenantByID", f.repo.calls)
	}

	row := f.repo.rows[0]
	if want := sha256.Sum256(creds.APIKey.Bytes()); row.hash != want {
		t.Errorf("stored hash = %x, want sha256 of the API key %x", row.hash, want)
	}
	if !slices.Equal(f.box.sealAADs, []string{acmeAAD}) {
		t.Errorf("Seal AADs = %q, want [%q]", f.box.sealAADs, acmeAAD)
	}
	if !bytes.Equal(f.box.sealed[0], ws) {
		t.Error("Seal got a different secret from the one Create returned")
	}
	if bytes.Contains(row.sealed, ws) {
		t.Error("the stored value contains the plaintext secret")
	}
	if !slices.Equal(f.hosts.hosts, []string{"hooks.example.com"}) {
		t.Errorf("checked hosts = %q, want [hooks.example.com]", f.hosts.hosts)
	}
}

func TestCreateWithoutWebhook(t *testing.T) {
	f := newFixture()
	tn, creds := f.create(t, CreateParams{Slug: "acme", TargetLUFS: -16})
	if tn.WebhookURL != "" {
		t.Errorf("WebhookURL = %q, want empty", tn.WebhookURL)
	}
	if creds.WebhookSecret.Bytes() != nil {
		t.Error("Create returned a webhook secret without a webhook URL")
	}
	if creds.APIKey.Bytes() == nil {
		t.Error("Create returned no API key")
	}
	if f.repo.rows[0].sealed != nil {
		t.Errorf("stored sealed secret = %x, want nil", f.repo.rows[0].sealed)
	}
	if len(f.box.sealAADs) != 0 || len(f.hosts.hosts) != 0 {
		t.Errorf("seals = %q, host checks = %q, want none", f.box.sealAADs, f.hosts.hosts)
	}
}

func TestCreateRoundsTargetLUFS(t *testing.T) {
	tests := []struct {
		in, want float64
	}{
		{-16, -16},
		{-23.5, -23.5},
		{-14.25, -14.3},
		{-14.24, -14.2},
		{-16.04, -16},
		{-69.96, -70},
		{-5.04, -5},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.in), func(t *testing.T) {
			f := newFixture()
			tn, _ := f.create(t, CreateParams{Slug: "acme", TargetLUFS: tc.in})
			if got := f.repo.rows[0].tenant.TargetLUFS; got != tc.want {
				t.Errorf("stored TargetLUFS = %v, want %v", got, tc.want)
			}
			if tn.TargetLUFS != tc.want {
				t.Errorf("returned TargetLUFS = %v, want %v", tn.TargetLUFS, tc.want)
			}
		})
	}
}

func TestCreateChecksWebhookHost(t *testing.T) {
	tests := []struct {
		url, wantHost string
	}{
		{"https://hooks.example.com/slumber", "hooks.example.com"},
		{"http://Hooks.Example.com:8443/x?y=1", "Hooks.Example.com"},
		{"https://10.0.0.1/x", "10.0.0.1"},
		{"https://[2001:db8::1]:8443/x", "2001:db8::1"},
	}
	for _, tc := range tests {
		t.Run(tc.url, func(t *testing.T) {
			f := newFixture()
			f.create(t, CreateParams{Slug: "acme", WebhookURL: tc.url, TargetLUFS: -16})
			if !slices.Equal(f.hosts.hosts, []string{tc.wantHost}) {
				t.Errorf("checked hosts = %q, want [%q]", f.hosts.hosts, tc.wantHost)
			}
		})
	}
}

func TestCreateRejectsBlockedHost(t *testing.T) {
	f := newFixture()
	f.hosts.err = errors.New("10.0.0.1 is not a public address")
	_, _, err := f.svc.Create(testCtx(t), validParams())
	got := fieldsOf(t, err)
	if len(got) != 1 || got["webhook_url"] != hostRejection {
		t.Errorf("fields = %v, want webhook_url: %s", got, hostRejection)
	}
	if strings.Contains(err.Error(), "10.0.0.1") {
		t.Errorf("error %q quotes the checker's error", err)
	}
	if len(f.repo.calls) != 0 || len(f.box.sealAADs) != 0 {
		t.Errorf("repo calls = %v, seals = %d, want none", f.repo.calls, len(f.box.sealAADs))
	}
}

func TestCreateChecksHostAlongsideOtherFields(t *testing.T) {
	f := newFixture()
	f.hosts.err = errors.New("no such host")
	_, _, err := f.svc.Create(testCtx(t), CreateParams{Slug: "", WebhookURL: testURL, TargetLUFS: -16})
	got := fieldsOf(t, err)
	if got["slug"] != "is required" || got["webhook_url"] != hostRejection {
		t.Errorf("fields = %v, want slug and webhook_url", got)
	}
}

func TestCreateSkipsHostCheckForBadURL(t *testing.T) {
	for _, u := range []string{
		"",
		"ftp://example.com/",
		"/hooks",
		"https://user@example.com/",
		"https://example.com/#top",
		"https://example.com/" + strings.Repeat("a", 2048),
		"https://exämple.com/",
		"https://example.com/a b",
	} {
		t.Run(u, func(t *testing.T) {
			f := newFixture()
			f.svc.Create(testCtx(t), CreateParams{Slug: "acme", WebhookURL: u, TargetLUFS: -16})
			if len(f.hosts.hosts) != 0 {
				t.Errorf("checked hosts = %q, want none", f.hosts.hosts)
			}
		})
	}
}

func TestCreateHostCheckCancelled(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func(t *testing.T) (context.Context, func())
		hostErr error
		want    error
	}{
		{
			name: "cancelled before the check",
			ctx: func(t *testing.T) (context.Context, func()) {
				ctx, cancel := context.WithCancel(testCtx(t))
				cancel()
				return ctx, func() {}
			},
			want: context.Canceled,
		},
		{
			name: "cancelled during the check",
			ctx: func(t *testing.T) (context.Context, func()) {
				return context.WithCancel(testCtx(t))
			},
			want: context.Canceled,
		},
		{
			name: "cancelled during a check that fails",
			ctx: func(t *testing.T) (context.Context, func()) {
				return context.WithCancel(testCtx(t))
			},
			hostErr: errors.New("lookup hooks.example.com: operation was canceled"),
			want:    context.Canceled,
		},
		{
			name: "past its deadline",
			ctx: func(t *testing.T) (context.Context, func()) {
				ctx, cancel := context.WithDeadline(testCtx(t), fakeNow)
				return ctx, cancel
			},
			want: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			ctx, cancel := tc.ctx(t)
			defer cancel()
			f.hosts.during = cancel
			f.hosts.err = tc.hostErr
			_, _, err := f.svc.Create(ctx, validParams())
			if !errors.Is(err, tc.want) {
				t.Errorf("Create() error = %v, want %v", err, tc.want)
			}
			if _, ok := errors.AsType[*validation.Error](err); ok {
				t.Errorf("Create() error = %v, want no validation error", err)
			}
			if len(f.repo.calls) != 0 {
				t.Errorf("repo calls = %v, want none", f.repo.calls)
			}
		})
	}
}

func TestCreateSlugTaken(t *testing.T) {
	f := newFixture()
	f.create(t, validParams())
	_, creds, err := f.svc.Create(testCtx(t), validParams())
	if !errors.Is(err, ErrSlugTaken) {
		t.Fatalf("Create() error = %v, want ErrSlugTaken", err)
	}
	if creds.APIKey.Bytes() != nil || creds.WebhookSecret.Bytes() != nil {
		t.Error("Create returned credentials with ErrSlugTaken")
	}
}

func TestCreateRepoErrors(t *testing.T) {
	dbErr := fmt.Errorf("error inserting tenant: %w: boom", errDatabase)
	tests := []struct {
		name   string
		errs   map[string]error
		wantIs error
		notIs  []error
	}{
		{"insert fails", map[string]error{"insertTenant": dbErr}, errDatabase, []error{ErrSlugTaken}},
		{"re-read fails", map[string]error{"tenantByID": ErrNotFound}, nil, []error{ErrNotFound}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			f.repo.errs = tc.errs
			tn, creds, err := f.svc.Create(testCtx(t), validParams())
			if err == nil {
				t.Fatal("Create() error = nil")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("Create() error = %v, want %v", err, tc.wantIs)
			}
			for _, e := range tc.notIs {
				if errors.Is(err, e) {
					t.Errorf("Create() error = %v, which is %v", err, e)
				}
			}
			if tn != (Tenant{}) || creds.APIKey.Bytes() != nil {
				t.Error("Create returned a tenant or credentials with an error")
			}
		})
	}
}

func TestMissingDependencies(t *testing.T) {
	tests := []struct {
		name          string
		box           box
		hosts         hostChecker
		wantCreate    error
		wantWebhook   error
		webhookOpened bool
	}{
		{"nil box", nil, &fakeHosts{}, ErrNoKeyRing, ErrNoKeyRing, false},
		{"nil *secret.Box", (*secret.Box)(nil), &fakeHosts{}, ErrNoKeyRing, ErrNoKeyRing, false},
		{"nil host checker", &fakeBox{}, nil, ErrNoHostChecker, nil, true},
		{"nil *fakeHosts", &fakeBox{}, (*fakeHosts)(nil), ErrNoHostChecker, nil, true},
		{"neither", nil, nil, ErrNoKeyRing, ErrNoKeyRing, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Seed the repository through a complete service, as the CLI would.
			seed := newFixture()
			_, creds := seed.create(t, validParams())
			seed.repo.calls = nil
			svc := NewService(seed.repo, tc.box, tc.hosts)

			for _, p := range []CreateParams{validParams(), {Slug: "globex", TargetLUFS: -16}, {}} {
				if _, _, err := svc.Create(testCtx(t), p); !errors.Is(err, tc.wantCreate) {
					t.Errorf("Create(%+v) error = %v, want %v", p, err, tc.wantCreate)
				}
			}
			if _, err := svc.Authenticate(testCtx(t), string(creds.APIKey.Bytes())); err != nil {
				t.Errorf("Authenticate() error = %v", err)
			}
			w, err := svc.WebhookTarget(testCtx(t), 1)
			if !errors.Is(err, tc.wantWebhook) {
				t.Errorf("WebhookTarget() error = %v, want %v", err, tc.wantWebhook)
			}
			if tc.webhookOpened != (w.Secret.Bytes() != nil) {
				t.Errorf("WebhookTarget() opened a secret: %v, want %v", w.Secret.Bytes() != nil, tc.webhookOpened)
			}
			if slices.Contains(seed.repo.calls, "insertTenant") {
				t.Error("Create reached the repository")
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	f := newFixture()
	want, creds := f.create(t, validParams())
	key := string(creds.APIKey.Bytes())
	other := string(secret.NewAPIKey().Bytes())

	got, err := f.svc.Authenticate(testCtx(t), key)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if got != want {
		t.Errorf("Authenticate() = %+v, want %+v", got, want)
	}

	misses := []struct {
		name    string
		key     string
		queries bool
	}{
		{"another tenant's key format, unknown", other, true},
		{"empty", "", false},
		{"prefix only", "sm_", false},
		{"one character short", key[:len(key)-1], false},
		{"one character long", key + "a", false},
		{"upper case", strings.ToUpper(key), false},
		{"upper-case body", "sm_" + strings.ToUpper(key[3:]), false},
		{"character outside base32", key[:len(key)-1] + "1", false},
		{"leading space", " " + key[:len(key)-1], false},
		{"trailing newline", key[:len(key)-1] + "\n", false},
		{"bearer scheme", "Bearer " + key, false},
		{"webhook secret", string(creds.WebhookSecret.Bytes()), false},
		{"hash of the key", hex.EncodeToString(f.repo.rows[0].hash[:]), false},
	}
	for _, tc := range misses {
		t.Run(tc.name, func(t *testing.T) {
			f.repo.calls = nil
			got, err := f.svc.Authenticate(testCtx(t), tc.key)
			if !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("Authenticate() error = %v, want ErrUnauthenticated", err)
			}
			if got != (Tenant{}) {
				t.Errorf("Authenticate() = %+v, want the zero Tenant", got)
			}
			if queried := slices.Contains(f.repo.calls, "tenantByAPIKeyHash"); queried != tc.queries {
				t.Errorf("queried the repository: %v, want %v", queried, tc.queries)
			}
		})
	}
}

func TestAuthenticateDatabaseError(t *testing.T) {
	f := newFixture()
	f.repo.errs = map[string]error{"tenantByAPIKeyHash": fmt.Errorf("error reading tenant: %w: boom", errDatabase)}
	_, err := f.svc.Authenticate(testCtx(t), string(secret.NewAPIKey().Bytes()))
	if !errors.Is(err, errDatabase) || errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Authenticate() error = %v, want a database error that isn't ErrUnauthenticated", err)
	}
}

func TestWebhookTarget(t *testing.T) {
	f := newFixture()
	tn, creds := f.create(t, validParams())
	w, err := f.svc.WebhookTarget(testCtx(t), tn.ID)
	if err != nil {
		t.Fatalf("WebhookTarget() error = %v", err)
	}
	if w.URL != testURL {
		t.Errorf("URL = %q, want %q", w.URL, testURL)
	}
	if !bytes.Equal(w.Secret.Bytes(), creds.WebhookSecret.Bytes()) {
		t.Error("the opened secret differs from the one Create returned")
	}
	if !slices.Equal(f.box.openAADs, []string{acmeAAD}) {
		t.Errorf("Open AADs = %q, want [%q]", f.box.openAADs, acmeAAD)
	}
}

func TestWebhookTargetErrors(t *testing.T) {
	f := newFixture()
	acme, _ := f.create(t, validParams())
	plain, _ := f.create(t, CreateParams{Slug: "plain", TargetLUFS: -16})
	moved, _ := f.create(t, CreateParams{Slug: "globex", WebhookURL: testURL, TargetLUFS: -16})
	f.repo.rows[moved.ID-1].sealed = bytes.Clone(f.repo.rows[acme.ID-1].sealed)
	broken, _ := f.create(t, CreateParams{Slug: "broken", WebhookURL: testURL, TargetLUFS: -16})
	f.repo.rows[broken.ID-1].sealed = nil

	tests := []struct {
		name     string
		tenantID uint64
		errs     map[string]error
		want     error
	}{
		{"no webhook URL", plain.ID, nil, ErrNoWebhook},
		{"no such tenant", 99, nil, ErrNotFound},
		{"secret moved from another row", moved.ID, nil, errFakeOpen},
		{"URL without a secret", broken.ID, nil, nil},
		{"database error", acme.ID, map[string]error{"webhook": errDatabase}, errDatabase},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f.repo.errs = tc.errs
			defer func() { f.repo.errs = nil }()
			w, err := f.svc.WebhookTarget(testCtx(t), tc.tenantID)
			if err == nil {
				t.Fatal("WebhookTarget() error = nil")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("WebhookTarget() error = %v, want %v", err, tc.want)
			}
			if w.URL != "" || w.Secret.Bytes() != nil {
				t.Errorf("WebhookTarget() = %+v with an error", w)
			}
		})
	}

	f.box.openAADs = nil
	if _, err := f.svc.WebhookTarget(testCtx(t), moved.ID); err == nil || !slices.Equal(f.box.openAADs,
		[]string{"tenants/globex/webhook_secret"}) {
		t.Errorf("moved secret: error = %v, Open AADs = %q, want an error under globex's AAD", err, f.box.openAADs)
	}
}

// leakForms returns the ways a secret could show up in text output.
func leakForms(b []byte) []string {
	return []string{string(b), hex.EncodeToString(b), base64.StdEncoding.EncodeToString(b),
		base64.RawURLEncoding.EncodeToString(b), fmt.Sprintf("%v", b)}
}

func TestSecretsStayOutOfLogsAndErrors(t *testing.T) {
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(testCtx(t),
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	f := newFixture()
	var errs []error
	call := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	acme, creds, err := f.svc.Create(ctx, validParams())
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	_, _, err = f.svc.Create(ctx, validParams())
	call(err)
	f.hosts.err = errors.New("hooks.example.com resolves to 10.0.0.1")
	_, _, err = f.svc.Create(ctx, CreateParams{Slug: "blocked", WebhookURL: testURL, TargetLUFS: -16})
	call(err)
	f.hosts.err = nil
	globex, _, err := f.svc.Create(ctx, CreateParams{Slug: "globex", WebhookURL: testURL, TargetLUFS: -16})
	call(err)
	f.repo.rows[globex.ID-1].sealed = bytes.Clone(f.repo.rows[acme.ID-1].sealed)

	_, err = f.svc.Authenticate(ctx, string(creds.APIKey.Bytes()))
	call(err)
	_, err = f.svc.Authenticate(ctx, string(creds.APIKey.Bytes())+"x")
	call(err)
	w, err := f.svc.WebhookTarget(ctx, acme.ID)
	call(err)
	_, err = f.svc.WebhookTarget(ctx, globex.ID)
	call(err)
	// What a careless caller might log.
	logging.LoggerFromContext(ctx).InfoContext(ctx, "careless", "creds", creds, "webhook", w, "tenant", acme)

	if !strings.Contains(logs.String(), `"msg":"tenant created"`) {
		t.Fatalf("no tenant created line, so the check below proves nothing:\n%s", logs.String())
	}
	if len(errs) < 4 {
		t.Fatalf("got %d errors, want at least 4: %v", len(errs), errs)
	}
	out := logs.String()
	for _, e := range errs {
		out += "\n" + e.Error() + "\n" + fmt.Sprintf("%+v %#v", e, e)
	}
	out += fmt.Sprintf("\n%v %+v %#v %v %+v %#v", creds, creds, creds, w, w, w)
	for _, s := range [][]byte{creds.APIKey.Bytes(), creds.WebhookSecret.Bytes()} {
		for _, form := range leakForms(s) {
			if strings.Contains(out, form) {
				t.Fatalf("output contains %q:\n%s", form, out)
			}
		}
	}
}
