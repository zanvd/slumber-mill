package tenant

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strings"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/validation"
)

const (
	maxSlugBytes       = 64
	maxWebhookURLBytes = 2048
	minTargetLUFS      = -70
	maxTargetLUFS      = -5

	apiKeyPrefix   = "sm_"
	apiKeyBytes    = len(apiKeyPrefix) + 32
	apiKeyAlphabet = "abcdefghijklmnopqrstuvwxyz234567"
)

type repo interface {
	insertTenant(ctx context.Context, t Tenant, apiKeyHash [sha256.Size]byte, webhookSecretEnc []byte) (uint64, error)
	tenantByID(ctx context.Context, id uint64) (Tenant, error)
	tenantByAPIKeyHash(ctx context.Context, hash [sha256.Size]byte) (Tenant, error)
	webhook(ctx context.Context, id uint64) (sealedWebhook, error)
}

type box interface {
	Seal(v secret.Value, aad []byte) []byte
	Open(sealed, aad []byte) (secret.Value, error)
}

// hostChecker rejects a webhook host that doesn't resolve, or resolves to an address that isn't public.
type hostChecker interface {
	CheckHost(ctx context.Context, host string) error
}

type Service struct {
	repo  repo
	box   box
	hosts hostChecker
}

// NewService returns a service that seals and opens webhook secrets with b and checks webhook hosts with hosts.
// Either may be nil, as in the API, which holds no key ring: Create then returns ErrNoKeyRing or ErrNoHostChecker,
// and WebhookTarget ErrNoKeyRing.
func NewService(r repo, b box, hosts hostChecker) *Service {
	s := &Service{repo: r}
	if !isNil(b) {
		s.box = b
	}
	if !isNil(hosts) {
		s.hosts = hosts
	}
	return s
}

// isNil reports whether v is nil or holds a nil pointer, such as a nil *secret.Box.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

type CreateParams struct {
	Slug string
	// WebhookURL is empty for a tenant without a webhook.
	WebhookURL string
	// TargetLUFS is rounded to one decimal place, as stored.
	TargetLUFS float64
}

// Create registers a tenant with a new API key and, if it has a webhook URL, a new webhook secret.
// The secrets are returned only here; the database keeps the key's hash and the secret sealed to the slug.
func (s *Service) Create(ctx context.Context, p CreateParams) (Tenant, Credentials, error) {
	switch {
	case s.box == nil:
		return Tenant{}, Credentials{}, fmt.Errorf("error creating tenant: %w", ErrNoKeyRing)
	case s.hosts == nil:
		return Tenant{}, Credentials{}, fmt.Errorf("error creating tenant: %w", ErrNoHostChecker)
	}
	if err := s.validateCreate(ctx, p); err != nil {
		return Tenant{}, Credentials{}, err
	}

	creds := Credentials{APIKey: secret.NewAPIKey()}
	var sealed []byte
	if p.WebhookURL != "" {
		creds.WebhookSecret = secret.NewWebhookSecret()
		sealed = s.box.Seal(creds.WebhookSecret, webhookSecretAAD(p.Slug))
	}
	id, err := s.repo.insertTenant(ctx, Tenant{
		Slug:       p.Slug,
		WebhookURL: p.WebhookURL,
		TargetLUFS: math.Round(p.TargetLUFS*10) / 10,
	}, sha256.Sum256(creds.APIKey.Bytes()), sealed)
	if errors.Is(err, errDuplicate) {
		return Tenant{}, Credentials{}, ErrSlugTaken
	}
	if err != nil {
		return Tenant{}, Credentials{}, fmt.Errorf("error creating tenant: %w", err)
	}

	t, err := s.repo.tenantByID(ctx, id)
	if err != nil {
		// %v: the tenant was just inserted, so ErrNotFound here is an internal error.
		return Tenant{}, Credentials{}, fmt.Errorf("error reading created tenant %d: %v", id, err)
	}
	logging.LoggerFromContext(ctx).InfoContext(ctx, "tenant created",
		"tenant_id", t.ID, "tenant_slug", t.Slug, "webhook", t.WebhookURL != "")
	return t, creds, nil
}

// Authenticate returns the tenant whose API key this is, or ErrUnauthenticated.
func (s *Service) Authenticate(ctx context.Context, apiKey string) (Tenant, error) {
	if !wellFormedAPIKey(apiKey) {
		return Tenant{}, ErrUnauthenticated
	}
	t, err := s.repo.tenantByAPIKeyHash(ctx, sha256.Sum256([]byte(apiKey)))
	if errors.Is(err, ErrNotFound) {
		return Tenant{}, ErrUnauthenticated
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("error authenticating: %w", err)
	}
	return t, nil
}

// WebhookTarget returns where the tenant's events go and the opened secret they are signed with,
// ErrNoWebhook if the tenant has no webhook URL, or ErrNotFound if there is no such tenant.
func (s *Service) WebhookTarget(ctx context.Context, tenantID uint64) (Webhook, error) {
	if s.box == nil {
		return Webhook{}, fmt.Errorf("error reading webhook target: %w", ErrNoKeyRing)
	}
	w, err := s.repo.webhook(ctx, tenantID)
	if err != nil {
		return Webhook{}, fmt.Errorf("error reading webhook target: %w", err)
	}
	if w.url == "" {
		return Webhook{}, ErrNoWebhook
	}
	if w.sealed == nil {
		return Webhook{}, fmt.Errorf("error reading webhook target: tenant %d has a webhook URL but no secret", tenantID)
	}
	v, err := s.box.Open(w.sealed, webhookSecretAAD(w.slug))
	if err != nil {
		return Webhook{}, fmt.Errorf("error opening webhook secret of tenant %d: %w", tenantID, err)
	}
	return Webhook{URL: w.url, Secret: v}, nil
}

// webhookSecretAAD binds a sealed secret to its row: slugs are unique, immutable and can't contain a slash.
func webhookSecretAAD(slug string) []byte {
	return []byte("tenants/" + slug + "/webhook_secret")
}

// validateCreate checks the fields, and the webhook URL's host only once its syntax is valid.
// A cancelled context is returned as itself rather than as a rejected host.
func (s *Service) validateCreate(ctx context.Context, p CreateParams) error {
	var v validation.Collector
	switch {
	case p.Slug == "":
		v.Add("slug", "is required")
	case !slugChars(p.Slug):
		v.Add("slug", "must be lower-case letters, digits and hyphens, and start and end with a letter or digit")
	case len(p.Slug) > maxSlugBytes:
		v.Add("slug", "must be at most 64 characters")
	}
	if len(p.WebhookURL) > maxWebhookURLBytes {
		v.Add("webhook_url", "must be at most 2048 bytes")
	} else if p.WebhookURL != "" {
		u, err := url.Parse(p.WebhookURL)
		switch {
		case err != nil || !httpURL(u):
			v.Add("webhook_url", "must be an absolute http or https URL with a host")
		case u.User != nil:
			v.Add("webhook_url", "must not contain a user name or password")
		case strings.ContainsRune(p.WebhookURL, '#'):
			v.Add("webhook_url", "must not contain a fragment")
		default:
			if err := s.hosts.CheckHost(ctx, u.Hostname()); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return fmt.Errorf("error checking webhook host: %w", ctxErr)
				}
				logging.LoggerFromContext(ctx).InfoContext(ctx, "webhook host rejected", "error", err)
				v.Add("webhook_url", "host must resolve, and only to public addresses")
			}
		}
	}
	if math.IsNaN(p.TargetLUFS) || p.TargetLUFS < minTargetLUFS || p.TargetLUFS > maxTargetLUFS {
		v.Add("target_lufs", "must be between -70 and -5")
	}
	return v.Err()
}

func slugChars(s string) bool {
	for i := range len(s) {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9':
		case c == '-' && i > 0 && i < len(s)-1:
		default:
			return false
		}
	}
	return true
}

func httpURL(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// wellFormedAPIKey reports whether s looks like a key from secret.NewAPIKey, so garbage never reaches the database.
func wellFormedAPIKey(s string) bool {
	rest, ok := strings.CutPrefix(s, apiKeyPrefix)
	if !ok || len(s) != apiKeyBytes {
		return false
	}
	for i := range len(rest) {
		if strings.IndexByte(apiKeyAlphabet, rest[i]) < 0 {
			return false
		}
	}
	return true
}
