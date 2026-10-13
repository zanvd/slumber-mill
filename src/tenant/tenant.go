package tenant

import (
	"errors"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
)

var (
	ErrNotFound        = errors.New("tenant not found")
	ErrSlugTaken       = errors.New("slug already taken")
	ErrUnauthenticated = errors.New("invalid API key")
	ErrNoWebhook       = errors.New("tenant has no webhook configured")
	ErrNoKeyRing       = errors.New("no secret key ring configured")
	ErrNoHostChecker   = errors.New("no webhook host checker configured")

	errDuplicate = errors.New("duplicate key")
	errDatabase  = errors.New("database error")
)

// Tenant is an account that uploads assets. It holds no secret material.
type Tenant struct {
	ID   uint64
	Slug string
	// WebhookURL is empty when the tenant has no webhook.
	WebhookURL string
	TargetLUFS float64
	CreatedAt  time.Time
}

// Credentials are a new tenant's secrets, the only time they exist in plaintext.
type Credentials struct {
	APIKey secret.Value
	// WebhookSecret is the zero Value when the tenant has no webhook URL.
	WebhookSecret secret.Value
}

// Webhook is where a tenant's events go and the secret they are signed with.
type Webhook struct {
	URL    string
	Secret secret.Value
}

// sealedWebhook is a tenant's webhook as stored: the secret is still sealed to the tenant's slug.
type sealedWebhook struct {
	slug   string
	url    string
	sealed []byte
}
