package tenant

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Repository holds the tenants table. Its methods are unexported, so other packages go through Service.
type Repository struct {
	db *gorm.DB
}

func NewRepository(gdb *gorm.DB) *Repository {
	return &Repository{db: gdb}
}

type tenantRow struct {
	ID               uint64    `gorm:"primaryKey"`
	Slug             string    `gorm:"column:slug"`
	APIKeyHash       []byte    `gorm:"column:api_key_hash"`
	WebhookURL       *string   `gorm:"column:webhook_url"`
	WebhookSecretEnc []byte    `gorm:"column:webhook_secret_enc"`
	TargetLUFS       float64   `gorm:"column:target_lufs"`
	CreatedAt        time.Time `gorm:"->"`
}

func (tenantRow) TableName() string { return "tenants" }

// publicColumns leaves out the API key's hash and the sealed webhook secret.
var publicColumns = []string{"id", "slug", "webhook_url", "target_lufs", "created_at"}

// insertTenant writes one tenant row and returns its ID. MySQL sets created_at.
// A nil webhookSecretEnc is stored as NULL. Only a taken slug is errDuplicate;
// any other duplicate key is a database error.
func (r *Repository) insertTenant(ctx context.Context, t Tenant, apiKeyHash [sha256.Size]byte,
	webhookSecretEnc []byte) (uint64, error) {
	row := tenantRow{
		Slug:             t.Slug,
		APIKeyHash:       apiKeyHash[:],
		WebhookURL:       nullString(t.WebhookURL),
		WebhookSecretEnc: webhookSecretEnc,
		TargetLUFS:       t.TargetLUFS,
	}
	err := r.db.WithContext(ctx).
		Select("slug", "api_key_hash", "webhook_url", "webhook_secret_enc", "target_lufs").
		Create(&row).Error
	if err == nil {
		return row.ID, nil
	}
	if !errors.Is(err, gorm.ErrDuplicatedKey) {
		return 0, translate("error inserting tenant", err)
	}
	// The translated error no longer names the key. Slugs are immutable and tenants are never deleted,
	// so the slug existing now means it was the slug that collided.
	taken, lookupErr := r.slugExists(ctx, t.Slug)
	switch {
	case lookupErr != nil:
		return 0, fmt.Errorf("error inserting tenant: duplicate key: %w", lookupErr)
	case taken:
		return 0, errDuplicate
	default:
		return 0, fmt.Errorf("error inserting tenant: %w: duplicate API key hash", errDatabase)
	}
}

func (r *Repository) slugExists(ctx context.Context, slug string) (bool, error) {
	var row tenantRow
	err := r.db.WithContext(ctx).Select("id").Where("slug = ?", slug).Take(&row).Error
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		return false, nil
	default:
		return false, translate("error looking up slug", err)
	}
}

func (r *Repository) tenantByID(ctx context.Context, id uint64) (Tenant, error) {
	return r.takeTenant(ctx, "id = ?", id)
}

func (r *Repository) tenantByAPIKeyHash(ctx context.Context, hash [sha256.Size]byte) (Tenant, error) {
	return r.takeTenant(ctx, "api_key_hash = ?", hash[:])
}

func (r *Repository) takeTenant(ctx context.Context, cond string, arg any) (Tenant, error) {
	var row tenantRow
	err := r.db.WithContext(ctx).Select(publicColumns).Where(cond, arg).Take(&row).Error
	if err != nil {
		return Tenant{}, translate("error reading tenant", err)
	}
	return newTenantFromRepo(row), nil
}

func (r *Repository) webhook(ctx context.Context, id uint64) (sealedWebhook, error) {
	var row tenantRow
	err := r.db.WithContext(ctx).
		Select("slug", "webhook_url", "webhook_secret_enc").
		Where("id = ?", id).
		Take(&row).Error
	if err != nil {
		return sealedWebhook{}, translate("error reading webhook", err)
	}
	return sealedWebhook{slug: row.Slug, url: stringOrEmpty(row.WebhookURL), sealed: row.WebhookSecretEnc}, nil
}

func translate(msg string, err error) error {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return ErrNotFound
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return errDuplicate
	default:
		return fmt.Errorf("%s: %w: %v", msg, errDatabase, err)
	}
}

func newTenantFromRepo(row tenantRow) Tenant {
	return Tenant{
		ID:         row.ID,
		Slug:       row.Slug,
		WebhookURL: stringOrEmpty(row.WebhookURL),
		TargetLUFS: row.TargetLUFS,
		CreatedAt:  row.CreatedAt,
	}
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
