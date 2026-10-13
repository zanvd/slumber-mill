package asset

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
)

// Repository holds the asset tables. Its methods are unexported, so other packages go through Service.
type Repository struct {
	db *gorm.DB
}

func NewRepository(gdb *gorm.DB) *Repository {
	return &Repository{db: gdb}
}

type assetRow struct {
	ID                uint64 `gorm:"primaryKey"`
	PublicID          db.ID  `gorm:"column:public_id"`
	TenantID          uint64 `gorm:"column:tenant_id"`
	IdempotencyKey    *string
	Title             string
	Status            Status
	SourceSHA256      []byte   `gorm:"column:source_sha256"`
	SourceBytes       uint64   `gorm:"column:source_bytes"`
	SourceCodec       *string  `gorm:"column:source_codec"`
	DurationMS        *uint64  `gorm:"column:duration_ms"`
	TargetLUFS        float64  `gorm:"column:target_lufs"`
	InputLUFS         *float64 `gorm:"column:input_lufs"`
	InputTruePeakDBTP *float64 `gorm:"column:input_true_peak_dbtp"`
	InputLRALU        *float64 `gorm:"column:input_lra_lu"`
	InputMaxMomentary *float64 `gorm:"column:input_max_momentary"`
	Warnings          warningSet
	Error             *string
	CreatedAt         time.Time      `gorm:"->"`
	UpdatedAt         time.Time      `gorm:"->"`
	Renditions        []renditionRow `gorm:"foreignKey:AssetID"`
}

func (assetRow) TableName() string { return "assets" }

type renditionRow struct {
	ID            uint64 `gorm:"primaryKey"`
	TenantID      uint64 `gorm:"column:tenant_id"`
	AssetID       uint64 `gorm:"column:asset_id"`
	Profile       Profile
	Codec         Codec
	BitrateKbps   uint32 `gorm:"column:bitrate_kbps"`
	Channels      uint8
	Bytes         uint64
	SHA256        []byte  `gorm:"column:sha256"`
	LUFS          float64 `gorm:"column:lufs"`
	TruePeakDBTP  float64 `gorm:"column:true_peak_dbtp"`
	LRALU         float64 `gorm:"column:lra_lu"`
	Normalization Normalization
	CreatedAt     time.Time `gorm:"->"`
}

func (renditionRow) TableName() string { return "renditions" }

// warningSet maps the warnings SET, which MySQL reads and writes as comma-separated members.
type warningSet []Warning

func (w warningSet) Value() (driver.Value, error) {
	members := make([]string, len(w))
	for i, warning := range w {
		members[i] = string(warning)
	}
	return strings.Join(members, ","), nil
}

func (w *warningSet) Scan(src any) error {
	var s string
	switch v := src.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	default:
		return fmt.Errorf("error scanning warnings: unsupported type %T", src)
	}
	*w = nil
	if s == "" {
		return nil
	}
	for member := range strings.SplitSeq(s, ",") {
		*w = append(*w, Warning(member))
	}
	return nil
}

// insertAsset writes the columns an upload sets; MySQL fills in the rest, timestamps included.
func (r *Repository) insertAsset(ctx context.Context, a Asset) error {
	row := toRepo(a)
	err := r.db.WithContext(ctx).
		Select("public_id", "tenant_id", "idempotency_key", "title", "status",
			"source_sha256", "source_bytes", "target_lufs").
		Create(&row).Error
	switch {
	case err == nil:
		return nil
	case rejected(err):
		return translate("error inserting asset", err)
	default:
		return fmt.Errorf("error inserting asset: %w: %v", errOutcomeUnknown, err)
	}
}

// rejected reports whether MySQL itself refused a statement, which rolls back an autocommit write.
// GORM's dialector turns the duplicate-key and foreign-key errors into its own.
// Anything else, such as a deadline or a lost connection, may have come after the commit.
func rejected(err error) bool {
	_, fromServer := errors.AsType[*mysql.MySQLError](err)
	return fromServer || errors.Is(err, gorm.ErrDuplicatedKey) || errors.Is(err, gorm.ErrForeignKeyViolated)
}

func (r *Repository) assetByID(ctx context.Context, tenantID uint64, id uuid.UUID) (Asset, error) {
	return r.takeAsset(ctx, tenantID, "public_id = ?", db.ID(id))
}

func (r *Repository) assetByIdempotencyKey(ctx context.Context, tenantID uint64, key string) (Asset, error) {
	return r.takeAsset(ctx, tenantID, "idempotency_key = ?", key)
}

func (r *Repository) takeAsset(ctx context.Context, tenantID uint64, cond string, arg any) (Asset, error) {
	var row assetRow
	err := r.db.WithContext(ctx).
		Preload("Renditions", renditionsOf(tenantID)).
		Where("tenant_id = ?", tenantID).
		Where(cond, arg).
		Take(&row).Error
	if err != nil {
		return Asset{}, translate("error reading asset", err)
	}
	return newAssetFromRepo(row), nil
}

// listAssets returns up to limit of the tenant's assets, newest first, starting below the internal ID before,
// or from the newest when before is 0. next is the internal ID to continue below, or 0 on the last page.
func (r *Repository) listAssets(ctx context.Context, tenantID, before uint64, limit int) (
	assets []Asset, next uint64, err error) {
	q := r.db.WithContext(ctx).
		Preload("Renditions", renditionsOf(tenantID)).
		Where("tenant_id = ?", tenantID)
	if before > 0 {
		q = q.Where("id < ?", before)
	}
	var rows []assetRow
	if err := q.Order("id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, 0, translate("error listing assets", err)
	}
	if len(rows) > limit {
		rows = rows[:limit]
		next = rows[limit-1].ID
	}
	assets = make([]Asset, len(rows))
	for i, row := range rows {
		assets[i] = newAssetFromRepo(row)
	}
	return assets, next, nil
}

func (r *Repository) rendition(ctx context.Context, tenantID uint64, assetID uuid.UUID, profile Profile) (
	Rendition, error) {
	var row renditionRow
	err := r.db.WithContext(ctx).
		Joins("JOIN assets ON assets.id = renditions.asset_id").
		Where("assets.tenant_id = ? AND assets.public_id = ?", tenantID, db.ID(assetID)).
		Where("renditions.tenant_id = ? AND renditions.profile = ?", tenantID, string(profile)).
		Take(&row).Error
	if err != nil {
		return Rendition{}, translate("error reading rendition", err)
	}
	return newRenditionFromRepo(row), nil
}

func renditionsOf(tenantID uint64) func(*gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Where("tenant_id = ?", tenantID).Order("profile")
	}
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

func toRepo(a Asset) assetRow {
	return assetRow{
		PublicID:          db.ID(a.ID),
		TenantID:          a.TenantID,
		IdempotencyKey:    nullString(a.IdempotencyKey),
		Title:             a.Title,
		Status:            a.Status,
		SourceSHA256:      a.SourceSHA256[:],
		SourceBytes:       a.SourceBytes,
		SourceCodec:       nullString(a.SourceCodec),
		DurationMS:        a.DurationMS,
		TargetLUFS:        a.TargetLUFS,
		InputLUFS:         a.InputLUFS,
		InputTruePeakDBTP: a.InputTruePeakDBTP,
		InputLRALU:        a.InputLRALU,
		InputMaxMomentary: a.InputMaxMomentary,
		Warnings:          a.Warnings,
		Error:             nullString(a.Error),
	}
}

func newAssetFromRepo(row assetRow) Asset {
	a := Asset{
		ID:                uuid.UUID(row.PublicID),
		TenantID:          row.TenantID,
		IdempotencyKey:    stringOrEmpty(row.IdempotencyKey),
		Title:             row.Title,
		Status:            row.Status,
		SourceBytes:       row.SourceBytes,
		SourceCodec:       stringOrEmpty(row.SourceCodec),
		DurationMS:        row.DurationMS,
		TargetLUFS:        row.TargetLUFS,
		InputLUFS:         row.InputLUFS,
		InputTruePeakDBTP: row.InputTruePeakDBTP,
		InputLRALU:        row.InputLRALU,
		InputMaxMomentary: row.InputMaxMomentary,
		Warnings:          row.Warnings,
		Error:             stringOrEmpty(row.Error),
		CreatedAt:         row.CreatedAt,
		UpdatedAt:         row.UpdatedAt,
	}
	copy(a.SourceSHA256[:], row.SourceSHA256)
	if len(row.Renditions) > 0 {
		a.Renditions = make([]Rendition, len(row.Renditions))
		for i, rr := range row.Renditions {
			a.Renditions[i] = newRenditionFromRepo(rr)
		}
	}
	return a
}

func newRenditionFromRepo(row renditionRow) Rendition {
	r := Rendition{
		Profile:       row.Profile,
		Codec:         row.Codec,
		BitrateKbps:   row.BitrateKbps,
		Channels:      row.Channels,
		Bytes:         row.Bytes,
		LUFS:          row.LUFS,
		TruePeakDBTP:  row.TruePeakDBTP,
		LRALU:         row.LRALU,
		Normalization: row.Normalization,
		CreatedAt:     row.CreatedAt,
	}
	copy(r.SHA256[:], row.SHA256)
	return r
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
