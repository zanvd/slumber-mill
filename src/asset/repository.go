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
	"gorm.io/gorm/clause"

	"github.com/zanvd/slumber-mill/src/db"
)

const (
	// maxAttempts is how many claims an asset gets, whether its attempts fail or its leases expire.
	maxAttempts = 3
	// failedMessage is the error of an asset that failed for any reason but a rejection.
	failedMessage = "processing failed after repeated attempts"

	errorMax       = 1024
	sourceCodecMax = 32
)

// Repository holds the asset tables. Its methods are unexported, so other packages go through Service.
type Repository struct {
	db     *gorm.DB
	leases *db.Leases
}

func NewRepository(gdb *gorm.DB) *Repository {
	return &Repository{db: gdb, leases: db.NewAssetLeases(gdb)}
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

type eventRow struct {
	ID        uint64 `gorm:"primaryKey"`
	EventID   db.ID  `gorm:"column:event_id"`
	TenantID  uint64 `gorm:"column:tenant_id"`
	AssetID   uint64 `gorm:"column:asset_id"`
	Type      EventType
	CreatedAt time.Time `gorm:"->"`
}

func (eventRow) TableName() string { return "asset_events" }

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

// unrelayedEvents returns up to limit events not yet marked relayed, oldest first, each with its asset as it is now.
// Each asset is read under its event's tenant, so an event can't carry another tenant's asset or renditions.
func (r *Repository) unrelayedEvents(ctx context.Context, limit int) ([]Event, error) {
	var rows []eventRow
	err := r.db.WithContext(ctx).Where("relayed_at IS NULL").Order("id").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, translate("error reading asset events", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	keys := make([][]any, len(rows))
	for i, e := range rows {
		keys[i] = []any{e.AssetID, e.TenantID}
	}
	var assets []assetRow
	err = r.db.WithContext(ctx).
		Preload("Renditions", func(tx *gorm.DB) *gorm.DB {
			return tx.Where("(asset_id, tenant_id) IN ?", keys).Order("profile")
		}).
		Where("(id, tenant_id) IN ?", keys).
		Find(&assets).Error
	if err != nil {
		return nil, translate("error reading the assets of events", err)
	}
	byID := make(map[uint64]assetRow, len(assets))
	for _, a := range assets {
		byID[a.ID] = a
	}
	events := make([]Event, len(rows))
	for i, e := range rows {
		a, ok := byID[e.AssetID]
		if !ok || a.TenantID != e.TenantID {
			return nil, fmt.Errorf("error reading asset events: event %s has no asset %d of tenant %d",
				e.EventID, e.AssetID, e.TenantID)
		}
		events[i] = Event{
			ID:        uuid.UUID(e.EventID),
			TenantID:  e.TenantID,
			Type:      e.Type,
			CreatedAt: e.CreatedAt,
			Asset:     newAssetFromRepo(a),
		}
	}
	return events, nil
}

// markRelayed sets relayed_at on those of the events that don't have it yet.
func (r *Repository) markRelayed(ctx context.Context, ids []uuid.UUID) error {
	eventIDs := make([]db.ID, len(ids))
	for i, id := range ids {
		eventIDs[i] = db.ID(id)
	}
	err := r.db.WithContext(ctx).Table("asset_events").
		Where("event_id IN ? AND relayed_at IS NULL", eventIDs).
		Updates(map[string]any{"relayed_at": gorm.Expr("NOW(6)")}).Error
	if err != nil {
		return translate("error marking asset events relayed", err)
	}
	return nil
}

// claim leases the asset that has been due longest to workerID for d. It returns false when nothing is due.
func (r *Repository) claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error) {
	l, ok, err := r.leases.Claim(ctx, workerID, d)
	if err != nil {
		return lease{}, false, translateLease("error claiming asset", err)
	}
	return lease(l), ok, nil
}

// extend renews the lease to expire d from now.
func (r *Repository) extend(ctx context.Context, l lease, d time.Duration) error {
	if err := r.leases.Extend(ctx, db.Lease(l), d); err != nil {
		return translateLease("error extending asset lease", err)
	}
	return nil
}

// finish marks the asset ready in one transaction with its measurements, its renditions and an asset.ready event.
// Renditions the asset already has are replaced.
func (r *Repository) finish(ctx context.Context, l lease, p processed) error {
	if len(p.Renditions) == 0 {
		return fmt.Errorf("error finishing asset %d: no renditions", l.ID)
	}
	err := r.leases.Complete(ctx, db.Lease(l), func(tx *gorm.DB) error {
		if err := upsertRenditions(tx, l, p.Renditions); err != nil {
			return err
		}
		err := updateAssets(tx, []lease{l}, map[string]any{
			"source_codec":         nullString(db.Clip(p.SourceCodec, sourceCodecMax)),
			"duration_ms":          p.DurationMS,
			"input_lufs":           p.InputLUFS,
			"input_true_peak_dbtp": p.InputTruePeakDBTP,
			"input_lra_lu":         p.InputLRALU,
			"input_max_momentary":  p.InputMaxMomentary,
			"warnings":             warningSet(p.Warnings),
		})
		if err != nil {
			return err
		}
		return insertEvents(tx, EventReady, []lease{l})
	})
	if err != nil {
		return translateLease("error finishing asset", err)
	}
	return nil
}

// fail fails the asset if f is permanent or this is its last attempt, and otherwise requeues it to be due
// after f.Delay. A failed asset gets its tenant-safe error and an asset.failed event in the same transaction.
// It reports whether the asset failed.
func (r *Repository) fail(ctx context.Context, l lease, f failure) (final bool, err error) {
	message := failedMessage
	if f.Permanent && f.Reason != "" {
		message = f.Reason
	}
	final, err = r.leases.Fail(ctx, db.Lease(l), db.Failure{
		Permanent:   f.Permanent,
		MaxAttempts: maxAttempts,
		Delay:       f.Delay,
		Message:     f.Detail,
		Writes:      func(tx *gorm.DB) error { return failAssets(tx, []lease{l}, message) },
	})
	if err != nil {
		return false, translateLease("error failing asset", err)
	}
	return final, nil
}

// release requeues the asset and gives back its attempt.
func (r *Repository) release(ctx context.Context, l lease) error {
	if err := r.leases.Release(ctx, db.Lease(l)); err != nil {
		return translateLease("error releasing asset", err)
	}
	return nil
}

// reap takes back up to 100 assets whose lease expired. Those with attempts to spare are requeued after a short
// delay, and the rest fail as a last attempt does, all in one transaction.
func (r *Repository) reap(ctx context.Context) (requeued, failed int, err error) {
	requeued, failed, err = r.leases.Reap(ctx, maxAttempts, func(tx *gorm.DB, dead []db.Lease) error {
		leases := make([]lease, len(dead))
		for i, d := range dead {
			leases[i] = lease(d)
		}
		return failAssets(tx, leases, failedMessage)
	})
	if err != nil {
		return 0, 0, translateLease("error reaping assets", err)
	}
	return requeued, failed, nil
}

func upsertRenditions(tx *gorm.DB, l lease, renditions []Rendition) error {
	rows := make([]renditionRow, len(renditions))
	for i, rd := range renditions {
		rows[i] = renditionRow{
			TenantID:      l.TenantID,
			AssetID:       l.ID,
			Profile:       rd.Profile,
			Codec:         rd.Codec,
			BitrateKbps:   rd.BitrateKbps,
			Channels:      rd.Channels,
			Bytes:         rd.Bytes,
			SHA256:        rd.SHA256[:],
			LUFS:          rd.LUFS,
			TruePeakDBTP:  rd.TruePeakDBTP,
			LRALU:         rd.LRALU,
			Normalization: rd.Normalization,
		}
	}
	replaced := []string{"tenant_id", "codec", "bitrate_kbps", "channels", "bytes", "sha256",
		"lufs", "true_peak_dbtp", "lra_lu", "normalization"}
	return tx.Select(append([]string{"asset_id", "profile"}, replaced...)).
		Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns(replaced)}).
		Create(&rows).Error
}

// failAssets stores message as the failed assets' error, and records an asset.failed event for each.
func failAssets(tx *gorm.DB, leases []lease, message string) error {
	if err := updateAssets(tx, leases, map[string]any{"error": db.Clip(message, errorMax)}); err != nil {
		return err
	}
	return insertEvents(tx, EventFailed, leases)
}

// updateAssets sets columns on the leased assets, each matched on its ID and tenant, and fails unless it
// matches every one of them.
func updateAssets(tx *gorm.DB, leases []lease, set map[string]any) error {
	keys := make([][]any, len(leases))
	for i, l := range leases {
		keys[i] = []any{l.ID, l.TenantID}
	}
	res := tx.Table("assets").Where("(id, tenant_id) IN ?", keys).Updates(set)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != int64(len(leases)) {
		return fmt.Errorf("updated %d assets, want %d", res.RowsAffected, len(leases))
	}
	return nil
}

func insertEvents(tx *gorm.DB, t EventType, leases []lease) error {
	rows := make([]eventRow, len(leases))
	for i, l := range leases {
		rows[i] = eventRow{EventID: db.NewID(), TenantID: l.TenantID, AssetID: l.ID, Type: t}
	}
	return tx.Select("event_id", "tenant_id", "asset_id", "type").Create(&rows).Error
}

// translateLease turns db's lost lease into errLeaseLost, without wrapping it, and wraps any other error.
func translateLease(msg string, err error) error {
	if errors.Is(err, db.ErrLeaseLost) {
		return fmt.Errorf("%s: %w", msg, errLeaseLost)
	}
	return fmt.Errorf("%s: %w: %v", msg, errDatabase, err)
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
