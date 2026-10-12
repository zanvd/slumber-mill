//go:build integration

package db_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
)

const (
	errDataTruncated        = 1265
	errConversionImpossible = 3988
)

type tenantRow struct {
	ID         uint64 `gorm:"primaryKey"`
	Slug       string
	APIKeyHash []byte `gorm:"column:api_key_hash"`
}

func (tenantRow) TableName() string { return "tenants" }

type assetRow struct {
	ID             uint64 `gorm:"primaryKey"`
	PublicID       db.ID  `gorm:"column:public_id"`
	TenantID       uint64 `gorm:"column:tenant_id"`
	IdempotencyKey *string
	Title          string
	Status         string
	SourceSHA256   []byte  `gorm:"column:source_sha256"`
	SourceBytes    uint64  `gorm:"column:source_bytes"`
	TargetLUFS     float64 `gorm:"column:target_lufs"`
}

func (assetRow) TableName() string { return "assets" }

func TestOpenSession(t *testing.T) {
	gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())

	var session struct {
		TimeZone string
		Now      time.Time
		IsUTC    bool
	}
	err := gdb.Raw("SELECT @@session.time_zone AS time_zone, NOW(6) AS now, NOW(6) = UTC_TIMESTAMP(6) AS is_utc").
		Scan(&session).Error
	if err != nil {
		t.Fatalf("error reading session settings: %v", err)
	}
	if session.TimeZone != "+00:00" {
		t.Errorf("time_zone = %q, want +00:00", session.TimeZone)
	}
	if session.Now.Location() != time.UTC || !session.IsUTC {
		t.Errorf("NOW(6) = %v (equal to UTC_TIMESTAMP: %t), want a UTC time", session.Now, session.IsUTC)
	}

	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("error getting the pool: %v", err)
	}
	if got := pool.Stats().MaxOpenConnections; got != 25 {
		t.Errorf("MaxOpenConnections = %d, want 25", got)
	}
}

func TestIDRoundTrip(t *testing.T) {
	gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())
	tenantID := createTenant(t, gdb, "ids")
	id := db.NewID()
	if err := gdb.Create(newAsset(tenantID, id, nil)).Error; err != nil {
		t.Fatalf("error creating asset: %v", err)
	}

	var stored string
	if err := gdb.Raw("SELECT LOWER(HEX(public_id)) FROM assets").Scan(&stored).Error; err != nil {
		t.Fatalf("error reading public_id: %v", err)
	}
	if want := strings.ReplaceAll(id.String(), "-", ""); stored != want {
		t.Errorf("stored public_id = %s, want %s", stored, want)
	}

	for _, s := range []string{id.String(), strings.ToUpper(id.String())} {
		t.Run(s, func(t *testing.T) {
			lookup, err := db.ParseID(s)
			if err != nil {
				t.Fatalf("ParseID(%q) error = %v", s, err)
			}
			var got assetRow
			if err := gdb.Where("public_id = ?", lookup).Take(&got).Error; err != nil {
				t.Fatalf("error finding asset by public_id: %v", err)
			}
			if got.PublicID != id {
				t.Errorf("PublicID = %s, want %s", got.PublicID, id)
			}
		})
	}
}

func TestClosedSetsRejectUnknownValues(t *testing.T) {
	gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())
	tenantID := createTenant(t, gdb, "enums")

	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{
			"assets.status",
			"INSERT INTO assets (public_id, tenant_id, title, status, source_sha256, source_bytes, target_lufs) " +
				"VALUES (?, ?, 't', 'paused', ?, 1, -16.0)",
			[]any{db.NewID(), tenantID, make([]byte, 32)},
		},
		{
			"assets.warnings",
			"INSERT INTO assets (public_id, tenant_id, title, warnings, source_sha256, source_bytes, target_lufs) " +
				"VALUES (?, ?, 't', 'too_quiet', ?, 1, -16.0)",
			[]any{db.NewID(), tenantID, make([]byte, 32)},
		},
		{
			"webhook_deliveries.state",
			"INSERT INTO webhook_deliveries (event_id, tenant_id, body, state) VALUES (?, ?, '{}', 'sent')",
			[]any{db.NewID(), tenantID},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := gdb.Exec(tc.sql, tc.args...).Error
			if !isMySQLError(err, errDataTruncated) {
				t.Errorf("insert error = %v, want MySQL error %d", err, errDataTruncated)
			}
		})
	}
}

func TestIdentifiersCompareByteForByte(t *testing.T) {
	tests := []struct {
		name          string
		first, second string
		wantSecondErr uint16
	}{
		{"case", "AbC", "abc", 0},
		{"accent", "resume-1", "résumé-1", errConversionImpossible},
		{"sharp s", "strasse", "straße", errConversionImpossible},
	}
	columns := []struct {
		name   string
		insert func(gdb *gorm.DB, tenantID uint64, value string) error
	}{
		{"tenants.slug", func(gdb *gorm.DB, _ uint64, value string) error {
			return gdb.Create(&tenantRow{Slug: value, APIKeyHash: randomHash()}).Error
		}},
		{"assets.idempotency_key", func(gdb *gorm.DB, tenantID uint64, value string) error {
			return gdb.Create(newAsset(tenantID, db.NewID(), &value)).Error
		}},
	}
	for _, col := range columns {
		for _, tc := range tests {
			t.Run(col.name+"/"+tc.name, func(t *testing.T) {
				gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())
				tenantID := createTenant(t, gdb, "owner")

				if err := col.insert(gdb, tenantID, tc.first); err != nil {
					t.Fatalf("inserting %q: %v", tc.first, err)
				}
				err := col.insert(gdb, tenantID, tc.second)
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					t.Fatalf("inserting %q after %q: duplicate key", tc.second, tc.first)
				}
				if tc.wantSecondErr == 0 && err != nil {
					t.Errorf("inserting %q after %q: %v", tc.second, tc.first, err)
				}
				if tc.wantSecondErr != 0 && !isMySQLError(err, tc.wantSecondErr) {
					t.Errorf("inserting %q: error = %v, want MySQL error %d", tc.second, err, tc.wantSecondErr)
				}
			})
		}
	}
}

func TestDuplicateKeys(t *testing.T) {
	key := "retry-1"
	id := db.NewID()

	tests := []struct {
		name          string
		first, second func(gdb *gorm.DB, tenantID uint64) error
		wantDuplicate bool
	}{
		{
			name: "tenants.slug",
			first: func(gdb *gorm.DB, _ uint64) error {
				return gdb.Create(&tenantRow{Slug: "dup", APIKeyHash: randomHash()}).Error
			},
			second: func(gdb *gorm.DB, _ uint64) error {
				return gdb.Create(&tenantRow{Slug: "dup", APIKeyHash: randomHash()}).Error
			},
			wantDuplicate: true,
		},
		{
			name:          "assets.public_id",
			first:         func(gdb *gorm.DB, tenantID uint64) error { return gdb.Create(newAsset(tenantID, id, nil)).Error },
			second:        func(gdb *gorm.DB, tenantID uint64) error { return gdb.Create(newAsset(tenantID, id, nil)).Error },
			wantDuplicate: true,
		},
		{
			name: "assets idempotency key in one tenant",
			first: func(gdb *gorm.DB, tenantID uint64) error {
				return gdb.Create(newAsset(tenantID, db.NewID(), &key)).Error
			},
			second: func(gdb *gorm.DB, tenantID uint64) error {
				return gdb.Create(newAsset(tenantID, db.NewID(), &key)).Error
			},
			wantDuplicate: true,
		},
		{
			name: "assets without idempotency keys",
			first: func(gdb *gorm.DB, tenantID uint64) error {
				return gdb.Create(newAsset(tenantID, db.NewID(), nil)).Error
			},
			second: func(gdb *gorm.DB, tenantID uint64) error {
				return gdb.Create(newAsset(tenantID, db.NewID(), nil)).Error
			},
		},
		{
			name: "assets idempotency key in two tenants",
			first: func(gdb *gorm.DB, tenantID uint64) error {
				return gdb.Create(newAsset(tenantID, db.NewID(), &key)).Error
			},
			second: func(gdb *gorm.DB, _ uint64) error {
				other := &tenantRow{Slug: "other", APIKeyHash: randomHash()}
				if err := gdb.Create(other).Error; err != nil {
					return err
				}
				return gdb.Create(newAsset(other.ID, db.NewID(), &key)).Error
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())
			tenantID := createTenant(t, gdb, "owner")
			if err := tc.first(gdb, tenantID); err != nil {
				t.Fatalf("first insert: %v", err)
			}
			err := tc.second(gdb, tenantID)
			if got := errors.Is(err, gorm.ErrDuplicatedKey); got != tc.wantDuplicate {
				t.Errorf("second insert error = %v, want duplicate %t", err, tc.wantDuplicate)
			}
			if !tc.wantDuplicate && err != nil {
				t.Errorf("second insert: %v", err)
			}
		})
	}
}

func TestRowsAffectedCountsMatchedRows(t *testing.T) {
	gdb := open(t, testutil.NewDatabase(t)).WithContext(t.Context())
	tenantID := createTenant(t, gdb, "fenced")

	tests := []struct {
		name     string
		id       uint64
		lufs     float64
		wantRows int64
	}{
		{"unchanged row", tenantID, -16.0, 1},
		{"changed row", tenantID, -18.0, 1},
		{"changed back", tenantID, -16.0, 1},
		{"no row", tenantID + 1000, -16.0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := gdb.Table("tenants").Where("id = ?", tc.id).Updates(map[string]any{"target_lufs": tc.lufs})
			if res.Error != nil {
				t.Fatalf("update error = %v", res.Error)
			}
			if res.RowsAffected != tc.wantRows {
				t.Errorf("RowsAffected = %d, want %d", res.RowsAffected, tc.wantRows)
			}
		})
	}
}

func createTenant(t *testing.T, gdb *gorm.DB, slug string) uint64 {
	t.Helper()
	row := &tenantRow{Slug: slug, APIKeyHash: randomHash()}
	if err := gdb.Create(row).Error; err != nil {
		t.Fatalf("error creating tenant %q: %v", slug, err)
	}
	return row.ID
}

func newAsset(tenantID uint64, id db.ID, key *string) *assetRow {
	return &assetRow{
		PublicID:       id,
		TenantID:       tenantID,
		IdempotencyKey: key,
		Title:          "Rain on a tin roof",
		Status:         "queued",
		SourceSHA256:   bytes.Repeat([]byte{0xab}, 32),
		SourceBytes:    1024,
		TargetLUFS:     -16.0,
	}
}

func randomHash() []byte {
	b := make([]byte, 32)
	rand.Read(b)
	return b
}

func isMySQLError(err error, number uint16) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == number
}
