package webhook

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
	"uuid"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/zanvd/slumber-mill/src/db"
)

const (
	// maxAttempts is how many claims a delivery gets, whether its attempts fail or its leases expire.
	maxAttempts = 12
	// relayLockPrefix starts the name of the lock that elects the one relay.
	relayLockPrefix = "slumber-mill.relay."
	// lockQueryTimeout bounds each query on the relay lock's session.
	lockQueryTimeout = 5 * time.Second
)

// Repository holds the webhook_deliveries table and the relay lock. Its methods are unexported,
// so other packages go through Relay and Deliverer.
type Repository struct {
	db     *gorm.DB
	leases *db.Leases
}

func NewRepository(gdb *gorm.DB) *Repository {
	return &Repository{db: gdb, leases: db.NewWebhookDeliveryLeases(gdb)}
}

type deliveryRow struct {
	ID       uint64 `gorm:"primaryKey"`
	EventID  db.ID  `gorm:"column:event_id"`
	TenantID uint64 `gorm:"column:tenant_id"`
	Body     []byte `gorm:"column:body"`
}

func (deliveryRow) TableName() string { return "webhook_deliveries" }

// insertDeliveries queues a delivery for each event that has none yet. An event that already has one
// keeps it as it is, body included.
func (r *Repository) insertDeliveries(ctx context.Context, ds []delivery) error {
	if len(ds) == 0 {
		return nil
	}
	rows := make([]deliveryRow, len(ds))
	for i, d := range ds {
		rows[i] = deliveryRow{EventID: db.ID(d.EventID), TenantID: d.TenantID, Body: d.Body}
	}
	err := r.db.WithContext(ctx).
		Select("event_id", "tenant_id", "body").
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&rows).Error
	if err != nil {
		return translate("error inserting deliveries", err)
	}
	return nil
}

// claim leases the delivery that has been due longest to workerID for d. It returns false when nothing is due.
func (r *Repository) claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error) {
	l, ok, err := r.leases.Claim(ctx, workerID, d)
	if err != nil {
		return lease{}, false, translateLease("error claiming delivery", err)
	}
	return lease(l), ok, nil
}

// extend renews the lease to expire d from now.
func (r *Repository) extend(ctx context.Context, l lease, d time.Duration) error {
	if err := r.leases.Extend(ctx, db.Lease(l), d); err != nil {
		return translateLease("error extending delivery lease", err)
	}
	return nil
}

// leasedDelivery returns the leased delivery's event ID and body.
func (r *Repository) leasedDelivery(ctx context.Context, l lease) (delivery, error) {
	var row deliveryRow
	err := r.db.WithContext(ctx).
		Select("event_id", "tenant_id", "body").
		Where("id = ? AND tenant_id = ?", l.ID, l.TenantID).
		Take(&row).Error
	if err != nil {
		return delivery{}, translate("error reading leased delivery", err)
	}
	return delivery{EventID: uuid.UUID(row.EventID), TenantID: row.TenantID, Body: row.Body}, nil
}

// complete marks the delivery delivered.
func (r *Repository) complete(ctx context.Context, l lease) error {
	if err := r.leases.Complete(ctx, db.Lease(l), nil); err != nil {
		return translateLease("error completing delivery", err)
	}
	return nil
}

// fail fails the delivery if f is permanent or this is its last attempt, and otherwise requeues it to be due
// after f.Delay. It reports whether the delivery failed.
func (r *Repository) fail(ctx context.Context, l lease, f failure) (final bool, err error) {
	final, err = r.leases.Fail(ctx, db.Lease(l), db.Failure{
		Permanent:   f.Permanent,
		MaxAttempts: maxAttempts,
		Delay:       f.Delay,
		Message:     f.Detail,
	})
	if err != nil {
		return false, translateLease("error failing delivery", err)
	}
	return final, nil
}

// release requeues the delivery and gives back its attempt.
func (r *Repository) release(ctx context.Context, l lease) error {
	if err := r.leases.Release(ctx, db.Lease(l)); err != nil {
		return translateLease("error releasing delivery", err)
	}
	return nil
}

// reap takes back up to 100 deliveries whose lease expired. Those with attempts to spare are requeued after
// a short delay, and the rest fail.
func (r *Repository) reap(ctx context.Context) (requeued, failed int, err error) {
	requeued, failed, err = r.leases.Reap(ctx, maxAttempts, nil)
	if err != nil {
		return 0, 0, translateLease("error reaping deliveries", err)
	}
	return requeued, failed, nil
}

// relayLockName names the relay lock of a database. GET_LOCK names are shared by the whole MySQL server,
// so the database's name is part of it, hashed to keep the name within MySQL's limit of 64 characters.
func relayLockName(database string) string {
	sum := sha256.Sum256([]byte(database))
	return relayLockPrefix + hex.EncodeToString(sum[:16])
}

// lockRelay tries once, without waiting, to take the relay lock of the connected database on a session of its own,
// which holds the lock until it is released or the session ends. It returns false when another session holds it.
func (r *Repository) lockRelay(ctx context.Context) (relayLock, bool, error) {
	pool, err := r.db.DB()
	if err != nil {
		return nil, false, fmt.Errorf("error taking the relay lock: %w: %v", errDatabase, err)
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("error taking the relay lock: %w: %v", errDatabase, err)
	}
	var database sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		discard(conn)
		return nil, false, fmt.Errorf("error taking the relay lock: %w: %v", errDatabase, err)
	}
	if !database.Valid {
		discard(conn)
		return nil, false, fmt.Errorf("error taking the relay lock: %w: no database selected", errDatabase)
	}
	name := relayLockName(database.String)
	var got sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got)
	switch {
	case err != nil:
		// MySQL may have granted the lock before the call failed, so the session can't go back to the pool.
		discard(conn)
		return nil, false, fmt.Errorf("error taking the relay lock: %w: %v", errDatabase, err)
	case !got.Valid:
		discard(conn)
		return nil, false, fmt.Errorf("error taking the relay lock: %w: GET_LOCK returned NULL", errDatabase)
	case got.Int64 != 1:
		_ = conn.Close()
		return nil, false, nil
	}
	return &sessionLock{conn: conn, name: name}, true, nil
}

// sessionLock is the relay lock called name, held by the session of conn.
type sessionLock struct {
	conn *sql.Conn
	name string
}

// check returns errLockLost unless the session is alive and still holds the lock. A lost lock's session is ended.
func (l *sessionLock) check(ctx context.Context) error {
	ctx, cancel := lockContext(ctx)
	defer cancel()
	var mine sql.NullInt64
	err := l.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", l.name).Scan(&mine)
	if err != nil {
		discard(l.conn)
		return fmt.Errorf("error checking the relay lock: %w: %v", errLockLost, err)
	}
	if mine.Int64 != 1 {
		discard(l.conn)
		return fmt.Errorf("error checking the relay lock: %w: this session doesn't hold it", errLockLost)
	}
	return nil
}

// release gives up the lock and returns the session to the pool. If it can't, it ends the session instead,
// which frees the lock too: a pooled session must never keep it, or no other relay could take over.
func (l *sessionLock) release(ctx context.Context) error {
	ctx, cancel := lockContext(ctx)
	defer cancel()
	var released sql.NullInt64
	err := l.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&released)
	if err != nil {
		discard(l.conn)
		return fmt.Errorf("error releasing the relay lock: %w: %v", errDatabase, err)
	}
	if released.Int64 != 1 {
		discard(l.conn)
		return fmt.Errorf("error releasing the relay lock: %w: this session didn't hold it", errLockLost)
	}
	if err := l.conn.Close(); err != nil {
		return fmt.Errorf("error releasing the relay lock: %w: %v", errDatabase, err)
	}
	return nil
}

// lockContext keeps ctx's values but not its cancellation: the driver closes a connection whose query is
// cancelled, and that would end the lock's session on shutdown before RELEASE_LOCK could run.
func lockContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), lockQueryTimeout)
}

// discard ends conn's session rather than returning it to the pool:
// database/sql closes a connection that reports driver.ErrBadConn.
func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

// translateLease turns db's lost lease into errLeaseLost, without wrapping it, and wraps any other error.
func translateLease(msg string, err error) error {
	if errors.Is(err, db.ErrLeaseLost) {
		return fmt.Errorf("%s: %w", msg, errLeaseLost)
	}
	return fmt.Errorf("%s: %w: %v", msg, errDatabase, err)
}

func translate(msg string, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%s: %w", msg, errNotFound)
	}
	return fmt.Errorf("%s: %w: %v", msg, errDatabase, err)
}
