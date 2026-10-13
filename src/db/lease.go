package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	lockedByMax  = 128
	lastErrorMax = 2048
	reapBatch    = 100
	reapDelay    = 5 * time.Second
	reapMessage  = "lease expired"
)

// ErrLeaseLost means a fenced write matched no row: the lease was reaped, or the row has been claimed again since.
var ErrLeaseLost = errors.New("lease lost")

// Lease is a claimed row. Attempt counts this claim, and with ID and the leased state it fences every later write.
type Lease struct {
	ID, TenantID uint64
	Attempt      int
}

// Failure describes a failed attempt.
type Failure struct {
	// Permanent fails the row whatever its attempts.
	Permanent   bool
	MaxAttempts int
	// Delay is how long a retried row waits before it is due again.
	Delay time.Duration
	// Message is the internal detail stored in last_error.
	Message string
	// Writes, if set, runs in the transaction that fails the row, and not on a retry.
	Writes func(tx *gorm.DB) error
}

// Leases runs the lease protocol on one table of work.
// locked_by and lease_expires_at are set while a row is leased and cleared whenever it leaves that state.
type Leases struct {
	db *gorm.DB
	t  leasedTable
}

// leasedTable holds the names that the lease SQL interpolates, so they come only from the constructors below.
type leasedTable struct {
	name, column                 string
	queued, leased, done, failed string
}

type leaseRow struct {
	ID       uint64
	TenantID uint64
	Attempts int
}

func NewAssetLeases(gdb *gorm.DB) *Leases {
	return &Leases{db: gdb, t: leasedTable{
		name: "assets", column: "status",
		queued: "queued", leased: "processing", done: "ready", failed: "failed",
	}}
}

func NewWebhookDeliveryLeases(gdb *gorm.DB) *Leases {
	return &Leases{db: gdb, t: leasedTable{
		name: "webhook_deliveries", column: "state",
		queued: "queued", leased: "sending", done: "delivered", failed: "failed",
	}}
}

// Claim leases the row that has been due longest to workerID for lease. It returns false when nothing is due.
func (l *Leases) Claim(ctx context.Context, workerID string, lease time.Duration) (Lease, bool, error) {
	if lease <= 0 {
		return Lease{}, false, fmt.Errorf("error claiming from %s: lease %v is not positive", l.t.name, lease)
	}
	var row leaseRow
	err := l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Table(l.t.name).
			Select("id", "tenant_id", "attempts").
			Where(l.t.column+" = ? AND run_after <= NOW(6)", l.t.queued).
			Order("run_after, id").
			Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate, Options: clause.LockingOptionsSkipLocked}).
			Take(&row).Error
		if err != nil {
			return err
		}
		res := tx.Table(l.t.name).Where("id = ?", row.ID).Updates(map[string]any{
			l.t.column:         l.t.leased,
			"attempts":         gorm.Expr("attempts + 1"),
			"locked_by":        Clip(workerID, lockedByMax),
			"lease_expires_at": fromNow(lease),
		})
		if res.Error == nil && res.RowsAffected != 1 {
			return fmt.Errorf("claiming locked row %d updated %d rows", row.ID, res.RowsAffected)
		}
		return res.Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Lease{}, false, nil
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("error claiming from %s: %w", l.t.name, err)
	}
	return Lease{ID: row.ID, TenantID: row.TenantID, Attempt: row.Attempts + 1}, true, nil
}

// Extend renews the lease to expire lease from now.
func (l *Leases) Extend(ctx context.Context, held Lease, lease time.Duration) error {
	if lease <= 0 {
		return fmt.Errorf("error extending %s lease: lease %v is not positive", l.t.name, lease)
	}
	err := l.fenced(l.db.WithContext(ctx), held, map[string]any{"lease_expires_at": fromNow(lease)})
	if err != nil {
		return fmt.Errorf("error extending %s lease: %w", l.t.name, err)
	}
	return nil
}

// Complete marks the row done. If writes is set, it runs in the same transaction,
// and a lost lease or an error from writes rolls back both.
func (l *Leases) Complete(ctx context.Context, held Lease, writes func(tx *gorm.DB) error) error {
	if err := l.finish(ctx, held, l.leaving(l.t.done), writes); err != nil {
		return fmt.Errorf("error completing %s lease: %w", l.t.name, err)
	}
	return nil
}

// Fail fails the row when f is permanent or the lease's attempt is the last one, and otherwise requeues it
// to be due after f.Delay. It reports whether the row failed.
func (l *Leases) Fail(ctx context.Context, held Lease, f Failure) (final bool, err error) {
	if f.MaxAttempts <= 0 || f.Delay < 0 {
		return false, fmt.Errorf("error failing %s lease: invalid max attempts %d or delay %v",
			l.t.name, f.MaxAttempts, f.Delay)
	}
	final = f.Permanent || held.Attempt >= f.MaxAttempts
	if final {
		set := l.leaving(l.t.failed)
		set["last_error"] = Clip(f.Message, lastErrorMax)
		err = l.finish(ctx, held, set, f.Writes)
	} else {
		set := l.leaving(l.t.queued)
		set["last_error"] = Clip(f.Message, lastErrorMax)
		set["run_after"] = fromNow(f.Delay)
		err = l.fenced(l.db.WithContext(ctx), held, set)
	}
	if err != nil {
		return false, fmt.Errorf("error failing %s lease: %w", l.t.name, err)
	}
	return final, nil
}

// Release requeues the row and gives back its attempt, so it is due again at once.
func (l *Leases) Release(ctx context.Context, held Lease) error {
	set := l.leaving(l.t.queued)
	set["attempts"] = gorm.Expr("attempts - 1")
	if err := l.fenced(l.db.WithContext(ctx), held, set); err != nil {
		return fmt.Errorf("error releasing %s lease: %w", l.t.name, err)
	}
	return nil
}

// Reap takes back up to 100 rows whose lease has expired. The expired lease counts as an attempt:
// rows with attempts to spare are requeued after a short fixed delay, and the rest fail,
// with writes for them in the same transaction.
func (l *Leases) Reap(ctx context.Context, maxAttempts int,
	writes func(tx *gorm.DB, failed []Lease) error) (requeued, failed int, err error) {
	if maxAttempts <= 0 {
		return 0, 0, fmt.Errorf("error reaping %s: invalid max attempts %d", l.t.name, maxAttempts)
	}
	err = l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []leaseRow
		err := tx.Table(l.t.name).
			Select("id", "tenant_id", "attempts").
			Where(l.t.column+" = ? AND lease_expires_at < NOW(6)", l.t.leased).
			Order("lease_expires_at, id").
			Limit(reapBatch).
			Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate, Options: clause.LockingOptionsSkipLocked}).
			Find(&rows).Error
		if err != nil {
			return err
		}
		var retry, dead []uint64
		var deadLeases []Lease
		for _, r := range rows {
			if r.Attempts < maxAttempts {
				retry = append(retry, r.ID)
				continue
			}
			dead = append(dead, r.ID)
			deadLeases = append(deadLeases, Lease{ID: r.ID, TenantID: r.TenantID, Attempt: r.Attempts})
		}
		if len(retry) > 0 {
			set := l.leaving(l.t.queued)
			set["last_error"] = reapMessage
			set["run_after"] = fromNow(reapDelay)
			if err := tx.Table(l.t.name).Where("id IN ?", retry).Updates(set).Error; err != nil {
				return err
			}
		}
		if len(dead) > 0 {
			set := l.leaving(l.t.failed)
			set["last_error"] = reapMessage
			if err := tx.Table(l.t.name).Where("id IN ?", dead).Updates(set).Error; err != nil {
				return err
			}
			if writes != nil {
				if err := writes(tx, deadLeases); err != nil {
					return err
				}
			}
		}
		requeued, failed = len(retry), len(dead)
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, 0, fmt.Errorf("error reaping %s: %w", l.t.name, err)
	}
	return requeued, failed, nil
}

// leaving returns the assignments that move a row out of the leased state.
func (l *Leases) leaving(state string) map[string]any {
	return map[string]any{
		l.t.column:         state,
		"locked_by":        nil,
		"lease_expires_at": nil,
	}
}

// finish applies a fenced update and then writes in one transaction, or only the update when writes is nil.
func (l *Leases) finish(ctx context.Context, held Lease, set map[string]any, writes func(tx *gorm.DB) error) error {
	if writes == nil {
		return l.fenced(l.db.WithContext(ctx), held, set)
	}
	return l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := l.fenced(tx, held, set); err != nil {
			return err
		}
		return writes(tx)
	})
}

// fenced updates the row only while held is still its lease, and returns ErrLeaseLost when it isn't.
func (l *Leases) fenced(tx *gorm.DB, held Lease, set map[string]any) error {
	res := tx.Table(l.t.name).
		Where("id = ? AND "+l.t.column+" = ? AND attempts = ?", held.ID, l.t.leased, held.Attempt).
		Updates(set)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLeaseLost
	}
	return nil
}

func fromNow(d time.Duration) clause.Expr {
	return gorm.Expr("NOW(6) + INTERVAL ? MICROSECOND", d.Microseconds())
}
