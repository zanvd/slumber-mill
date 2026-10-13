//go:build soak

// The lease soak runs db.Leases and worker.Pool together against MySQL. Uploaders insert assets throughout;
// encode and webhook pools, each with its reaper, work through them on leases short enough to expire constantly;
// and a relay turns events into deliveries. Then it stops everything, drains what's left and checks the invariants.
// It isn't in CI. Run it in the dev service for 15 s with
//
//	docker compose run --rm dev go test -race -tags soak -run TestLeaseSoak -count 1 -v ./worker/
//
// or for any other Go duration with
//
//	docker compose run --rm -e SLUMBER_MILL_SOAK_DURATION=1m dev \
//	  go test -race -tags soak -run TestLeaseSoak -count 1 -v ./worker/
package worker_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/worker"
)

const (
	soakDurationEnv     = "SLUMBER_MILL_SOAK_DURATION"
	defaultSoakDuration = 15 * time.Second

	uploaders    = 8
	uploadPause  = 60 * time.Millisecond // each uploader waits up to this long between uploads
	encodePools  = 4
	encodeSlots  = 8
	webhookPools = 4
	webhookSlots = 2
	lease        = 150 * time.Millisecond
	pollInterval = 50 * time.Millisecond
	grace        = 100 * time.Millisecond // shorter than some work, so shutdown releases it
	maxAttempts  = 3
	backoffBase  = 100 * time.Millisecond
	backoffMax   = time.Second

	relayBatch = 100
	relayPause = 100 * time.Millisecond
	relayCrash = 0.1 // share of relay batches that stop before marking their events relayed

	drainRounds  = 20
	drainTimeout = time.Minute

	lockWaitTimeout = 1 // seconds

	rejectedMessage = "The file isn't audio."
	failedMessage   = "Processing failed. Try uploading the file again."
)

var (
	errRetryable = errors.New("simulated transient failure")
	errRejected  = errors.New("simulated rejection")
	sourceSHA256 = bytes.Repeat([]byte{0xab}, 32)
)

func TestLeaseSoak(t *testing.T) {
	duration := soakDuration(t)
	errs := newErrorLog()
	log := slog.New(errs)
	dsn := withLockWaitTimeout(t, testutil.NewDatabase(t))
	gdb := open(t, dsn, log)
	tenantID := createTenant(t, gdb)

	assets := &leasedTable{
		name: "assets",
		work: work{
			took: 200 * time.Millisecond, stall: 0.03, retryable: 0.08, permanent: 0.02,
			poisoned: func(id uint64) bool { return id%64 == 0 },
		},
		onReady:   assetReady,
		onFailed:  assetFailed,
		onReaped:  assetsReaped,
		completed: map[uint64]int{},
		unsure:    map[uint64]bool{},
	}
	deliveries := &leasedTable{
		name:      "webhook_deliveries",
		work:      work{took: 20 * time.Millisecond, stall: 0.02, retryable: 0.1, permanent: 0.02},
		completed: map[uint64]int{},
		unsure:    map[uint64]bool{},
	}
	// Each table gets one connection pool: one per worker.Pool would take more connections than MySQL allows.
	assetLeases := db.NewAssetLeases(open(t, dsn, log))
	deliveryLeases := db.NewWebhookDeliveryLeases(open(t, dsn, log))
	var pools []*worker.Pool
	var reapers []*leaseSource
	for i := range encodePools {
		p, src := newPool(t, assets, assetLeases, "encode", i, encodeSlots, log)
		pools = append(pools, p)
		if i == 0 {
			reapers = append(reapers, src)
		}
	}
	for i := range webhookPools {
		p, src := newPool(t, deliveries, deliveryLeases, "webhook", i, webhookSlots, log)
		pools = append(pools, p)
		if i == 0 {
			reapers = append(reapers, src)
		}
	}
	r := &relay{gdb: gdb, log: log, seen: map[db.ID]bool{}}

	running, stop := context.WithCancel(t.Context())
	var stopped sync.WaitGroup
	for _, p := range pools {
		stopped.Go(func() { p.Run(running) })
	}
	stopped.Go(func() { r.run(running) })
	uploading, stopUploads := context.WithCancel(t.Context())
	var uploads atomic.Int64
	var uploaded sync.WaitGroup
	for range uploaders {
		uploaded.Go(func() { upload(uploading, gdb, tenantID, &uploads, log) })
	}

	pause(t.Context(), duration)
	stopUploads()
	uploaded.Wait()
	errs.stopping.Store(true)
	stop()
	stopped.Wait()
	errs.stopping.Store(false)

	if rows := violations(t, gdb, leasedQuery); len(rows) > 0 {
		t.Errorf("rows still leased after the pools shut down: %v", rows)
	}
	run := struct{ assets, deliveries counts }{assets.counts(), deliveries.counts()}
	relayed, repeats := r.relayed.Load(), r.repeats.Load()
	leftAssets := count(t, gdb, "SELECT COUNT(*) FROM assets WHERE status NOT IN ('ready', 'failed')")
	leftDeliveries := count(t, gdb, "SELECT COUNT(*) FROM webhook_deliveries WHERE state NOT IN ('delivered', 'failed')")
	drainErr := drain(t, gdb, pools, reapers, r, assets, deliveries)

	t.Logf("soak %v: uploads %d; assets: completions %d, stale completions fenced out %d, unknown outcome %d, "+
		"reaps %d, failures %d, retries %d, releases %d; deliveries: delivered %d, stale completions fenced out %d, "+
		"unknown outcome %d, reaps %d, failures %d, retries %d, releases %d; relayed %d events, repeating %d; "+
		"then drained %d assets and %d deliveries; logged errors: %v",
		duration, uploads.Load(),
		run.assets.completions, run.assets.stale, run.assets.unsure, run.assets.reaps, run.assets.failures,
		run.assets.retries, run.assets.releases,
		run.deliveries.completions, run.deliveries.stale, run.deliveries.unsure, run.deliveries.reaps,
		run.deliveries.failures, run.deliveries.retries, run.deliveries.releases,
		relayed, repeats, leftAssets, leftDeliveries, errs)
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	checkInvariants(t, gdb, uploads.Load(), assets, deliveries)
	for _, kind := range []errorKind{kindDeadlock, kindLockWait, kindUnexpected} {
		if n := errs.count(kind); n > 0 {
			t.Errorf("%d errors of kind %q, such as %q", n, kind, errs.examples(kind))
		}
	}
}

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	s := os.Getenv(soakDurationEnv)
	if s == "" {
		return defaultSoakDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		t.Fatalf("%s = %q, want a positive duration such as 1m", soakDurationEnv, s)
	}
	return d
}

// work is how the simulated work on a table behaves.
type work struct {
	took time.Duration // each attempt works for up to this long
	// Shares of attempts that stall, fail and are retried, or fail for good.
	stall, retryable, permanent float64
	// Poisoned rows stall on every attempt, like work that keeps killing its worker.
	poisoned func(id uint64) bool
}

// leasedTable is a table of leased work and the domain writes that go with its outcomes;
// its counts add up what every pool working on it did.
type leasedTable struct {
	name     string
	work     work
	onReady  func(tx *gorm.DB, l worker.Lease) error
	onFailed func(tx *gorm.DB, l worker.Lease, cause error) error
	onReaped func(tx *gorm.DB, failed []db.Lease) error

	draining atomic.Bool // the drain skips the waiting and stalling, but still fails some work
	stalled  sync.Map    // worker.Lease → struct{} while its holder stalls

	completions, stale, retries, failures, releases, reaps atomic.Int64

	mu        sync.Mutex
	completed map[uint64]int
	unsure    map[uint64]bool // completions that failed without losing the lease, so may have committed
}

type counts struct {
	completions, stale, unsure, retries, failures, releases, reaps int64
}

func (t *leasedTable) counts() counts {
	t.mu.Lock()
	defer t.mu.Unlock()
	return counts{
		completions: t.completions.Load(), stale: t.stale.Load(), unsure: int64(len(t.unsure)),
		retries: t.retries.Load(), failures: t.failures.Load(), releases: t.releases.Load(), reaps: t.reaps.Load(),
	}
}

// leaseSource is one pool's worker on a table, implementing the pool's source on top of db.Leases.
type leaseSource struct {
	*leasedTable
	leases   *db.Leases
	workerID string
}

func newPool(t *testing.T, table *leasedTable, leases *db.Leases, pool string, i, slots int,
	log *slog.Logger) (*worker.Pool, *leaseSource) {
	t.Helper()
	src := &leaseSource{leasedTable: table, leases: leases, workerID: fmt.Sprintf("%s-%d", pool, i)}
	p, err := worker.NewPool(src, worker.Config{
		WorkerID:     src.workerID,
		Pool:         pool,
		Concurrency:  slots,
		Lease:        lease,
		PollInterval: pollInterval,
		Grace:        grace,
		Log:          log,
	})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	return p, src
}

func (s *leaseSource) Claim(ctx context.Context) (worker.Lease, bool, error) {
	l, ok, err := s.leases.Claim(ctx, s.workerID, lease)
	return worker.Lease(l), ok, err
}

func (s *leaseSource) Extend(ctx context.Context, l worker.Lease) error {
	if _, ok := s.stalled.Load(l); ok {
		return nil
	}
	return lost(s.leases.Extend(ctx, db.Lease(l), lease))
}

func (s *leaseSource) Process(ctx context.Context, l worker.Lease) error {
	if !s.draining.Load() {
		if rand.Float64() < s.work.stall || s.work.poisoned != nil && s.work.poisoned(l.ID) {
			return s.stall(ctx, l)
		}
		if err := pause(ctx, rand.N(s.work.took)); err != nil {
			return err
		}
	}
	switch r := rand.Float64(); {
	case r < s.work.permanent:
		return worker.Permanent(errRejected)
	case r < s.work.permanent+s.work.retryable:
		return errRetryable
	}
	return s.complete(ctx, l)
}

// stall stops heartbeating and works past the lease, as a paused process would,
// so the row is reaped before the holder tries to complete it.
func (s *leaseSource) stall(ctx context.Context, l worker.Lease) error {
	s.stalled.Store(l, struct{}{})
	defer s.stalled.Delete(l)
	if err := pause(ctx, 2*lease+rand.N(lease)); err != nil {
		return err
	}
	return s.complete(ctx, l)
}

func (s *leaseSource) complete(ctx context.Context, l worker.Lease) error {
	var writes func(*gorm.DB) error
	if s.onReady != nil {
		writes = func(tx *gorm.DB) error { return s.onReady(tx, l) }
	}
	err := s.leases.Complete(ctx, db.Lease(l), writes)
	switch {
	case errors.Is(err, db.ErrLeaseLost):
		s.stale.Add(1)
		return lost(err)
	case err != nil:
		// A cancelled autocommit update may still have committed, such as when a heartbeat that the completion
		// fenced out cancels the work while the reply is on its way.
		s.mu.Lock()
		defer s.mu.Unlock()
		s.unsure[l.ID] = true
		return err
	}
	s.completions.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed[l.ID]++
	return nil
}

func (s *leaseSource) Fail(ctx context.Context, l worker.Lease, cause error) error {
	f := db.Failure{
		Permanent:   worker.IsPermanent(cause),
		MaxAttempts: maxAttempts,
		Delay:       worker.Backoff(l.Attempt, backoffBase, backoffMax),
		Message:     cause.Error(),
	}
	if s.onFailed != nil {
		f.Writes = func(tx *gorm.DB) error { return s.onFailed(tx, l, cause) }
	}
	final, err := s.leases.Fail(ctx, db.Lease(l), f)
	if err != nil {
		return lost(err)
	}
	if final {
		s.failures.Add(1)
	} else {
		s.retries.Add(1)
	}
	return nil
}

func (s *leaseSource) Release(ctx context.Context, l worker.Lease) error {
	if err := s.leases.Release(ctx, db.Lease(l)); err != nil {
		return lost(err)
	}
	s.releases.Add(1)
	return nil
}

func (s *leaseSource) Reap(ctx context.Context) error {
	requeued, failed, err := s.leases.Reap(ctx, maxAttempts, s.onReaped)
	if err != nil {
		return err
	}
	s.reaps.Add(int64(requeued + failed))
	s.failures.Add(int64(failed))
	return nil
}

// lost translates db's lost lease into the worker's, which is the one the pool recognises.
func lost(err error) error {
	if errors.Is(err, db.ErrLeaseLost) {
		return fmt.Errorf("%w: %v", worker.ErrLeaseLost, err)
	}
	return err
}

func assetReady(tx *gorm.DB, l worker.Lease) error {
	res := tx.Exec("UPDATE assets SET source_codec = 'pcm_s16le', duration_ms = 600000, input_lufs = -23.50, "+
		"input_true_peak_dbtp = -3.20, input_lra_lu = 4.10, input_max_momentary = -18.00 "+
		"WHERE id = ? AND tenant_id = ?", l.ID, l.TenantID)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("updating asset %d matched %d rows", l.ID, res.RowsAffected)
	}
	err := tx.Exec("INSERT INTO renditions (tenant_id, asset_id, profile, codec, bitrate_kbps, channels, bytes, sha256, "+
		"lufs, true_peak_dbtp, lra_lu, normalization) VALUES "+
		"(?, ?, 'aac_128k', 'aac', 128, 2, 9600000, ?, -16.02, -1.61, 4.00, 'linear'), "+
		"(?, ?, 'aac_64k', 'aac', 64, 1, 4800000, ?, -15.97, -1.72, 3.90, 'linear')",
		l.TenantID, l.ID, sourceSHA256, l.TenantID, l.ID, sourceSHA256).Error
	if err != nil {
		return err
	}
	return insertEvent(tx, l, "asset.ready")
}

func assetFailed(tx *gorm.DB, l worker.Lease, cause error) error {
	msg := failedMessage
	if worker.IsPermanent(cause) {
		msg = rejectedMessage
	}
	res := tx.Exec("UPDATE assets SET error = ? WHERE id = ? AND tenant_id = ?", msg, l.ID, l.TenantID)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("updating asset %d matched %d rows", l.ID, res.RowsAffected)
	}
	return insertEvent(tx, l, "asset.failed")
}

func assetsReaped(tx *gorm.DB, failed []db.Lease) error {
	for _, l := range failed {
		if err := assetFailed(tx, worker.Lease(l), nil); err != nil {
			return err
		}
	}
	return nil
}

func insertEvent(tx *gorm.DB, l worker.Lease, eventType string) error {
	return tx.Exec("INSERT INTO asset_events (event_id, tenant_id, asset_id, type) VALUES (?, ?, ?, ?)",
		db.NewID(), l.TenantID, l.ID, eventType).Error
}

// upload inserts queued assets until ctx is done. An insert that has started always finishes,
// so uploads counts every row.
func upload(ctx context.Context, gdb *gorm.DB, tenantID uint64, uploads *atomic.Int64, log *slog.Logger) {
	insert := gdb.WithContext(context.WithoutCancel(ctx))
	for ctx.Err() == nil {
		err := insert.Exec("INSERT INTO assets (public_id, tenant_id, title, source_sha256, source_bytes, target_lufs) "+
			"VALUES (?, ?, 'Rain on a tin roof', ?, 1024, -16.0)", db.NewID(), tenantID, sourceSHA256).Error
		if err != nil {
			log.ErrorContext(ctx, "upload failed", "error", err)
		} else {
			uploads.Add(1)
		}
		pause(ctx, rand.N(uploadPause))
	}
}

type eventRow struct {
	ID       uint64
	EventID  db.ID
	TenantID uint64
	Type     string
}

type deliveryRow struct {
	ID       uint64 `gorm:"primaryKey"`
	EventID  db.ID  `gorm:"column:event_id"`
	TenantID uint64 `gorm:"column:tenant_id"`
	Body     []byte
}

func (deliveryRow) TableName() string { return "webhook_deliveries" }

// relay copies unrelayed events into deliveries and marks them relayed, as the webhook relay does.
type relay struct {
	gdb              *gorm.DB
	log              *slog.Logger
	relayed, repeats atomic.Int64

	mu   sync.Mutex
	seen map[db.ID]bool
}

// run relays batches back to back until one comes back short, then pauses, until ctx is done.
func (r *relay) run(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.batch(context.WithoutCancel(ctx), relayCrash)
		if err != nil {
			r.log.ErrorContext(ctx, "relay failed", "error", err)
		}
		if err != nil || n < relayBatch {
			pause(ctx, relayPause)
		}
	}
}

func (r *relay) drain(ctx context.Context) error {
	for {
		n, err := r.batch(ctx, 0)
		if err != nil || n < relayBatch {
			return err
		}
	}
}

// batch relays up to relayBatch of the oldest unrelayed events and returns how many it read.
// With probability crash it stops before marking them relayed, as a relay that dies there would,
// so the next batch inserts their deliveries again.
func (r *relay) batch(ctx context.Context, crash float64) (int, error) {
	gdb := r.gdb.WithContext(ctx)
	var events []eventRow
	err := gdb.Raw("SELECT id, event_id, tenant_id, type FROM asset_events WHERE relayed_at IS NULL ORDER BY id LIMIT ?",
		relayBatch).Scan(&events).Error
	if err != nil {
		return 0, fmt.Errorf("error reading events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	rows := make([]deliveryRow, len(events))
	ids := make([]uint64, len(events))
	for i, e := range events {
		rows[i] = deliveryRow{
			EventID:  e.EventID,
			TenantID: e.TenantID,
			Body:     fmt.Appendf(nil, `{"id":%q,"type":%q}`, e.EventID, e.Type),
		}
		ids[i] = e.ID
	}
	if err := gdb.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error; err != nil {
		return 0, fmt.Errorf("error inserting deliveries: %w", err)
	}
	r.mu.Lock()
	for _, e := range events {
		if r.seen[e.EventID] {
			r.repeats.Add(1)
		}
		r.seen[e.EventID] = true
	}
	r.mu.Unlock()
	if rand.Float64() < crash {
		return len(events), nil
	}
	res := gdb.Exec("UPDATE asset_events SET relayed_at = NOW(6) WHERE id IN ? AND relayed_at IS NULL", ids)
	if res.Error != nil {
		return 0, fmt.Errorf("error marking events relayed: %w", res.Error)
	}
	r.relayed.Add(res.RowsAffected)
	return len(events), nil
}

// drain finishes what the soak left: each round reaps expired leases, relays every event, makes every queued row
// due and works through them, until nothing is unfinished.
func drain(t *testing.T, gdb *gorm.DB, pools []*worker.Pool, reapers []*leaseSource, r *relay,
	tables ...*leasedTable) error {
	t.Helper()
	for _, table := range tables {
		table.draining.Store(true)
	}
	ctx, cancel := context.WithTimeout(t.Context(), drainTimeout)
	defer cancel()
	for range drainRounds {
		for _, src := range reapers {
			if err := src.Reap(ctx); err != nil {
				return fmt.Errorf("draining: Reap() error = %w", err)
			}
		}
		if err := r.drain(ctx); err != nil {
			return fmt.Errorf("draining: relay error = %w", err)
		}
		exec(t, gdb, "UPDATE assets SET run_after = NOW(6) WHERE status = 'queued'")
		exec(t, gdb, "UPDATE webhook_deliveries SET run_after = NOW(6) WHERE state = 'queued'")
		var wg sync.WaitGroup
		errs := make([]error, len(pools))
		for i, p := range pools {
			wg.Go(func() { errs[i] = p.RunUntilIdle(ctx) })
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return fmt.Errorf("draining: RunUntilIdle() error = %w", err)
		}
		if err := r.drain(ctx); err != nil {
			return fmt.Errorf("draining: relay error = %w", err)
		}
		unfinished := count(t, gdb, "SELECT "+
			"(SELECT COUNT(*) FROM assets WHERE status NOT IN ('ready', 'failed')) + "+
			"(SELECT COUNT(*) FROM webhook_deliveries WHERE state NOT IN ('delivered', 'failed')) + "+
			"(SELECT COUNT(*) FROM asset_events WHERE relayed_at IS NULL)")
		if unfinished == 0 {
			return nil
		}
	}
	return fmt.Errorf("rows still unfinished after %d drain rounds", drainRounds)
}

const leasedQuery = "SELECT 'assets' AS tbl, id, status AS state, attempts, locked_by, lease_expires_at FROM assets " +
	"WHERE status = 'processing' OR locked_by IS NOT NULL OR lease_expires_at IS NOT NULL " +
	"UNION ALL SELECT 'webhook_deliveries', id, state, attempts, locked_by, lease_expires_at FROM webhook_deliveries " +
	"WHERE state = 'sending' OR locked_by IS NOT NULL OR lease_expires_at IS NOT NULL"

func checkInvariants(t *testing.T, gdb *gorm.DB, uploads int64, assets, deliveries *leasedTable) {
	t.Helper()
	invariants := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name: "every asset is finished, with its measurements when ready and a tenant-safe message when failed",
			query: "SELECT id, status, input_lufs, error FROM assets WHERE NOT (" +
				"(status = 'ready' AND input_lufs IS NOT NULL AND error IS NULL) OR " +
				"(status = 'failed' AND input_lufs IS NULL AND error IN (?, ?)))",
			args: []any{rejectedMessage, failedMessage},
		},
		{
			name: "every finished asset has exactly one event, matching its state",
			query: "SELECT a.id, a.status, GROUP_CONCAT(e.type ORDER BY e.id) AS events " +
				"FROM assets a LEFT JOIN asset_events e ON e.asset_id = a.id GROUP BY a.id " +
				"HAVING COUNT(e.id) != 1 OR COALESCE(SUM(e.type = CONCAT('asset.', a.status)), 0) != 1",
		},
		{
			name: "only ready assets have renditions, two each",
			query: "SELECT a.id, a.status, COUNT(r.id) AS renditions " +
				"FROM assets a LEFT JOIN renditions r ON r.asset_id = a.id GROUP BY a.id " +
				"HAVING COUNT(r.id) != IF(a.status = 'ready', 2, 0)",
		},
		{
			name: "every event was relayed and has exactly one delivery",
			query: "SELECT e.id, e.relayed_at, COUNT(d.id) AS deliveries " +
				"FROM asset_events e LEFT JOIN webhook_deliveries d ON d.event_id = e.event_id GROUP BY e.id " +
				"HAVING COUNT(d.id) != 1 OR e.relayed_at IS NULL",
		},
		{
			name: "every delivery has its event and is finished",
			query: "SELECT d.id, d.state, e.id AS event FROM webhook_deliveries d " +
				"LEFT JOIN asset_events e ON e.event_id = d.event_id " +
				"WHERE e.id IS NULL OR d.state NOT IN ('delivered', 'failed')",
		},
		{name: "nothing is left leased", query: leasedQuery},
		{
			name: "every row used at least one attempt and no more than the maximum",
			query: "SELECT 'assets' AS tbl, id, attempts FROM assets WHERE attempts NOT BETWEEN 1 AND ? " +
				"UNION ALL SELECT 'webhook_deliveries', id, attempts FROM webhook_deliveries " +
				"WHERE attempts NOT BETWEEN 1 AND ?",
			args: []any{maxAttempts, maxAttempts},
		},
	}
	for _, inv := range invariants {
		if rows := violations(t, gdb, inv.query, inv.args...); len(rows) > 0 {
			t.Errorf("%s; violated by %v", inv.name, rows)
		}
	}

	if n := count(t, gdb, "SELECT COUNT(*) FROM assets"); n != uploads {
		t.Errorf("%d assets, want the %d uploaded", n, uploads)
	}
	for _, table := range []struct {
		*leasedTable
		column, done string
	}{{assets, "status", "ready"}, {deliveries, "state", "delivered"}} {
		done := ids(t, gdb, fmt.Sprintf("SELECT id FROM %s WHERE %s = ?", table.name, table.column), table.done)
		var twice, notDone, unexplained []uint64
		for id, n := range table.completed {
			if n > 1 {
				twice = append(twice, id)
			}
			if !done[id] {
				notDone = append(notDone, id)
			}
		}
		// A completion with domain writes is a transaction, which has committed only if Complete returned nil.
		for id := range done {
			if table.completed[id] == 0 && (table.onReady != nil || !table.unsure[id]) {
				unexplained = append(unexplained, id)
			}
		}
		if len(twice) > 0 {
			t.Errorf("%s completed more than once: %v", table.name, twice)
		}
		if len(notDone) > 0 {
			t.Errorf("%s completed but not %s: %v", table.name, table.done, notDone)
		}
		if len(unexplained) > 0 {
			t.Errorf("%s %s without a completion that could have committed: %v", table.name, table.done, unexplained)
		}
		got := count(t, gdb, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = 'failed'", table.name, table.column))
		if want := table.failures.Load(); got != want {
			t.Errorf("%d %s failed, want the %d failures the sources reported", got, table.name, want)
		}
		if table.stale.Load() == 0 {
			t.Errorf("no stale completion of %s was fenced out, so no lease expired under a holder", table.name)
		}
	}
}

// violations runs query and returns up to ten of its rows, formatted, as examples of a broken invariant.
func violations(t *testing.T, gdb *gorm.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := gdb.WithContext(t.Context()).Raw(query+" LIMIT 10", args...).Rows()
	if err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("error reading the columns of %q: %v", query, err)
	}
	var found []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("error scanning %q: %v", query, err)
		}
		fields := make([]string, len(cols))
		for i, col := range cols {
			fields[i] = col + "=NULL"
			if vals[i].Valid {
				fields[i] = col + "=" + vals[i].String
			}
		}
		found = append(found, "{"+strings.Join(fields, " ")+"}")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading %q: %v", query, err)
	}
	return found
}

func ids(t *testing.T, gdb *gorm.DB, query string, args ...any) map[uint64]bool {
	t.Helper()
	var list []uint64
	if err := gdb.WithContext(t.Context()).Raw(query, args...).Scan(&list).Error; err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	set := make(map[uint64]bool, len(list))
	for _, id := range list {
		set[id] = true
	}
	return set
}

func count(t *testing.T, gdb *gorm.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := gdb.WithContext(t.Context()).Raw(query, args...).Scan(&n).Error; err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	return n
}

func exec(t *testing.T, gdb *gorm.DB, query string, args ...any) {
	t.Helper()
	if err := gdb.WithContext(t.Context()).Exec(query, args...).Error; err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
}

// withLockWaitTimeout returns dsn with the session's lock wait cut to a second. Every statement in the soak runs on
// a context shorter than MySQL's default of 50 s, so without it a lock wait would end as a timeout or cancellation
// rather than error 1205.
func withLockWaitTimeout(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("error parsing the test DSN: %v", err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["innodb_lock_wait_timeout"] = strconv.Itoa(lockWaitTimeout)
	return cfg.FormatDSN()
}

func open(t *testing.T, dsn string, log *slog.Logger) *gorm.DB {
	t.Helper()
	gdb, err := db.Open(dsn, log)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("error getting the connection pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	if n := count(t, gdb, "SELECT @@SESSION.innodb_lock_wait_timeout"); n != lockWaitTimeout {
		t.Fatalf("innodb_lock_wait_timeout = %d, want %d", n, lockWaitTimeout)
	}
	return gdb
}

func createTenant(t *testing.T, gdb *gorm.DB) uint64 {
	t.Helper()
	exec(t, gdb, "INSERT INTO tenants (slug, api_key_hash, webhook_url) VALUES ('soak', ?, 'https://example.com/hooks')",
		bytes.Repeat([]byte{0x5a}, 32))
	return uint64(count(t, gdb, "SELECT id FROM tenants WHERE slug = 'soak'"))
}

// pause waits for d, or until ctx is done, and returns ctx's error in that case.
func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
