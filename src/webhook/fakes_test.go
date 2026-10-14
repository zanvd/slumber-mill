package webhook

import (
	"context"
	"errors"
	"slices"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/tenant"
)

var errBoom = errors.New("boom")

// fakeRelayRepo is the relay's repository. Times are measured from start.
// Another relay holds the lock whenever busy says so; a nil busy means never.
type fakeRelayRepo struct {
	start     time.Time
	busy      func(elapsed time.Duration) bool
	lockErr   error
	insertErr error
	// lostAfter, indexed by the order locks are handed out, makes a lock's checks fail from that time on.
	lostAfter []time.Duration

	lockCalls []time.Duration
	locks     []*fakeLock
	inserts   [][]delivery
}

func (f *fakeRelayRepo) elapsed() time.Duration {
	return time.Since(f.start)
}

func (f *fakeRelayRepo) lockRelay(ctx context.Context) (relayLock, bool, error) {
	f.lockCalls = append(f.lockCalls, f.elapsed())
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if f.lockErr != nil {
		return nil, false, f.lockErr
	}
	if f.busy != nil && f.busy(f.elapsed()) {
		return nil, false, nil
	}
	l := &fakeLock{repo: f}
	if n := len(f.locks); n < len(f.lostAfter) {
		l.lostAfter = f.lostAfter[n]
	}
	f.locks = append(f.locks, l)
	return l, true, nil
}

func (f *fakeRelayRepo) insertDeliveries(ctx context.Context, ds []delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserts = append(f.inserts, slices.Clone(ds))
	return nil
}

// inserted returns the event IDs of every delivery inserted, in order.
func (f *fakeRelayRepo) inserted() []uuid.UUID {
	var ids []uuid.UUID
	for _, ds := range f.inserts {
		for _, d := range ds {
			ids = append(ids, d.EventID)
		}
	}
	return ids
}

// fakeLock is a relay lock that is lost once lost is set, or from lostAfter on if that is set.
type fakeLock struct {
	repo       *fakeRelayRepo
	lost       bool
	lostAfter  time.Duration
	releaseErr error

	checks []time.Duration
	// releases counts calls to release, and releaseCtxErrs and releaseDeadlines hold each one's context state.
	releases         int
	releaseCtxErrs   []error
	releaseDeadlines []time.Time
}

func (l *fakeLock) check(context.Context) error {
	now := l.repo.elapsed()
	l.checks = append(l.checks, now)
	if l.lost || l.lostAfter > 0 && now >= l.lostAfter {
		l.lost = true
		return errLockLost
	}
	return nil
}

func (l *fakeLock) release(ctx context.Context) error {
	l.releases++
	l.releaseCtxErrs = append(l.releaseCtxErrs, ctx.Err())
	deadline, _ := ctx.Deadline()
	l.releaseDeadlines = append(l.releaseDeadlines, deadline)
	return l.releaseErr
}

// fakeEvents is the outbox, oldest first. Event i appears visibleAfter[i] after start, or at once without one.
type fakeEvents struct {
	start        time.Time
	events       []asset.Event
	visibleAfter []time.Duration
	readErr      error
	markErr      error

	relayed map[uuid.UUID]bool
	// reads holds when each read happened, and sizes how many events it returned.
	reads  []time.Duration
	sizes  []int
	limits []int
	marked [][]uuid.UUID
}

func (f *fakeEvents) UnrelayedEvents(ctx context.Context, limit int) ([]asset.Event, error) {
	now := time.Since(f.start)
	f.reads = append(f.reads, now)
	f.limits = append(f.limits, limit)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.readErr != nil {
		return nil, f.readErr
	}
	var out []asset.Event
	for i, e := range f.events {
		if len(out) == limit {
			break
		}
		if f.relayed[e.ID] || i < len(f.visibleAfter) && now < f.visibleAfter[i] {
			continue
		}
		out = append(out, e)
	}
	f.sizes = append(f.sizes, len(out))
	return out, nil
}

func (f *fakeEvents) MarkRelayed(ctx context.Context, ids []uuid.UUID) error {
	f.marked = append(f.marked, slices.Clone(ids))
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.markErr != nil {
		return f.markErr
	}
	if f.relayed == nil {
		f.relayed = make(map[uuid.UUID]bool)
	}
	for _, id := range ids {
		f.relayed[id] = true
	}
	return nil
}

// relayedIDs returns the IDs of the events marked relayed, in the outbox's order.
func (f *fakeEvents) relayedIDs() []uuid.UUID {
	var ids []uuid.UUID
	for _, e := range f.events {
		if f.relayed[e.ID] {
			ids = append(ids, e.ID)
		}
	}
	return ids
}

// fakeTenants answers WebhookTarget with errs or targets, and a tenant in neither has no webhook.
type fakeTenants struct {
	targets map[uint64]tenant.Webhook
	errs    map[uint64]error

	lookups []uint64
}

func (f *fakeTenants) WebhookTarget(ctx context.Context, tenantID uint64) (tenant.Webhook, error) {
	f.lookups = append(f.lookups, tenantID)
	if err := ctx.Err(); err != nil {
		return tenant.Webhook{}, err
	}
	if err, ok := f.errs[tenantID]; ok {
		return tenant.Webhook{}, err
	}
	if w, ok := f.targets[tenantID]; ok {
		return w, nil
	}
	return tenant.Webhook{}, tenant.ErrNoWebhook
}
