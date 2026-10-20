package webhook

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
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

// fakeDeliveryRepo is the deliverer's repository. It records what each call got, and like a database driver,
// every method fails once its context is done. A pool's heartbeats call it alongside Process, hence the lock.
type fakeDeliveryRepo struct {
	mu   sync.Mutex
	errs map[string]error
	// due is the lease the next claim hands out, once; nil means nothing is due.
	due *lease
	// pending is what leasedDelivery returns, and its trace context what leasedTraceContext returns.
	pending delivery
	// stallTraceRead makes leasedTraceContext answer only when its context ends, as on a stalled connection.
	stallTraceRead bool
	// requeued and failed are what reap returns.
	requeued, failed int

	workerIDs []string
	// durations are the lease durations claim and extend got.
	durations []time.Duration
	// leases are the leases every other method got.
	leases   []lease
	failures []failure
	// completed counts calls to complete; completeAt is when the last one happened, and completeDeadline,
	// completeErr and completeLog are what its context carried.
	completed        int
	completeAt       time.Time
	completeDeadline time.Time
	completeErr      error
	completeLog      *slog.Logger
	// expired is how many deliveries are past retention, with IDs 1 to expired; afterPurge, if set,
	// runs after each retention batch with its number, counting from 1.
	expired    int
	afterPurge func(batch int)
	purges     []purgeArgs
}

type purgeArgs struct {
	age   time.Duration
	after deliveryCursor
	limit int
}

// deleteDelivered deletes up to limit of the expired deliveries after the cursor.
func (f *fakeDeliveryRepo) deleteDelivered(ctx context.Context, age time.Duration, after deliveryCursor, limit int) (
	int, deliveryCursor, bool, error) {
	f.mu.Lock()
	f.purges = append(f.purges, purgeArgs{age, after, limit})
	batch := len(f.purges)
	err := f.err(ctx, "deleteDelivered")
	f.mu.Unlock()
	if f.afterPurge != nil {
		defer f.afterPurge(batch)
	}
	if err != nil {
		return 0, after, false, err
	}
	n := min(limit, f.expired-int(after.ID))
	if n == 0 {
		return 0, after, false, nil
	}
	return n, deliveryCursor{RunAfter: eventTime, ID: after.ID + uint64(n)}, n == limit, nil
}

func (f *fakeDeliveryRepo) err(ctx context.Context, method string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.errs[method]
}

func (f *fakeDeliveryRepo) claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workerIDs = append(f.workerIDs, workerID)
	f.durations = append(f.durations, d)
	if err := f.err(ctx, "claim"); err != nil {
		return lease{}, false, err
	}
	if f.due == nil {
		return lease{}, false, nil
	}
	l := *f.due
	f.due = nil
	return l, true, nil
}

func (f *fakeDeliveryRepo) extend(ctx context.Context, l lease, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, l)
	f.durations = append(f.durations, d)
	return f.err(ctx, "extend")
}

func (f *fakeDeliveryRepo) leasedTraceContext(ctx context.Context, l lease) (string, error) {
	f.mu.Lock()
	f.leases = append(f.leases, l)
	stall := f.stallTraceRead
	f.mu.Unlock()
	if stall {
		<-ctx.Done()
		return "", ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err(ctx, "leasedTraceContext"); err != nil {
		return "", err
	}
	return f.pending.TraceContext, nil
}

func (f *fakeDeliveryRepo) leasedDelivery(ctx context.Context, l lease) (delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, l)
	if err := f.err(ctx, "leasedDelivery"); err != nil {
		return delivery{}, err
	}
	return f.pending, nil
}

func (f *fakeDeliveryRepo) complete(ctx context.Context, l lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, l)
	f.completed++
	f.completeAt = time.Now()
	f.completeDeadline, _ = ctx.Deadline()
	f.completeErr = ctx.Err()
	f.completeLog = logging.LoggerFromContext(ctx)
	return f.err(ctx, "complete")
}

func (f *fakeDeliveryRepo) fail(ctx context.Context, l lease, fl failure) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, l)
	f.failures = append(f.failures, fl)
	if err := f.err(ctx, "fail"); err != nil {
		return false, err
	}
	return fl.Permanent || l.Attempt >= maxAttempts, nil
}

func (f *fakeDeliveryRepo) release(ctx context.Context, l lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, l)
	return f.err(ctx, "release")
}

func (f *fakeDeliveryRepo) reap(ctx context.Context) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err(ctx, "reap"); err != nil {
		return 0, 0, err
	}
	return f.requeued, f.failed, nil
}

// post is a request the fake client was asked to send.
type post struct {
	url, eventID, signature string
	body                    []byte
	at                      time.Time
}

// fakePoster answers every post with status and err, or with respond if it is set.
type fakePoster struct {
	status  int
	err     error
	respond func(ctx context.Context) (int, error)

	posts []post
}

func (f *fakePoster) Post(ctx context.Context, url, eventID, signature string, body []byte) (int, error) {
	f.posts = append(f.posts, post{url, eventID, signature, slices.Clone(body), time.Now()})
	if f.respond != nil {
		return f.respond(ctx)
	}
	return f.status, f.err
}

// fakeWebhookTenants is the relay's tenant lookup. Like a database driver, it fails once its context is done.
type fakeWebhookTenants struct {
	// has holds the tenants that have a webhook.
	has map[uint64]bool
	err error
	// errFrom, if set, makes the lookups from that one on (counting from 1) fail with errBoom.
	errFrom int

	// lookups holds the tenant IDs each lookup got.
	lookups [][]uint64
}

func (f *fakeWebhookTenants) WebhookTenants(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	f.lookups = append(f.lookups, slices.Clone(ids))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.errFrom > 0 && len(f.lookups) >= f.errFrom {
		return nil, errBoom
	}
	found := make(map[uint64]bool)
	for _, id := range ids {
		if f.has[id] {
			found[id] = true
		}
	}
	return found, nil
}

// fakeTenants answers WebhookTarget with errs or targets, and a tenant in neither has no webhook.
type fakeTenants struct {
	targets map[uint64]tenant.Webhook
	errs    map[uint64]error
}

func (f *fakeTenants) WebhookTarget(ctx context.Context, tenantID uint64) (tenant.Webhook, error) {
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
