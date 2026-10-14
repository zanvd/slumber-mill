package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/egress"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/worker"
)

const (
	relayInterval = time.Second
	// releaseTimeout bounds giving up the relay lock once Run's context is done.
	releaseTimeout = 15 * time.Second

	completeTimeout = 15 * time.Second
	retryBase       = 30 * time.Second
	retryMax        = 6 * time.Hour
)

var (
	errDatabase = errors.New("database error")
	errNotFound = errors.New("delivery not found")
	// errLeaseLost means a fenced write matched no row: the lease expired and was reaped,
	// or the delivery has been claimed again or finished since.
	errLeaseLost = errors.New("lease lost")
	// errLockLost means the relay lock's session has ended or no longer holds the lock.
	errLockLost = errors.New("relay lock lost")
)

// delivery is a webhook for a tenant: the event's envelope, the exact bytes that get signed and sent.
type delivery struct {
	EventID  uuid.UUID
	TenantID uint64
	Body     []byte
}

// lease is a claimed delivery. Its fields are those of worker.Lease and db.Lease, so it converts to and from either.
type lease struct {
	ID, TenantID uint64
	Attempt      int
}

// failure is why an attempt failed.
type failure struct {
	// Permanent fails the delivery whatever its attempts. Otherwise it is retried after Delay,
	// unless this was its last attempt.
	Permanent bool
	// Detail is the internal cause, kept in last_error.
	Detail string
	Delay  time.Duration
}

type relayRepo interface {
	lockRelay(ctx context.Context) (relayLock, bool, error)
	insertDeliveries(ctx context.Context, ds []delivery) error
}

// relayLock is the relay lock while this process holds it.
type relayLock interface {
	// check returns an error matching errLockLost unless the lock is still held, and the lock is then gone for good.
	check(ctx context.Context) error
	release(ctx context.Context) error
}

type eventSource interface {
	UnrelayedEvents(ctx context.Context, limit int) ([]asset.Event, error)
	MarkRelayed(ctx context.Context, ids []uuid.UUID) error
}

type webhookTenants interface {
	WebhookTenants(ctx context.Context, ids []uint64) (map[uint64]bool, error)
}

type webhookTargets interface {
	WebhookTarget(ctx context.Context, tenantID uint64) (tenant.Webhook, error)
}

// Relay turns asset events into deliveries. Only the relay holding the relay lock does so,
// so the webhook pool's workers don't all read the same events. A Relay is used by one goroutine at a time.
type Relay struct {
	repo    relayRepo
	events  eventSource
	tenants webhookTenants
	// lock is nil unless this relay holds the relay lock.
	lock relayLock
}

func NewRelay(r relayRepo, events eventSource, tenants webhookTenants) (*Relay, error) {
	switch {
	case r == nil:
		return nil, errors.New("a repository is required")
	case events == nil:
		return nil, errors.New("an event source is required")
	case tenants == nil:
		return nil, errors.New("a tenant service is required")
	}
	return &Relay{repo: r, events: events, tenants: tenants}, nil
}

// Run calls Once, waiting a second after each call, until ctx is cancelled. Then it gives up the relay lock.
func (r *Relay) Run(ctx context.Context) {
	log := logging.LoggerFromContext(ctx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			log.ErrorContext(ctx, "error releasing the relay lock", "error", err)
		}
	}()
	for ctx.Err() == nil {
		if _, _, err := r.Once(ctx); err != nil && ctx.Err() == nil {
			log.ErrorContext(ctx, "relay failed", "error", err)
		}
		sleep(ctx, relayInterval)
	}
}

// Once takes the relay lock if neither this relay nor another holds it. Holding the lock, it relays batches of
// the oldest unrelayed events back to back until a batch comes back short. It reports whether this relay holds
// the lock, and how many events it relayed.
func (r *Relay) Once(ctx context.Context) (held bool, relayed int, err error) {
	log := logging.LoggerFromContext(ctx)
	if r.lock == nil {
		lock, ok, err := r.repo.lockRelay(ctx)
		if err != nil || !ok {
			return false, 0, err
		}
		r.lock = lock
		log.InfoContext(ctx, "took the relay lock")
	}
	for {
		if err := r.lock.check(ctx); err != nil {
			r.lock = nil
			log.WarnContext(ctx, "lost the relay lock", "error", err)
			return false, relayed, nil
		}
		n, err := r.relayBatch(ctx)
		relayed += n
		if err != nil || n < asset.MaxEventBatch {
			return true, relayed, err
		}
	}
}

// Close gives up the relay lock if this relay holds it.
func (r *Relay) Close(ctx context.Context) error {
	if r.lock == nil {
		return nil
	}
	lock := r.lock
	r.lock = nil
	if err := lock.release(ctx); err != nil {
		return err
	}
	logging.LoggerFromContext(ctx).InfoContext(ctx, "released the relay lock")
	return nil
}

// relayBatch queues deliveries for up to a batch of the oldest unrelayed events, then marks the events relayed.
// It returns how many events it relayed.
func (r *Relay) relayBatch(ctx context.Context) (int, error) {
	events, err := r.events.UnrelayedEvents(ctx, asset.MaxEventBatch)
	if err != nil {
		return 0, fmt.Errorf("error relaying events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	deliveries, err := r.deliveries(ctx, events)
	if err != nil {
		return 0, fmt.Errorf("error relaying events: %w", err)
	}
	if len(deliveries) > 0 {
		if err := r.repo.insertDeliveries(ctx, deliveries); err != nil {
			return 0, fmt.Errorf("error relaying events: %w", err)
		}
	}
	ids := make([]uuid.UUID, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	if err := r.events.MarkRelayed(ctx, ids); err != nil {
		return 0, fmt.Errorf("error relaying events: %w", err)
	}
	return len(events), nil
}

// deliveries builds a delivery for each event whose tenant has a webhook, finding the tenants that do in one lookup.
func (r *Relay) deliveries(ctx context.Context, events []asset.Event) ([]delivery, error) {
	var tenantIDs []uint64
	seen := make(map[uint64]bool)
	for _, e := range events {
		if !seen[e.TenantID] {
			seen[e.TenantID] = true
			tenantIDs = append(tenantIDs, e.TenantID)
		}
	}
	hasWebhook, err := r.tenants.WebhookTenants(ctx, tenantIDs)
	if err != nil {
		return nil, fmt.Errorf("error looking up the tenants with a webhook: %w", err)
	}
	var out []delivery
	for _, e := range events {
		if !hasWebhook[e.TenantID] {
			continue
		}
		body, err := json.Marshal(apiv1.NewEvent(e))
		if err != nil {
			return nil, fmt.Errorf("error encoding event %s: %w", e.ID, err)
		}
		out = append(out, delivery{EventID: e.ID, TenantID: e.TenantID, Body: body})
	}
	return out, nil
}

type deliveryRepo interface {
	claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error)
	extend(ctx context.Context, l lease, d time.Duration) error
	leasedDelivery(ctx context.Context, l lease) (delivery, error)
	complete(ctx context.Context, l lease) error
	fail(ctx context.Context, l lease, f failure) (bool, error)
	release(ctx context.Context, l lease) error
	reap(ctx context.Context) (requeued, failed int, err error)
}

type poster interface {
	Post(ctx context.Context, url, eventID, signature string, body []byte) (int, error)
}

type DelivererConfig struct {
	WorkerID string
	// Lease is how long a claim or a heartbeat holds a delivery: the webhook pool's Lease.
	Lease time.Duration
}

// Deliverer is the webhook pool's source of work. It signs a claimed delivery, posts it to the tenant's webhook,
// and completes or fails the delivery fenced on its lease.
type Deliverer struct {
	repo    deliveryRepo
	tenants webhookTargets
	client  poster
	cfg     DelivererConfig
}

func NewDeliverer(r deliveryRepo, tenants webhookTargets, client poster, cfg DelivererConfig) (*Deliverer, error) {
	switch {
	case r == nil:
		return nil, errors.New("a repository is required")
	case tenants == nil:
		return nil, errors.New("a tenant service is required")
	case client == nil:
		return nil, errors.New("a client is required")
	case cfg.WorkerID == "":
		return nil, errors.New("a worker ID is required")
	case cfg.Lease <= 0:
		return nil, fmt.Errorf("lease %v is not positive", cfg.Lease)
	}
	return &Deliverer{repo: r, tenants: tenants, client: client, cfg: cfg}, nil
}

func (d *Deliverer) Claim(ctx context.Context) (worker.Lease, bool, error) {
	l, ok, err := d.repo.claim(ctx, d.cfg.WorkerID, d.cfg.Lease)
	if err != nil {
		return worker.Lease{}, false, err
	}
	return worker.Lease(l), ok, nil
}

func (d *Deliverer) Extend(ctx context.Context, l worker.Lease) error {
	return toWorker(d.repo.extend(ctx, lease(l), d.cfg.Lease))
}

// Process posts the delivery to the tenant's webhook URL, signed now with the tenant's secret, both as they are now,
// and marks it delivered on a 2xx answer. A 410, a blocked address, an empty secret and a tenant without a webhook
// are permanent failures; any other answer or error is worth a retry.
func (d *Deliverer) Process(ctx context.Context, l worker.Lease) error {
	held := lease(l)
	p, err := d.repo.leasedDelivery(ctx, held)
	if err != nil {
		return fmt.Errorf("error loading the leased delivery: %w", err)
	}
	target, err := d.tenants.WebhookTarget(ctx, l.TenantID)
	switch {
	case errors.Is(err, tenant.ErrNoWebhook), errors.Is(err, tenant.ErrNotFound):
		return worker.Permanent(fmt.Errorf("error reading the webhook target: %w", err))
	case err != nil:
		return fmt.Errorf("error reading the webhook target: %w", err)
	}
	signature, err := Sign(target.Secret, time.Now(), p.Body)
	switch {
	case errors.Is(err, ErrEmptySecret):
		return worker.Permanent(fmt.Errorf("error signing the delivery: %w", err))
	case err != nil:
		return fmt.Errorf("error signing the delivery: %w", err)
	}

	status, err := d.client.Post(ctx, target.URL, p.EventID.String(), signature, p.Body)
	switch {
	case errors.Is(err, egress.ErrBlockedAddress):
		return worker.Permanent(err)
	case err != nil:
		return err
	case status == http.StatusGone:
		return worker.Permanent(fmt.Errorf("webhook endpoint answered %d", status))
	case status < 200 || status > 299:
		return fmt.Errorf("webhook endpoint answered %d", status)
	}

	// A heartbeat that finds the delivery already finished cancels ctx, which could make the driver
	// report a committed completion as cancelled.
	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancel()
	return toWorker(d.repo.complete(completeCtx, held))
}

// Fail fails the delivery for good if cause is worker.Permanent, and otherwise retries it after a backoff
// while attempts remain.
func (d *Deliverer) Fail(ctx context.Context, l worker.Lease, cause error) error {
	f := failure{Permanent: worker.IsPermanent(cause), Detail: cause.Error()}
	if !f.Permanent {
		f.Delay = worker.Backoff(l.Attempt, retryBase, retryMax)
	}
	_, err := d.repo.fail(ctx, lease(l), f)
	return toWorker(err)
}

func (d *Deliverer) Release(ctx context.Context, l worker.Lease) error {
	return toWorker(d.repo.release(ctx, lease(l)))
}

func (d *Deliverer) Reap(ctx context.Context) error {
	requeued, failed, err := d.repo.reap(ctx)
	if err != nil {
		return err
	}
	if requeued > 0 || failed > 0 {
		logging.LoggerFromContext(ctx).WarnContext(ctx, "reaped deliveries whose lease expired",
			"requeued", requeued, "failed", failed)
	}
	return nil
}

// toWorker passes on a repository error, matching worker.ErrLeaseLost if the lease was lost.
func toWorker(err error) error {
	if errors.Is(err, errLeaseLost) {
		return &leaseLostError{err: err}
	}
	return err
}

type leaseLostError struct{ err error }

func (e *leaseLostError) Error() string { return e.err.Error() }
func (e *leaseLostError) Unwrap() error { return worker.ErrLeaseLost }

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
