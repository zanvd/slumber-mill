package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
)

const (
	relayInterval = time.Second
	// releaseTimeout bounds giving up the relay lock once Run's context is done.
	releaseTimeout = 15 * time.Second
)

var (
	errDatabase = errors.New("database error")
	errNotFound = errors.New("delivery not found")
	// errLockLost means the relay lock's session has ended or no longer holds the lock.
	errLockLost = errors.New("relay lock lost")
)

// delivery is a webhook for a tenant: the event's envelope, the exact bytes that get signed and sent.
type delivery struct {
	EventID  uuid.UUID
	TenantID uint64
	Body     []byte
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

type webhookTargets interface {
	WebhookTarget(ctx context.Context, tenantID uint64) (tenant.Webhook, error)
}

// Relay turns asset events into deliveries. Only the relay holding the relay lock does so,
// so the webhook pool's workers don't all read the same events. A Relay is used by one goroutine at a time.
type Relay struct {
	repo    relayRepo
	events  eventSource
	tenants webhookTargets
	// lock is nil unless this relay holds the relay lock.
	lock relayLock
}

func NewRelay(r relayRepo, events eventSource, tenants webhookTargets) (*Relay, error) {
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

// Run calls Once every second until ctx is cancelled, then gives up the relay lock.
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

// deliveries builds a delivery for each event whose tenant has a webhook, looking each tenant up once.
func (r *Relay) deliveries(ctx context.Context, events []asset.Event) ([]delivery, error) {
	hasWebhook := make(map[uint64]bool)
	var out []delivery
	for _, e := range events {
		has, seen := hasWebhook[e.TenantID]
		if !seen {
			var err error
			if has, err = r.hasWebhook(ctx, e.TenantID); err != nil {
				return nil, err
			}
			hasWebhook[e.TenantID] = has
		}
		if !has {
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

// hasWebhook reports whether the tenant has a webhook URL. Delivery reads the tenant's target again when it sends,
// so a lookup that fails for a reason other than the tenant counts as yes and leaves the outcome to delivery:
// a secret that can't be opened then fails that tenant's deliveries rather than holding up every other tenant's.
func (r *Relay) hasWebhook(ctx context.Context, tenantID uint64) (bool, error) {
	_, err := r.tenants.WebhookTarget(ctx, tenantID)
	log := logging.LoggerFromContext(ctx)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, tenant.ErrNoWebhook):
		return false, nil
	case errors.Is(err, tenant.ErrNotFound):
		log.WarnContext(ctx, "relaying events of an unknown tenant without delivering them", "tenant_id", tenantID)
		return false, nil
	case ctx.Err() != nil, errors.Is(err, tenant.ErrNoKeyRing):
		return false, fmt.Errorf("error looking up the webhook of tenant %d: %w", tenantID, err)
	default:
		log.WarnContext(ctx, "error looking up a tenant's webhook, queueing its deliveries anyway",
			"tenant_id", tenantID, "error", err)
		return true, nil
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
